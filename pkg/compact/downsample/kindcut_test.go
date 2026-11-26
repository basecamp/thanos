// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package downsample

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/logutil"
)

// kcRun downsamples a raw series to 5m and on to 1h, checks both results
// with the oracle, and returns them.
func kcRun(t *testing.T, raw []sample, gauge bool) (ds5m, ds1h []kcChunk) {
	t.Helper()
	rawItems := kcItemsFromRaw(raw)

	chks5m, err := DownsampleRaw(raw, ResLevel1)
	testutil.Ok(t, err)
	ds5m = kcParse(t, chks5m)
	kcCheck(t, rawItems, ds5m, ResLevel1, gauge)
	testutil.Equals(t, kcItemKinds(rawItems), kcKinds(ds5m), "kinds at 5m")

	ds1h = kcParse(t, kcDownsampleBlock(t, chks5m, ResLevel1, ResLevel2))
	kcCheck(t, kcItemsFromChunks(ds5m), ds1h, ResLevel2, gauge)
	kcCheckWindows(t, rawItems, ds1h, ResLevel2)
	testutil.Equals(t, kcItemKinds(rawItems), kcKinds(ds1h), "kinds at 1h")
	return ds5m, ds1h
}

// TestDownsampleKindCut downsamples series that switch between exponential
// and custom buckets histograms, raw to 5m to 1h, and checks every aggregate
// sample of both passes against its source samples.
func TestDownsampleKindCut(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pattern string
		gauge   bool
		// chunks5m is the number of 5m aggregate chunks: one per run of a
		// kind, unless a run is long or changes its custom layout.
		chunks5m int
	}{
		{name: "exponential to custom mid window", pattern: kcRepeat("e", 137, "c", 103), chunks5m: 2},
		{name: "custom to exponential mid window", pattern: kcRepeat("c", 137, "e", 103), chunks5m: 2},
		{name: "four runs, two flips in one 5m window", pattern: kcRepeat("c", 61, "e", 2, "c", 68, "e", 109), chunks5m: 4},
		{name: "four runs, the other way round", pattern: kcRepeat("e", 61, "c", 2, "e", 68, "c", 109), chunks5m: 4},
		{name: "flip on a 5m boundary", pattern: kcRepeat("e", 135, "c", 105), chunks5m: 2},
		{name: "flip on a 1h boundary", pattern: kcRepeat("c", 120, "e", 120), chunks5m: 2},
		{name: "flip with a genuine counter reset", pattern: kcRepeat("c", 137, "E", 1, "e", 102), chunks5m: 2},
		{name: "resets inside both runs", pattern: kcRepeat("c", 50, "C", 1, "c", 86, "e", 40, "E", 1, "e", 62), chunks5m: 2},
		{name: "kind alternates on every sample", pattern: kcRepeat("ec", 30), chunks5m: 60},
		{name: "single sample runs", pattern: kcRepeat("c", 7, "e", 1, "c", 7, "e", 1, "c", 1, "e", 13), chunks5m: 6},
		{name: "single sample first and last", pattern: kcRepeat("e", 1, "c", 28, "e", 1), chunks5m: 3},
		{name: "stale marker between the runs", pattern: kcRepeat("c", 22, "s", 1, "e", 37), chunks5m: 2},
		{name: "NaN sums between the runs", pattern: kcRepeat("c", 22, "m", 1, "n", 1, "e", 36), chunks5m: 2},
		{name: "stale markers and NaN sums at the run edges", pattern: kcRepeat("s", 1, "n", 1, "c", 21, "m", 1, "s", 1, "n", 1, "e", 33, "s", 1, "m", 1), chunks5m: 2},
		{name: "NaN sum of the other kind inside a run", pattern: kcRepeat("c", 22, "n", 1, "c", 37), chunks5m: 1},
		{name: "scrape gap across a 5m boundary", pattern: kcRepeat("c", 138, "_", 5, "e", 97), chunks5m: 2},
		{name: "gauge, exponential to custom", pattern: kcRepeat("e", 137, "c", 103), gauge: true, chunks5m: 2},
		{name: "gauge, four runs", pattern: kcRepeat("c", 61, "e", 2, "c", 68, "e", 109), gauge: true, chunks5m: 4},
		{name: "flip where the whole series would cut a batch", pattern: kcRepeat("e", 405, "c", 395), chunks5m: 2},
		{name: "runs of two batches each", pattern: kcRepeat("c", 800, "e", 800), chunks5m: 4},
		{name: "custom layout change and kind flip in one window", pattern: kcRepeat("c", 136, "d", 2, "e", 102), chunks5m: 3},
		{name: "kind flip and custom layout change in one window", pattern: kcRepeat("e", 136, "c", 1, "d", 103), chunks5m: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ds5m, _ := kcRun(t, kcSeries(kcStart, kcStep, tc.pattern, tc.gauge), tc.gauge)
			testutil.Equals(t, tc.chunks5m, len(ds5m), "5m chunks")
		})
	}
}

// TestDownsampleKindCutTimestamps pins where a kind change puts the
// aggregate samples around it.
func TestDownsampleKindCutTimestamps(t *testing.T) {
	type stamp struct {
		t     int64
		count float64
	}
	stamps := func(c kcChunk) []stamp {
		var out []stamp
		for _, s := range c.samples {
			out = append(out, stamp{s.t, s.count})
		}
		return out
	}
	minute := func(m int) int64 { return kcStart + int64(m)*kcStep }

	t.Run("mid window", func(t *testing.T) {
		// One hour; the series switches to custom buckets with sample 23, the
		// fourth sample of the window 20..24.
		ds5m, ds1h := kcRun(t, kcSeries(kcStart, kcStep, kcRepeat("e", 23, "c", 37), false), false)

		testutil.Equals(t, 2, len(ds5m))
		// The window is split. Its exponential part is stamped at the last
		// exponential sample, its custom part at the window end.
		testutil.Equals(t, []stamp{
			{minute(5) - 1, 5}, {minute(10) - 1, 5}, {minute(15) - 1, 5}, {minute(20) - 1, 5}, {minute(22), 3},
		}, stamps(ds5m[0]))
		testutil.Equals(t, []stamp{
			{minute(25) - 1, 2}, {minute(30) - 1, 5}, {minute(35) - 1, 5}, {minute(40) - 1, 5},
			{minute(45) - 1, 5}, {minute(50) - 1, 5}, {minute(55) - 1, 5}, {minute(59), 5},
		}, stamps(ds5m[1]))

		// The counter of the custom part starts from its own first sample:
		// sample 24 has n=25 and a count of 21n.
		testutil.Equals(t, 7.0*23, ds5m[0].samples[4].counter.Count)
		testutil.Equals(t, 21.0*25, ds5m[1].samples[0].counter.Count)
		testutil.Equals(t, 21.0*60, ds5m[1].samples[7].counter.Count)

		// The hour is split the same way, at the same timestamp.
		testutil.Equals(t, 2, len(ds1h))
		testutil.Equals(t, []stamp{{minute(22), 23}}, stamps(ds1h[0]))
		testutil.Equals(t, []stamp{{minute(59), 37}}, stamps(ds1h[1]))
	})

	t.Run("on a 5m boundary", func(t *testing.T) {
		ds5m, _ := kcRun(t, kcSeries(kcStart, kcStep, kcRepeat("c", 20, "e", 40), false), false)

		testutil.Equals(t, 2, len(ds5m))
		// No window is split, but the custom chunk still ends at its last
		// sample rather than at the end of that sample's window.
		testutil.Equals(t, []stamp{
			{minute(5) - 1, 5}, {minute(10) - 1, 5}, {minute(15) - 1, 5}, {minute(19), 5},
		}, stamps(ds5m[0]))
		testutil.Equals(t, stamp{minute(25) - 1, 5}, stamps(ds5m[1])[0])
	})

	t.Run("on a 1h boundary", func(t *testing.T) {
		_, ds1h := kcRun(t, kcSeries(kcStart, kcStep, kcRepeat("c", 60, "e", 120), false), false)

		testutil.Equals(t, 2, len(ds1h))
		testutil.Equals(t, []stamp{{minute(59), 60}}, stamps(ds1h[0]))
		testutil.Equals(t, []stamp{{minute(120) - 1, 60}, {minute(179), 60}}, stamps(ds1h[1]))
	})
}

// TestDownsampleKindCutAggregatedSource downsamples 5m aggregate chunks to
// 1h whose kind changes on a 5m window boundary inside an hour. That is what
// a series looks like in a 5m block when its kind changed across a scrape
// gap: the previous downsampler wrote such blocks and then refused to
// downsample them further.
func TestDownsampleKindCutAggregatedSource(t *testing.T) {
	custom := func(t int64, n float64) *histogram.FloatHistogram {
		return kcCustom(t, n, kcBoundsA, histogram.UnknownCounterReset).fh
	}
	exponential := func(t int64, n float64) *histogram.FloatHistogram {
		return kcExp(t, n, histogram.UnknownCounterReset).fh
	}
	// aggr is the aggregate sample of 5m window k of a series with one
	// sample per minute, sample i having n=i+1. It is stamped at the window
	// end, as in a block where the kind change fell between two windows.
	aggr := func(k int, mk func(int64, float64) *histogram.FloatHistogram) kcOut {
		ts := kcStart + int64(k+1)*ResLevel1 - 1
		sumN := 0.0
		for i := 5*k + 1; i <= 5*k+5; i++ {
			sumN += float64(i)
		}
		return kcOut{t: ts, count: 5, sum: mk(ts, sumN), counter: mk(ts, float64(5*k+5))}
	}

	for _, tc := range []struct {
		name          string
		first, second func(int64, float64) *histogram.FloatHistogram
		nanSums       bool
		// Histogram count per unit of n, for the kind after the change.
		secondCount float64
	}{
		{name: "custom to exponential", first: custom, second: exponential, secondCount: 7},
		{name: "exponential to custom", first: exponential, second: custom, secondCount: 21},
		{name: "NaN sums next to the change", first: custom, second: exponential, nanSums: true, secondCount: 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Three hours. The first kind covers the windows up to 01:20, the
			// second kind the rest: hour two holds both.
			var a, b []kcOut
			for k := 0; k < 16; k++ {
				a = append(a, aggr(k, tc.first))
			}
			for k := 16; k < 36; k++ {
				b = append(b, aggr(k, tc.second))
			}
			if tc.nanSums {
				// An ordinary NaN sum is kept when aggregates are downsampled.
				for _, fh := range []*histogram.FloatHistogram{a[15].sum, a[15].counter, b[0].sum, b[0].counter} {
					fh.Sum = math.NaN()
				}
			}
			src := []chunks.Meta{kcAggrChunk(t, a), kcAggrChunk(t, b)}

			out := kcParse(t, kcDownsampleBlock(t, src, ResLevel1, ResLevel2))
			kcCheck(t, kcItemsFromChunks(kcParse(t, src)), out, ResLevel2, false)

			testutil.Equals(t, 2, len(out))
			hour := int64(time.Hour / time.Millisecond)

			// First kind: hour one, then the 20 minutes of hour two, stamped
			// at the last 5m sample before the change.
			testutil.Equals(t, 2, len(out[0].samples))
			testutil.Equals(t, kcStart+hour-1, out[0].samples[0].t)
			testutil.Equals(t, 60.0, out[0].samples[0].count)
			testutil.Equals(t, kcStart+hour+20*kcStep-1, out[0].samples[1].t)
			testutil.Equals(t, 20.0, out[0].samples[1].count)

			// Second kind: the other 40 minutes of hour two, then hour three.
			testutil.Equals(t, 2, len(out[1].samples))
			testutil.Equals(t, kcStart+2*hour-1, out[1].samples[0].t)
			testutil.Equals(t, 40.0, out[1].samples[0].count)
			testutil.Equals(t, kcStart+3*hour-1, out[1].samples[1].t)
			testutil.Equals(t, 60.0, out[1].samples[1].count)

			// The counter starts again with the second kind: its first value
			// is the absolute value at the end of hour two (n=120).
			testutil.Equals(t, tc.secondCount*120, out[1].samples[0].counter.Count)
			testutil.Equals(t, tc.secondCount*180, out[1].samples[1].counter.Count)
		})
	}
}

// TestDownsampleKindCutProperty runs the oracle over random series: random
// kind changes, custom layout changes, counter resets, stale markers, NaN
// sums and scrape gaps, at random offsets to the downsampling windows.
func TestDownsampleKindCutProperty(t *testing.T) {
	for seed := int64(1); seed <= 60; seed++ {
		rnd := rand.New(rand.NewSource(seed))
		var (
			gauge = seed%6 == 0
			// Some series flap between kinds on almost every sample.
			flip    = []float64{0.01, 0.04, 0.5}[rnd.Intn(3)]
			n       = 200 + rnd.Intn(1500)
			kind    = "ec"[rnd.Intn(2)]
			pattern []byte
		)
		for len(pattern) < n {
			r := rnd.Float64()
			switch {
			case r < flip:
				if kind == 'e' {
					kind = "cd"[rnd.Intn(2)]
				} else {
					kind = 'e'
				}
			case r < flip+0.01:
				if kind != 'e' {
					kind = 'c' + 'd' - kind
				}
			case r < flip+0.03:
				pattern = append(pattern, 's')
			case r < flip+0.05:
				pattern = append(pattern, "nm"[rnd.Intn(2)])
			case r < flip+0.08:
				pattern = append(pattern, strings.Repeat("_", 1+rnd.Intn(30))...)
			case r < flip+0.085:
				pattern = append(pattern, strings.Repeat("_", 60+rnd.Intn(200))...)
			case r < flip+0.1 && !gauge:
				pattern = append(pattern, kind-'a'+'A')
			default:
				pattern = append(pattern, kind)
			}
		}
		step := []int64{15_000, 30_000, 60_000}[rnd.Intn(3)]
		start := kcStart + rnd.Int63n(int64(time.Hour/time.Millisecond))

		t.Run(fmt.Sprintf("seed %d", seed), func(t *testing.T) {
			raw := kcSeries(start, step, string(pattern), gauge)
			if len(kcItemsFromRaw(raw)) == 0 {
				t.Skip("no sample to downsample")
			}
			kcRun(t, raw, gauge)
		})
	}
}

// kcTSDBSample is a chunks.Sample of any of the three sample types.
type kcTSDBSample struct {
	t  int64
	v  float64
	h  *histogram.Histogram
	fh *histogram.FloatHistogram
}

func (s kcTSDBSample) T() int64                      { return s.t }
func (s kcTSDBSample) F() float64                    { return s.v }
func (s kcTSDBSample) H() *histogram.Histogram       { return s.h }
func (s kcTSDBSample) FH() *histogram.FloatHistogram { return s.fh }
func (s kcTSDBSample) Copy() chunks.Sample           { return s }
func (s kcTSDBSample) Type() chunkenc.ValueType {
	switch {
	case s.h != nil:
		return chunkenc.ValHistogram
	case s.fh != nil:
		return chunkenc.ValFloatHistogram
	default:
		return chunkenc.ValFloat
	}
}

// kcIntHistogram converts a float histogram with whole-number counts to an
// integer histogram, which is what a scrape stores.
func kcIntHistogram(fh *histogram.FloatHistogram) *histogram.Histogram {
	deltas := func(buckets []float64) []int64 {
		out := make([]int64, len(buckets))
		prev := int64(0)
		for i, b := range buckets {
			out[i] = int64(b) - prev
			prev = int64(b)
		}
		return out
	}
	return &histogram.Histogram{
		Schema: fh.Schema, ZeroThreshold: fh.ZeroThreshold, CustomValues: fh.CustomValues,
		ZeroCount: uint64(fh.ZeroCount), Count: uint64(fh.Count), Sum: fh.Sum,
		CounterResetHint: fh.CounterResetHint,
		PositiveSpans:    fh.PositiveSpans,
		PositiveBuckets:  deltas(fh.PositiveBuckets),
	}
}

// TestDownsampleKindCutThroughBlock runs Downsample end to end, raw to 5m to
// 1h, on a TSDB block written by a Prometheus head. The block holds series
// that change histogram kind, as integer and as float histograms, a series
// that also switches between floats and histograms, and series that do
// neither. Both written blocks must pass the index check the compactor runs
// before it uploads a downsampled block.
func TestDownsampleKindCutThroughBlock(t *testing.T) {
	var floats []sample
	for i := 0; i < 240; i++ {
		floats = append(floats, sample{t: kcStart + int64(i)*kcStep, v: float64(i)})
	}
	mixed := append([]sample(nil), floats[:60]...)
	mixed = append(mixed, kcSeries(kcStart+60*kcStep, kcStep, kcRepeat("e", 62, "c", 58), false)...)
	mixed = append(mixed, floats[180:]...)

	raws := map[string][]sample{
		"kc_float_histograms":      kcSeries(kcStart, kcStep, kcRepeat("c", 137, "e", 103), false),
		"kc_integer_histograms":    kcSeries(kcStart, kcStep, kcRepeat("e", 61, "c", 2, "e", 68, "c", 109), false),
		"kc_integer_with_reset":    kcSeries(kcStart, kcStep, kcRepeat("c", 77, "E", 1, "e", 100, "C", 1, "c", 61), false),
		"kc_stable_custom":         kcSeries(kcStart, kcStep, kcRepeat("c", 240), false),
		"kc_stable_exponential":    kcSeries(kcStart, kcStep, kcRepeat("e", 240), false),
		"kc_floats":                floats,
		"kc_floats_and_histograms": mixed,
	}
	var input []storage.Series
	for name, raw := range raws {
		var samples []chunks.Sample
		for _, s := range raw {
			switch {
			case s.fh == nil:
				samples = append(samples, kcTSDBSample{t: s.t, v: s.v})
			case strings.HasPrefix(name, "kc_integer"):
				samples = append(samples, kcTSDBSample{t: s.t, h: kcIntHistogram(s.fh)})
			default:
				samples = append(samples, kcTSDBSample{t: s.t, fh: s.fh})
			}
		}
		input = append(input, storage.NewListSeries(labels.FromStrings(labels.MetricName, name), samples))
	}

	dir := t.TempDir()
	logger := log.NewNopLogger()
	// The head takes one series after the other, so its range must span the
	// whole fixture for the second series not to be too old.
	rawDir, err := tsdb.CreateBlock(input, dir, int64(24*time.Hour/time.Millisecond), logutil.GoKitLogToSlog(logger))
	testutil.Ok(t, err)
	rawID := ulid.MustParse(filepath.Base(rawDir))

	// downsampleBlock downsamples block id and returns the new block with
	// the chunks of every series, by metric name.
	downsampleBlock := func(id ulid.ULID, resolution int64) (ulid.ULID, map[string][]kcChunk) {
		meta, err := metadata.ReadFromDir(filepath.Join(dir, id.String()))
		testutil.Ok(t, err)
		blk, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(logger), filepath.Join(dir, id.String()), NewPool(), tsdb.DefaultPostingsDecoderFactory)
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, blk.Close()) }()

		out, err := Downsample(context.Background(), logger, meta, blk, dir, resolution)
		testutil.Ok(t, err)
		kcCheckIndexHealth(t, dir, out, meta.MinTime, meta.MaxTime)

		lsets, chkss := kcReadBlock(t, dir, out)
		testutil.Equals(t, len(raws), len(lsets))
		parsed := map[string][]kcChunk{}
		for i, lset := range lsets {
			parsed[lset.Get(labels.MetricName)] = kcParse(t, chkss[i])
		}
		return out, parsed
	}

	id5m, ds5m := downsampleBlock(rawID, ResLevel1)
	_, ds1h := downsampleBlock(id5m, ResLevel2)

	for name, raw := range raws {
		t.Run(name, func(t *testing.T) {
			rawItems := kcItemsFromRaw(raw)
			kcCheck(t, rawItems, ds5m[name], ResLevel1, false)
			kcCheck(t, kcItemsFromChunks(ds5m[name]), ds1h[name], ResLevel2, false)
			kcCheckWindows(t, rawItems, ds1h[name], ResLevel2)
			testutil.Equals(t, kcItemKinds(rawItems), kcKinds(ds5m[name]), "kinds at 5m")
			testutil.Equals(t, kcItemKinds(rawItems), kcKinds(ds1h[name]), "kinds at 1h")
		})
	}
}

// TestDownsampleKindCutFailClosed verifies the inputs that are still
// refused.
func TestDownsampleKindCutFailClosed(t *testing.T) {
	hint := histogram.UnknownCounterReset

	// A histogram chunk cannot change kind, so an aggregate chunk holds one
	// kind. One whose counter and sum disagree is not something a downsampler
	// writes, and it is not downsampled.
	for _, tc := range []struct {
		name         string
		sum, counter func(int64, float64) sample
	}{
		{
			name:    "exponential sum, custom counter",
			sum:     func(t int64, n float64) sample { return kcExp(t, n, hint) },
			counter: func(t int64, n float64) sample { return kcCustom(t, n, kcBoundsA, hint) },
		},
		{
			name:    "custom sum, exponential counter",
			sum:     func(t int64, n float64) sample { return kcCustom(t, n, kcBoundsA, hint) },
			counter: func(t int64, n float64) sample { return kcExp(t, n, hint) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var samples []kcOut
			for k := 0; k < 24; k++ {
				ts := kcStart + int64(k+1)*ResLevel1 - 1
				samples = append(samples, kcOut{t: ts, count: 5, sum: tc.sum(ts, float64(25*k+15)).fh, counter: tc.counter(ts, float64(5*k+5)).fh})
			}
			_, _, err := kcTryDownsampleBlock(t, []chunks.Meta{kcAggrChunk(t, samples)}, ResLevel1, ResLevel2)
			testutil.NotOk(t, err)
			testutil.Assert(t, strings.Contains(err.Error(), "mixed exponential and custom bucket histograms"), "unexpected error: %v", err)
		})
	}

	// Two chunks of one series must not touch, so a kind change needs a later
	// timestamp.
	for _, tc := range []struct {
		name string
		data []sample
	}{
		{name: "kind change at the same timestamp", data: []sample{kcExp(kcStart, 1, hint), kcCustom(kcStart, 2, kcBoundsA, hint)}},
		{name: "kind change back in time", data: []sample{kcCustom(kcStart+kcStep, 1, kcBoundsA, hint), kcExp(kcStart, 2, hint)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DownsampleRaw(tc.data, ResLevel1)
			testutil.NotOk(t, err)
			testutil.Assert(t, strings.Contains(err.Error(), "histogram bucket kind changes at timestamp"), "unexpected error: %v", err)
		})
	}
}

func TestHistogramKindRuns(t *testing.T) {
	lens := func(runs [][]sample) []int {
		var out []int
		for _, r := range runs {
			out = append(out, len(r))
		}
		return out
	}
	for _, tc := range []struct {
		pattern string
		runs    []int
	}{
		{pattern: "eeee", runs: []int{4}},
		{pattern: "cdcd", runs: []int{4}},
		{pattern: "s", runs: []int{1}},
		{pattern: "snm", runs: []int{3}},
		{pattern: "eecc", runs: []int{2, 2}},
		{pattern: "ececec", runs: []int{1, 1, 1, 1, 1, 1}},
		// Samples without a kind stay with the run they follow and never cut.
		{pattern: "ssccsmnseesn", runs: []int{8, 4}},
		{pattern: "ccncc", runs: []int{5}},
		{pattern: "eemee", runs: []int{5}},
	} {
		t.Run(tc.pattern, func(t *testing.T) {
			data := kcSeries(kcStart, kcStep, tc.pattern, false)
			runs, err := histogramKindRuns(data)
			testutil.Ok(t, err)
			testutil.Equals(t, tc.runs, lens(runs))

			// The runs are the input, cut in pieces.
			i := 0
			for _, run := range runs {
				for _, s := range run {
					testutil.Assert(t, s.fh == data[i].fh, "runs do not cover the input in order")
					i++
				}
			}
			testutil.Equals(t, len(data), i)
		})
	}
}

func TestKindOfAggrChunk(t *testing.T) {
	hint := histogram.UnknownCounterReset
	one := func(mk func(int64, float64) sample) kcOut {
		return kcOut{t: kcStart, count: 1, sum: mk(kcStart, 1).fh, counter: mk(kcStart, 1).fh}
	}
	exponential := func(t int64, n float64) sample { return kcExp(t, n, hint) }
	custom := func(t int64, n float64) sample { return kcCustom(t, n, kcBoundsA, hint) }
	stale := func(t int64, _ float64) sample {
		return sample{t: t, fh: &histogram.FloatHistogram{Sum: math.Float64frombits(value.StaleNaN)}}
	}
	nanSum := func(t int64, n float64) sample {
		s := custom(t, n)
		s.fh.Sum = math.NaN()
		return s
	}
	floats, err := DownsampleRaw([]sample{{t: kcStart, v: 1}}, ResLevel1)
	testutil.Ok(t, err)

	// The counter decides. Stale markers are skipped; the sum is only asked
	// if the counter holds nothing else.
	staleCounter := one(custom)
	staleCounter.counter = stale(kcStart, 0).fh
	disagreeing := one(custom)
	disagreeing.counter = exponential(kcStart, 1).fh

	for _, tc := range []struct {
		name string
		chk  chunks.Meta
		kind aggrChunkKind
	}{
		{name: "float", chk: floats[0], kind: aggrChunkFloat},
		{name: "exponential", chk: kcAggrChunk(t, []kcOut{one(exponential)}), kind: aggrChunkExponential},
		{name: "custom", chk: kcAggrChunk(t, []kcOut{one(custom)}), kind: aggrChunkCustom},
		{name: "custom with a NaN sum", chk: kcAggrChunk(t, []kcOut{one(nanSum)}), kind: aggrChunkCustom},
		{name: "stale markers only", chk: kcAggrChunk(t, []kcOut{one(stale)}), kind: aggrChunkHistogramNoKind},
		{name: "stale counter, custom sum", chk: kcAggrChunk(t, []kcOut{staleCounter}), kind: aggrChunkCustom},
		{name: "exponential counter, custom sum", chk: kcAggrChunk(t, []kcOut{disagreeing}), kind: aggrChunkExponential},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kind, err := kindOfAggrChunk(tc.chk.Chunk.(*AggrChunk))
			testutil.Ok(t, err)
			testutil.Equals(t, tc.kind, kind)
		})
	}

	// Floats never share a group with histograms. A histogram chunk without
	// a kind fits either histogram kind.
	kinds := []aggrChunkKind{aggrChunkFloat, aggrChunkHistogramNoKind, aggrChunkExponential, aggrChunkCustom}
	same := map[[2]aggrChunkKind]bool{
		{aggrChunkFloat, aggrChunkFloat}:                     true,
		{aggrChunkHistogramNoKind, aggrChunkHistogramNoKind}: true,
		{aggrChunkExponential, aggrChunkExponential}:         true,
		{aggrChunkCustom, aggrChunkCustom}:                   true,
		{aggrChunkHistogramNoKind, aggrChunkExponential}:     true,
		{aggrChunkHistogramNoKind, aggrChunkCustom}:          true,
	}
	for _, a := range kinds {
		for _, b := range kinds {
			want := same[[2]aggrChunkKind{a, b}] || same[[2]aggrChunkKind{b, a}]
			testutil.Equals(t, want, a.sameGroup(b), "sameGroup(%d, %d)", a, b)
		}
	}
}

// TestApplyCounterResetsIteratorKindCut verifies that the iterator a querier
// reads counter aggregates with returns every sample of a series whose
// chunks switch kind, unchanged and in order, and seeks correctly around the
// switches.
func TestApplyCounterResetsIteratorKindCut(t *testing.T) {
	raw := kcSeries(kcStart, kcStep, kcRepeat("c", 61, "e", 2, "c", 68, "e", 109), false)
	chks, err := DownsampleRaw(raw, ResLevel1)
	testutil.Ok(t, err)

	var (
		want []kcOut
		cuts []int // Index in want of the first sample after each kind change.
	)
	for _, c := range kcParse(t, chks) {
		if len(want) > 0 {
			cuts = append(cuts, len(want))
		}
		want = append(want, c.samples...)
	}
	testutil.Equals(t, 3, len(cuts))

	newIterator := func() *ApplyCounterResetsSeriesIterator {
		var its []chunkenc.Iterator
		for _, m := range chks {
			c, err := m.Chunk.(*AggrChunk).Get(AggrCounter)
			testutil.Ok(t, err)
			its = append(its, c.Iterator(nil))
		}
		return NewApplyCounterResetsIterator(its...)
	}
	assertAt := func(it *ApplyCounterResetsSeriesIterator, i int) {
		t.Helper()
		ts, fh := it.AtFloatHistogram(nil)
		testutil.Equals(t, want[i].t, ts)
		testutil.Equals(t, want[i].t, it.AtT())
		testutil.Assert(t, fh.Equals(want[i].counter), "sample %d: histogram changed", i)
		testutil.Equals(t, want[i].counter.CounterResetHint, fh.CounterResetHint, "sample %d: hint", i)
	}

	it := newIterator()
	i := 0
	for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
		testutil.Equals(t, chunkenc.ValFloatHistogram, vt)
		assertAt(it, i)
		i++
	}
	testutil.Ok(t, it.Err())
	testutil.Equals(t, len(want), i)

	// Seek from the start, and from the sample two before the target, to
	// just before, at and just after the samples on both sides of each cut.
	for _, cut := range cuts {
		for _, idx := range []int{cut - 1, cut} {
			for _, delta := range []int64{-1, 0, 1} {
				x := want[idx].t + delta
				first := sort.Search(len(want), func(i int) bool { return want[i].t >= x })
				for _, warm := range []bool{false, true} {
					it := newIterator()
					if warm {
						testutil.Equals(t, chunkenc.ValFloatHistogram, it.Seek(want[idx-2].t))
					}
					testutil.Equals(t, chunkenc.ValFloatHistogram, it.Seek(x))
					assertAt(it, first)
					testutil.Equals(t, chunkenc.ValFloatHistogram, it.Next())
					assertAt(it, first+1)
				}
			}
		}
	}
	it = newIterator()
	testutil.Equals(t, chunkenc.ValNone, it.Seek(want[len(want)-1].t+1))
	testutil.Ok(t, it.Err())
}
