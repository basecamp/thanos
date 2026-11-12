// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package downsample

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"testing"
	"unicode"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// Fixtures and an oracle for series that switch between exponential and
// custom buckets histograms. The oracle does not share logic with the
// downsampler: it takes the source samples and the emitted aggregate chunks
// and checks that the chunks are a faithful, well-formed aggregation.

const (
	// kcStart is aligned to a 1h window, and so to a 5m window.
	kcStart = int64(1688526000000)
	kcStep  = int64(60_000)
)

var (
	kcBoundsA = []float64{0.1, 0.5, 1, 2, 5}
	kcBoundsB = []float64{0.1, 0.75, 1, 2, 5}
)

type kcKind uint8

const (
	kcKindFloat kcKind = iota
	kcKindExponential
	kcKindCustom
)

func (k kcKind) String() string {
	return [...]string{"float", "exponential", "custom"}[k]
}

// kcExp returns an exponential histogram in which every count is a multiple
// of n and the buckets add up to Count.
func kcExp(t int64, n float64, hint histogram.CounterResetHint) sample {
	return sample{t: t, fh: &histogram.FloatHistogram{
		Schema: 3, ZeroThreshold: 0.001, ZeroCount: n,
		Count: 7 * n, Sum: 5.25 * n,
		CounterResetHint: hint,
		PositiveSpans:    []histogram.Span{{Offset: 0, Length: 3}},
		PositiveBuckets:  []float64{n, 2 * n, 3 * n},
	}}
}

// kcCustom returns a custom buckets histogram in which every count is a
// multiple of n and the buckets add up to Count.
func kcCustom(t int64, n float64, bounds []float64, hint histogram.CounterResetHint) sample {
	buckets := make([]float64, len(bounds)+1)
	count := 0.0
	for j := range buckets {
		buckets[j] = float64(j+1) * n
		count += buckets[j]
	}
	return sample{t: t, fh: &histogram.FloatHistogram{
		Schema: histogram.CustomBucketsSchema, CustomValues: append([]float64(nil), bounds...),
		Count: count, Sum: 0.75 * count,
		CounterResetHint: hint,
		PositiveSpans:    []histogram.Span{{Offset: 0, Length: uint32(len(buckets))}},
		PositiveBuckets:  buckets,
	}}
}

// kcSeries builds a histogram series from a pattern with one character per
// step:
//
//	e  exponential histogram
//	c  custom buckets histogram with bounds kcBoundsA
//	d  custom buckets histogram with bounds kcBoundsB
//	E, C, D  the same, and the counter starts again from its lowest value
//	s  stale marker
//	n  exponential histogram with a NaN sum
//	m  custom buckets histogram with a NaN sum
//	_  no sample
//
// Counts grow with every step, across kind changes too. With gauge set, the
// samples are gauge histograms whose counts go up and down.
func kcSeries(start, step int64, pattern string, gauge bool) []sample {
	var (
		out  []sample
		base int
	)
	for i, ch := range pattern {
		if ch == '_' {
			continue
		}
		if unicode.IsUpper(ch) {
			base = i
			ch = unicode.ToLower(ch)
		}
		var (
			t    = start + int64(i)*step
			n    = float64(i + 1 - base)
			hint = histogram.UnknownCounterReset
		)
		if gauge {
			hint = histogram.GaugeType
			n = 10 + float64((i*7)%5)
		}
		var s sample
		switch ch {
		case 'e':
			s = kcExp(t, n, hint)
		case 'c':
			s = kcCustom(t, n, kcBoundsA, hint)
		case 'd':
			s = kcCustom(t, n, kcBoundsB, hint)
		case 's':
			s = sample{t: t, fh: &histogram.FloatHistogram{Sum: math.Float64frombits(value.StaleNaN)}}
		case 'n':
			s = kcExp(t, n, hint)
			s.fh.Sum = math.NaN()
		case 'm':
			s = kcCustom(t, n, kcBoundsA, hint)
			s.fh.Sum = math.NaN()
		default:
			panic("unknown pattern character " + string(ch))
		}
		out = append(out, s)
	}
	return out
}

// kcRepeat concatenates pattern pieces given as (piece, repetitions) pairs.
func kcRepeat(parts ...any) string {
	var out []byte
	for i := 0; i < len(parts); i += 2 {
		for j := 0; j < parts[i+1].(int); j++ {
			out = append(out, parts[i].(string)...)
		}
	}
	return string(out)
}

func kcKindOf(fh *histogram.FloatHistogram) kcKind {
	if fh.UsesCustomBuckets() {
		return kcKindCustom
	}
	return kcKindExponential
}

// kcApprox compares two numbers up to rounding. NaN equals NaN: a histogram
// sum may be NaN and has to stay NaN.
func kcApprox(a, b float64) bool {
	if math.IsNaN(a) || math.IsNaN(b) {
		return math.IsNaN(a) && math.IsNaN(b)
	}
	if math.IsInf(a, 0) || math.IsInf(b, 0) {
		return a == b
	}
	return math.Abs(a-b) <= 1e-9*math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
}

// kcUnits returns the n a fixture histogram stands for: kcExp and kcCustom
// build a histogram with a count of 7n and 21n. Adding and subtracting
// fixture histograms of one kind adds and subtracts their units.
func kcUnits(fh *histogram.FloatHistogram) float64 {
	if fh.UsesCustomBuckets() {
		return fh.Count / 21
	}
	return fh.Count / 7
}

// kcShapeError checks that fh is what fixture histograms of its kind add up
// to: the given number of units in every count, bucket by bucket, and the
// given sum of observations. It returns what is wrong, or nothing.
//
// Custom buckets histograms may have been reduced to the bounds two layouts
// have in common, so they are compared at the bounds every fixture layout
// has: 0.1, 1, 2, 5 and +Inf.
func kcShapeError(fh *histogram.FloatHistogram, units, sum float64) string {
	if !kcApprox(sum, fh.Sum) {
		return fmt.Sprintf("sum of observations %g, expected %g", fh.Sum, sum)
	}
	if len(fh.NegativeBuckets) > 0 || len(fh.NegativeSpans) > 0 {
		return "negative buckets"
	}
	if !fh.UsesCustomBuckets() {
		if fh.Schema != 3 || fh.ZeroThreshold != 0.001 {
			return fmt.Sprintf("schema %d and zero threshold %g, expected 3 and 0.001", fh.Schema, fh.ZeroThreshold)
		}
		if len(fh.PositiveSpans) != 1 || fh.PositiveSpans[0] != (histogram.Span{Offset: 0, Length: 3}) || len(fh.PositiveBuckets) != 3 {
			return fmt.Sprintf("spans %v with %d buckets, expected one span of three buckets", fh.PositiveSpans, len(fh.PositiveBuckets))
		}
		for i, want := range []float64{units, 2 * units, 3 * units} {
			if !kcApprox(want, fh.PositiveBuckets[i]) {
				return fmt.Sprintf("bucket %d holds %g, expected %g", i, fh.PositiveBuckets[i], want)
			}
		}
		if !kcApprox(units, fh.ZeroCount) || !kcApprox(7*units, fh.Count) {
			return fmt.Sprintf("zero count %g and count %g, expected %g and %g", fh.ZeroCount, fh.Count, units, 7*units)
		}
		return ""
	}

	if fh.ZeroCount != 0 || !kcApprox(21*units, fh.Count) {
		return fmt.Sprintf("zero count %g and count %g, expected 0 and %g", fh.ZeroCount, fh.Count, 21*units)
	}
	// Counts at or below every upper bound, the last one being +Inf.
	bounds := append(append([]float64(nil), fh.CustomValues...), math.Inf(1))
	cumulative := make([]float64, len(bounds))
	idx, bucket := 0, 0
	for _, span := range fh.PositiveSpans {
		idx += int(span.Offset)
		for k := 0; k < int(span.Length); k++ {
			if idx >= len(cumulative) || bucket >= len(fh.PositiveBuckets) {
				return "more buckets than bounds"
			}
			cumulative[idx] = fh.PositiveBuckets[bucket]
			idx++
			bucket++
		}
	}
	if bucket != len(fh.PositiveBuckets) {
		return "more buckets than spans"
	}
	for i := 1; i < len(cumulative); i++ {
		cumulative[i] += cumulative[i-1]
	}
	for _, want := range []struct{ bound, units float64 }{{0.1, 1}, {1, 6}, {2, 10}, {5, 15}, {math.Inf(1), 21}} {
		i := sort.SearchFloat64s(bounds, want.bound)
		if i == len(bounds) || bounds[i] != want.bound {
			return fmt.Sprintf("no bucket boundary at %g in %v", want.bound, fh.CustomValues)
		}
		if !kcApprox(want.units*units, cumulative[i]) {
			return fmt.Sprintf("%g observations up to %g, expected %g", cumulative[i], want.bound, want.units*units)
		}
	}
	return ""
}

// kcItem is one source sample as the oracle sees it: a raw sample, or an
// aggregate sample of a lower resolution.
type kcItem struct {
	t      int64
	kind   kcKind
	weight float64 // Number of raw samples it stands for.
	v      float64 // Float items: what the sum aggregate adds up.

	// Histogram items: what the sum aggregate adds up and what the counter
	// aggregate chains, each as units (see kcUnits) and sum of observations.
	units, sum       float64
	ctrUnits, ctrSum float64
}

// kcItemsFromRaw returns the raw samples that are downsampled: no stale
// markers and no NaN sums.
func kcItemsFromRaw(data []sample) []kcItem {
	var items []kcItem
	for _, s := range data {
		if s.fh == nil {
			if !math.IsNaN(s.v) {
				items = append(items, kcItem{t: s.t, kind: kcKindFloat, weight: 1, v: s.v})
			}
			continue
		}
		if math.IsNaN(s.fh.Sum) {
			continue
		}
		items = append(items, kcItem{
			t: s.t, kind: kcKindOf(s.fh), weight: 1,
			units: kcUnits(s.fh), sum: s.fh.Sum,
			ctrUnits: kcUnits(s.fh), ctrSum: s.fh.Sum,
		})
	}
	return items
}

// kcItemsFromChunks returns the aggregate samples of parsed chunks as source
// items for the next downsampling pass.
func kcItemsFromChunks(chks []kcChunk) []kcItem {
	var items []kcItem
	for _, c := range chks {
		for _, s := range c.samples {
			it := kcItem{t: s.t, kind: c.kind, weight: s.count}
			if c.kind == kcKindFloat {
				it.v = s.fsum
			} else {
				it.units, it.sum = kcUnits(s.sum), s.sum.Sum
				it.ctrUnits, it.ctrSum = kcUnits(s.counter), s.counter.Sum
			}
			items = append(items, it)
		}
	}
	return items
}

// kcOut is one aggregate sample: the count, sum and counter aggregates at
// one timestamp.
type kcOut struct {
	t       int64
	count   float64
	sum     *histogram.FloatHistogram // nil in a float chunk.
	counter *histogram.FloatHistogram // nil in a float chunk.
	fsum    float64                   // Float chunks only.
}

type kcChunk struct {
	mint, maxt int64
	kind       kcKind
	samples    []kcOut
}

// kcParse decodes aggregate chunks and checks their layout: every chunk is
// non-empty, its aggregates are aligned, its histograms have one kind and
// one custom layout, its time range is that of its samples, and the chunks
// are ordered and do not touch.
func kcParse(t *testing.T, chks []chunks.Meta) []kcChunk {
	t.Helper()
	var out []kcChunk
	for i, m := range chks {
		ac, ok := m.Chunk.(*AggrChunk)
		testutil.Assert(t, ok, "chunk %d: expected *AggrChunk, got %T", i, m.Chunk)

		var counts []sample
		cnt, err := ac.Get(AggrCount)
		testutil.Ok(t, err)
		testutil.Equals(t, chunkenc.EncXOR, cnt.Encoding())
		cntIt := cnt.Iterator(nil)
		for cntIt.Next() != chunkenc.ValNone {
			ts, v := cntIt.At()
			counts = append(counts, sample{t: ts, v: v})
		}
		testutil.Ok(t, cntIt.Err())
		testutil.Assert(t, len(counts) > 0, "chunk %d is empty", i)

		c := kcChunk{mint: m.MinTime, maxt: m.MaxTime}
		ctr, err := ac.Get(AggrCounter)
		testutil.Ok(t, err)
		sum, err := ac.Get(AggrSum)
		testutil.Ok(t, err)

		if ctr.Encoding() == chunkenc.EncXOR {
			c.kind = kcKindFloat
			testutil.Equals(t, chunkenc.EncXOR, sum.Encoding())
			n := 0
			sumIt := sum.Iterator(nil)
			for ; sumIt.Next() != chunkenc.ValNone; n++ {
				ts, v := sumIt.At()
				testutil.Assert(t, n < len(counts) && counts[n].t == ts, "chunk %d: sum and count are not aligned", i)
				c.samples = append(c.samples, kcOut{t: ts, count: counts[n].v, fsum: v})
			}
			testutil.Ok(t, sumIt.Err())
			testutil.Equals(t, len(counts), n, "chunk %d: sum and count are not aligned", i)
		} else {
			testutil.Equals(t, chunkenc.EncFloatHistogram, ctr.Encoding())
			testutil.Equals(t, chunkenc.EncFloatHistogram, sum.Encoding())
			read := func(chk chunkenc.Chunk, name string) []sample {
				var hs []sample
				it := chk.Iterator(nil)
				for it.Next() != chunkenc.ValNone {
					ts, fh := it.AtFloatHistogram(nil)
					hs = append(hs, sample{t: ts, fh: fh})
				}
				testutil.Ok(t, it.Err())
				testutil.Equals(t, len(counts), len(hs), "chunk %d: %s and count are not aligned", i, name)
				return hs
			}
			sums, ctrs := read(sum, "sum"), read(ctr, "counter")
			c.kind = kcKindOf(ctrs[0].fh)
			for n := range counts {
				testutil.Assert(t, sums[n].t == counts[n].t && ctrs[n].t == counts[n].t, "chunk %d: timestamps are not aligned at sample %d", i, n)
				for _, fh := range []*histogram.FloatHistogram{sums[n].fh, ctrs[n].fh} {
					testutil.Assert(t, kcKindOf(fh) == c.kind, "chunk %d: bucket kind changes inside the chunk at sample %d", i, n)
					testutil.Assert(t, histogram.FloatBucketsMatch(fh.CustomValues, ctrs[0].fh.CustomValues), "chunk %d: custom layout changes inside the chunk at sample %d", i, n)
				}
				c.samples = append(c.samples, kcOut{t: counts[n].t, count: counts[n].v, sum: sums[n].fh, counter: ctrs[n].fh})
			}
		}

		for n := 1; n < len(c.samples); n++ {
			testutil.Assert(t, c.samples[n].t > c.samples[n-1].t, "chunk %d: timestamps do not increase at sample %d", i, n)
		}
		testutil.Equals(t, c.samples[0].t, c.mint, "chunk %d: MinTime", i)
		testutil.Equals(t, c.samples[len(c.samples)-1].t, c.maxt, "chunk %d: MaxTime", i)
		if i > 0 {
			testutil.Assert(t, out[i-1].maxt < c.mint, "chunk %d starts at %d, not after chunk %d ending at %d", i, c.mint, i-1, out[i-1].maxt)
		}
		out = append(out, c)
	}
	return out
}

// kcCheck verifies aggregate chunks against the source samples they were
// made from.
//
// Every aggregate sample stands for a fragment: the source samples after the
// previous aggregate sample, up to its own timestamp. For each fragment the
// oracle checks that it is not empty, lies in one window and has the kind of
// its chunk; that the count is the number of its samples; that the sum is
// the sum of its samples, bucket by bucket and in the sum of observations;
// that it is stamped at the window end or at its last source sample; and
// that the counter is, bucket by bucket, the chain of deltas (or of
// restarts, where the counts drop) since the first source sample of the
// chunk. Together with the final "no source sample left" check this means no
// sample is lost or counted twice.
//
// At a change of kind, the last fragment before it must be stamped at its
// last source sample and must end its chunk, and the first counter sample of
// every histogram chunk must read back as a possible counter reset.
func kcCheck(t *testing.T, src []kcItem, out []kcChunk, resolution int64, gauge bool) {
	t.Helper()
	var (
		p     int
		lastT = int64(math.MinInt64)
	)
	for ci, c := range out {
		var ctr, ctrSum, prev, prevSum float64
		first := true
		for si, e := range c.samples {
			testutil.Assert(t, e.t > lastT, "chunk %d sample %d: timestamp %d does not increase", ci, si, e.t)
			lastT = e.t

			var weight, fsum, units, sum float64
			from := p
			for ; p < len(src) && src[p].t <= e.t; p++ {
				it := src[p]
				testutil.Assert(t, it.kind == c.kind, "chunk %d sample %d: %s chunk covers a %s source sample at %d", ci, si, c.kind, it.kind, it.t)
				testutil.Assert(t, currentWindow(it.t, resolution) == currentWindow(e.t, resolution),
					"chunk %d sample %d at %d covers the source sample at %d of another window", ci, si, e.t, it.t)
				weight += it.weight
				fsum += it.v
				units += it.units
				sum += it.sum
				switch {
				case first:
					ctr, ctrSum, first = it.ctrUnits, it.ctrSum, false
				case gauge || it.ctrUnits >= prev:
					ctr += it.ctrUnits - prev
					ctrSum += it.ctrSum - prevSum
				default:
					// The counts dropped: a counter reset. The counter goes on
					// from where it was, by the full new value.
					ctr += it.ctrUnits
					ctrSum += it.ctrSum
				}
				prev, prevSum = it.ctrUnits, it.ctrSum
			}
			testutil.Assert(t, p > from, "chunk %d sample %d at %d has no source sample", ci, si, e.t)
			last := src[p-1]
			testutil.Assert(t, e.t == last.t || e.t == currentWindow(last.t, resolution),
				"chunk %d sample %d is stamped %d: neither its last source sample %d nor its window end", ci, si, e.t, last.t)
			if p < len(src) && src[p].kind != last.kind {
				testutil.Assert(t, e.t == last.t, "chunk %d sample %d: the fragment before a kind change is stamped %d, not %d", ci, si, e.t, last.t)
				testutil.Assert(t, si == len(c.samples)-1, "chunk %d does not end at the kind change after %d", ci, last.t)
			}

			testutil.Equals(t, weight, e.count, "chunk %d sample %d at %d: count", ci, si, e.t)
			if c.kind == kcKindFloat {
				testutil.Assert(t, kcApprox(fsum, e.fsum), "chunk %d sample %d: sum %g, expected %g", ci, si, e.fsum, fsum)
				continue
			}
			msg := kcShapeError(e.sum, units, sum)
			testutil.Assert(t, msg == "", "chunk %d sample %d at %d: sum aggregate: %s", ci, si, e.t, msg)
			msg = kcShapeError(e.counter, ctr, ctrSum)
			testutil.Assert(t, msg == "", "chunk %d sample %d at %d: counter aggregate: %s", ci, si, e.t, msg)

			testutil.Equals(t, histogram.GaugeType, e.sum.CounterResetHint, "chunk %d sample %d: sum hint", ci, si)
			if si == 0 {
				hint := e.counter.CounterResetHint
				if gauge {
					testutil.Equals(t, histogram.GaugeType, hint, "chunk %d: first counter hint", ci)
				} else {
					testutil.Assert(t, hint == histogram.UnknownCounterReset || hint == histogram.CounterReset,
						"chunk %d: the first counter sample reads hint %d, which hides the restart from a reader", ci, hint)
				}
			}
		}
	}
	testutil.Equals(t, len(src), p, "source samples after the last aggregate sample")
}

// kcKinds returns the kinds of the given chunks with repeats collapsed.
func kcKinds(chks []kcChunk) []kcKind {
	var kinds []kcKind
	for _, c := range chks {
		if len(kinds) == 0 || kinds[len(kinds)-1] != c.kind {
			kinds = append(kinds, c.kind)
		}
	}
	return kinds
}

// kcItemKinds returns the kinds of the given source items with repeats
// collapsed.
func kcItemKinds(items []kcItem) []kcKind {
	var kinds []kcKind
	for _, it := range items {
		if len(kinds) == 0 || kinds[len(kinds)-1] != it.kind {
			kinds = append(kinds, it.kind)
		}
	}
	return kinds
}

// kcCheckWindows verifies against the raw samples that no window of the
// given resolution lost or gained a sample of either kind. It is how a 1h
// block is checked against the raw series, two passes back.
func kcCheckWindows(t *testing.T, raw []kcItem, out []kcChunk, resolution int64) {
	t.Helper()
	type key struct {
		window int64
		kind   kcKind
	}
	want, got := map[key]float64{}, map[key]float64{}
	for _, it := range raw {
		want[key{currentWindow(it.t, resolution), it.kind}] += it.weight
	}
	for _, c := range out {
		for _, s := range c.samples {
			got[key{currentWindow(s.t, resolution), c.kind}] += s.count
		}
	}
	testutil.Equals(t, want, got, "samples per window and kind")
}

// kcReadBlock returns the series of a block with their chunks loaded.
func kcReadBlock(t *testing.T, dir string, id ulid.ULID) ([]labels.Labels, [][]chunks.Meta) {
	t.Helper()
	_, lsets, chkss := GetMetaLabelsAndChunks(t, dir, id)
	chunkr, err := chunks.NewDirReader(filepath.Join(dir, id.String(), block.ChunksDirname), NewPool())
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, chunkr.Close()) }()

	for _, chks := range chkss {
		for i, m := range chks {
			chk, _, err := chunkr.ChunkOrIterable(m)
			testutil.Ok(t, err)
			ac, ok := chk.(*AggrChunk)
			testutil.Assert(t, ok, "expected *AggrChunk, got %T", chk)
			// The reader's memory goes away with the reader.
			cp := AggrChunk(append([]byte(nil), ac.Bytes()...))
			chks[i].Chunk = &cp
		}
	}
	return lsets, chkss
}

// kcCheckIndexHealth runs the check the compactor runs on every downsampled
// block before it uploads it.
func kcCheckIndexHealth(t *testing.T, dir string, id ulid.ULID, mint, maxt int64) {
	t.Helper()
	stats, err := block.GatherIndexHealthStats(context.Background(), log.NewNopLogger(), filepath.Join(dir, id.String(), block.IndexFilename), mint, maxt)
	testutil.Ok(t, err)
	testutil.Ok(t, stats.AnyErr())
}

// kcDownsampleBlock runs Downsample over a block holding one series with the
// given chunks, checks the index of the written block, and returns the
// chunks of the series in it.
func kcDownsampleBlock(t *testing.T, chks []chunks.Meta, inRes, outRes int64) []chunks.Meta {
	t.Helper()
	id, dir, err := kcTryDownsampleBlock(t, chks, inRes, outRes)
	testutil.Ok(t, err)
	kcCheckIndexHealth(t, dir, id, chks[0].MinTime, chks[len(chks)-1].MaxTime+1)

	_, chkss := kcReadBlock(t, dir, id)
	testutil.Equals(t, 1, len(chkss))
	return chkss[0]
}

func kcTryDownsampleBlock(t *testing.T, chks []chunks.Meta, inRes, outRes int64) (ulid.ULID, string, error) {
	t.Helper()
	cp := make([]chunks.Meta, 0, len(chks))
	for _, m := range chks {
		if ac, ok := m.Chunk.(*AggrChunk); ok {
			c := AggrChunk(ac.Bytes())
			m.Chunk = &c
		}
		cp = append(cp, m)
	}
	mb := newMemBlock()
	mb.addSeries(&series{lset: labels.FromStrings(labels.MetricName, "kindcut"), chunks: cp})

	dir := t.TempDir()
	id, err := Downsample(context.Background(), log.NewNopLogger(), &metadata.Meta{
		BlockMeta: tsdb.BlockMeta{MinTime: chks[0].MinTime, MaxTime: chks[len(chks)-1].MaxTime + 1},
		Thanos:    metadata.Thanos{Downsample: metadata.ThanosDownsample{Resolution: inRes}},
	}, mb, dir, outRes)
	return id, dir, err
}

// kcAggrChunk encodes histogram aggregate samples as one aggregate chunk,
// for sources that DownsampleRaw does not produce.
func kcAggrChunk(t *testing.T, samples []kcOut) chunks.Meta {
	t.Helper()
	ab := newHistogramAggrChunkBuilder(false)
	for _, s := range samples {
		ab.apps[AggrCount].Append(s.t, s.count)
		sum := s.sum.Copy()
		sum.CounterResetHint = histogram.GaugeType
		testutil.Ok(t, ab.appendFloatHistogram(AggrSum, s.t, sum))
		testutil.Ok(t, ab.appendFloatHistogram(AggrCounter, s.t, s.counter.Copy()))
		ab.extendTimeRange(s.t)
	}
	return ab.encode()
}

// kcHash is a fingerprint of chunk time ranges and bytes.
func kcHash(chks []chunks.Meta) string {
	h := sha256.New()
	var buf [8]byte
	put := func(v int64) {
		binary.BigEndian.PutUint64(buf[:], uint64(v))
		h.Write(buf[:])
	}
	for _, m := range chks {
		put(m.MinTime)
		put(m.MaxTime)
		put(int64(len(m.Chunk.Bytes())))
		h.Write(m.Chunk.Bytes())
	}
	return hex.EncodeToString(h.Sum(nil))
}
