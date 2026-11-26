// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package downsample

import (
	"context"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
)

// TestPromQLNHCBDownsampleDifferential evaluates PromQL over raw NHCB series
// and over their downsampled aggregates and bounds the divergence: exact in
// steady state, and within a per-scenario tolerance across bucket layout
// transitions, where PromQL over raw data applies its own reset semantics.
// Where raw PromQL returns no result at all (bounds differ inside the range
// window) the downsampled data must still answer, never the reverse.
func TestPromQLNHCBDownsampleDifferential(t *testing.T) {
	const (
		step        = int64(time.Minute / time.Millisecond)
		resolution  = int64(5 * time.Minute / time.Millisecond)
		queryWindow = "20m"
	)

	full := []float64{0.1, 0.5, 1, 2, 5}
	prefix := []float64{0.1, 0.5, 1}
	alternate := []float64{0.1, 0.75, 1, 2, 5}

	// start is aligned to a downsampling window. With 800 samples the public
	// DownsampleRaw chunk-sizing heuristic cuts its first input batch after the
	// window ending at sample 404, so sample 405 also exercises an input-batch
	// boundary without inspecting or depending on the implementation itself.
	const start = int64(1_700_000_100_000)
	scenarios := []struct {
		name        string
		n           int
		layout      func(int) []float64
		evalFrom    int
		evalThrough int
		// maxDivergence bounds the relative divergence at evaluation points
		// where both raw and downsampled data answer; transitions coarsen
		// the affected windows, steady state must agree exactly.
		maxDivergence float64
	}{
		{
			name:          "no transition",
			maxDivergence: 0,
			n:             180,
			layout:        func(int) []float64 { return full },
			evalFrom:      30, evalThrough: 170,
		},
		{
			name:          "one transition mid window",
			maxDivergence: 0.3,
			n:             180,
			layout: func(i int) []float64 {
				if i < 77 {
					return full
				}
				return alternate
			},
			evalFrom: 65, evalThrough: 100,
		},
		{
			name:          "transition at window boundary",
			maxDivergence: 0.3,
			n:             180,
			layout: func(i int) []float64 {
				if i < 60 {
					return full
				}
				return alternate
			},
			evalFrom: 50, evalThrough: 85,
		},
		{
			name:          "transition at batch boundary",
			maxDivergence: 0.3,
			n:             800,
			layout: func(i int) []float64 {
				if i < 405 {
					return full
				}
				return alternate
			},
			evalFrom: 395, evalThrough: 430,
		},
		{
			// Every emitted-layout change is a segment boundary in a
			// segmenting implementation. This transient covers both boundaries.
			name:          "full prefix full segment boundaries",
			maxDivergence: 0.3,
			n:             240,
			layout: func(i int) []float64 {
				switch {
				case i < 90:
					return full
				case i < 105:
					return prefix
				default:
					return full
				}
			},
			evalFrom: 80, evalThrough: 130,
		},
	}

	queries := []struct {
		name string
		expr string
	}{
		{name: "rate", expr: "rate(nhcb_differential[" + queryWindow + "])"},
		{name: "increase", expr: "increase(nhcb_differential[" + queryWindow + "])"},
		{name: "histogram count rate", expr: "histogram_count(rate(nhcb_differential[" + queryWindow + "]))"},
		{name: "histogram quantile 0.9", expr: "histogram_quantile(0.9, rate(nhcb_differential[" + queryWindow + "]))"},
	}

	engine := promql.NewEngine(promql.EngineOpts{
		MaxSamples:               1_000_000,
		Timeout:                  30 * time.Second,
		LookbackDelta:            30 * time.Minute,
		NoStepSubqueryIntervalFn: func(int64) int64 { return step },
	})

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			raw := differentialNHCBFixture(start, step, sc.n, sc.layout)
			downsampled, err := DownsampleRaw(raw, resolution)
			testutil.Ok(t, err)
			testutil.Assert(t, len(downsampled) > 0, "DownsampleRaw returned no chunks")

			rawQueryable := newDifferentialQueryable(raw, nil)
			downsampledQueryable := newDifferentialQueryable(nil, downsampled)
			from := time.UnixMilli(start + int64(sc.evalFrom)*step)
			through := time.UnixMilli(start + int64(sc.evalThrough)*step)

			for _, query := range queries {
				t.Run(query.name, func(t *testing.T) {
					var diffs []float64
					for at := from; !at.After(through); at = at.Add(5 * time.Minute) {
						rawValue := differentialEval(t, engine, rawQueryable, query.expr, at)
						dsValue := differentialEval(t, engine, downsampledQueryable, query.expr, at)
						d := differentialValueDistance(rawValue, dsValue)
						if math.IsInf(d, 1) {
							// A presence mismatch is only acceptable in one
							// direction: raw PromQL drops NHCB series whose
							// bounds differ inside a range window, while the
							// downsampled segments still answer.
							rv, rok := rawValue.(promql.Vector)
							dv, dok := dsValue.(promql.Vector)
							testutil.Assert(t, rok && dok && len(rv) == 0 && len(dv) == 1,
								"at %s: raw answered but downsampled did not (raw=%v ds=%v)", at.UTC().Format("15:04"), rawValue, dsValue)
							continue
						}
						testutil.Assert(t, d <= sc.maxDivergence, "at %s: divergence %g exceeds %g", at.UTC().Format("15:04"), d, sc.maxDivergence)
						diffs = append(diffs, d)
					}
					testutil.Assert(t, len(diffs) > 0, "no comparable PromQL evaluations")
					var sum, maxDiff float64
					for _, d := range diffs {
						sum += d
						maxDiff = math.Max(maxDiff, d)
					}
					t.Logf("scenario=%s query=%s points=%d max_divergence=%g mean_divergence=%g",
						sc.name, query.expr, len(diffs), maxDiff, sum/float64(len(diffs)))
				})
			}
		})
	}
}

// TestPromQLKindCutDownsampleDifferential evaluates PromQL over raw series
// that switch between exponential and custom buckets histograms, and over
// their 5m aggregates.
//
// PromQL does not compute across a change of kind: it treats the change as
// a counter reset, and it gives no sample and a warning for a range it
// cannot evaluate because of it. That is so for raw data and has to be so
// for aggregates. Two things are checked at every evaluation time:
//
//   - On each side, whether the query returns a sample is exactly what
//     PromQL's rules say for the kinds of the samples in the range (see
//     kcExpectSample), and resets counts exactly the changes of kind.
//   - Steady state: unless the range holds the last sample before a change
//     of kind or the first one after it, the aggregates give what raw data
//     gives, bucket by bucket. In between, the two sides cannot agree number
//     for number, because an aggregate sample stands for a whole window.
func TestPromQLKindCutDownsampleDifferential(t *testing.T) {
	const (
		queryWindow = 20 * kcStep
		rng         = "[20m]"
	)
	queries := []struct {
		name  string
		expr  string
		class kcQueryClass
		// wholeWindows marks a query that adds up samples instead of taking
		// a rate. Raw data and aggregates only cover the same samples when the
		// range ends where a downsampling window ends.
		wholeWindows bool
	}{
		{name: "rate", expr: "rate(nhcb_differential" + rng + ")", class: kcClassRate},
		{name: "increase", expr: "increase(nhcb_differential" + rng + ")", class: kcClassRate},
		{name: "histogram count of rate", expr: "histogram_count(rate(nhcb_differential" + rng + "))", class: kcClassRate},
		{name: "histogram quantile of rate", expr: "histogram_quantile(0.9, rate(nhcb_differential" + rng + "))", class: kcClassRate},
		{name: "irate", expr: "irate(nhcb_differential" + rng + ")", class: kcClassIRate},
		{name: "resets", expr: "resets(nhcb_differential" + rng + ")", class: kcClassResets},
		{name: "sum over time", expr: "sum_over_time(nhcb_differential" + rng + ")", class: kcClassSum, wholeWindows: true},
	}
	engine := promql.NewEngine(promql.EngineOpts{
		MaxSamples:               1_000_000,
		Timeout:                  30 * time.Second,
		LookbackDelta:            30 * time.Minute,
		NoStepSubqueryIntervalFn: func(int64) int64 { return kcStep },
	})
	eval := func(t *testing.T, queryable storage.Queryable, expr string, at int64) (promql.Vector, annotations.Annotations) {
		t.Helper()
		q, err := engine.NewInstantQuery(context.Background(), queryable, promql.NewPrometheusQueryOpts(false, 0), expr, time.UnixMilli(at))
		testutil.Ok(t, err)
		defer q.Close()
		res := q.Exec(context.Background())
		testutil.Ok(t, res.Err)
		v, ok := res.Value.(promql.Vector)
		testutil.Assert(t, ok, "expected a vector, got %T", res.Value)
		return v, res.Warnings
	}

	for _, sc := range []struct {
		name    string
		pattern string
	}{
		{name: "exponential to custom mid window", pattern: kcRepeat("e", 137, "c", 103)},
		{name: "custom to exponential mid window", pattern: kcRepeat("c", 137, "e", 103)},
		{name: "exponential to custom on a 5m boundary", pattern: kcRepeat("e", 135, "c", 105)},
		{name: "custom to exponential on a 1h boundary", pattern: kcRepeat("c", 120, "e", 120)},
		{name: "two flips", pattern: kcRepeat("c", 61, "e", 60, "c", 119)},
		{name: "custom to exponential across a scrape gap", pattern: kcRepeat("c", 138, "_", 5, "e", 97)},
	} {
		t.Run(sc.name, func(t *testing.T) {
			raw := kcSeries(kcStart, kcStep, sc.pattern, false)
			downsampled, err := DownsampleRaw(raw, ResLevel1)
			testutil.Ok(t, err)
			rawQueryable := newDifferentialQueryable(raw, nil)
			dsQueryable := newDifferentialQueryable(nil, downsampled)

			// The kind changes: last timestamp of the old kind, first of the new.
			type cut struct{ ta, tb int64 }
			var cuts []cut
			for i := 1; i < len(raw); i++ {
				if raw[i].fh.UsesCustomBuckets() != raw[i-1].fh.UsesCustomBuckets() {
					cuts = append(cuts, cut{raw[i-1].t, raw[i].t})
				}
			}
			testutil.Assert(t, len(cuts) > 0, "fixture without a kind change")
			// Steady: the range holds neither the last sample before a change
			// of kind, nor a scrape gap that follows it.
			steady := func(at int64) bool {
				for _, c := range cuts {
					if at >= c.ta && at < c.tb+queryWindow {
						return false
					}
				}
				return true
			}

			// The kinds of the samples either side has in a range.
			var rawSamples, dsSamples []kcKindAt
			for _, s := range raw {
				rawSamples = append(rawSamples, kcKindAt{s.t, s.fh.UsesCustomBuckets()})
			}
			for _, c := range kcParse(t, downsampled) {
				for _, s := range c.samples {
					dsSamples = append(dsSamples, kcKindAt{s.t, c.kind == kcKindCustom})
				}
			}

			for _, query := range queries {
				t.Run(query.name, func(t *testing.T) {
					var compared, noSample int
					// Every minute, just before the next sample, from the first
					// full range to the last sample: every fifth evaluation
					// time is the end of a downsampling window.
					for at := kcStart + queryWindow - 1; at < raw[len(raw)-1].t; at += kcStep {
						minute := (at + 1 - kcStart) / kcStep
						rawValue, rawWarnings := eval(t, rawQueryable, query.expr, at)
						dsValue, dsWarnings := eval(t, dsQueryable, query.expr, at)

						for _, side := range []struct {
							name     string
							value    promql.Vector
							warnings annotations.Annotations
							kinds    []bool
						}{
							{"raw", rawValue, rawWarnings, kcKindsInRange(rawSamples, at-queryWindow, at)},
							{"downsampled", dsValue, dsWarnings, kcKindsInRange(dsSamples, at-queryWindow, at)},
						} {
							if query.class == kcClassResets {
								testutil.Assert(t, len(side.value) == 1 && side.value[0].F == float64(kcKindChanges(side.kinds)),
									"minute %d, %s: expected %d resets, got %v", minute, side.name, kcKindChanges(side.kinds), side.value)
								continue
							}
							if !kcExpectSample(query.class, side.kinds) {
								testutil.Assert(t, len(side.value) == 0 && len(side.warnings) > 0,
									"minute %d, %s: expected no sample and a warning, got %v with %d warnings", minute, side.name, side.value, len(side.warnings))
								if side.name == "downsampled" {
									noSample++
								}
								continue
							}
							testutil.Assert(t, len(side.value) == 1, "minute %d, %s: expected one sample, got %v", minute, side.name, side.value)
							if h := side.value[0].H; h != nil {
								testutil.Assert(t, !hasNegativeDelta(h), "minute %d, %s: negative histogram %v", minute, side.name, h)
							} else {
								testutil.Assert(t, side.value[0].F >= 0, "minute %d, %s: negative value %g", minute, side.name, side.value[0].F)
							}
						}

						if !steady(at) || (query.wholeWindows && (at+1)%ResLevel1 != 0) {
							continue
						}
						compared++
						testutil.Assert(t, len(rawValue) == 1 && len(dsValue) == 1, "minute %d: raw=%v downsampled=%v", minute, rawValue, dsValue)
						testutil.Assert(t, kcSamplesApprox(rawValue[0], dsValue[0]), "minute %d: raw=%v downsampled=%v", minute, rawValue, dsValue)
					}
					testutil.Assert(t, compared > 0, "no evaluation compared with raw data")
					if query.class == kcClassRate || query.class == kcClassSum {
						testutil.Assert(t, noSample > 0, "no evaluation over both kinds")
					}
					t.Logf("%s: %d evaluations equal to raw, %d over aggregates of both kinds without a sample", query.expr, compared, noSample)
				})
			}
		})
	}
}

// kcQueryClass is how a PromQL function treats a range in which the
// histogram kind changes.
type kcQueryClass int

const (
	// kcClassRate is rate and increase. A counter reset between the first
	// two samples of the range makes them ignore the first one, and a change
	// of kind is a counter reset. Any other change of kind in the range gives
	// no sample and a warning.
	kcClassRate kcQueryClass = iota
	// kcClassIRate looks at the last two samples only. A change of kind
	// between them is a counter reset, so it always has a result.
	kcClassIRate
	// kcClassResets counts counter resets, and with them changes of kind.
	kcClassResets
	// kcClassSum adds up all samples of the range, and cannot if their kinds
	// differ: no sample and a warning.
	kcClassSum
)

// kcKindAt is the bucket kind of a sample: custom or exponential.
type kcKindAt struct {
	t      int64
	custom bool
}

// kcKindsInRange returns the kinds of the samples in the range (from, to].
func kcKindsInRange(samples []kcKindAt, from, to int64) []bool {
	var kinds []bool
	for _, s := range samples {
		if s.t > from && s.t <= to {
			kinds = append(kinds, s.custom)
		}
	}
	return kinds
}

func kcKindChanges(kinds []bool) int {
	changes := 0
	for i := 1; i < len(kinds); i++ {
		if kinds[i] != kinds[i-1] {
			changes++
		}
	}
	return changes
}

// kcExpectSample tells whether a query of the given class returns a sample
// for a range holding samples of the given kinds.
func kcExpectSample(class kcQueryClass, kinds []bool) bool {
	switch class {
	case kcClassRate:
		if len(kinds) < 2 {
			return false
		}
		if kinds[0] != kinds[1] {
			kinds = kinds[1:]
		}
		return kcKindChanges(kinds) == 0
	case kcClassIRate:
		return len(kinds) >= 2
	case kcClassSum:
		return len(kinds) > 0 && kcKindChanges(kinds) == 0
	default:
		return len(kinds) > 0
	}
}

// kcSamplesApprox compares two query results up to rounding: floats by
// value, histograms by kind, layout, counts, sum and every bucket.
func kcSamplesApprox(a, b promql.Sample) bool {
	if a.H == nil || b.H == nil {
		return a.H == nil && b.H == nil && kcApprox(a.F, b.F)
	}
	if a.H.Schema != b.H.Schema || !histogram.FloatBucketsMatch(a.H.CustomValues, b.H.CustomValues) ||
		!slices.Equal(a.H.PositiveSpans, b.H.PositiveSpans) || !slices.Equal(a.H.NegativeSpans, b.H.NegativeSpans) ||
		len(a.H.PositiveBuckets) != len(b.H.PositiveBuckets) || len(a.H.NegativeBuckets) != len(b.H.NegativeBuckets) {
		return false
	}
	if !kcApprox(a.H.Count, b.H.Count) || !kcApprox(a.H.Sum, b.H.Sum) || !kcApprox(a.H.ZeroCount, b.H.ZeroCount) {
		return false
	}
	for i := range a.H.PositiveBuckets {
		if !kcApprox(a.H.PositiveBuckets[i], b.H.PositiveBuckets[i]) {
			return false
		}
	}
	for i := range a.H.NegativeBuckets {
		if !kcApprox(a.H.NegativeBuckets[i], b.H.NegativeBuckets[i]) {
			return false
		}
	}
	return true
}

// differentialNHCBFixture produces counter histograms. Bucket counters grow
// monotonically within a layout. At a layout transition their absolute values
// deliberately continue growing; raw PromQL, rather than the fixture builder,
// decides whether that transition is a reset.
func differentialNHCBFixture(start, step int64, n int, layout func(int) []float64) []sample {
	out := make([]sample, 0, n)
	for i := 0; i < n; i++ {
		bounds := append([]float64(nil), layout(i)...)
		buckets := make([]float64, len(bounds)+1)
		count := 0.0
		for j := range buckets {
			buckets[j] = float64((i + 1) * (j + 1))
			count += buckets[j]
		}
		out = append(out, sample{t: start + int64(i)*step, fh: &histogram.FloatHistogram{
			Schema: histogram.CustomBucketsSchema, CustomValues: bounds,
			Count: count, Sum: count * 0.75,
			CounterResetHint: histogram.UnknownCounterReset,
			PositiveSpans:    []histogram.Span{{Offset: 0, Length: uint32(len(buckets))}},
			PositiveBuckets:  buckets,
		}})
	}
	return out
}

type differentialSample struct {
	t  int64
	fh *histogram.FloatHistogram
}

func (s differentialSample) T() int64                      { return s.t }
func (s differentialSample) F() float64                    { return 0 }
func (s differentialSample) H() *histogram.Histogram       { return nil }
func (s differentialSample) FH() *histogram.FloatHistogram { return s.fh }
func (s differentialSample) Type() chunkenc.ValueType      { return chunkenc.ValFloatHistogram }
func (s differentialSample) Copy() chunks.Sample {
	return differentialSample{t: s.t, fh: s.fh.Copy()}
}

type differentialSeriesSet struct {
	series storage.Series
	used   bool
}

func (s *differentialSeriesSet) Next() bool {
	if s.used {
		return false
	}
	s.used = true
	return true
}
func (s *differentialSeriesSet) At() storage.Series              { return s.series }
func (*differentialSeriesSet) Err() error                        { return nil }
func (*differentialSeriesSet) Warnings() annotations.Annotations { return nil }

type differentialQuerier struct {
	raw         []sample
	downsampled []chunks.Meta
}

func newDifferentialQueryable(raw []sample, downsampled []chunks.Meta) storage.Queryable {
	return storage.QueryableFunc(func(int64, int64) (storage.Querier, error) {
		return &differentialQuerier{raw: raw, downsampled: downsampled}, nil
	})
}

func (q *differentialQuerier) Select(_ context.Context, _ bool, hints *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
	var samples []chunks.Sample
	if q.raw != nil {
		for _, s := range q.raw {
			samples = append(samples, differentialSample{t: s.t, fh: s.fh})
		}
	} else {
		// This mirrors the aggregate choice made by Thanos' query reader:
		// counter for counter range functions, sum for ordinary instant reads.
		aggr := AggrSum
		if hints != nil && (hints.Func == "rate" || hints.Func == "increase" || hints.Func == "irate" || hints.Func == "resets") {
			aggr = AggrCounter
		}
		var iterators []chunkenc.Iterator
		for _, meta := range q.downsampled {
			ac, ok := meta.Chunk.(*AggrChunk)
			if !ok {
				return storage.ErrSeriesSet(errors.Errorf("downsample raw returned %T, expected *AggrChunk", meta.Chunk))
			}
			chunk, err := ac.Get(aggr)
			if err != nil {
				return storage.ErrSeriesSet(err)
			}
			iterators = append(iterators, chunk.Iterator(nil))
		}
		var expanded []sample
		if aggr == AggrCounter {
			if err := expandChunkIterator(NewApplyCounterResetsIterator(iterators...), chunkenc.EncFloatHistogram, &expanded); err != nil {
				return storage.ErrSeriesSet(err)
			}
		} else {
			for _, it := range iterators {
				if err := expandChunkIterator(it, chunkenc.EncFloatHistogram, &expanded); err != nil {
					return storage.ErrSeriesSet(err)
				}
			}
		}
		for _, s := range expanded {
			samples = append(samples, differentialSample{t: s.t, fh: s.fh})
		}
	}
	lset := labels.FromStrings(labels.MetricName, "nhcb_differential", "source", "fixture")
	return &differentialSeriesSet{series: storage.NewListSeries(lset, samples)}
}

func (*differentialQuerier) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (*differentialQuerier) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (*differentialQuerier) Close() error { return nil }

func differentialEval(t *testing.T, engine *promql.Engine, queryable storage.Queryable, expr string, at time.Time) parser.Value {
	t.Helper()
	q, err := engine.NewInstantQuery(context.Background(), queryable, promql.NewPrometheusQueryOpts(false, 0), expr, at)
	if err != nil {
		t.Fatalf("create query %q: %v", expr, err)
	}
	defer q.Close()
	result := q.Exec(context.Background())
	if result.Err != nil {
		t.Fatalf("execute query %q at %s: %v", expr, at, result.Err)
	}
	return result.Value
}

// Histogram-valued rate/increase results are summarized by Count. The scalar
// histogram_count query validates that projection independently, while the
// quantile query is sensitive to bucket-shape divergence. Empty on both sides
// is equal; an empty/non-empty mismatch is +Inf.
func differentialValueDistance(raw, downsampled parser.Value) float64 {
	rv, rok := raw.(promql.Vector)
	dv, dok := downsampled.(promql.Vector)
	if !rok || !dok {
		return math.Inf(1)
	}
	if len(rv) == 0 && len(dv) == 0 {
		return 0
	}
	if len(rv) != 1 || len(dv) != 1 {
		return math.Inf(1)
	}
	value := func(s promql.Sample) float64 {
		if s.H != nil {
			return s.H.Count
		}
		return s.F
	}
	r, d := value(rv[0]), value(dv[0])
	if math.IsNaN(r) && math.IsNaN(d) {
		return 0
	}
	if math.IsNaN(r) || math.IsNaN(d) {
		return math.Inf(1)
	}
	return math.Abs(d-r) / math.Max(1, math.Abs(r))
}
