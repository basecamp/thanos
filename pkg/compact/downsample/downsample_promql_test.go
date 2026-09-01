// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package downsample

import (
	"context"
	"math"
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
