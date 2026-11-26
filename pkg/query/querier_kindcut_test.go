// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"context"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/dedup"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// The tests in this file read series that switch between exponential and
// custom buckets histograms the way production reads them: blocks in an
// object storage bucket, a store gateway (BucketStore) on top, and the
// querier with the Prometheus engine on top of that. The bucket holds the
// raw block, the 5m block and the 1h block of two replicas.

const (
	// kcStart is aligned to a 1h window.
	kcStart = int64(1688526000000)
	kcStep  = int64(60_000)
	// kcSamples is 20 hours at one sample per minute. A kind change in the
	// middle leaves each kind in one 5m aggregate chunk, so that only the
	// change itself breaks the regular spacing of aggregate samples.
	kcSamples = 1200
	// kcCut is the index of the first sample of the new kind: the third
	// sample of a 5m window, 17 minutes into an hour.
	kcCut = 617
	// kcReplicaOffset is how much later replica b scrapes.
	kcReplicaOffset = int64(20_000)
)

// kcCase is one series of the fixture, per replica.
type kcCase struct {
	name string
	// custom tells the kind of sample i.
	custom func(replica string, i int) bool
	// skip tells whether sample i is missing.
	skip func(replica string, i int) bool
	// cut is the index of the first sample of the second kind in replica a,
	// or zero for a series that does not change kind.
	cut int
	// chunkEnd is the index of the last sample of a 5m aggregate chunk that
	// ends for its size, or zero if the series has none.
	chunkEnd int
}

// kcCutOf returns where a replica changes kind: replica b one scrape later.
func kcCutOf(replica string, cut int) int {
	if replica == "b" {
		return cut + 1
	}
	return cut
}

var kcCases = []kcCase{
	{
		name:   "exp_to_custom",
		custom: func(r string, i int) bool { return i >= kcCutOf(r, kcCut) },
		cut:    kcCut,
	},
	{
		name:   "custom_to_exp",
		custom: func(r string, i int) bool { return i < kcCutOf(r, kcCut) },
		cut:    kcCut,
	},
	{
		// The change falls on the boundary of a 1h window.
		name:   "on_boundary",
		custom: func(r string, i int) bool { return i < kcCutOf(r, 600) },
		cut:    600,
	},
	{
		// Replica a has a scrape gap of three hours around its change of
		// kind, so that deduplication has to take the change from replica b,
		// at 5m and at 1h.
		name:   "gap_at_cut",
		custom: func(r string, i int) bool { return i >= kcCutOf(r, kcCut) },
		skip:   func(r string, i int) bool { return r == "a" && i >= kcCut-90 && i < kcCut+90 },
		cut:    kcCut,
	},
	{
		// 240 windows do not fit one 5m aggregate chunk. The first chunk
		// ends with sample 604.
		name:     "stable",
		custom:   func(string, int) bool { return true },
		chunkEnd: 604,
	},
}

// kcHistogram returns an integer histogram, as a scrape stores it, in which
// every count is a multiple of n.
func kcHistogram(custom bool, n int64) *histogram.Histogram {
	if custom {
		return &histogram.Histogram{
			Schema: histogram.CustomBucketsSchema, CustomValues: []float64{0.1, 0.5, 1, 2, 5},
			Count: uint64(21 * n), Sum: 15.75 * float64(n),
			PositiveSpans:   []histogram.Span{{Offset: 0, Length: 6}},
			PositiveBuckets: []int64{n, n, n, n, n, n},
		}
	}
	return &histogram.Histogram{
		Schema: 3, ZeroThreshold: 0.001, ZeroCount: uint64(n),
		Count: uint64(7 * n), Sum: 5.25 * float64(n),
		PositiveSpans:   []histogram.Span{{Offset: 0, Length: 3}},
		PositiveBuckets: []int64{n, n, n},
	}
}

type kcTSDBSample struct {
	t int64
	h *histogram.Histogram
}

func (s kcTSDBSample) T() int64                      { return s.t }
func (s kcTSDBSample) F() float64                    { return 0 }
func (s kcTSDBSample) H() *histogram.Histogram       { return s.h }
func (s kcTSDBSample) FH() *histogram.FloatHistogram { return nil }
func (s kcTSDBSample) Type() chunkenc.ValueType      { return chunkenc.ValHistogram }
func (s kcTSDBSample) Copy() chunks.Sample           { return s }

// kcPoint is one sample of an aggregate of a series in a block.
type kcPoint struct {
	t  int64
	v  float64
	fh *histogram.FloatHistogram
}

// kcBlockChunk is one aggregate chunk of a series in a block.
type kcBlockChunk struct {
	mint, maxt int64
	chunk      *downsample.AggrChunk
}

// kcFixture is the bucket with its blocks, and the store gateway on it.
type kcFixture struct {
	store *store.BucketStore
	// chunks holds the aggregate chunks as they are in the blocks, by
	// resolution, replica and case.
	chunks map[int64]map[string]map[string][]kcBlockChunk
}

func (f *kcFixture) points(t *testing.T, resolution int64, replica, name string, at downsample.AggrType) []kcPoint {
	t.Helper()
	var out []kcPoint
	for _, c := range f.chunks[resolution][replica][name] {
		chk, err := c.chunk.Get(at)
		testutil.Ok(t, err)
		it := chk.Iterator(nil)
		for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
			if vt == chunkenc.ValFloat {
				ts, v := it.At()
				out = append(out, kcPoint{t: ts, v: v})
				continue
			}
			ts, fh := it.AtFloatHistogram(nil)
			out = append(out, kcPoint{t: ts, fh: fh})
		}
		testutil.Ok(t, it.Err())
	}
	return out
}

func (f *kcFixture) queryable(maxResolution int64, deduplicate bool) storage.Queryable {
	return NewQueryableCreator(nil, nil, newProxyStore(f.store), 2, time.Minute, dedup.AlgorithmPenalty)(
		deduplicate,
		[]string{"replica"},
		nil,
		maxResolution,
		false,
		false,
		nil,
		NoopSeriesStatsReporter,
	)
}

func newKindCutFixture(t *testing.T) *kcFixture {
	t.Helper()
	var (
		ctx    = context.Background()
		logger = log.NewNopLogger()
		bktDir = t.TempDir()
		f      = &kcFixture{chunks: map[int64]map[string]map[string][]kcBlockChunk{
			downsample.ResLevel1: {},
			downsample.ResLevel2: {},
		}}
	)

	// downsampleBlock downsamples a block of the bucket into the bucket,
	// runs the index check the compactor runs before an upload, and records
	// the chunks of the new block.
	downsampleBlock := func(replica string, id ulid.ULID, resolution int64) ulid.ULID {
		bdir := filepath.Join(bktDir, id.String())
		meta, err := metadata.ReadFromDir(bdir)
		testutil.Ok(t, err)
		blk, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(logger), bdir, downsample.NewPool(), tsdb.DefaultPostingsDecoderFactory)
		testutil.Ok(t, err)
		out, err := downsample.Downsample(ctx, logger, meta, blk, bktDir, resolution)
		testutil.Ok(t, err)
		testutil.Ok(t, blk.Close())

		outDir := filepath.Join(bktDir, out.String())
		stats, err := block.GatherIndexHealthStats(ctx, logger, filepath.Join(outDir, block.IndexFilename), meta.MinTime, meta.MaxTime)
		testutil.Ok(t, err)
		testutil.Ok(t, stats.AnyErr())

		outBlk, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(logger), outDir, downsample.NewPool(), tsdb.DefaultPostingsDecoderFactory)
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, outBlk.Close()) }()
		indexr, err := outBlk.Index()
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, indexr.Close()) }()
		chunkr, err := outBlk.Chunks()
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, chunkr.Close()) }()

		byCase := map[string][]kcBlockChunk{}
		k, v := index.AllPostingsKey()
		p, err := indexr.Postings(ctx, k, v)
		testutil.Ok(t, err)
		for p.Next() {
			var (
				builder labels.ScratchBuilder
				metas   []chunks.Meta
			)
			testutil.Ok(t, indexr.Series(p.At(), &builder, &metas))
			name := builder.Labels().Get("case")
			for _, m := range metas {
				chk, _, err := chunkr.ChunkOrIterable(m)
				testutil.Ok(t, err)
				ac, ok := chk.(*downsample.AggrChunk)
				testutil.Assert(t, ok, "expected an aggregate chunk, got %T", chk)
				// The reader's memory goes away with the reader.
				cp := downsample.AggrChunk(append([]byte(nil), ac.Bytes()...))
				byCase[name] = append(byCase[name], kcBlockChunk{mint: m.MinTime, maxt: m.MaxTime, chunk: &cp})
			}
		}
		testutil.Ok(t, p.Err())
		f.chunks[resolution][replica] = byCase
		return out
	}

	for _, replica := range []string{"a", "b"} {
		var input []storage.Series
		for _, c := range kcCases {
			var samples []chunks.Sample
			for i := 0; i < kcSamples; i++ {
				if c.skip != nil && c.skip(replica, i) {
					continue
				}
				ts := kcStart + int64(i)*kcStep
				if replica == "b" {
					ts += kcReplicaOffset
				}
				samples = append(samples, kcTSDBSample{t: ts, h: kcHistogram(c.custom(replica, i), int64(i+1))})
			}
			input = append(input, storage.NewListSeries(labels.FromStrings(labels.MetricName, "kc", "case", c.name), samples))
		}
		// The head takes one series after the other, so its range must span
		// the whole fixture.
		rawDir, err := tsdb.CreateBlock(input, bktDir, int64(48*time.Hour/time.Millisecond), logutil.GoKitLogToSlog(logger))
		testutil.Ok(t, err)
		_, err = metadata.InjectThanos(logger, rawDir, metadata.Thanos{
			Labels:     map[string]string{"replica": replica},
			Downsample: metadata.ThanosDownsample{Resolution: downsample.ResLevel0},
			Source:     metadata.TestSource,
		}, nil)
		testutil.Ok(t, err)

		id5m := downsampleBlock(replica, ulid.MustParse(filepath.Base(rawDir)), downsample.ResLevel1)
		downsampleBlock(replica, id5m, downsample.ResLevel2)
	}

	bkt, err := filesystem.NewBucket(bktDir)
	testutil.Ok(t, err)
	t.Cleanup(func() { testutil.Ok(t, bkt.Close()) })
	insBkt := objstore.WithNoopInstr(bkt)
	metaFetcher, err := block.NewMetaFetcher(logger, 20, insBkt, block.NewConcurrentLister(logger, insBkt), t.TempDir(), nil, nil)
	testutil.Ok(t, err)

	f.store, err = store.NewBucketStore(
		insBkt,
		metaFetcher,
		t.TempDir(),
		store.NewChunksLimiterFactory(0),
		store.NewSeriesLimiterFactory(0),
		store.NewBytesLimiterFactory(0),
		store.NewGapBasedPartitioner(store.PartitionerMaxGapSize),
		20,
		true,
		store.DefaultPostingOffsetInMemorySampling,
		false,
		false,
		time.Minute,
	)
	testutil.Ok(t, err)
	t.Cleanup(func() { testutil.Ok(t, f.store.Close()) })
	testutil.Ok(t, f.store.SyncBlocks(ctx))
	return f
}

func kcMatchers(name, replica string) []*labels.Matcher {
	ms := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "kc"),
		labels.MustNewMatcher(labels.MatchEqual, "case", name),
	}
	if replica != "" {
		ms = append(ms, labels.MustNewMatcher(labels.MatchEqual, "replica", replica))
	}
	return ms
}

// kcSelect reads one series through the querier, the way the engine does
// for a selector inside the given function.
func kcSelect(t *testing.T, q storage.Queryable, fn string, ms ...*labels.Matcher) storage.Series {
	t.Helper()
	querier, err := q.Querier(math.MinInt64, math.MaxInt64)
	testutil.Ok(t, err)
	t.Cleanup(func() { testutil.Ok(t, querier.Close()) })

	set := querier.Select(context.Background(), true, &storage.SelectHints{
		Start: kcStart - 1,
		End:   kcStart + kcSamples*kcStep,
		Func:  fn,
	}, ms...)
	var out []storage.Series
	for set.Next() {
		out = append(out, set.At())
	}
	testutil.Ok(t, set.Err())
	testutil.Equals(t, 1, len(out), "series for %v", ms)
	return out[0]
}

func kcExpand(t *testing.T, it chunkenc.Iterator) []kcPoint {
	t.Helper()
	var out []kcPoint
	for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
		if vt == chunkenc.ValFloat {
			ts, v := it.At()
			out = append(out, kcPoint{t: ts, v: v})
			continue
		}
		ts, fh := it.AtFloatHistogram(nil)
		testutil.Equals(t, ts, it.AtT())
		out = append(out, kcPoint{t: ts, fh: fh})
	}
	testutil.Ok(t, it.Err())
	return out
}

func kcAssertPoints(t *testing.T, want, got []kcPoint) {
	t.Helper()
	testutil.Equals(t, len(want), len(got), "number of samples")
	for i := range want {
		testutil.Equals(t, want[i].t, got[i].t, "sample %d: timestamp", i)
		if want[i].fh == nil {
			testutil.Assert(t, got[i].fh == nil, "sample %d: expected a float", i)
			testutil.Equals(t, want[i].v, got[i].v, "sample %d", i)
			continue
		}
		testutil.Assert(t, got[i].fh != nil, "sample %d: expected a histogram", i)
		testutil.Assert(t, want[i].fh.Equals(got[i].fh), "sample %d at %d: histogram changed: want %v, got %v", i, want[i].t, want[i].fh, got[i].fh)
		testutil.Equals(t, want[i].fh.CounterResetHint, got[i].fh.CounterResetHint, "sample %d at %d: counter reset hint", i, want[i].t)
	}
}

// kcKinds returns the bucket kinds of histogram samples, repeats collapsed.
func kcKinds(points []kcPoint) []bool {
	var out []bool
	for _, p := range points {
		if c := p.fh.UsesCustomBuckets(); len(out) == 0 || out[len(out)-1] != c {
			out = append(out, c)
		}
	}
	return out
}

// TestBucketStore_KindCutSeries verifies that the store gateway returns
// every aggregate chunk of a series whose chunks switch histogram kind, in
// order and byte for byte, for every set of aggregates a querier asks for.
func TestBucketStore_KindCutSeries(t *testing.T) {
	f := newKindCutFixture(t)
	client := storepb.ServerAsClient(f.store)

	for _, resolution := range []int64{downsample.ResLevel1, downsample.ResLevel2} {
		for _, aggrs := range [][]storepb.Aggr{
			{storepb.Aggr_COUNT},
			{storepb.Aggr_SUM},
			{storepb.Aggr_COUNTER},
			{storepb.Aggr_COUNT, storepb.Aggr_SUM},
		} {
			t.Run(fmt.Sprintf("resolution %d, aggregates %v", resolution, aggrs), func(t *testing.T) {
				stream, err := client.Series(context.Background(), &storepb.SeriesRequest{
					MinTime:             math.MinInt64,
					MaxTime:             math.MaxInt64,
					Matchers:            []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: labels.MetricName, Value: "kc"}},
					MaxResolutionWindow: resolution,
					Aggregates:          aggrs,
				})
				testutil.Ok(t, err)

				seen := 0
				for {
					resp, err := stream.Recv()
					if err == io.EOF {
						break
					}
					testutil.Ok(t, err)
					testutil.Equals(t, "", resp.GetWarning())
					series := resp.GetSeries()
					if series == nil {
						continue
					}
					seen++
					lset := series.PromLabels()
					want := f.chunks[resolution][lset.Get("replica")][lset.Get("case")]
					testutil.Assert(t, len(want) > 0, "unexpected series %s", lset)
					testutil.Equals(t, len(want), len(series.Chunks), "%s: number of chunks", lset)

					for i, got := range series.Chunks {
						testutil.Equals(t, want[i].mint, got.MinTime, "%s chunk %d: MinTime", lset, i)
						testutil.Equals(t, want[i].maxt, got.MaxTime, "%s chunk %d: MaxTime", lset, i)
						testutil.Assert(t, got.Raw == nil && got.Min == nil && got.Max == nil, "%s chunk %d: unexpected raw, min or max chunk", lset, i)

						requested := map[storepb.Aggr]bool{}
						for _, a := range aggrs {
							requested[a] = true
						}
						for _, sub := range []struct {
							aggr storepb.Aggr
							at   downsample.AggrType
							got  *storepb.Chunk
							enc  storepb.Chunk_Encoding
						}{
							{storepb.Aggr_COUNT, downsample.AggrCount, got.Count, storepb.Chunk_XOR},
							{storepb.Aggr_SUM, downsample.AggrSum, got.Sum, storepb.Chunk_FLOAT_HISTOGRAM},
							{storepb.Aggr_COUNTER, downsample.AggrCounter, got.Counter, storepb.Chunk_FLOAT_HISTOGRAM},
						} {
							if !requested[sub.aggr] {
								testutil.Assert(t, sub.got == nil, "%s chunk %d: %s was not requested", lset, i, sub.aggr)
								continue
							}
							testutil.Assert(t, sub.got != nil, "%s chunk %d: %s is missing", lset, i, sub.aggr)
							chk, err := want[i].chunk.Get(sub.at)
							testutil.Ok(t, err)
							testutil.Equals(t, sub.enc, sub.got.Type, "%s chunk %d: %s encoding", lset, i, sub.aggr)
							testutil.Equals(t, chk.Bytes(), sub.got.Data, "%s chunk %d: %s bytes", lset, i, sub.aggr)
						}
					}
				}
				testutil.Equals(t, 2*len(kcCases), seen, "number of series")
			})
		}
	}
}

// TestQuerier_KindCutSeries verifies what the querier's iterators make of a
// series whose aggregate chunks switch histogram kind: every sample comes
// out, in order and unchanged, for every aggregate the querier picks, and
// seeking across the switch lands on the right sample.
func TestQuerier_KindCutSeries(t *testing.T) {
	f := newKindCutFixture(t)

	for _, resolution := range []int64{downsample.ResLevel1, downsample.ResLevel2} {
		q := f.queryable(resolution, false)
		for _, c := range kcCases {
			t.Run(fmt.Sprintf("resolution %d, %s", resolution, c.name), func(t *testing.T) {
				counters := f.points(t, resolution, "a", c.name, downsample.AggrCounter)
				sums := f.points(t, resolution, "a", c.name, downsample.AggrSum)
				counts := f.points(t, resolution, "a", c.name, downsample.AggrCount)
				if c.cut > 0 {
					testutil.Equals(t, 2, len(kcKinds(counters)), "the fixture changes kind once")
				}

				// Counter functions read the counter aggregate through the
				// iterator that stitches counter chunks together.
				for _, fn := range []string{"rate", "increase", "irate", "resets"} {
					kcAssertPoints(t, counters, kcExpand(t, kcSelect(t, q, fn, kcMatchers(c.name, "a")...).Iterator(nil)))
				}
				kcAssertPoints(t, sums, kcExpand(t, kcSelect(t, q, "sum_over_time", kcMatchers(c.name, "a")...).Iterator(nil)))
				kcAssertPoints(t, counts, kcExpand(t, kcSelect(t, q, "count_over_time", kcMatchers(c.name, "a")...).Iterator(nil)))

				// Without a function the querier divides sum by count, which
				// needs the two aggregates of every chunk to line up.
				//
				// Only the count and the sum of the result are compared.
				// AverageChunkIterator divides the histogram it gets from the
				// chunk iterator in place, and with it the bucket values the
				// chunk iterator decodes the next sample from, so the buckets
				// of every sample but the first of a chunk come out wrong.
				// That is so for any downsampled histogram series, with or
				// without a change of kind, and is not what this test is about.
				avg := kcExpand(t, kcSelect(t, q, "", kcMatchers(c.name, "a")...).Iterator(nil))
				testutil.Equals(t, len(sums), len(avg))
				for i := range avg {
					testutil.Equals(t, sums[i].t, avg[i].t)
					testutil.Equals(t, sums[i].fh.UsesCustomBuckets(), avg[i].fh.UsesCustomBuckets(), "sample %d: bucket kind", i)
					testutil.Equals(t, sums[i].fh.Count/counts[i].v, avg[i].fh.Count, "sample %d: count", i)
					testutil.Equals(t, sums[i].fh.Sum/counts[i].v, avg[i].fh.Sum, "sample %d: sum", i)
				}

				// Seek to just before, at and just after every counter sample
				// around the change of kind, from a fresh iterator and from
				// one that is already under way.
				for i := 1; i < len(counters); i++ {
					if counters[i].fh.UsesCustomBuckets() == counters[i-1].fh.UsesCustomBuckets() {
						continue
					}
					for _, idx := range []int{i - 1, i} {
						for _, delta := range []int64{-1, 0, 1} {
							x := counters[idx].t + delta
							first := sort.Search(len(counters), func(j int) bool { return counters[j].t >= x })
							for _, warm := range []bool{false, true} {
								it := kcSelect(t, q, "rate", kcMatchers(c.name, "a")...).Iterator(nil)
								if warm {
									testutil.Equals(t, chunkenc.ValFloatHistogram, it.Seek(counters[idx-2].t))
								}
								testutil.Equals(t, chunkenc.ValFloatHistogram, it.Seek(x))
								kcAssertPoints(t, counters[first:], append([]kcPoint{func() kcPoint {
									ts, fh := it.AtFloatHistogram(nil)
									return kcPoint{t: ts, fh: fh}
								}()}, kcExpand(t, it)...))
							}
						}
					}
				}
			})
		}
	}
}

// TestQuerier_KindCutDedup verifies replica deduplication over series whose
// replicas change histogram kind at slightly different times: it must not
// fail, every sample it returns must be a sample of one of the replicas,
// unchanged, and the change of kind must still be there. In one case the
// replica deduplication starts with has a scrape gap around the change, so
// the change has to come from the other replica.
func TestQuerier_KindCutDedup(t *testing.T) {
	f := newKindCutFixture(t)

	for _, resolution := range []int64{downsample.ResLevel1, downsample.ResLevel2} {
		q := f.queryable(resolution, true)
		for _, c := range kcCases {
			for _, sel := range []struct {
				fn string
				at downsample.AggrType
			}{
				{"rate", downsample.AggrCounter},
				{"resets", downsample.AggrCounter},
				{"sum_over_time", downsample.AggrSum},
			} {
				t.Run(fmt.Sprintf("resolution %d, %s, %s", resolution, c.name, sel.fn), func(t *testing.T) {
					replicas := map[string]map[int64]*histogram.FloatHistogram{"a": {}, "b": {}}
					for r, byT := range replicas {
						for _, p := range f.points(t, resolution, r, c.name, sel.at) {
							byT[p.t] = p.fh
						}
					}

					got := kcExpand(t, kcSelect(t, q, sel.fn, kcMatchers(c.name, "")...).Iterator(nil))
					testutil.Assert(t, len(got) > 0, "no samples")
					fromB := 0
					for i, p := range got {
						if i > 0 {
							testutil.Assert(t, p.t > got[i-1].t, "sample %d: timestamp %d does not increase", i, p.t)
						}
						a, b := replicas["a"][p.t], replicas["b"][p.t]
						testutil.Assert(t, (a != nil && a.Equals(p.fh)) || (b != nil && b.Equals(p.fh)), "sample %d at %d is not a sample of either replica", i, p.t)
						if a == nil || !a.Equals(p.fh) {
							fromB++
						}
					}
					if c.cut > 0 {
						testutil.Equals(t, 2, len(kcKinds(got)), "kinds after deduplication")
					}
					if c.name == "gap_at_cut" {
						testutil.Assert(t, fromB > 0, "deduplication did not switch to replica b")
					}
				})
			}
		}
	}
}

// TestQuerier_KindCutPromQLDifferential evaluates PromQL through the
// querier over the raw blocks and over the 5m and 1h blocks of series that
// switch histogram kind.
//
// PromQL does not compute across a change of kind: it treats the change as
// a counter reset, and it gives no sample and a warning for a range it
// cannot evaluate because of it. That is so for raw data and has to be so
// for aggregates. Two things are checked at every evaluation time:
//
//   - On each side, whether the query returns a sample is exactly what
//     PromQL's rules say for the kinds of the samples in the range (see
//     kcExpectSample), and resets counts exactly the changes of kind.
//   - Steady state: unless the range holds the last sample before the
//     change of kind, the aggregates give what raw data gives, bucket by
//     bucket. In between, the two sides cannot agree number for number,
//     because an aggregate sample stands for a whole window.
func TestQuerier_KindCutPromQLDifferential(t *testing.T) {
	f := newKindCutFixture(t)
	engine := promql.NewEngine(promql.EngineOpts{
		MaxSamples:    50_000_000,
		Timeout:       time.Minute,
		LookbackDelta: 5 * time.Minute,
	})
	eval := func(t *testing.T, q storage.Queryable, expr string, at int64) (promql.Vector, annotations.Annotations) {
		t.Helper()
		qry, err := engine.NewInstantQuery(context.Background(), q, promql.NewPrometheusQueryOpts(false, 0), expr, time.UnixMilli(at))
		testutil.Ok(t, err)
		defer qry.Close()
		res := qry.Exec(context.Background())
		testutil.Ok(t, res.Err)
		v, ok := res.Value.(promql.Vector)
		testutil.Assert(t, ok, "expected a vector, got %T", res.Value)
		return v, res.Warnings
	}
	negative := func(s promql.Sample) bool {
		if s.H == nil {
			return s.F < 0
		}
		if s.H.Count < 0 || s.H.ZeroCount < 0 {
			return true
		}
		for _, b := range append(append([]float64(nil), s.H.PositiveBuckets...), s.H.NegativeBuckets...) {
			if b < 0 {
				return true
			}
		}
		return false
	}

	raw := f.queryable(downsample.ResLevel0, false)
	for _, level := range []struct {
		name       string
		resolution int64
		rng        string
		rangeMs    int64
		// every is the distance between evaluation times away from the cut.
		every int
	}{
		{name: "5m", resolution: downsample.ResLevel1, rng: "20m", rangeMs: 20 * kcStep, every: 7},
		{name: "1h", resolution: downsample.ResLevel2, rng: "4h", rangeMs: 240 * kcStep, every: 5},
	} {
		ds := f.queryable(level.resolution, false)
		windowMinutes := int(level.resolution / kcStep)

		for _, c := range kcCases {
			if c.skip != nil {
				continue
			}
			sel := fmt.Sprintf(`kc{case=%q,replica="a"}`, c.name)
			type query struct {
				name string
				// expr is evaluated over the aggregates, rawExpr over raw data.
				expr, rawExpr string
				class         kcQueryClass
				// wholeWindows marks a query that adds up samples instead of
				// taking a rate. Raw data and aggregates only cover the same
				// samples when the range ends where a window ends.
				wholeWindows bool
			}
			same := func(name, format string, class kcQueryClass, wholeWindows bool) query {
				expr := fmt.Sprintf(format, sel, level.rng)
				return query{name: name, expr: expr, rawExpr: expr, class: class, wholeWindows: wholeWindows}
			}
			queries := []query{
				same("rate", "rate(%s[%s])", kcClassRate, false),
				same("increase", "increase(%s[%s])", kcClassRate, false),
				same("histogram quantile of rate", "histogram_quantile(0.9, rate(%s[%s]))", kcClassRate, false),
				same("irate", "irate(%s[%s])", kcClassIRate, false),
				same("resets", "resets(%s[%s])", kcClassResets, false),
				same("sum over time", "sum_over_time(%s[%s])", kcClassSum, true),
				// A bare selector over aggregates is the average of a window.
				{name: "bare selector", expr: sel, rawExpr: fmt.Sprintf("avg_over_time(%s[%dm])", sel, windowMinutes), class: kcClassBare, wholeWindows: true},
			}

			var ta, tb int64
			if c.cut > 0 {
				ta, tb = kcStart+int64(c.cut-1)*kcStep, kcStart+int64(c.cut)*kcStep
			}
			// An aggregate sample is stamped at the end of its window, except
			// the last one of an aggregate chunk, which is stamped at the last
			// source sample of the chunk. A range holding such a sample is not
			// evenly spaced and extrapolates differently than raw data does;
			// that is how aggregate chunks have always ended and does not
			// depend on a change of kind. A 5m chunk that ends for its size is
			// therefore not steady state either.
			chunkEnd := int64(-1)
			if c.chunkEnd > 0 && level.resolution == downsample.ResLevel1 {
				chunkEnd = kcStart + int64(c.chunkEnd)*kcStep
			}
			steady := func(at int64) bool {
				nearCut := c.cut > 0 && at >= ta && at < tb+level.rangeMs
				nearChunkEnd := chunkEnd >= 0 && at >= chunkEnd && at < chunkEnd+level.rangeMs
				return !nearCut && !nearChunkEnd
			}
			// The kinds of the samples either side has in a range.
			var rawSamples, dsSamples []kcKindAt
			for i := 0; i < kcSamples; i++ {
				rawSamples = append(rawSamples, kcKindAt{kcStart + int64(i)*kcStep, c.custom("a", i)})
			}
			for _, p := range f.points(t, level.resolution, "a", c.name, downsample.AggrCounter) {
				dsSamples = append(dsSamples, kcKindAt{p.t, p.fh.UsesCustomBuckets()})
			}
			// The fixture must be what the above assumes: the only aggregate
			// samples not stamped at a window end are the one before the
			// change of kind, the one ending a full chunk, and the last one.
			var wantEarly, gotEarly []int64
			if c.cut > 0 {
				wantEarly = append(wantEarly, ta)
			}
			if chunkEnd >= 0 {
				wantEarly = append(wantEarly, chunkEnd)
			}
			wantEarly = append(wantEarly, kcStart+int64(kcSamples-1)*kcStep)
			for _, p := range f.points(t, level.resolution, "a", c.name, downsample.AggrCounter) {
				if (p.t+1)%level.resolution != 0 {
					gotEarly = append(gotEarly, p.t)
				}
			}
			testutil.Equals(t, wantEarly, gotEarly, "%s, %s: aggregate samples not stamped at a window end", level.name, c.name)

			for _, qr := range queries {
				t.Run(fmt.Sprintf("%s, %s, %s", level.name, c.name, qr.name), func(t *testing.T) {
					t.Parallel()
					var compared, noSample int
					// Just before a sample, from the first full range to the
					// last sample: every minute around the cut, less often
					// elsewhere, and always at the end of a window.
					for j := int(level.rangeMs / kcStep); j < kcSamples; j++ {
						at := kcStart + int64(j)*kcStep - 1
						windowEnd := j%windowMinutes == 0
						everyMinute := !steady(at) && j <= c.cut+70
						if !everyMinute && !windowEnd && j%level.every != 0 {
							continue
						}
						rawValue, rawWarnings := eval(t, raw, qr.rawExpr, at)
						dsValue, dsWarnings := eval(t, ds, qr.expr, at)

						for _, side := range []struct {
							name     string
							value    promql.Vector
							warnings annotations.Annotations
							kinds    []bool
						}{
							{"raw", rawValue, rawWarnings, kcKindsInRange(rawSamples, at-level.rangeMs, at)},
							{"downsampled", dsValue, dsWarnings, kcKindsInRange(dsSamples, at-level.rangeMs, at)},
						} {
							testutil.Assert(t, len(side.value) <= 1, "minute %d, %s: more than one sample: %v", j, side.name, side.value)
							if len(side.value) == 1 {
								testutil.Assert(t, !negative(side.value[0]), "minute %d, %s: negative value %v", j, side.name, side.value[0])
							}
							switch {
							case qr.class == kcClassBare:
								// What a selector finds within the lookback of an
								// evaluation time is not about a range.
							case qr.class == kcClassResets:
								testutil.Assert(t, len(side.value) == 1 && side.value[0].F == float64(kcKindChanges(side.kinds)),
									"minute %d, %s: expected %d resets, got %v", j, side.name, kcKindChanges(side.kinds), side.value)
							case kcExpectSample(qr.class, side.kinds):
								testutil.Assert(t, len(side.value) == 1, "minute %d, %s: expected one sample, got none", j, side.name)
							default:
								testutil.Assert(t, len(side.value) == 0 && len(side.warnings) > 0,
									"minute %d, %s: expected no sample and a warning, got %v with %d warnings", j, side.name, side.value, len(side.warnings))
								if side.name == "downsampled" {
									noSample++
								}
							}
						}

						if !steady(at) || (qr.wholeWindows && !windowEnd) {
							continue
						}
						compared++
						testutil.Assert(t, len(rawValue) == 1 && len(dsValue) == 1, "minute %d: raw=%v downsampled=%v", j, rawValue, dsValue)
						if qr.class == kcClassBare {
							// Count and sum only: the querier's average iterator
							// gets the buckets wrong, see TestQuerier_KindCutSeries.
							r, d := rawValue[0].H, dsValue[0].H
							testutil.Assert(t, r != nil && d != nil && r.UsesCustomBuckets() == d.UsesCustomBuckets() && kcApprox(r.Count, d.Count) && kcApprox(r.Sum, d.Sum),
								"minute %d: raw=%v downsampled=%v", j, rawValue, dsValue)
							continue
						}
						testutil.Assert(t, kcSamplesApprox(rawValue[0], dsValue[0]), "minute %d: raw=%v downsampled=%v", j, rawValue, dsValue)
					}
					testutil.Assert(t, compared > 0, "no evaluation compared with raw data")
					if c.cut > 0 && (qr.class == kcClassRate || qr.class == kcClassSum) {
						testutil.Assert(t, noSample > 0, "no evaluation over both kinds")
					}
				})
			}
		}
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
	// kcClassBare is a selector without a range.
	kcClassBare
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

func kcApprox(a, b float64) bool {
	return math.Abs(a-b) <= 1e-9*math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
}

// kcSamplesApprox compares two query results up to rounding: floats by
// value, histograms by kind, layout, counts, sum and every bucket.
func kcSamplesApprox(a, b promql.Sample) bool {
	if a.H == nil || b.H == nil {
		return a.H == nil && b.H == nil && kcApprox(a.F, b.F)
	}
	if a.H.Schema != b.H.Schema || !histogram.FloatBucketsMatch(a.H.CustomValues, b.H.CustomValues) ||
		!slices.Equal(a.H.PositiveSpans, b.H.PositiveSpans) || len(a.H.PositiveBuckets) != len(b.H.PositiveBuckets) ||
		len(a.H.NegativeBuckets) != 0 || len(b.H.NegativeBuckets) != 0 {
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
	return true
}

// TestQuerier_KindCutPromQLDedup evaluates PromQL through the querier with
// replica deduplication over series whose replicas change histogram kind at
// slightly different times, for raw data and for aggregates: no query may
// fail, and every answer is nothing plus a warning, or a value that is not
// negative.
func TestQuerier_KindCutPromQLDedup(t *testing.T) {
	f := newKindCutFixture(t)
	engine := promql.NewEngine(promql.EngineOpts{
		MaxSamples:    50_000_000,
		Timeout:       time.Minute,
		LookbackDelta: 5 * time.Minute,
	})

	for _, level := range []struct {
		name       string
		resolution int64
		rng        string
		from, to   int
	}{
		{name: "raw", resolution: downsample.ResLevel0, rng: "20m", from: kcCut - 120, to: kcCut + 150},
		{name: "5m", resolution: downsample.ResLevel1, rng: "20m", from: kcCut - 120, to: kcCut + 150},
		{name: "1h", resolution: downsample.ResLevel2, rng: "4h", from: kcCut - 120, to: kcCut + 450},
	} {
		q := f.queryable(level.resolution, true)
		for _, c := range kcCases {
			for _, format := range []string{
				"rate(%s[%s])", "increase(%s[%s])", "irate(%s[%s])", "resets(%s[%s])", "sum_over_time(%s[%s])",
				"histogram_quantile(0.9, rate(%s[%s]))",
			} {
				expr := fmt.Sprintf(format, fmt.Sprintf("kc{case=%q}", c.name), level.rng)
				t.Run(level.name+", "+expr, func(t *testing.T) {
					t.Parallel()
					answers := 0
					for j := level.from; j <= level.to; j++ {
						// Every minute around the change, less often elsewhere.
						if (j < kcCut-12 || j > kcCut+40) && j%5 != 0 {
							continue
						}
						qry, err := engine.NewInstantQuery(context.Background(), q, promql.NewPrometheusQueryOpts(false, 0), expr, time.UnixMilli(kcStart+int64(j)*kcStep-1))
						testutil.Ok(t, err)
						res := qry.Exec(context.Background())
						testutil.Ok(t, res.Err)
						v, ok := res.Value.(promql.Vector)
						testutil.Assert(t, ok, "expected a vector, got %T", res.Value)
						testutil.Assert(t, len(v) <= 1, "minute %d: more than one sample: %v", j, v)
						if len(v) == 0 {
							testutil.Assert(t, len(res.Warnings) > 0, "minute %d: no sample and no warning", j)
						} else {
							answers++
							if h := v[0].H; h != nil {
								testutil.Assert(t, h.Count >= 0, "minute %d: negative count %v", j, h)
							} else {
								testutil.Assert(t, v[0].F >= 0, "minute %d: negative value %g", j, v[0].F)
							}
						}
						qry.Close()
					}
					testutil.Assert(t, answers > 0, "no query returned a sample")
				})
			}
		}
	}
}
