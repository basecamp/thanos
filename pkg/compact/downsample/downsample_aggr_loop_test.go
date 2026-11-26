// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package downsample

import (
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

// TestDownsampleAggrFewerChunksThanTarget verifies that downsampling
// aggregates terminates when a series has fewer chunks than the number of
// chunks targeted for the output: one 5m chunk with more samples than fit
// 140 samples per 1h chunk.
func TestDownsampleAggrFewerChunksThanTarget(t *testing.T) {
	const windows = 1681
	hint := histogram.UnknownCounterReset

	var hist []kcOut
	floats := map[AggrType][]sample{}
	for k := 0; k < windows; k++ {
		ts := kcStart + int64(k+1)*ResLevel1 - 1
		hist = append(hist, kcOut{t: ts, count: 5, sum: kcExp(ts, float64(25*k+15), hint).fh, counter: kcExp(ts, float64(5*k+5), hint).fh})
		for _, at := range []AggrType{AggrCount, AggrSum, AggrMin, AggrMax, AggrCounter} {
			floats[at] = append(floats[at], sample{t: ts, v: float64(k + 1)})
		}
	}
	floatChunk := encodeTestAggrSeries(floats)
	floatChunk.MinTime, floatChunk.MaxTime = hist[0].t, hist[windows-1].t

	for _, tc := range []struct {
		name string
		src  chunks.Meta
		// total is the sum of the count aggregate, which must be kept.
		total float64
	}{
		{name: "histograms", src: kcAggrChunk(t, hist), total: 5 * windows},
		{name: "floats", src: floatChunk, total: windows * (windows + 1) / 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			type result struct {
				id  ulid.ULID
				dir string
				err error
			}
			done := make(chan result, 1)
			go func() {
				id, dir, err := kcTryDownsampleBlock(t, []chunks.Meta{tc.src}, ResLevel1, ResLevel2)
				done <- result{id, dir, err}
			}()
			select {
			case res := <-done:
				testutil.Ok(t, res.err)
				_, chkss := kcReadBlock(t, res.dir, res.id)
				testutil.Equals(t, 1, len(chkss))
				total := 0.0
				for _, m := range chkss[0] {
					for _, s := range expandAggrChunk(t, m, AggrCount) {
						total += s.v
					}
				}
				testutil.Equals(t, tc.total, total)
			case <-time.After(30 * time.Second):
				t.Fatal("Downsample did not return")
			}
		})
	}
}
