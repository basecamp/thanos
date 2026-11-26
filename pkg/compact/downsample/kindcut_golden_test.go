// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package downsample

import (
	"testing"

	"github.com/efficientgo/core/testutil"
)

// TestDownsampleKindStableGolden pins the aggregate chunks of series that
// never change histogram kind, byte for byte, at 5m and at 1h. The
// fingerprints were recorded with this same test on the commit before kind
// changes were cut into segments (f82c5d34), so handling kind changes must
// not have changed anything for a series without one.
func TestDownsampleKindStableGolden(t *testing.T) {
	var floats []sample
	for i := 0; i < 500; i++ {
		v := float64(i * 3)
		if i >= 317 {
			v = float64((i - 317) * 3)
		}
		floats = append(floats, sample{t: kcStart + int64(i)*kcStep, v: v})
	}

	for _, tc := range []struct {
		name   string
		data   []sample
		res5m  string
		res1h  string
		chunks int
	}{
		{
			name:   "exponential with two resets, two batches",
			data:   kcSeries(kcStart, kcStep, kcRepeat("e", 300, "E", 1, "e", 250, "E", 1, "e", 248), false),
			chunks: 2,
			res5m:  "d6d1ef8ab5fd889339f12a7231ff04c9ebcd8d79fc9e80c3fd8fad279475f3c3",
			res1h:  "1602ecc2a0bbfb1654932926a13f4f3625677d41efb7c6f029b8c55ad7a14697",
		},
		{
			name:   "custom with layout changes and a reset, two batches",
			data:   kcSeries(kcStart, kcStep, kcRepeat("c", 302, "d", 200, "C", 1, "c", 297), false),
			chunks: 6,
			res5m:  "35fc1fb8cc875d8229b41d4cc2305329738592d15c508c1620e5e5e324c2f4e5",
			res1h:  "ca2d673bdc6d0b84cc2f7cf27f2b89ff654f035ecceaef88c03e801acd564338",
		},
		{
			name:   "exponential gauge",
			data:   kcSeries(kcStart, kcStep, kcRepeat("e", 400), true),
			chunks: 1,
			res5m:  "1ab6b2d94f6108b29193f762995b563201a9f57c205f45457e48a5baaac0ad15",
			res1h:  "8ed92eca2ca49be3ce5c7a69f660ccbbeab5759b294af3b0c6251066dde04b79",
		},
		{
			name:   "custom gauge",
			data:   kcSeries(kcStart, kcStep, kcRepeat("c", 400), true),
			chunks: 1,
			res5m:  "486549e13c271bad8328a3bd168f0c40ac4ffe9d4e5c504dca16bc70b4a078de",
			res1h:  "8e0a673ce86efc9c3b53e0d641068321db28a470372819a90532f93ee2dd21b1",
		},
		{
			name:   "custom with stale markers and NaN sums",
			data:   kcSeries(kcStart, kcStep, kcRepeat("s", 1, "c", 50, "s", 1, "c", 20, "m", 2, "c", 100, "s", 1), false),
			chunks: 1,
			res5m:  "4204f0375fdb128cb3f3b085c962a80b64bc5b81607aeda7f730caf356968541",
			res1h:  "11375062c92f0edfdb0874edd67dfca96b3c697278eaeeea5091f0ebbc775b14",
		},
		{
			name:   "exponential at 15s with scrape gaps",
			data:   kcSeries(kcStart, 15_000, kcRepeat("e", 700, "_", 45, "e", 500, "_", 300, "e", 455), false),
			chunks: 1,
			res5m:  "c2cd7662c219024a93857ebe16bfa18dc8ecbcbf4ca4f2fcf2ec894b92144e4d",
			res1h:  "ebceb7a672b5140614c473a79a01a1cb2c6b0c18eafe37424fc36918da261049",
		},
		{
			name:   "float counter with a reset",
			data:   floats,
			chunks: 1,
			res5m:  "ed45348f518b1578ef74632015f162a748c634012ccac963cb7ce2031e2d5a82",
			res1h:  "bd2d9e18b647e328e8330e72e8d61bdcb5843037923e997e15d220d6a8e7ffb7",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ds5m, err := DownsampleRaw(tc.data, ResLevel1)
			testutil.Ok(t, err)
			ds1h := kcDownsampleBlock(t, ds5m, ResLevel1, ResLevel2)
			t.Logf("5m chunks=%d %s 1h chunks=%d %s", len(ds5m), kcHash(ds5m), len(ds1h), kcHash(ds1h))

			testutil.Equals(t, tc.chunks, len(ds5m), "5m chunks")
			testutil.Equals(t, tc.res5m, kcHash(ds5m), "5m chunks changed")
			testutil.Equals(t, tc.res1h, kcHash(ds1h), "1h chunks changed")
		})
	}
}
