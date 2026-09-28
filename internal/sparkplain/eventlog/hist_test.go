package eventlog

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

func TestDistExactForSmallCounts(t *testing.T) {
	t.Parallel()
	var d distAcc
	for _, v := range []int64{5, 1, 9, 3, 7} {
		d.add(v)
	}
	got := d.dist()
	if got.Count != 5 || got.Sum != 25 || got.Min != 1 || got.Max != 9 || got.P50 != 5 || got.P95 != 9 {
		t.Fatalf("%+v", got)
	}
}

func TestDistHistogramWithinThreePercent(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(1, 2))
	var d distAcc
	var all []int64
	for range 200000 {
		v := int64(math.Exp(r.NormFloat64()*1.5 + 8)) // log-normal, like task times
		d.add(v)
		all = append(all, v)
	}
	slices.Sort(all)
	for _, q := range []float64{0.5, 0.95} {
		want := float64(all[int(math.Ceil(q*float64(len(all))))-1])
		got := float64(d.quantile(q))
		if math.Abs(got-want)/want > 0.035 {
			t.Errorf("q%.2f = %.0f, exact %.0f", q, got, want)
		}
	}
	if d.dist().Max != all[len(all)-1] || d.dist().Min != all[0] {
		t.Error("min/max must be exact")
	}
}

func TestBucketBoundsCoverValue(t *testing.T) {
	t.Parallel()
	for _, v := range []int64{0, 15, 16, 17, 31, 32, 1000, 123456789, math.MaxInt64 / 2} {
		i := bucketOf(v)
		mid := bucketMid(i)
		if v >= 16 && math.Abs(float64(mid-v))/float64(v) > 0.07 {
			t.Errorf("bucket mid %d far from %d", mid, v)
		}
	}
}
