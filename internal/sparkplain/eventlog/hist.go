package eventlog

import (
	"math/bits"
	"slices"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// distAcc folds a stream of values into count, sum, min, max and quantiles
// without keeping them. The first exactN values are kept so small stages get
// exact quantiles; after that a fixed log-scale histogram (16 buckets per
// power of two, about 3% error) takes over.
type distAcc struct {
	n, sum, min, max int64
	exact            []int64
	buckets          []uint32
}

const exactN = 64

func (d *distAcc) add(v int64) {
	if v < 0 {
		v = 0
	}
	if d.n == 0 || v < d.min {
		d.min = v
	}
	if v > d.max {
		d.max = v
	}
	d.n++
	d.sum += v
	if d.buckets == nil {
		if len(d.exact) < exactN {
			d.exact = append(d.exact, v)
			return
		}
		d.buckets = make([]uint32, 0, 128)
		for _, x := range d.exact {
			d.bucketAdd(x)
		}
		d.exact = nil
	}
	d.bucketAdd(v)
}

func bucketOf(v int64) int {
	if v < 16 {
		return int(v)
	}
	e := bits.Len64(uint64(v)) - 1
	m := int(v>>(e-4)) & 15
	return 16 + (e-4)*16 + m
}

// bucketMid returns the middle of a bucket's value range.
func bucketMid(i int) int64 {
	if i < 16 {
		return int64(i)
	}
	e := (i-16)/16 + 4
	m := int64((i - 16) % 16)
	lo := (16 + m) << (e - 4)
	hi := (17+m)<<(e-4) - 1
	return lo + (hi-lo)/2
}

func (d *distAcc) bucketAdd(v int64) {
	i := bucketOf(v)
	if i >= len(d.buckets) {
		d.buckets = append(d.buckets, make([]uint32, i+1-len(d.buckets))...)
	}
	d.buckets[i]++
}

// quantile uses the nearest-rank method.
func (d *distAcc) quantile(q float64) int64 {
	if d.n == 0 {
		return 0
	}
	rank := int64(q*float64(d.n) + 0.999999)
	rank = max(1, min(rank, d.n))
	if d.buckets == nil {
		s := slices.Clone(d.exact)
		slices.Sort(s)
		return s[rank-1]
	}
	var seen int64
	for i, c := range d.buckets {
		seen += int64(c)
		if seen >= rank {
			return max(d.min, min(d.max, bucketMid(i)))
		}
	}
	return d.max
}

func (d *distAcc) dist() model.Dist {
	return model.Dist{Count: d.n, Sum: d.sum, Min: d.min, Max: d.max, P50: d.quantile(0.5), P95: d.quantile(0.95)}
}

func (d *distAcc) quartiles() model.Quartiles {
	if d.n == 0 {
		return model.Quartiles{}
	}
	return model.Quartiles{Count: d.n, Sum: d.sum, Min: d.min, Max: d.max,
		P25: d.quantile(0.25), P50: d.quantile(0.5), P75: d.quantile(0.75)}
}

// bucketLo returns the smallest value in bucket i.
func bucketLo(i int) int64 {
	if i < 16 {
		return int64(i)
	}
	e := (i-16)/16 + 4
	return (16 + int64((i-16)%16)) << (e - 4)
}

// histogram groups the values into at most maxBins bars of equal width on
// the log scale, skipping empty ones.
func (d *distAcc) histogram(maxBins int) []model.HistBin {
	if d.n == 0 {
		return nil
	}
	counts := map[int]int64{}
	top := 0
	if d.buckets == nil {
		for _, v := range d.exact {
			i := bucketOf(v)
			counts[i]++
			top = max(top, i)
		}
	} else {
		for i, c := range d.buckets {
			if c > 0 {
				counts[i] += int64(c)
				top = i
			}
		}
	}
	lo := bucketOf(d.min)
	per := max(1, (top-lo+maxBins)/maxBins) // fine buckets per bar
	var out []model.HistBin
	for b := lo; b <= top; b += per {
		var n int64
		for i := b; i < b+per; i++ {
			n += counts[i]
		}
		if n == 0 {
			continue
		}
		hi := bucketLo(b+per) - 1
		out = append(out, model.HistBin{Lo: max(d.min, bucketLo(b)), Hi: min(d.max, hi), Count: n})
	}
	return out
}
