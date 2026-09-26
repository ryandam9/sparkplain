package model

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Bytes formats a byte count with binary units ("1.4 GiB").
func Bytes(n int64) string {
	if n < 0 {
		return "−" + Bytes(-n)
	}
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	units := []string{"KiB", "MiB", "GiB", "TiB", "PiB"}
	v := float64(n) / unit
	i := 0
	for v >= unit && i < len(units)-1 {
		v /= unit
		i++
	}
	if v >= 100 {
		return fmt.Sprintf("%.0f %s", v, units[i])
	}
	return fmt.Sprintf("%.1f %s", v, units[i])
}

// Duration formats milliseconds as "47 min 12 s", "1 h 3 min", "850 ms".
func Duration(ms int64) string {
	switch {
	case ms < 0:
		return "—"
	case ms < 1000:
		return fmt.Sprintf("%d ms", ms)
	case ms < 60_000:
		s := float64(ms) / 1000
		if s < 10 {
			return fmt.Sprintf("%.1f s", s)
		}
		return fmt.Sprintf("%.0f s", s)
	case ms < 3_600_000:
		return fmt.Sprintf("%d min %d s", ms/60_000, ms%60_000/1000)
	case ms < 86_400_000:
		return fmt.Sprintf("%d h %d min", ms/3_600_000, ms%3_600_000/60_000)
	}
	return fmt.Sprintf("%d d %d h", ms/86_400_000, ms%86_400_000/3_600_000)
}

// Percent formats a 0–1 share as "42%" (one decimal under 10%).
func Percent(share float64) string {
	if math.IsNaN(share) || math.IsInf(share, 0) {
		return "—"
	}
	p := share * 100
	if p > 0 && p < 10 {
		return strconv.FormatFloat(p, 'f', 1, 64) + "%"
	}
	return fmt.Sprintf("%.0f%%", p)
}

// Num formats an integer with thousands separators.
func Num(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}

// Plural returns "1 job" or "3 jobs".
func Plural(n int, one, many string) string {
	if n == 1 {
		return "1 " + one
	}
	return Num(int64(n)) + " " + many
}
