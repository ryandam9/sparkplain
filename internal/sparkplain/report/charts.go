package report

import (
	"fmt"
	"html/template"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

func esc(s string) string { return template.HTMLEscapeString(s) }

// timeAxis draws x ticks as <text data-time> so the page script can relabel
// them in the viewer's time zone.
func timeAxis(b *strings.Builder, start, end time.Time, x func(time.Time) float64, y, top float64, loc *time.Location) {
	span := end.Sub(start)
	if span <= 0 {
		return
	}
	steps := []time.Duration{time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second, time.Minute, 5 * time.Minute, 10 * time.Minute, 15 * time.Minute, 30 * time.Minute, time.Hour, 2 * time.Hour, 6 * time.Hour, 12 * time.Hour, 24 * time.Hour}
	step := steps[len(steps)-1]
	for _, s := range steps {
		if span/s <= 7 {
			step = s
			break
		}
	}
	fmtName, layout := "hm", "15:04"
	if step < time.Minute {
		fmtName, layout = "hms", "15:04:05"
	}
	for t := start.Truncate(step); !t.After(end); t = t.Add(step) {
		if t.Before(start) {
			continue
		}
		px := x(t)
		iso := t.UTC().Format(time.RFC3339)
		fmt.Fprintf(b, `<line class="grid" x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f"/>`, px, px, top, y+4)
		fmt.Fprintf(b, `<text class="ax" x="%.1f" y="%.1f" text-anchor="middle" data-time="%s" data-fmt="%s">%s</text>`, px, y+18, iso, fmtName, t.In(loc).Format(layout))
	}
}

func execChart(r *model.Report, loc *time.Location) template.HTML {
	t := r.Timeline
	if len(t.ExecutorSeries) == 0 || t.Start.IsZero() || !t.End.After(t.Start) {
		return ""
	}
	const W, H = 820.0, 230.0
	ml, mr, mt, mb := 40.0, 18.0, 28.0, 32.0
	iw, ih := W-ml-mr, H-mt-mb
	peak := 1
	for _, p := range t.ExecutorSeries {
		peak = max(peak, p.Count)
	}
	ymax := float64(niceMax(peak))
	span := float64(t.End.Sub(t.Start))
	x := func(v time.Time) float64 { return ml + float64(v.Sub(t.Start))/span*iw }
	y := func(v float64) float64 { return mt + ih - v/ymax*ih }
	var b strings.Builder
	fmt.Fprintf(&b, `<svg viewBox="0 0 %.0f %.0f" role="img" aria-label="Executors running over time, peaking at %d">`, W, H, peak)
	for _, v := range []float64{0, ymax / 2, ymax} {
		fmt.Fprintf(&b, `<line class="grid" x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f"/><text class="ax" x="%.1f" y="%.1f" text-anchor="end">%.0f</text>`, ml, W-mr, y(v), y(v), ml-8, y(v)+4, v)
	}
	timeAxis(&b, t.Start, t.End, x, mt+ih, mt, loc)
	var d strings.Builder
	prev := 0
	fmt.Fprintf(&d, "M%.1f %.1f", x(t.Start), y(0))
	for _, p := range t.ExecutorSeries {
		fmt.Fprintf(&d, " L%.1f %.1f L%.1f %.1f", x(p.Time), y(float64(prev)), x(p.Time), y(float64(p.Count)))
		prev = p.Count
	}
	fmt.Fprintf(&d, " L%.1f %.1f", x(t.End), y(float64(prev)))
	fmt.Fprintf(&b, `<path d="%s L%.1f %.1f Z" fill="var(--accent)" opacity=".16"/>`, d.String(), x(t.End), y(0))
	fmt.Fprintf(&b, `<path d="%s" fill="none" stroke="var(--accent)" stroke-width="2" stroke-linejoin="round"/>`, d.String())
	for _, e := range t.Events {
		px := x(e.Time)
		switch e.Kind {
		case "executor-killed", "executor-lost":
			fmt.Fprintf(&b, `<g><title>%s</title><line x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f" stroke="var(--crit)" stroke-dasharray="3 3" opacity=".7"/><circle cx="%.1f" cy="%.1f" r="4.5" class="mk-crit"/></g>`,
				esc(e.Text), px, px, mt-12, mt+ih, px, mt-12)
		case "job-failed":
			fmt.Fprintf(&b, `<g><title>%s</title><line x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f" stroke="var(--crit)" opacity=".7"/><rect x="%.1f" y="%.1f" width="10" height="10" class="mk-crit"/></g>`,
				esc(e.Text), px, px, mt-12, mt+ih, px-5, mt-17)
		case "stage-retry":
			fmt.Fprintf(&b, `<g><title>%s</title><rect x="%.1f" y="%.1f" width="8" height="8" transform="rotate(45 %.1f %.1f)" class="mk-warn"/></g>`,
				esc(e.Text), px-4, mt-16, px, mt-12)
		}
	}
	b.WriteString(`</svg>`)
	return template.HTML(b.String())
}

func niceMax(n int) int {
	for _, s := range []int{2, 4, 6, 8, 10, 12, 16, 20, 24, 30, 40, 50, 60, 80, 100} {
		if n <= s {
			return s
		}
	}
	p := int(math.Pow(10, math.Floor(math.Log10(float64(n)))))
	return (n + p - 1) / p * p
}

const ganttMax = 80

func gantt(r *model.Report, loc *time.Location) template.HTML {
	t := r.Timeline
	bars := t.Jobs
	if len(bars) == 0 || t.Start.IsZero() || !t.End.After(t.Start) {
		return ""
	}
	if len(bars) > ganttMax {
		bars = append([]model.Bar(nil), bars...)
		sort.SliceStable(bars, func(i, j int) bool { return bars[i].End.Sub(bars[i].Start) > bars[j].End.Sub(bars[j].Start) })
		bars = bars[:ganttMax]
		sort.SliceStable(bars, func(i, j int) bool { return bars[i].ID < bars[j].ID })
	}
	const W = 820.0
	ml, mr, mt, rowH := 70.0, 18.0, 8.0, 16.0
	ih := float64(len(bars)) * rowH
	H := mt + ih + 32
	iw := W - ml - mr
	span := float64(t.End.Sub(t.Start))
	x := func(v time.Time) float64 { return ml + math.Max(0, float64(v.Sub(t.Start)))/span*iw }
	var b strings.Builder
	fmt.Fprintf(&b, `<svg class="gantt" viewBox="0 0 %.0f %.0f" role="img" aria-label="Jobs over time: %d bars">`, W, H, len(bars))
	timeAxis(&b, t.Start, t.End, x, mt+ih, mt, loc)
	for i, bar := range bars {
		yy := mt + float64(i)*rowH
		cls := "st-ok"
		switch bar.Status {
		case model.StatusFailed:
			cls = "st-failed"
		case model.StatusSucceeded:
		default:
			cls = "st-other"
		}
		x0, x1 := x(bar.Start), x(bar.End)
		w := math.Max(1.5, x1-x0)
		dur := model.Duration(bar.End.Sub(bar.Start).Milliseconds())
		fmt.Fprintf(&b, `<g class="row"><title>Job %d: %s (%s, %s)</title><text class="lbl" x="%.1f" y="%.1f" text-anchor="end">job %d</text><rect class="job %s" x="%.1f" y="%.1f" width="%.1f" height="%.1f" rx="2"/>`,
			bar.ID, esc(bar.Label), esc(statusLabel(bar.Status)), dur, ml-8, yy+rowH-5, bar.ID, cls, x0, yy+3, w, rowH-6)
		for _, st := range bar.Stages {
			sx0, sx1 := x(st.Start), x(st.End)
			fmt.Fprintf(&b, `<rect x="%.1f" y="%.1f" width="%.1f" height="2" fill="var(--ink)" opacity=".35"><title>%s (%s)</title></rect>`,
				sx0, yy+rowH-4, math.Max(1, sx1-sx0), esc(st.Label), model.Duration(st.End.Sub(st.Start).Milliseconds()))
		}
		b.WriteString(`</g>`)
	}
	b.WriteString(`</svg>`)
	return template.HTML(b.String())
}

func memChart(r *model.Report) template.HTML {
	ex := append([]model.ExecMemory(nil), r.Memory.Executors...)
	if len(ex) == 0 {
		return ""
	}
	sort.SliceStable(ex, func(i, j int) bool { return ex[i].PeakHeap > ex[j].PeakHeap })
	if len(ex) > 40 {
		ex = ex[:40]
	}
	var scale int64 = 1
	for _, e := range ex {
		scale = max(scale, e.HeapBytes, e.PeakHeap)
	}
	const W = 820.0
	ml, mr, rowH := 150.0, 190.0, 20.0
	iw := W - ml - mr
	H := float64(len(ex))*rowH + 8
	var b strings.Builder
	maxW := 1000
	if len(ex) <= 3 {
		maxW = 760 // a few rows stay small rather than filling a wide page
	}
	fmt.Fprintf(&b, `<svg viewBox="0 0 %.0f %.0f" role="img" aria-label="Peak heap against configured heap for %d executors" style="max-width:%dpx">`, W, H, len(ex), maxW)
	for i, e := range ex {
		yy := 4 + float64(i)*rowH
		full := float64(e.HeapBytes) / float64(scale) * iw
		used := float64(e.PeakHeap) / float64(scale) * iw
		color := "var(--accent)"
		if e.HeapBytes > 0 && float64(e.PeakHeap) > 0.9*float64(e.HeapBytes) {
			color = "var(--crit)"
		}
		fmt.Fprintf(&b, `<text class="lbl" x="%.1f" y="%.1f" text-anchor="end">executor %s</text>`, ml-8, yy+13, esc(e.ID))
		fmt.Fprintf(&b, `<rect x="%.1f" y="%.1f" width="%.1f" height="12" rx="2" fill="var(--surface-2)" stroke="var(--line)"/>`, ml, yy+3, full)
		fmt.Fprintf(&b, `<rect x="%.1f" y="%.1f" width="%.1f" height="12" rx="2" fill="%s" opacity=".85"/>`, ml, yy+3, used, color)
		pct := "—"
		if e.HeapBytes > 0 {
			pct = model.Percent(float64(e.PeakHeap) / float64(e.HeapBytes))
		}
		fmt.Fprintf(&b, `<text class="lbl" x="%.1f" y="%.1f">%s of %s (%s)</text>`, ml+iw+8, yy+13, model.Bytes(e.PeakHeap), model.Bytes(e.HeapBytes), pct)
	}
	b.WriteString(`</svg>`)
	return template.HTML(b.String())
}

// clusterChart draws the cluster's containers over the metrics window from
// CloudWatch: allocated and waiting, with the application's run shaded.
func clusterChart(r *model.Report, loc *time.Location) template.HTML {
	m := r.Metrics
	if m == nil {
		return ""
	}
	var pending, allocated *model.Series
	for i := range m.Cluster {
		switch m.Cluster[i].Name {
		case "ContainerPending":
			pending = &m.Cluster[i]
		case "ContainerAllocated":
			allocated = &m.Cluster[i]
		}
	}
	if pending == nil || allocated == nil || len(pending.Points)+len(allocated.Points) == 0 || !m.To.After(m.From) {
		return ""
	}
	const W, H = 820.0, 210.0
	ml, mr, mt, mb := 40.0, 18.0, 16.0, 32.0
	iw, ih := W-ml-mr, H-mt-mb
	peak := 1.0
	for _, s := range []*model.Series{pending, allocated} {
		if v, ok := s.Max(); ok {
			peak = max(peak, v)
		}
	}
	ymax := float64(niceMax(int(peak)))
	if ymax < peak {
		ymax = peak
	}
	span := float64(m.To.Sub(m.From))
	x := func(v time.Time) float64 { return ml + float64(v.Sub(m.From))/span*iw }
	y := func(v float64) float64 { return mt + ih - v/ymax*ih }
	var b strings.Builder
	fmt.Fprintf(&b, `<svg viewBox="0 0 %.0f %.0f" role="img" aria-label="Containers allocated and waiting on the cluster over time">`, W, H)
	if a := r.Application; !a.Start.IsZero() {
		end := a.End
		if end.IsZero() || end.After(m.To) {
			end = m.To
		}
		fmt.Fprintf(&b, `<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" fill="var(--accent)" opacity=".07"><title>This application's run</title></rect>`, x(a.Start), mt, max(1, x(end)-x(a.Start)), ih)
	}
	for _, v := range []float64{0, ymax / 2, ymax} {
		fmt.Fprintf(&b, `<line class="grid" x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f"/><text class="ax" x="%.1f" y="%.1f" text-anchor="end">%.0f</text>`, ml, W-mr, y(v), y(v), ml-8, y(v)+4, v)
	}
	timeAxis(&b, m.From, m.To, x, mt+ih, mt, loc)
	step := func(s *model.Series, color string, dash string) {
		if len(s.Points) == 0 {
			return
		}
		var d strings.Builder
		per := time.Duration(s.PeriodS) * time.Second
		for i, p := range s.Points {
			cmd := "L"
			if i == 0 {
				cmd = "M"
			}
			fmt.Fprintf(&d, "%s%.1f %.1f L%.1f %.1f ", cmd, x(p.T), y(p.V), x(minTime(p.T.Add(per), m.To)), y(p.V))
		}
		fmt.Fprintf(&b, `<path d="%s" fill="none" stroke="%s" stroke-width="2" stroke-dasharray="%s" stroke-linejoin="round"/>`, d.String(), color, dash)
	}
	step(allocated, "var(--accent)", "")
	step(pending, "var(--warn)", "5 3")
	b.WriteString(`</svg>`)
	return template.HTML(b.String())
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
