package report

import (
	"fmt"
	"html/template"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// Chart colours, by role. Categorical slots come from the validated
// reference palette (report.css, --viz-1 … --viz-5); status and neutral
// colours keep their own tokens.
const (
	cInput   = "var(--viz-1)"
	cShRead  = "var(--viz-2)"
	cShWrite = "var(--viz-3)"
	cOutput  = "var(--viz-4)"
	cSpill   = "var(--viz-5)"
	cNeutral = "var(--na)"
	cFail    = "var(--crit)"
)

// seg is one coloured piece of a horizontal bar.
type seg struct {
	v     float64
	color string
	title string // hover text
}

// hbar is one labelled row of a horizontal bar chart.
type hbar struct {
	label string
	href  string // links the label, such as to the explorer's stage page
	segs  []seg
	note  string  // text right of the bar
	max   float64 // an outline for the row's limit (such as a node's capacity), when larger than its bars
	lo    float64 // a whisker from lo to hi, drawn over the bar (task time spread)
	hi    float64
	mid   float64 // a tick at mid (the median)
	// texts[i] is written inside segs[i], the first that fits (longest
	// first), such as "Executor 3 · 4 vCPU", "E3 · 4 vCPU", "4".
	texts [][]string
}

// legendItem names a colour under a chart.
type legendItem struct{ color, label string }

// shown keeps the legend items whose colour some bar actually uses, so a
// run that spilled nothing has no "Spilled to disk" key.
func shown(rows []hbar, legend []legendItem) []legendItem {
	used := map[string]bool{}
	for _, r := range rows {
		for _, sg := range r.segs {
			if sg.v > 0 {
				used[sg.color] = true
			}
		}
	}
	var out []legendItem
	for _, l := range legend {
		if used[l.color] {
			out = append(out, l)
		}
	}
	return out
}

// chartGuide explains a chart under it, the same way everywhere:
//   - Axes names what each axis or mark stands for ("Across", "Up", "Rows",
//     "Bar length", "Colour", ...), one line each;
//   - Read says how to read it: what a good picture looks like and what a
//     bad one means, one point each, shown as bullets when there are several;
//   - Note is fine print: sampling, what was left out, what clicking does.
type chartGuide struct {
	Axes [][2]string `json:"axes"`
	Read []string    `json:"read"`
	Note string      `json:"note,omitempty"`
	// Run says what this run's chart shows (runNotes), with links to the
	// findings it is evidence for.
	Run []runPoint `json:"run,omitempty"`
}

// html renders the guide. The explorer's guideNodes draws the same shape.
func (g chartGuide) html() template.HTML {
	var b strings.Builder
	if len(g.Axes) > 0 {
		b.WriteString(`<dl class="axes">`)
		for _, a := range g.Axes {
			fmt.Fprintf(&b, `<div><dt>%s</dt><dd>%s</dd></div>`, esc(a[0]), esc(a[1]))
		}
		b.WriteString(`</dl>`)
	}
	b.WriteString(string(pointList("read", "How to read it", g.Read)))
	b.WriteString(string(runList(g.Run)))
	if g.Note != "" {
		fmt.Fprintf(&b, `<p class="cap">%s</p>`, esc(g.Note))
	}
	return template.HTML(b.String())
}

// pointList is a titled paragraph for one point, or a titled bullet list
// for several.
func pointList(cls, title string, points []string) template.HTML {
	switch len(points) {
	case 0:
		return ""
	case 1:
		return template.HTML(fmt.Sprintf(`<p class="%s"><b>%s</b> %s</p>`, cls, esc(title), esc(points[0])))
	}
	var b strings.Builder
	fmt.Fprintf(&b, `<div class="%s"><b>%s</b><ul>`, cls, esc(title))
	for _, p := range points {
		fmt.Fprintf(&b, `<li>%s</li>`, esc(p))
	}
	b.WriteString(`</ul></div>`)
	return template.HTML(b.String())
}

// runList renders a chart's "In this run": a paragraph for one point, or a
// bullet list, with each finding linked to the report's findings list.
func runList(points []runPoint) template.HTML {
	item := func(p runPoint) string {
		if p.Finding > 0 {
			return fmt.Sprintf(`<a href="#finding-%d">%s</a>`, p.Finding, esc(p.Text))
		}
		return esc(p.Text)
	}
	switch len(points) {
	case 0:
		return ""
	case 1:
		return template.HTML(`<p class="run"><b>In this run</b> ` + item(points[0]) + `</p>`)
	}
	var b strings.Builder
	b.WriteString(`<div class="run"><b>In this run</b><ul>`)
	for _, p := range points {
		b.WriteString(`<li>` + item(p) + `</li>`)
	}
	b.WriteString(`</ul></div>`)
	return template.HTML(b.String())
}

// chartBox wraps a chart with a title, a legend and its guide.
func chartBox(title string, g chartGuide, svg string, legend []legendItem) template.HTML {
	if svg == "" {
		return ""
	}
	var b strings.Builder
	b.WriteString(`<div class="chart">`)
	if title != "" {
		fmt.Fprintf(&b, `<h4>%s</h4>`, esc(title))
	}
	b.WriteString(svg)
	if len(legend) > 0 {
		b.WriteString(`<div class="legend">`)
		for _, l := range legend {
			fmt.Fprintf(&b, `<span><i class="sw" style="background:%s"></i>%s</span>`, l.color, esc(l.label))
		}
		b.WriteString(`</div>`)
	}
	b.WriteString(string(g.html()))
	b.WriteString(`</div>`)
	return template.HTML(b.String())
}

// hbars draws horizontal bars, one row each, stacked from their segments,
// against one scale.
func hbars(rows []hbar, format func(float64) string) string {
	if len(rows) == 0 {
		return ""
	}
	var scale float64
	for _, r := range rows {
		var t float64
		for _, s := range r.segs {
			t += s.v
		}
		scale = max(scale, t, r.max, r.hi)
	}
	if scale <= 0 {
		return ""
	}
	// Drawn for the full width of the report's column (about 1,100 px on
	// a laptop), so text shows near its set size; phones scroll it sideways.
	const W = 1000.0
	ml, mr, rowH := 270.0, 170.0, 22.0 // 36 characters of 11 px mono fit in ml
	for _, r := range rows {
		mr = max(mr, min(textW(r.note, 11, false)+16, 320)) // room for the longest note
	}
	iw := W - ml - mr
	H := float64(len(rows))*rowH + 8
	var b strings.Builder
	fmt.Fprintf(&b, `<svg class="wide" viewBox="0 0 %.0f %.0f" role="img">`, W, H)
	x := func(v float64) float64 { return ml + v/scale*iw }
	for i, r := range rows {
		y := 4 + float64(i)*rowH
		label := esc(clipLabel(r.label, 36))
		if r.href != "" {
			fmt.Fprintf(&b, `<a href="%s"><text class="lbl" x="%.1f" y="%.1f" text-anchor="end">%s<title>%s</title></text></a>`, esc(r.href), ml-8, y+13, label, esc(r.label))
		} else {
			fmt.Fprintf(&b, `<text class="lbl" x="%.1f" y="%.1f" text-anchor="end">%s<title>%s</title></text>`, ml-8, y+13, label, esc(r.label))
		}
		if r.max > 0 {
			fmt.Fprintf(&b, `<rect x="%.1f" y="%.1f" width="%.1f" height="14" rx="3" fill="var(--surface-2)" stroke="var(--line)"/>`, ml, y+2, x(r.max)-ml)
		}
		at := 0.0
		for si, s := range r.segs {
			if s.v <= 0 {
				continue
			}
			w := s.v / scale * iw
			// A 1 px surface gap between stacked pieces keeps them apart.
			fmt.Fprintf(&b, `<rect x="%.1f" y="%.1f" width="%.1f" height="14" rx="2" fill="%s"><title>%s</title></rect>`, x(at)+0.5, y+2, max(w-1, 1), s.color, esc(s.title))
			var texts []string
			if si < len(r.texts) {
				texts = r.texts[si]
			}
			for _, t := range texts {
				if textW(t, 10, true) <= w-8 {
					fmt.Fprintf(&b, `<text class="inbar" x="%.1f" y="%.1f" text-anchor="middle">%s<title>%s</title></text>`, x(at)+w/2, y+12.5, esc(t), esc(s.title))
					break
				}
			}
			at += s.v
		}
		if r.hi > 0 {
			fmt.Fprintf(&b, `<line x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f" stroke="var(--ink)" stroke-width="1.5"/>`, x(r.lo), x(r.hi), y+9, y+9)
			fmt.Fprintf(&b, `<line x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f" stroke="var(--ink)" stroke-width="1.5"/>`, x(r.hi), x(r.hi), y+4, y+14)
			if r.mid > 0 {
				fmt.Fprintf(&b, `<rect x="%.1f" y="%.1f" width="3" height="12" fill="var(--ink)"><title>median %s</title></rect>`, x(r.mid)-1.5, y+3, esc(format(r.mid)))
			}
		}
		fmt.Fprintf(&b, `<text class="lbl" x="%.1f" y="%.1f">%s</text>`, ml+iw+8, y+13, esc(r.note))
	}
	b.WriteString(`</svg>`)
	return b.String()
}

func clipLabel(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}

// line is one series of a chart over time.
type line struct {
	label  string
	color  string
	points []model.Point
	step   bool // hold each value until the next point
	area   bool // shade under the line
	dash   bool
}

// lines draws series over time on one axis.
func lines(series []line, start, end time.Time, format func(float64) string, loc *time.Location) string {
	var peak float64
	n := 0
	for _, s := range series {
		for _, p := range s.points {
			peak = max(peak, p.V)
			n++
		}
	}
	if n == 0 || !end.After(start) {
		return ""
	}
	if peak <= 0 {
		peak = 1
	}
	const W, H = 1000.0, 260.0
	ml, mr, mt, mb := 64.0, 14.0, 14.0, 32.0
	iw, ih := W-ml-mr, H-mt-mb
	span := float64(end.Sub(start))
	x := func(t time.Time) float64 { return ml + float64(t.Sub(start))/span*iw }
	y := func(v float64) float64 { return mt + ih - v/peak*ih }
	var b strings.Builder
	fmt.Fprintf(&b, `<svg class="wide" viewBox="0 0 %.0f %.0f" role="img">`, W, H)
	for _, v := range []float64{0, peak / 2, peak} {
		fmt.Fprintf(&b, `<line class="grid" x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f"/><text class="ax" x="%.1f" y="%.1f" text-anchor="end">%s</text>`, ml, W-mr, y(v), y(v), ml-8, y(v)+4, esc(format(v)))
	}
	timeAxis(&b, start, end, x, mt+ih, mt, loc)
	for _, s := range series {
		if len(s.points) == 0 {
			continue
		}
		var d strings.Builder
		for i, p := range s.points {
			switch {
			case i == 0:
				fmt.Fprintf(&d, "M%.1f %.1f", x(p.T), y(p.V))
			case s.step:
				fmt.Fprintf(&d, " L%.1f %.1f L%.1f %.1f", x(p.T), y(s.points[i-1].V), x(p.T), y(p.V))
			default:
				fmt.Fprintf(&d, " L%.1f %.1f", x(p.T), y(p.V))
			}
		}
		last := s.points[len(s.points)-1]
		if s.step {
			fmt.Fprintf(&d, " L%.1f %.1f", x(end), y(last.V))
		}
		if s.area {
			first := s.points[0]
			endX := x(last.T)
			if s.step {
				endX = x(end)
			}
			fmt.Fprintf(&b, `<path d="%s L%.1f %.1f L%.1f %.1f Z" fill="%s" opacity=".14"/>`, d.String(), endX, y(0), x(first.T), y(0), s.color)
		}
		dash := ""
		if s.dash {
			dash = ` stroke-dasharray="5 3"`
		}
		fmt.Fprintf(&b, `<path d="%s" fill="none" stroke="%s" stroke-width="2" stroke-linejoin="round"%s><title>%s</title></path>`, d.String(), s.color, dash, esc(s.label))
		if !s.step && len(s.points) <= 60 {
			for _, p := range s.points {
				fmt.Fprintf(&b, `<circle cx="%.1f" cy="%.1f" r="3" fill="%s"><title>%s: %s at %s</title></circle>`, x(p.T), y(p.V), s.color, esc(s.label), esc(format(p.V)), p.T.In(loc).Format("15:04:05"))
			}
		}
	}
	b.WriteString(`</svg>`)
	return b.String()
}

func bytesF(v float64) string { return model.Bytes(int64(v)) }
func msF(v float64) string    { return model.Duration(int64(v)) }
func pctF(v float64) string   { return fmt.Sprintf("%.0f%%", v) }

// stageLabel names a stage for a chart row.
func stageLabel(s *model.Stage) string {
	l := fmt.Sprintf("Stage %d", s.ID)
	if s.Attempt > 0 {
		l += fmt.Sprintf(" (attempt %d)", s.Attempt+1)
	}
	if name := strings.TrimSpace(s.Name); name != "" {
		l += ": " + name
	}
	return l
}

func stageMs(s *model.Stage) float64 {
	if s.Submitted.IsZero() || s.Completed.IsZero() {
		return 0
	}
	return float64(s.Completed.Sub(s.Submitted).Milliseconds())
}

// top returns up to n items with the highest key, highest first.
func top[T any](items []T, n int, key func(T) float64) []T {
	out := make([]T, 0, len(items))
	for _, it := range items {
		if key(it) > 0 {
			out = append(out, it)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return key(out[i]) > key(out[j]) })
	if len(out) > n {
		out = out[:n]
	}
	return out
}

// explorerStage links a stage to its explorer page, when there is one.
func explorerStage(explorer string, s *model.Stage) string {
	if explorer == "" {
		return ""
	}
	return fmt.Sprintf("%s#stage/%d.%d", explorer, s.ID, s.Attempt)
}

// stagesChart shows the longest stages with each one's task time spread:
// a long whisker is a straggler.
func stagesChart(r *model.Report, explorer string) template.HTML {
	longest := top(r.Jobs.Stages, 15, stageMs)
	var rows []hbar
	for _, s := range longest {
		color := cInput
		if s.Status == model.StatusFailed {
			color = cFail
		}
		rows = append(rows, hbar{label: stageLabel(s), href: explorerStage(explorer, s),
			segs: []seg{{stageMs(s), color, fmt.Sprintf("%s ran for %s, %s tasks", stageLabel(s), model.Duration(int64(stageMs(s))), model.Num(s.Totals.Tasks))}},
			note: model.Duration(int64(stageMs(s)))})
	}
	return chartBox("The longest stages", chartGuide{
		Run: runNotes(r)["stages"],
		Axes: [][2]string{
			{"Rows", "One stage each, the longest at the top."},
			{"Bar length", "How long the stage ran, from when it was submitted to when it finished."},
			{"Colour", "Red: the stage failed."},
		},
		Read: []string{
			"Shorter is better.",
			"The top few bars are where speeding things up shortens the run most.",
		},
	}, hbars(rows, msF),
		shown(rows, []legendItem{{cInput, "Stage run time"}, {cFail, "Failed stage"}}))
}

// spreadChart shows the spread of task times in the slowest stages: the
// box runs from the quarter to three-quarter mark, the whisker from the
// fastest to the slowest task, the tick is the median.
func spreadChart(r *model.Report, explorer string) template.HTML {
	longest := top(r.Jobs.Stages, 12, func(s *model.Stage) float64 { return float64(s.TaskDuration.Max) })
	var rows []hbar
	for _, s := range longest {
		d := s.TaskDuration
		if d.Count < 2 {
			continue
		}
		rows = append(rows, hbar{label: stageLabel(s), href: explorerStage(explorer, s),
			segs: []seg{{float64(d.P50), "transparent", ""}, {float64(d.P95 - d.P50), cInput, fmt.Sprintf("from the median %s to the 95th percentile %s", model.Duration(d.P50), model.Duration(d.P95))}},
			lo:   float64(d.Min), hi: float64(d.Max), mid: float64(d.P50),
			note: "slowest " + model.Duration(d.Max)})
	}
	return chartBox("Task time spread", chartGuide{
		Run: runNotes(r)["spread"],
		Axes: [][2]string{
			{"Rows", "The stages whose slowest task took longest."},
			{"Across", "How long a task took."},
			{"Marks", "The tick is the median task. The box runs from the median to the 95th percentile (the time 19 tasks in 20 beat). The line runs from the fastest task to the slowest."},
		},
		Read: []string{
			"A short line and a narrow box: the stage's tasks took similar times, which is what you want.",
			"A line reaching far past its box: one or two tasks (stragglers) took much longer than the rest and held the stage up, usually because their data was skewed.",
			"A wide box: many tasks were slow, not just one; the stage may need more, smaller partitions.",
		},
	}, hbars(rows, msF),
		[]legendItem{{cInput, "Median to 95th percentile"}, {"var(--ink)", "Fastest to slowest task, and the median"}})
}

// dataChart shows what each of the busiest stages read, shuffled, wrote and
// spilled.
func dataChart(r *model.Report, explorer string) template.HTML {
	moved := func(s *model.Stage) float64 {
		t := s.Totals
		return float64(t.InputBytes + t.ShuffleReadBytes + t.ShuffleWriteBytes + t.OutputBytes + t.DiskSpillBytes)
	}
	var rows []hbar
	for _, s := range top(r.Jobs.Stages, 15, moved) {
		t := s.Totals
		rows = append(rows, hbar{label: stageLabel(s), href: explorerStage(explorer, s), segs: []seg{
			{float64(t.InputBytes), cInput, "read from files and tables: " + model.Bytes(t.InputBytes)},
			{float64(t.ShuffleReadBytes), cShRead, "shuffle read: " + model.Bytes(t.ShuffleReadBytes)},
			{float64(t.ShuffleWriteBytes), cShWrite, "shuffle write: " + model.Bytes(t.ShuffleWriteBytes)},
			{float64(t.OutputBytes), cOutput, "written out: " + model.Bytes(t.OutputBytes)},
			{float64(t.DiskSpillBytes), cSpill, "spilled to disk: " + model.Bytes(t.DiskSpillBytes)},
		}, note: model.Bytes(int64(moved(s)))})
	}
	return chartBox("Data each stage moved", chartGuide{
		Run: runNotes(r)["data"],
		Axes: [][2]string{
			{"Rows", "The stages that moved the most data."},
			{"Bar length", "Bytes, split into what the stage read, shuffled in (shuffle read), shuffled out (shuffle write), wrote and spilled to disk."},
		},
		Read: []string{
			"Longer bars moved more data. Moving a lot of data is not a problem in itself.",
			"Shuffle is the costly part: it goes through local disk and across the network between executors.",
			"Spilled to disk should be absent: it means the data did not fit in memory.",
		},
	}, hbars(rows, bytesF),
		shown(rows, []legendItem{{cInput, "Read"}, {cShRead, "Shuffle read"}, {cShWrite, "Shuffle write"}, {cOutput, "Written"}, {cSpill, "Spilled to disk"}}))
}

// dataOverTime shows data read, shuffled and written, summed as stages
// finished: the shape of the application.
func dataOverTime(r *model.Report, loc *time.Location) template.HTML {
	stages := make([]*model.Stage, 0, len(r.Jobs.Stages))
	for _, s := range r.Jobs.Stages {
		if !s.Completed.IsZero() {
			stages = append(stages, s)
		}
	}
	if len(stages) < 2 {
		return ""
	}
	sort.SliceStable(stages, func(i, j int) bool { return stages[i].Completed.Before(stages[j].Completed) })
	start, end := r.Timeline.Start, r.Timeline.End
	if start.IsZero() || !end.After(start) {
		start, end = stages[0].Submitted, stages[len(stages)-1].Completed
	}
	var in, sh, out float64
	series := []line{{label: "Read", color: cInput, step: true}, {label: "Shuffle write", color: cShWrite, step: true}, {label: "Written", color: cOutput, step: true}}
	for i := range series {
		series[i].points = []model.Point{{T: start, V: 0}}
	}
	for _, s := range stages {
		in += float64(s.Totals.InputBytes)
		sh += float64(s.Totals.ShuffleWriteBytes)
		out += float64(s.Totals.OutputBytes)
		series[0].points = append(series[0].points, model.Point{T: s.Completed, V: in})
		series[1].points = append(series[1].points, model.Point{T: s.Completed, V: sh})
		series[2].points = append(series[2].points, model.Point{T: s.Completed, V: out})
	}
	if in+sh+out == 0 {
		return ""
	}
	return chartBox("Data over time", chartGuide{
		Run: runNotes(r)["dataOverTime"],
		Axes: [][2]string{
			{"Across", "Time of day, while the application ran."},
			{"Up", "Bytes so far: a running total of data read, shuffled out and written, added as each stage finished."},
		},
		Read: []string{
			"Steep rises are when the work happened.",
			"Long flat stretches are time spent not moving data: driver code, planning, or waiting for executors.",
		},
	}, lines(series, start, end, bytesF, loc),
		[]legendItem{{cInput, "Read"}, {cShWrite, "Shuffle write"}, {cOutput, "Written"}})
}

// spillChart shows memory and disk spill in the stages that spilled most.
func spillChart(r *model.Report) template.HTML {
	sp := top(r.Memory.Spill, 15, func(s model.StageSpill) float64 { return float64(s.MemoryBytes + s.DiskBytes) })
	var rows []hbar
	for _, s := range sp {
		rows = append(rows, hbar{label: fmt.Sprintf("Stage %d: %s", s.StageID, s.Name), segs: []seg{
			{float64(s.MemoryBytes), cShRead, "spilled from memory: " + model.Bytes(s.MemoryBytes)},
			{float64(s.DiskBytes), cSpill, "written to disk: " + model.Bytes(s.DiskBytes)},
		}, note: model.Bytes(s.DiskBytes) + " to disk"})
	}
	return chartBox("Spill by stage", chartGuide{
		Run: runNotes(r)["spill"],
		Axes: [][2]string{
			{"Rows", "The stages that spilled, most first."},
			{"Bar length", "Bytes spilled: the data's size as it was held in memory, and what it came to on disk (smaller, because it is compressed)."},
		},
		Read: []string{
			"None is ideal.",
			"Spill means tasks had less memory than their data needed, so Spark wrote part of it to disk and read it back, which is slow.",
			"More partitions (less data per task) or more memory per executor core help.",
		},
	}, hbars(rows, bytesF),
		shown(rows, []legendItem{{cShRead, "Spilled (size in memory)"}, {cSpill, "Written to disk"}}))
}

// timeChart shows where each executor's task time went: computing, garbage
// collection, or neither (waiting on shuffle, I/O or Python workers).
func timeChart(r *model.Report) template.HTML {
	var rows []hbar
	for _, x := range top(r.Executors.Executors, 25, func(x *model.Executor) float64 { return float64(x.Tasks.RunTimeMs) }) {
		t := x.Tasks
		cpu := float64(t.CPUTimeNs) / 1e6
		gc := float64(t.GCTimeMs)
		other := max(float64(t.RunTimeMs)-cpu-gc, 0)
		host, _, _ := strings.Cut(x.Host, ".")
		rows = append(rows, hbar{label: "Executor " + x.ID + " (" + host + ")", segs: []seg{
			{cpu, cInput, "computing: " + model.Duration(int64(cpu))},
			{gc, cShRead, "garbage collection: " + model.Duration(int64(gc))},
			{other, cNeutral, "other (waiting on shuffle, I/O or Python): " + model.Duration(int64(other))},
		}, note: model.Duration(t.RunTimeMs)})
	}
	return chartBox("Where executor time went", chartGuide{
		Run: runNotes(r)["execTime"],
		Axes: [][2]string{
			{"Rows", "One executor each."},
			{"Bar length", "The run time of all its tasks added up, split into computing on the JVM, garbage collection, and other or waiting (for shuffle data, storage or Python workers)."},
		},
		Read: []string{
			"More computing is better.",
			"Garbage collection above about 10% of a bar means memory pressure.",
			"A large other-or-waiting part means tasks waited instead of computing. In PySpark, time spent in Python counts there.",
		},
	}, hbars(rows, msF),
		shown(rows, []legendItem{{cInput, "Computing"}, {cShRead, "Garbage collection"}, {cNeutral, "Other or waiting"}}))
}

// splitChart shows where task time went in the stages with the most of
// it: starting, computing, garbage collection, shuffle, result handling
// and the rest (model.TimeSplit).
func splitChart(r *model.Report, explorer string) template.HTML {
	var rows []hbar
	for _, s := range top(r.Jobs.Stages, 10, func(s *model.Stage) float64 { return float64(s.Totals.TimeSplit().Total()) }) {
		p := s.Totals.TimeSplit()
		d := func(ms int64) string { return model.Duration(ms) }
		rows = append(rows, hbar{label: stageLabel(s), href: explorerStage(explorer, s), note: d(p.Total()), segs: []seg{
			{float64(p.SchedulerDelayMs + p.DeserializeMs), cSpill, "starting: " + d(p.SchedulerDelayMs+p.DeserializeMs) + " (scheduler delay " + d(p.SchedulerDelayMs) + ", deserializing " + d(p.DeserializeMs) + ")"},
			{float64(p.ComputeMs), cInput, "computing: " + d(p.ComputeMs)},
			{float64(p.GCMs), cShRead, "garbage collection: " + d(p.GCMs)},
			{float64(p.ShuffleFetchMs + p.ShuffleWriteMs), cShWrite, "shuffle: " + d(p.ShuffleFetchMs+p.ShuffleWriteMs) + " (waiting for data " + d(p.ShuffleFetchMs) + ", writing " + d(p.ShuffleWriteMs) + ")"},
			{float64(p.ResultMs), cOutput, "sending the result: " + d(p.ResultMs)},
			{float64(p.OtherMs), cNeutral, "other (file I/O, Python workers, waiting): " + d(p.OtherMs)},
		}})
	}
	return chartBox("Where stage time went", chartGuide{
		Run: runNotes(r)["split"],
		Axes: [][2]string{
			{"Rows", "The stages with the most task time."},
			{"Bar length", "Every task's time added up, split by what it was spent on: starting (scheduler delay and unpacking the task), computing, garbage collection, shuffle (waiting for data from other executors and writing it out), sending the result, and other (reading files, waiting on Python)."},
		},
		Read: []string{
			"More computing is better.",
			"A large starting share: tasks were too small, or the driver was busy.",
			"A large shuffle share: a lot of data moved between executors.",
			"Garbage collection above about 10%: memory pressure.",
			"A large other share: waiting on files, S3 or Python.",
		},
		Note: "Spark measures these separately and they can overlap a little, so the split is approximate.",
	},
		hbars(rows, msF), shown(rows, []legendItem{{cSpill, "Starting"}, {cInput, "Computing"}, {cShRead, "Garbage collection"}, {cShWrite, "Shuffle"}, {cOutput, "Sending the result"}, {cNeutral, "Other"}}))
}

// ranHere says what of the application ran on a host: "the driver and 2
// executors", or "" when nothing did.
func ranHere(h model.Host) string {
	var parts []string
	if h.Driver {
		parts = append(parts, "the driver")
	}
	if n := len(h.Executors); n > 0 {
		parts = append(parts, model.Plural(n, "executor", "executors"))
	}
	return strings.Join(parts, " and ")
}

// unknownCapacity lists the hosts that ran part of the application but
// have no YARN capacity to draw, as "ip-10-0-2-13 (the driver and 2
// executors)", so the node chart never leaves one out silently. Without
// the cluster's logs no node has a capacity, and the Sources panel already
// says the logs were not read, so it lists none.
func unknownCapacity(r *model.Report) []string {
	if r.Logs == nil {
		return nil
	}
	var out []string
	for _, h := range r.Nodes.Hosts {
		if ran := ranHere(h); h.YARNMemoryBytes <= 0 && ran != "" {
			short, _, _ := strings.Cut(h.Name, ".")
			out = append(out, short+" ("+ran+")")
		}
	}
	return out
}

// unknownNote names the nodes that ran part of the application but whose
// YARN capacity is not in the logs read, so they are not drawn.
func unknownNote(nodes []string) string {
	if len(nodes) == 0 {
		return ""
	}
	return "Not drawn, because the logs read do not say how much memory their NodeManager offered YARN (its registration and container placement lines may have rotated out of the ResourceManager's log): " + strings.Join(nodes, "; ") + "."
}

// execColors tell a node's executor containers apart, one colour each in
// turn; never the driver's orange.
var execColors = []string{cInput, cShWrite, cOutput, cSpill}

// nodeTitle is the node chart's heading.
const nodeTitle = "CPU and memory the executors took on each node"

// execPiece is one executor container drawn on a node: its executor, when
// the node's executors at once are the ones that ran there, and its vCPUs.
type execPiece struct {
	id    string
	cores int
}

// nodeExecPieces lists the executor containers a node held at its busiest.
// When more executors ran there over the run than at once (some replaced
// others), which ones were together is not known, so none is named.
func nodeExecPieces(h model.Host, cores map[string]int) []execPiece {
	n := h.PeakExecutors
	if n == 0 {
		n = len(h.Executors)
	}
	ids := slices.Clone(h.Executors)
	slices.SortFunc(ids, func(a, b string) int {
		if idLess(a, b) {
			return -1
		}
		if idLess(b, a) {
			return 1
		}
		return 0
	})
	out := make([]execPiece, n)
	for i := range out {
		if len(ids) == n {
			out[i] = execPiece{id: ids[i], cores: cores[ids[i]]}
		} else if len(ids) > 0 {
			out[i].cores = cores[ids[0]]
		}
	}
	return out
}

// pieceText is what an executor container says inside its bar, longest
// first: "Executor 3 · 4 vCPU", "E3 · 4 vCPU", "4 vCPU", "4".
func pieceText(p execPiece) []string {
	var t []string
	cpu := ""
	if p.cores > 0 {
		cpu = fmt.Sprintf("%d vCPU", p.cores)
	}
	switch {
	case p.id != "" && cpu != "":
		t = append(t, "Executor "+p.id+" · "+cpu, "E"+p.id+" · "+cpu, cpu, strconv.Itoa(p.cores))
	case p.id != "":
		t = append(t, "Executor "+p.id, "E"+p.id)
	case cpu != "":
		t = append(t, "Executor · "+cpu, cpu, strconv.Itoa(p.cores))
	}
	return t
}

// nodeMemoryChart shows, for each node, the memory and vCPUs this
// application's containers took of what the node offered YARN: each
// executor container in its own colour with its vCPUs, so the executors
// can be counted, and the executor-fit problem shows in one picture.
func nodeMemoryChart(r *model.Report) template.HTML {
	cores := map[string]int{}
	for _, x := range r.Executors.Executors {
		cores[x.ID] = x.Cores
	}
	var rows []hbar
	for _, h := range r.Nodes.Hosts {
		if h.YARNMemoryBytes <= 0 {
			continue // named in the note (unknownCapacity)
		}
		pieces := nodeExecPieces(h, cores)
		execs := h.ExecutorContainerBytes * int64(len(pieces))
		free := max(h.YARNMemoryBytes-h.DriverContainerBytes-execs, 0)
		short, _, _ := strings.Cut(h.Name, ".")
		segs := []seg{{float64(h.DriverContainerBytes), cShRead, "the driver's container: " + model.Bytes(h.DriverContainerBytes)}}
		texts := [][]string{{"Driver", "D"}}
		vcpu := 0
		for i, p := range pieces {
			who := "an executor container"
			if p.id != "" {
				who = "executor " + p.id + "'s container"
			}
			title := fmt.Sprintf("%s: %s", who, model.Bytes(h.ExecutorContainerBytes))
			if p.cores > 0 {
				title += fmt.Sprintf(", %d vCPU", p.cores)
			}
			segs = append(segs, seg{float64(h.ExecutorContainerBytes), execColors[i%len(execColors)], title})
			texts = append(texts, pieceText(p))
			vcpu += p.cores
		}
		segs = append(segs, seg{float64(free), "transparent", fmt.Sprintf("free: %s of the %s YARN offered on %s", model.Bytes(free), model.Bytes(h.YARNMemoryBytes), h.Name)})
		note := model.Bytes(h.YARNMemoryBytes-free) + " of " + model.Bytes(h.YARNMemoryBytes)
		if vcpu > 0 && h.YARNVCores > 0 {
			note += fmt.Sprintf(" · %d of %d vCPU", vcpu, h.YARNVCores)
		}
		rows = append(rows, hbar{label: short, max: float64(h.YARNMemoryBytes), segs: segs, texts: texts, note: note})
	}
	unknown := unknownCapacity(r)
	if len(rows) == 0 {
		if len(unknown) == 0 {
			return ""
		}
		// Nothing to draw, but nodes ran the application: say so, rather
		// than leave the chart out as if there were nothing to see.
		return template.HTML(`<div class="chart"><h4>` + nodeTitle + `</h4><p class="cap">` + esc(unknownNote(unknown)) + `</p></div>`)
	}
	return chartBox(nodeTitle, chartGuide{
		Run: runNotes(r)["nodeMemory"],
		Axes: [][2]string{
			{"Rows", "One worker node each."},
			{"Bar length", "The memory the node offered YARN. Coloured pieces are what this application took at its busiest: the driver's container, then one piece per executor container, each in its own colour. The rest is free."},
			{"Inside a piece", "Which executor it is and the vCPUs it had, such as \"Executor 3 · 4 vCPU\" (shortened when the piece is narrow)."},
			{"Right", "Memory taken of what the node offered, and vCPUs taken of what it offered."},
		},
		Read: []string{
			"Count the coloured pieces to see how many executors ran on the node at once.",
			"A full bar: the node was used well.",
			"Free space helps only if it is at least one executor container wide. Smaller gaps are memory paid for but unusable.",
			"A mostly free node did little work for this run.",
		},
		Note: unknownNote(unknown),
	}, hbars(rows, bytesF),
		[]legendItem{{cShRead, "Driver container"}, {cInput, "Executor containers (one colour each)"}, {"var(--surface-2)", "Free"}})
}

// nodeCPUChart shows each node's CPU from CloudWatch over the run.
func nodeCPUChart(r *model.Report, loc *time.Location) template.HTML {
	m := r.Metrics
	if m == nil {
		return ""
	}
	colors := []string{cInput, cShRead, cShWrite, cOutput, cSpill}
	name := map[string]string{}
	for _, h := range r.Nodes.Hosts {
		if h.Instance != nil {
			who := "idle"
			switch {
			case h.Driver && len(h.Executors) > 0:
				who = "driver and executors"
			case h.Driver:
				who = "driver"
			case len(h.Executors) > 0:
				who = fmt.Sprintf("%d executors", len(h.Executors))
			case h.Instance.Primary || h.Instance.Role == "MASTER":
				who = "primary"
			}
			name[h.Instance.ID] = h.Instance.ID + " (" + who + ")"
		}
	}
	var series []line
	var legend []legendItem
	for _, s := range m.Hosts {
		if s.Name != "CPUUtilization" || s.Stat != "Average" || len(s.Points) == 0 || len(series) == len(colors) {
			continue
		}
		label := name[s.Scope]
		if label == "" {
			label = s.Scope
		}
		c := colors[len(series)]
		series = append(series, line{label: label, color: c, points: s.Points})
		legend = append(legend, legendItem{c, label})
	}
	return chartBox("Node CPU", chartGuide{
		Run: runNotes(r)["nodeCPU"],
		Axes: [][2]string{
			{"Across", "Time of day, while the application ran."},
			{"Up", "CPU use of the whole machine, from 0 to 100%, one line per node, averaged over EC2's 5-minute periods."},
		},
		Read: []string{
			"Above about 85% for long: tasks queued for CPU.",
			"Low CPU on a node that ran executors: its tasks were waiting on disk, the network or Python rather than computing.",
			"Daemons and other applications on the node count too.",
		},
	}, lines(series, m.From, m.To, pctF, loc), legend)
}

// hbaseLoadChart shows how many HBase scan tasks each region server served
// at once over the run: the five busiest, and all of them together.
func hbaseLoadChart(r *model.Report, loc *time.Location) template.HTML {
	h := r.HBase
	if h == nil || len(h.Load) == 0 {
		return ""
	}
	colors := []string{cInput, cShRead, cShWrite, cOutput, cSpill}
	var series []line
	var legend []legendItem
	for i, l := range h.Load {
		if i == len(colors) {
			break
		}
		series = append(series, line{label: shortHost(l.Server), color: colors[i], points: l.Points, step: true})
		legend = append(legend, legendItem{colors[i], shortHost(l.Server)})
	}
	if len(h.Load) > 1 {
		series = append(series, line{label: "All region servers", color: cNeutral, points: h.LoadTotal, step: true, dash: true})
		legend = append(legend, legendItem{cNeutral, "All region servers (dashed)"})
	}
	note := ""
	if len(h.Load) > len(colors) {
		note = fmt.Sprintf("The %d busiest of %d region servers; the table below lists them all.", len(colors), len(h.Load))
	}
	end := h.LoadTo.Add(time.Duration(h.LoadStepMs) * time.Millisecond)
	return chartBox("Region server load over time", chartGuide{
		Run: runNotes(r)["hbaseLoad"],
		Axes: [][2]string{
			{"Across", "Time of day, while the run's HBase scans ran."},
			{"Up", fmt.Sprintf("HBase scan tasks reading from the region server at once: the most in each %s step. One line per region server, the dashed line all of them together.", model.Duration(h.LoadStepMs))},
		},
		Read: []string{
			"Lines of similar height: the reads were spread across the region servers.",
			"One line close to the dashed line while the others sit low: that server served almost all the reads, and its regions' reads queued on it.",
			"A server's line that stays up after the others drop: its regions took longest, and the stage waited on them.",
		},
		Note: note,
	}, lines(series, h.LoadFrom, end, countF, loc), legend)
}

func countF(v float64) string { return model.Num(int64(v)) }

// queriesChart shows the longest SQL and DataFrame queries.
func queriesChart(r *model.Report, explorer string) template.HTML {
	dur := func(q *model.SQLQuery) float64 {
		if q.Start.IsZero() || q.End.IsZero() {
			return 0
		}
		return float64(q.End.Sub(q.Start).Milliseconds())
	}
	var rows []hbar
	for _, q := range top(r.Jobs.SQL, 12, dur) {
		color := cInput
		if q.Error != "" {
			color = cFail
		}
		href := ""
		if explorer != "" {
			href = fmt.Sprintf("%s#query/%d", explorer, q.ID)
		}
		rows = append(rows, hbar{label: fmt.Sprintf("Query %d: %s", q.ID, q.Description), href: href,
			segs: []seg{{dur(q), color, q.Description}}, note: model.Duration(int64(dur(q)))})
	}
	return chartBox("The longest queries", chartGuide{
		Run: runNotes(r)["queries"],
		Axes: [][2]string{
			{"Rows", "One SQL statement or DataFrame action each, the longest at the top."},
			{"Bar length", "How long it ran."},
			{"Colour", "Red: the query failed."},
		},
		Read: []string{
			"Shorter is better.",
			"The longest queries are where tuning pays off; open one in the explorer to see its plan.",
		},
	}, hbars(rows, msF),
		shown(rows, []legendItem{{cInput, "Query run time"}, {cFail, "Failed query"}}))
}

// chartFuncs are the report template's chart functions; explorer links
// chart rows to the explorer's pages when it was written.
func chartFuncs(loc *time.Location, explorer string) template.FuncMap {
	return template.FuncMap{
		"stagesChart":  func(r *model.Report) template.HTML { return stagesChart(r, explorer) },
		"spreadChart":  func(r *model.Report) template.HTML { return spreadChart(r, explorer) },
		"dataChart":    func(r *model.Report) template.HTML { return dataChart(r, explorer) },
		"dataOverTime": func(r *model.Report) template.HTML { return dataOverTime(r, loc) },
		"spillChart":   spillChart,
		"guide": func(axes [][2]string, read []string, note string, run []runPoint) template.HTML {
			return chartGuide{Axes: axes, Read: read, Note: note, Run: run}.html()
		},
		"runNotes": func(r *model.Report, chart string) []runPoint { return runNotes(r)[chart] },
		"axes": func(kv ...string) [][2]string {
			var out [][2]string
			for i := 0; i+1 < len(kv); i += 2 {
				out = append(out, [2]string{kv[i], kv[i+1]})
			}
			return out
		},
		"points":          func(p ...string) []string { return p },
		"anatomy":         func(r *model.Report) template.HTML { return anatomyHTML(r, explorer) },
		"timeChart":       timeChart,
		"splitChart":      func(r *model.Report) template.HTML { return splitChart(r, explorer) },
		"nodeMemoryChart": nodeMemoryChart,
		"nodeCPUChart":    func(r *model.Report) template.HTML { return nodeCPUChart(r, loc) },
		"queriesChart":    func(r *model.Report) template.HTML { return queriesChart(r, explorer) },
		"hbaseLoadChart":  func(r *model.Report) template.HTML { return hbaseLoadChart(r, loc) },
		"flowChart":       func(r *model.Report) template.HTML { return flowChart(r, loc) },
		"memoryChart":     func(r *model.Report) template.HTML { return memoryChart(r, loc) },
	}
}
