package report

import (
	"fmt"
	"html/template"
	"sort"
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

// chartBox wraps a chart with a title, a caption and a legend.
func chartBox(title, caption string, svg string, legend []legendItem) template.HTML {
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
	if caption != "" {
		fmt.Fprintf(&b, `<p class="cap">%s</p>`, esc(caption))
	}
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
	// Drawn for a half-width column; the style keeps text from growing
	// too large when the chart spans the page.
	const W = 620.0
	ml, mr, rowH := 196.0, 150.0, 22.0
	iw := W - ml - mr
	H := float64(len(rows))*rowH + 8
	var b strings.Builder
	// A chart of a few rows stays small rather than filling a wide page.
	maxW := 860
	if len(rows) <= 3 {
		maxW = 700
	}
	fmt.Fprintf(&b, `<svg viewBox="0 0 %.0f %.0f" role="img" style="max-width:%dpx">`, W, H, maxW)
	x := func(v float64) float64 { return ml + v/scale*iw }
	for i, r := range rows {
		y := 4 + float64(i)*rowH
		label := esc(clipLabel(r.label, 28))
		if r.href != "" {
			fmt.Fprintf(&b, `<a href="%s"><text class="lbl" x="%.1f" y="%.1f" text-anchor="end">%s<title>%s</title></text></a>`, esc(r.href), ml-8, y+13, label, esc(r.label))
		} else {
			fmt.Fprintf(&b, `<text class="lbl" x="%.1f" y="%.1f" text-anchor="end">%s<title>%s</title></text>`, ml-8, y+13, label, esc(r.label))
		}
		if r.max > 0 {
			fmt.Fprintf(&b, `<rect x="%.1f" y="%.1f" width="%.1f" height="14" rx="3" fill="var(--surface-2)" stroke="var(--line)"/>`, ml, y+2, x(r.max)-ml)
		}
		at := 0.0
		for _, s := range r.segs {
			if s.v <= 0 {
				continue
			}
			w := s.v / scale * iw
			// A 1 px surface gap between stacked pieces keeps them apart.
			fmt.Fprintf(&b, `<rect x="%.1f" y="%.1f" width="%.1f" height="14" rx="2" fill="%s"><title>%s</title></rect>`, x(at)+0.5, y+2, max(w-1, 1), s.color, esc(s.title))
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
	const W, H = 620.0, 240.0
	ml, mr, mt, mb := 64.0, 14.0, 14.0, 32.0
	iw, ih := W-ml-mr, H-mt-mb
	span := float64(end.Sub(start))
	x := func(t time.Time) float64 { return ml + float64(t.Sub(start))/span*iw }
	y := func(v float64) float64 { return mt + ih - v/peak*ih }
	var b strings.Builder
	fmt.Fprintf(&b, `<svg viewBox="0 0 %.0f %.0f" role="img" style="max-width:860px">`, W, H)
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
	return chartBox("The longest stages", "Each bar is how long a stage ran, from submission to completion. Red means it failed.", hbars(rows, msF),
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
	return chartBox("Task time spread", "For the stages with the slowest tasks: the tick is the median task, the box runs from the median to the 95th percentile, and the line from the fastest to the slowest task. A line reaching far past the box is a straggler, often skew.", hbars(rows, msF),
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
	return chartBox("Data each stage moved", "The stages that moved the most data, split into what they read, shuffled between executors, wrote out and spilled to disk.", hbars(rows, bytesF),
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
	return chartBox("Data over time", "Bytes read, shuffled and written, added up as each stage finished: a steep rise is where the work happened.", lines(series, start, end, bytesF, loc),
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
	return chartBox("Spill by stage", "Data Spark had to move out of memory while running each stage (as held in memory), and what that came to on disk. Spill means tasks had less memory than their data needed.", hbars(rows, bytesF),
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
	return chartBox("Where executor time went", "Task run time on each executor: computing on the JVM, collecting garbage, or neither, which is waiting for shuffle data, storage or Python workers.", hbars(rows, msF),
		shown(rows, []legendItem{{cInput, "Computing"}, {cShRead, "Garbage collection"}, {cNeutral, "Other or waiting"}}))
}

// nodeMemoryChart shows what YARN placed on each node against what the
// node offered: the executor-fit problem in one picture.
func nodeMemoryChart(r *model.Report) template.HTML {
	var rows []hbar
	for _, h := range r.Nodes.Hosts {
		if h.YARNMemoryBytes <= 0 {
			continue
		}
		n := h.PeakExecutors
		if n == 0 {
			n = len(h.Executors)
		}
		execs := h.ExecutorContainerBytes * int64(n)
		free := max(h.YARNMemoryBytes-h.DriverContainerBytes-execs, 0)
		note := model.Bytes(free) + " free"
		short, _, _ := strings.Cut(h.Name, ".")
		rows = append(rows, hbar{label: short, max: float64(h.YARNMemoryBytes), segs: []seg{
			{float64(h.DriverContainerBytes), cShRead, "the driver's container: " + model.Bytes(h.DriverContainerBytes)},
			{float64(execs), cInput, fmt.Sprintf("%d executor containers at once: %s", n, model.Bytes(execs))},
			{float64(free), "transparent", fmt.Sprintf("free: %s of the %s YARN offered on %s", model.Bytes(free), model.Bytes(h.YARNMemoryBytes), h.Name)},
		}, note: note})
	}
	if len(rows) == 0 {
		return ""
	}
	return chartBox("What YARN placed on each node", "Each node's YARN memory, and the containers of this application on it. Free space smaller than one executor's container cannot hold another executor.", hbars(rows, bytesF),
		[]legendItem{{cShRead, "Driver container"}, {cInput, "Executor containers"}, {"var(--surface-2)", "Free"}})
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
	return chartBox("Node CPU", "Each node's CPU from CloudWatch, averaged over EC2's 5-minute periods; the whole machine, so daemons and other applications count too.", lines(series, m.From, m.To, pctF, loc), legend)
}

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
	return chartBox("The longest queries", "Each SQL statement or DataFrame action, by how long it ran. Red means it failed.", hbars(rows, msF),
		shown(rows, []legendItem{{cInput, "Query run time"}, {cFail, "Failed query"}}))
}

// chartFuncs are the report template's chart functions; explorer links
// chart rows to the explorer's pages when it was written.
func chartFuncs(loc *time.Location, explorer string) template.FuncMap {
	return template.FuncMap{
		"stagesChart":     func(r *model.Report) template.HTML { return stagesChart(r, explorer) },
		"spreadChart":     func(r *model.Report) template.HTML { return spreadChart(r, explorer) },
		"dataChart":       func(r *model.Report) template.HTML { return dataChart(r, explorer) },
		"dataOverTime":    func(r *model.Report) template.HTML { return dataOverTime(r, loc) },
		"spillChart":      spillChart,
		"timeChart":       timeChart,
		"nodeMemoryChart": nodeMemoryChart,
		"nodeCPUChart":    func(r *model.Report) template.HTML { return nodeCPUChart(r, loc) },
		"queriesChart":    func(r *model.Report) template.HTML { return queriesChart(r, explorer) },
	}
}
