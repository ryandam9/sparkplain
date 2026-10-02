package report

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// runPoint is one line of a chart's "In this run": what this run's chart
// shows, from the analysis, and the finding it points at, when there is one
// (1-based, as the findings are numbered on both pages).
type runPoint struct {
	Text    string `json:"text"`
	Finding int    `json:"finding,omitempty"`
}

// chartRules are the finding rules each chart is evidence for. A finding
// that fired is linked from the chart's "In this run", so a reader looking
// at the chart is pointed at the problem it shows.
var chartRules = map[string][]string{
	"stages":     {"job-failed", "stage-retried"},
	"spread":     {"stage-skew"},
	"skew":       {"stage-skew"},
	"health":     {"stage-skew", "memory-spill", "stage-retried"},
	"heatmap":    {"stage-skew", "executors-excluded"},
	"data":       {"memory-spill"},
	"spill":      {"memory-spill"},
	"execTime":   {"memory-gc-pressure", "cpu-low", "cpu-idle-executors"},
	"split":      {"scheduler-delay", "memory-gc-pressure", "large-results"},
	"nodeMemory": {"executor-fit", "idle-nodes"},
	"nodeCPU":    {"host-cpu-saturated", "cpu-low"},
	"containers": {"waited-for-capacity", "shared-cluster"},
	"executors":  {"executor-lost", "executor-memory-kill", "spot-interrupted", "executor-decommissioned", "slow-executor-startup"},
	"lifetimes":  {"executor-lost", "executor-memory-kill", "spot-interrupted", "executor-decommissioned", "slow-executor-startup"},
	"jobs":       {"driver-gaps", "job-failed"},
	"timeline":   {"driver-gaps", "job-failed", "executor-lost", "waited-for-capacity"},
	"running":    {"driver-gaps", "cpu-idle-executors"},
	"chain":      {"driver-gaps"},
	"peakHeap":   {"memory-heap-near-limit", "memory-over-provisioned", "out-of-memory"},
	"hbaseLoad":  {"hbase-server-load", "hbase-hotspot"},
	"flows":      {"shuffle-network", "task-spill", "cache-evicted"},
	"memory":     {"cache-evicted"},
}

// runNotes says, for each chart, what this run's version of it shows: a
// line or two worked out from the analysis, then the findings the chart is
// evidence for. A chart with nothing to point out says so, so the reader
// knows it was looked at. Keys are the chart names in chartRules.
func runNotes(r *model.Report) map[string][]runPoint {
	n := map[string][]runPoint{}
	add := func(chart, format string, args ...any) {
		n[chart] = append(n[chart], runPoint{Text: fmt.Sprintf(format, args...)})
	}
	runMs := r.Application.DurationMs
	share := func(ms int64) string {
		if runMs <= 0 {
			return ""
		}
		return fmt.Sprintf(" (%s of the run)", model.Percent(float64(ms)/float64(runMs)))
	}
	stages := r.Jobs.Stages

	// Stages: the longest, and failures.
	var longest *model.Stage
	failed := 0
	for _, st := range stages {
		if st.Status == "failed" {
			failed++
		}
		if d := span(st.Submitted, st.Completed); d > 0 && (longest == nil || d > span(longest.Submitted, longest.Completed)) {
			longest = st
		}
	}
	if longest != nil {
		d := span(longest.Submitted, longest.Completed)
		add("stages", "Stage %d (%s) ran longest: %s%s.", longest.ID, shortName(longest.Name), model.Duration(d), share(d))
	}
	if failed > 0 {
		add("stages", "%s failed.", model.Plural(failed, "stage", "stages"))
	}

	// Stragglers: the stage whose slowest task was furthest past its median.
	var worst *model.Stage
	// A straggler costs real time: its slowest task ran at least a second
	// past the median, so millisecond-long tasks do not count.
	ratio := func(st *model.Stage) float64 {
		d := st.TaskDuration
		if d.Count < 3 || d.P50 <= 0 || d.Max-d.P50 < 1000 {
			return 0
		}
		return float64(d.Max) / float64(d.P50)
	}
	for _, st := range stages {
		if ratio(st) > 0 && (worst == nil || ratio(st) > ratio(worst)) {
			worst = st
		}
	}
	for _, chart := range []string{"spread", "skew"} {
		switch {
		case worst == nil:
			add(chart, "No stage had a task run a second or more past its median.")
		case ratio(worst) >= 3:
			add(chart, "Stage %d's slowest task took %.1f× its median task (%s against %s): a straggler held it up.", worst.ID, ratio(worst), model.Duration(worst.TaskDuration.Max), model.Duration(worst.TaskDuration.P50))
		default:
			add(chart, "No stage had a task over 3× its median; the most was %.1f×, in stage %d.", ratio(worst), worst.ID)
		}
	}

	// Data: the biggest mover and the run's totals.
	var mover *model.Stage
	moved := func(st *model.Stage) int64 {
		t := st.Totals
		return t.InputBytes + t.ShuffleReadBytes + t.ShuffleWriteBytes + t.OutputBytes
	}
	for _, st := range stages {
		if moved(st) > 0 && (mover == nil || moved(st) > moved(mover)) {
			mover = st
		}
	}
	if mover != nil {
		add("data", "Stage %d moved the most: %s.", mover.ID, model.Bytes(moved(mover)))
	}
	if t := r.IO.Totals; t.InputBytes+t.ShuffleWriteBytes+t.OutputBytes > 0 {
		add("dataOverTime", "The run read %s, shuffled %s and wrote %s.", model.Bytes(t.InputBytes), model.Bytes(t.ShuffleWriteBytes), model.Bytes(t.OutputBytes))
	}

	// Spill.
	if m := r.Memory; m.TotalMemSpill > 0 {
		top := m.Spill[0]
		for _, s := range m.Spill {
			if s.MemoryBytes > top.MemoryBytes {
				top = s
			}
		}
		text := fmt.Sprintf("%s spilled %s (%s on disk); the most was stage %d, with %s.", model.Plural(len(m.Spill), "stage", "stages"),
			model.Bytes(m.TotalMemSpill), model.Bytes(m.TotalDiskSpill), top.StageID, model.Bytes(top.MemoryBytes))
		for _, c := range []string{"spill", "data"} {
			n[c] = append(n[c], runPoint{Text: text})
		}
	} else if r.EventLog != nil {
		add("spill", "No stage spilled.")
	}

	// Where executor time went.
	if c := r.CPU; c.RunMs > 0 {
		add("execTime", "Tasks spent %s of their run time computing on the JVM, and %s in garbage collection.", model.Percent(c.Share), model.Percent(r.Memory.GCShare))
		var gcWorst *model.ExecMemory
		for i := range r.Memory.Executors {
			x := &r.Memory.Executors[i]
			if gcWorst == nil || x.GCShare > gcWorst.GCShare {
				gcWorst = x
			}
		}
		if gcWorst != nil && gcWorst.GCShare >= 0.1 {
			add("execTime", "Executor %s spent %s of its task time in garbage collection, over the 10%% mark.", gcWorst.ID, model.Percent(gcWorst.GCShare))
		}
	}
	if c := r.CPU; c.AllocatedCoreMs > 0 {
		add("running", "Tasks kept %s of the task slots busy on average (%s of %s core time).", model.Percent(float64(c.RunMs)/float64(c.AllocatedCoreMs)), model.Duration(c.RunMs), model.Duration(c.AllocatedCoreMs))
	}

	// Where stage time went: of the stages the chart shows (the ten with
	// the most task time), the one that lost the most time to anything
	// but computing, and what took most of it. The same split as the bars.
	var low *model.Stage
	for _, st := range top(stages, 10, func(s *model.Stage) float64 { return float64(s.Totals.TimeSplit().Total()) }) {
		p, q := st.Totals.TimeSplit(), model.TimeSplit{}
		if low != nil {
			q = low.Totals.TimeSplit()
		}
		if p.Total() > 0 && (low == nil || p.Total()-p.ComputeMs > q.Total()-q.ComputeMs) {
			low = st
		}
	}
	if low != nil {
		p := low.Totals.TimeSplit()
		parts := []struct {
			name string
			ms   int64
		}{{"starting", p.SchedulerDelayMs + p.DeserializeMs}, {"garbage collection", p.GCMs}, {"shuffle", p.ShuffleFetchMs + p.ShuffleWriteMs},
			{"sending the result", p.ResultMs}, {"other (files, S3 or Python)", p.OtherMs}}
		most := parts[0]
		for _, x := range parts {
			if x.ms > most.ms {
				most = x
			}
		}
		comp := float64(p.ComputeMs) / float64(p.Total())
		if comp >= 0.5 {
			add("split", "Every stage shown computed at least half its task time; the least was stage %d, at %s.", low.ID, model.Percent(comp))
		} else {
			add("split", "Stage %d lost the most time to anything but computing: it computed %s of its %s of task time, and %s went on %s.", low.ID,
				model.Percent(comp), model.Duration(p.Total()), model.Percent(float64(most.ms)/float64(p.Total())), most.name)
		}
	}

	// Nodes: one that ran only the driver and why, and ones that ran
	// nothing though YARN offered them memory.
	shared := slices.ContainsFunc(r.Findings, func(f model.Finding) bool { return f.Rule == "shared-cluster" })
	var idle []string
	for _, h := range r.Nodes.Hosts {
		if s := h.NoRoomBesideDriver(); s != "" {
			add("nodeMemory", "%s ran only the driver. %s", shortHost(h.Name), s)
		}
		if h.YARNMemoryBytes > 0 && !h.Driver && len(h.Executors) == 0 {
			idle = append(idle, shortHost(h.Name))
		}
	}
	if len(idle) > 0 {
		text := fmt.Sprintf("%s ran nothing for this application, though YARN offered %s memory.", strings.Join(idle, ", "), map[bool]string{true: "it", false: "them"}[len(idle) == 1])
		if shared {
			text += " Other applications ran on the cluster at the same time and may have used it."
		}
		add("nodeMemory", "%s", text)
	}
	var busy *model.Host
	for i := range r.Nodes.Hosts {
		h := &r.Nodes.Hosts[i]
		if h.HostCPU != nil && (busy == nil || h.HostCPU.Average > busy.HostCPU.Average) {
			busy = h
		}
	}
	if busy != nil {
		verdict := "On average that is under the 85% mark, so tasks did not wait long for CPU."
		if busy.HostCPU.Average >= 85 {
			verdict = "That is over the 85% mark on average: tasks queued for CPU."
		}
		add("nodeCPU", "The busiest node, %s, averaged %.0f%% CPU and peaked at %.0f%%. %s", shortHost(busy.Name), busy.HostCPU.Average, busy.HostCPU.Peak, verdict)
	}

	// Executors.
	if e := r.Executors; e.Started > 0 {
		early := 0
		for _, x := range e.Executors {
			switch x.RemovalKind {
			case model.RemovalLost, model.RemovalMemoryKill, model.RemovalDecommissioned:
				early++
			}
		}
		text := fmt.Sprintf("%s started, %d at most at once; none was lost, killed or removed with its node.", model.Plural(e.Started, "executor", "executors"), e.Peak)
		if early > 0 {
			text = fmt.Sprintf("%s started, %d at most at once; %d ended early: lost, killed or removed with its node.", model.Plural(e.Started, "executor", "executors"), e.Peak, early)
		}
		for _, c := range []string{"executors", "lifetimes"} {
			n[c] = append(n[c], runPoint{Text: text})
		}
	}
	if m := r.Memory; m.HeapKnown && m.Config.HeapBytes > 0 && len(m.Executors) > 0 {
		top := m.Executors[0]
		for _, x := range m.Executors {
			if x.PeakHeap > top.PeakHeap {
				top = x
			}
		}
		add("peakHeap", "The highest was executor %s: %s of %s (%s).", top.ID, model.Bytes(top.PeakHeap), model.Bytes(m.Config.HeapBytes), model.Percent(float64(top.PeakHeap)/float64(m.Config.HeapBytes)))
	}

	// Jobs, gaps and queries.
	if len(r.Jobs.Jobs) > 0 {
		text := fmt.Sprintf("%s ran.", model.Plural(len(r.Jobs.Jobs), "job", "jobs"))
		if g := r.Jobs.DriverGapMs; g > 0 {
			text = fmt.Sprintf("%s ran. For %s%s no job was running, so the executors waited on the driver.", model.Plural(len(r.Jobs.Jobs), "job", "jobs"), model.Duration(g), share(g))
		}
		for _, c := range []string{"jobs", "timeline"} {
			n[c] = append(n[c], runPoint{Text: text})
		}
	}
	if len(r.Jobs.RunPath) > 0 {
		var top model.PathStep
		for _, p := range r.Jobs.RunPath {
			if p.DurationMs() > top.DurationMs() {
				top = p
			}
		}
		what := map[string]string{model.PathDriver: "a pause on the driver, with no job running", model.PathWaiting: "a pause while Spark scheduled the next stage"}[top.Kind]
		if what == "" {
			what = fmt.Sprintf("stage %d", top.StageID)
		}
		add("chain", "The longest step on the chain was %s: %s%s.", what, model.Duration(top.DurationMs()), share(top.DurationMs()))
	}
	var q *model.SQLQuery
	for _, s := range r.Jobs.SQL {
		if d := span(s.Start, s.End); d > 0 && (q == nil || d > span(q.Start, q.End)) {
			q = s
		}
	}
	if q != nil {
		add("queries", "Query %d ran longest: %s%s.", q.ID, model.Duration(span(q.Start, q.End)), share(span(q.Start, q.End)))
	}

	// HBase region server load: the busiest server.
	if h := r.HBase; h != nil && len(h.Load) > 0 {
		l, all := h.Load[0], span(h.LoadFrom, h.LoadTo)
		add("hbaseLoad", "%s did the most scan work: %s of task time, up to %d tasks at once, busy for %s of the %s the scans ran, across %s.",
			shortHost(l.Server), model.Duration(l.TaskMs), l.Peak, model.Duration(l.BusyMs), model.Duration(all), model.Plural(len(h.Load), "region server", "region servers"))
	}

	// Data moved and storage memory, from the executors' logs.
	if f := r.Flows; f != nil && len(f.Local) > 0 {
		all := f.LocalBytes + f.RemoteBytes
		peak := 0.0
		for i := range f.Local {
			peak = max(peak, f.Local[i].V+f.Remote[i].V)
		}
		if all > 0 {
			add("flows", "Tasks read %s of shuffle data, %s of it over the network; the most in one %s step was %s.",
				model.Bytes(all), model.Percent(float64(f.RemoteBytes)/float64(all)), model.Duration(f.StepMs), model.Bytes(int64(peak)))
		} else {
			add("flows", "No shuffle reads were logged; the chart shows what was spilled and cached.")
		}
	}
	if f := r.Flows; f != nil {
		var low *model.ExecFlow
		for i := range f.Executors {
			if e := &f.Executors[i]; len(e.Free) > 0 && (low == nil || e.MinFree < low.MinFree) {
				low = e
			}
		}
		if low != nil {
			add("memory", "Executor %s came closest to a full cache: %s of storage memory free at the least.%s",
				low.Executor, model.Bytes(low.MinFree),
				map[bool]string{true: fmt.Sprintf(" %s dropped and %s not cached in all.", model.Plural(f.Dropped, "block was", "blocks were"), model.Num(int64(f.NotCached))), false: " No cached block was dropped."}[f.Dropped+f.NotCached > 0])
		}
	}

	// The findings each chart is evidence for.
	for chart, rules := range chartRules {
		for i, f := range r.Findings {
			if slices.Contains(rules, f.Rule) {
				n[chart] = append(n[chart], runPoint{Text: "See finding " + fmt.Sprint(i+1) + ": " + f.Title + ".", Finding: i + 1})
			}
		}
	}
	// A chart with nothing to say says so, once the analysis had data.
	for chart := range chartRules {
		if len(n[chart]) == 0 {
			add(chart, "Nothing unusual here.")
		}
	}
	return n
}

// span is the milliseconds from a to b, or 0 when either is unknown.
func span(a, b time.Time) int64 {
	if a.IsZero() || b.IsZero() {
		return 0
	}
	return b.Sub(a).Milliseconds()
}

// shortName is a stage's name up to " at ", the call site.
func shortName(s string) string {
	s, _, _ = strings.Cut(s, " at ")
	return s
}
