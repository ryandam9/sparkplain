package analyze

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// Log times on HBase's servers have whole seconds, so an event counts as
// during a task when it is within a second of it; the same event on the
// same region and server is folded when a minute apart at most; and the
// finding lists at most maxSlowRegions tasks.
const (
	eventSlack     = time.Second
	eventFold      = time.Minute
	maxSlowRegions = 5
)

// causeEvents are the region events that can slow a scan of the region:
// a flush only when it took long.
var causeEvents = map[string]bool{"compaction": true, "offline": true, "closed": true, "move": true, "split": true, "busy": true, "slow-call": true}

// hbaseRegionEvents ties what the HBase servers logged about single
// regions (flushes, compactions, closes and opens, moves, splits, refused
// writes, slow calls) to the run's tasks reading those regions at the
// time, marks the slow tasks (at least twice their stage's median), and
// reports slow tasks whose region had something happen to it:
// hbase-region-events.
func hbaseRegionEvents(c *ctx, r *model.Report, h *model.HBaseSection) {
	if r.Logs == nil || len(h.Tasks) == 0 {
		return
	}
	markSlow(c, h.Tasks)
	byRegion := map[string][]int{}
	for i, t := range h.Tasks {
		if t.Region != "" {
			byRegion[t.Region] = append(byRegion[t.Region], i)
		}
	}
	var evs []model.HBaseRegionEvent
	for _, f := range r.Logs.Files {
		for _, e := range f.HBaseRegionEvents {
			if byRegion[e.Region] != nil {
				evs = append(evs, e)
			}
		}
	}
	if len(evs) == 0 {
		return
	}
	sort.SliceStable(evs, func(i, j int) bool { return evs[i].Time.Before(evs[j].Time) })
	evs = foldEvents(offline(evs))
	for _, e := range evs {
		from := e.Time.Add(-time.Duration(e.DurationMs) * time.Millisecond)
		if !e.First.IsZero() && e.First.Before(from) {
			from = e.First
		}
		var tasks []int
		for _, i := range byRegion[e.Region] {
			t := h.Tasks[i]
			if !t.Start.IsZero() && !t.End.IsZero() && !t.Start.After(e.Time.Add(eventSlack)) && !t.End.Before(from.Add(-eventSlack)) {
				tasks = append(tasks, i)
			}
		}
		if len(tasks) == 0 {
			continue
		}
		for _, i := range tasks {
			e.Tasks = append(e.Tasks, taskName(h.Tasks[i]))
			if h.Tasks[i].Slow {
				e.Slow = append(e.Slow, taskName(h.Tasks[i]))
			}
		}
		h.RegionEvents = append(h.RegionEvents, e)
	}
	// Slow tasks' events first, then by time.
	sort.SliceStable(h.RegionEvents, func(i, j int) bool {
		a, b := len(h.RegionEvents[i].Slow) > 0, len(h.RegionEvents[j].Slow) > 0
		return a && !b
	})
	for k, e := range h.RegionEvents {
		for _, i := range byRegion[e.Region] {
			if slices.Contains(e.Tasks, taskName(h.Tasks[i])) {
				h.Tasks[i].Events = append(h.Tasks[i].Events, k)
			}
		}
	}
	slowRegions(c, h)
}

// markSlow marks the tasks that took at least twice their stage's median,
// and skew-min-task more, among a stage's timed tasks.
func markSlow(c *ctx, tasks []model.HBaseTaskRead) {
	type key struct{ stage, attempt int }
	durs := map[key][]int64{}
	for _, t := range tasks {
		if t.Timed && t.Stage >= 0 {
			k := key{t.Stage, t.StageAttempt}
			durs[k] = append(durs[k], t.DurationMs)
		}
	}
	median := map[key]int64{}
	for k, d := range durs {
		if len(d) < 2 {
			continue
		}
		slices.Sort(d)
		m := d[len(d)/2]
		if len(d)%2 == 0 {
			m = (d[len(d)/2-1] + d[len(d)/2]) / 2
		}
		median[k] = m
	}
	for i := range tasks {
		t := &tasks[i]
		m, ok := median[key{t.Stage, t.StageAttempt}]
		t.Slow = ok && t.Timed && t.DurationMs >= 2*m && t.DurationMs-m >= c.t.SkewMinTask.Milliseconds()
	}
}

// offline joins a region's close on one server to its next open (on any
// server) into one event: the region served no reads in between, so a
// scan of it waited and retried. A close with no open in the logs stays.
func offline(evs []model.HBaseRegionEvent) []model.HBaseRegionEvent {
	var out []model.HBaseRegionEvent
	used := make([]bool, len(evs))
	for i, e := range evs {
		if used[i] {
			continue
		}
		switch e.Event {
		case "closed":
			found := false
			for j := i + 1; j < len(evs) && !found; j++ {
				if o := evs[j]; !used[j] && o.Event == "opened" && o.Region == e.Region {
					used[j], found = true, true
					d := o.Time.Sub(e.Time)
					out = append(out, model.HBaseRegionEvent{Time: o.Time, Event: "offline", Region: e.Region, Table: o.Table, Host: e.Host,
						DurationMs: d.Milliseconds(), Source: e.Source,
						Detail: fmt.Sprintf("closed on %s and opened on %s %s later: it served no reads in between", shortServer(e.Host), shortServer(o.Host), model.Duration(d.Milliseconds()))})
				}
			}
			if !found {
				out = append(out, e)
			}
		case "opened":
			// an open with no close before it in the run's time: the
			// region was already offline, or this is its first open
			out = append(out, e)
		default:
			out = append(out, e)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Time.Before(out[j].Time) })
	return out
}

// foldEvents folds the same event on the same region and server a minute
// apart at most into one, counted: a busy region refuses writes many
// times a second.
func foldEvents(evs []model.HBaseRegionEvent) []model.HBaseRegionEvent {
	var out []model.HBaseRegionEvent
	last := map[string]int{}
	for _, e := range evs {
		k := e.Event + "\x00" + e.Region + "\x00" + e.Host
		if i, ok := last[k]; ok && e.Time.Sub(out[i].Time) <= eventFold && e.Event != "offline" && e.Event != "split" {
			o := &out[i]
			if o.Count == 0 {
				o.Count, o.First = 1, o.Time
			}
			o.Count++
			o.Time = e.Time
			o.DurationMs = max(o.DurationMs, e.DurationMs)
			continue
		}
		last[k] = len(out)
		out = append(out, e)
	}
	return out
}

func shortServer(h string) string {
	if h == "" {
		return "a server"
	}
	return strings.SplitN(h, ".", 2)[0]
}

// slowRegions reports slow tasks whose region had something happen to it
// on its server while they read it: a compaction, the region offline or
// moving or splitting, refused writes, a slow call, or a long flush.
func slowRegions(c *ctx, h *model.HBaseSection) {
	type hit struct {
		t   model.HBaseTaskRead
		evs []model.HBaseRegionEvent
	}
	var hits []hit
	kinds := map[string]bool{}
	for _, t := range h.Tasks {
		if !t.Slow {
			continue
		}
		var evs []model.HBaseRegionEvent
		for _, k := range t.Events {
			e := h.RegionEvents[k]
			if causeEvents[e.Event] || e.Event == "flush" && e.DurationMs >= 10_000 {
				evs = append(evs, e)
				kinds[e.Event] = true
			}
		}
		if len(evs) > 0 {
			hits = append(hits, hit{t, evs})
		}
	}
	if len(hits) == 0 {
		return
	}
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].t.DurationMs > hits[j].t.DurationMs })
	what := func(e model.HBaseRegionEvent) string {
		s := map[string]string{"compaction": "a compaction", "offline": "an offline period", "closed": "a close", "move": "a move", "split": "a split",
			"busy": "write refusals", "slow-call": "slow calls", "flush": "a flush"}[e.Event]
		if e.DurationMs > 0 && e.Event != "slow-call" {
			s += " (" + model.Duration(e.DurationMs) + ")"
		}
		return s
	}
	var lines []string
	var ev []model.Evidence
	for i, x := range hits {
		if i == maxSlowRegions {
			lines = append(lines, fmt.Sprintf("The HBase section shows %d more.", len(hits)-maxSlowRegions))
			break
		}
		var ws []string
		for _, e := range x.evs {
			ws = append(ws, what(e))
		}
		lines = append(lines, fmt.Sprintf("Task %s took %s to read region %s of %s on %s. The region had %s at that time.", taskName(x.t), model.Duration(x.t.DurationMs), x.t.Region, x.t.Table, shortServer(x.t.Server), listAnd(dedupe(ws))))
		if len(ev) < 8 {
			ev = append(ev, model.Evidence{Source: x.t.Source, Text: fmt.Sprintf("the split of task %s: region %s of %s on %s", taskName(x.t), x.t.Region, x.t.Table, x.t.Server)})
			for _, e := range x.evs[:min(len(x.evs), 2)] {
				when := "at " + c.clock(e.Time)
				if e.Count > 1 {
					when = fmt.Sprintf("%d times from %s to %s", e.Count, c.clock(e.First), c.clock(e.Time))
				}
				ev = append(ev, model.Evidence{Source: e.Source, Text: fmt.Sprintf("%s logged %s: %s", shortServer(e.Host), when, e.Detail)})
			}
		}
	}
	top := hits[0]
	var ws []string
	for _, e := range top.evs {
		ws = append(ws, what(e))
	}
	title := fmt.Sprintf("Task %s took %s, and its region on %s had %s at that time", taskName(top.t), model.Duration(top.t.DurationMs), shortServer(top.t.Server), listAnd(dedupe(ws)))
	if len(hits) > 1 {
		title = fmt.Sprintf("%d slow HBase reads occurred while the region server did work on the same region", len(hits))
	}
	var fixes []string
	if kinds["compaction"] || kinds["flush"] {
		fixes = append(fixes, "Run major compactions outside the hours of the job. Set hbase.hregion.majorcompaction to 0, and schedule major_compact for a quiet time.")
	}
	if kinds["offline"] || kinds["closed"] || kinds["move"] {
		fixes = append(fixes, "Stop the balancer while the job reads. In the HBase shell, use balance_switch false before the job and balance_switch true after it.")
	}
	if kinds["split"] {
		fixes = append(fixes, "Pre-split the table, so that its regions do not split during the job.")
	}
	if kinds["busy"] {
		fixes = append(fixes, "Do not write to the same regions while the job reads them. The region refused writes because its memstore was full.")
	}
	if kinds["slow-call"] {
		fixes = append(fixes, "Examine the log and the garbage collection pauses of that region server at the time of the slow calls.")
	}
	c.add(model.Finding{Rule: "hbase-region-events", Severity: model.Warning, Section: "stages",
		Title: title,
		Explanation: "Each TableInputFormat task reads one region. While these tasks read, the region server logged work on the same region.\n- " + strings.Join(lines, "\n- ") +
			"\nA slow task took at least two times the median time of its stage. This work uses the disk and the memory of the region at the same time as the scan. Or it makes the client wait and try again.",
		Evidence: ev,
		Fix:      "- " + strings.Join(fixes, "\n- ")})
}

func dedupe(xs []string) []string {
	var out []string
	for _, x := range xs {
		if !slices.Contains(out, x) {
			out = append(out, x)
		}
	}
	return out
}
