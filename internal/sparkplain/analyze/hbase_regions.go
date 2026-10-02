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
		s := map[string]string{"compaction": "compacting", "offline": "offline", "closed": "closed", "move": "being moved", "split": "being split",
			"busy": "refusing writes", "slow-call": "answering slowly", "flush": "flushing"}[e.Event]
		if e.DurationMs > 0 && e.Event != "slow-call" {
			s += " for " + model.Duration(e.DurationMs)
		}
		return s
	}
	var lines []string
	var ev []model.Evidence
	for i, x := range hits {
		if i == maxSlowRegions {
			lines = append(lines, fmt.Sprintf("and %d more in the HBase section", len(hits)-maxSlowRegions))
			break
		}
		var ws []string
		for _, e := range x.evs {
			ws = append(ws, what(e))
		}
		lines = append(lines, fmt.Sprintf("task %s took %s reading region %s of %s on %s, which was %s", taskName(x.t), model.Duration(x.t.DurationMs), x.t.Region, x.t.Table, shortServer(x.t.Server), strings.Join(dedupe(ws), ", ")))
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
	title := fmt.Sprintf("Task %s took %s while its region on %s was %s", taskName(top.t), model.Duration(top.t.DurationMs), shortServer(top.t.Server), strings.Join(dedupe(ws), ", "))
	if len(hits) > 1 {
		title = fmt.Sprintf("%d slow HBase reads overlapped work their region's server logged on that region", len(hits))
	}
	var fixes []string
	if kinds["compaction"] || kinds["flush"] {
		fixes = append(fixes, "run major compactions outside the job's hours (set hbase.hregion.majorcompaction to 0 and schedule major_compact off-peak)")
	}
	if kinds["offline"] || kinds["closed"] || kinds["move"] {
		fixes = append(fixes, "keep the balancer from moving regions while the job reads (HBase shell: balance_switch false for the job's window, then true)")
	}
	if kinds["split"] {
		fixes = append(fixes, "pre-split the table so its regions do not split under the job")
	}
	if kinds["busy"] {
		fixes = append(fixes, "move writes into the same regions away from the job's reads: the region was refusing writes, so its memstore was full and flushing")
	}
	if kinds["slow-call"] {
		fixes = append(fixes, "check that region server's own log and GC pauses for the slow calls' time")
	}
	c.add(model.Finding{Rule: "hbase-region-events", Severity: model.Warning, Section: "stages",
		Title: title,
		Explanation: "Each TableInputFormat task reads one region, and its region server logged work on that same region while the task read it: " + strings.Join(lines, "; ") +
			". A slow task is one that took at least twice its stage's median. Work like this competes with the scan for the region's disk and memory, or makes the client wait and retry.",
		Evidence: ev,
		Fix:      strings.ToUpper(strings.Join(fixes, "; ")[:1]) + strings.Join(fixes, "; ")[1:] + "."})
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
