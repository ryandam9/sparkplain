package analyze

import (
	"fmt"
	"sort"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

var removalLabels = []struct{ kind, label string }{
	{model.RemovalNone, "still running at the end"},
	{model.RemovalKilledByDriver, "removed by Spark"},
	{model.RemovalIdle, "idle"},
	{model.RemovalMemoryKill, "killed (exit 137, usually memory)"},
	{model.RemovalLost, "lost"},
	{model.RemovalDecommissioned, "decommissioned"},
	{model.RemovalOther, "other"},
}

// lifetime returns when an executor started and stopped (the app end if it
// was never removed).
func (c *ctx) lifetime(x *model.Executor) (time.Time, time.Time) {
	start, end := x.Added, x.Removed
	if start.IsZero() {
		start = c.log.Application.Start
	}
	if end.IsZero() {
		end = c.end
	}
	return start, end
}

// series rebuilds how many executors were running over time.
func (c *ctx) series() []model.CountPoint {
	type ev struct {
		t time.Time
		d int
	}
	var evs []ev
	for _, x := range c.log.Executors {
		start, end := c.lifetime(x)
		if start.IsZero() {
			continue
		}
		evs = append(evs, ev{start, +1})
		if !x.Removed.IsZero() {
			evs = append(evs, ev{end, -1})
		}
	}
	sort.SliceStable(evs, func(i, j int) bool { return evs[i].t.Before(evs[j].t) })
	var pts []model.CountPoint
	if s := c.log.Application.Start; !s.IsZero() {
		pts = append(pts, model.CountPoint{Time: s, Count: 0})
	}
	n := 0
	for _, e := range evs {
		n += e.d
		if len(pts) > 0 && pts[len(pts)-1].Time.Equal(e.t) {
			pts[len(pts)-1].Count = n
			continue
		}
		pts = append(pts, model.CountPoint{Time: e.t, Count: n})
	}
	if !c.end.IsZero() {
		pts = append(pts, model.CountPoint{Time: c.end, Count: n})
	}
	return pts
}

func analyzeExecutors(c *ctx, r *model.Report) {
	s := &r.Executors
	if !c.has() {
		s.Coverage = model.NeedsEventLog
		s.Missing = []string{"How many executors ran, where, and why each one ended", "Tasks, CPU time and data per executor"}
		return
	}
	s.Coverage = model.Complete
	s.Executors = c.log.Executors
	if s.Executors == nil {
		s.Executors = []*model.Executor{}
	}
	s.Driver = c.log.Driver
	s.Exclusions = c.log.Exclusions
	s.Started = len(c.log.Executors)
	for _, p := range c.series() {
		if p.Count > s.Peak {
			s.Peak, s.PeakAt = p.Count, p.Time
		}
	}
	counts := map[string]int{}
	cores := map[int]int{}
	for _, x := range c.log.Executors {
		counts[x.RemovalKind]++
		cores[x.Cores]++
	}
	for _, rl := range removalLabels {
		if n := counts[rl.kind]; n > 0 {
			s.EndedBy = append(s.EndedBy, model.Count{Label: rl.label, N: n})
		}
	}
	best := 0
	for k, n := range cores {
		if n > best || (n == best && k > s.Cores) {
			s.Cores, best = k, n
		}
	}
	if c.confBool("spark.dynamicAllocation.enabled", false) {
		lo, hi := c.conf["spark.dynamicAllocation.minExecutors"], c.conf["spark.dynamicAllocation.maxExecutors"]
		if lo == "" {
			lo = "0"
		}
		if hi == "" {
			hi = "no limit"
		}
		s.DynamicAlloc = fmt.Sprintf("on, %s to %s executors", lo, hi)
	} else if n := c.conf["spark.executor.instances"]; n != "" {
		s.DynamicAlloc = "off, " + n + " executors requested"
	} else {
		s.DynamicAlloc = "off"
	}
	executorFindings(c)
}

func executorFindings(c *ctx) {
	var killed, lost, decom []*model.Executor
	for _, x := range c.log.Executors {
		switch x.RemovalKind {
		case model.RemovalMemoryKill:
			killed = append(killed, x)
		case model.RemovalLost:
			lost = append(lost, x)
		case model.RemovalDecommissioned:
			decom = append(decom, x)
		}
	}
	evidence := func(xs []*model.Executor) []model.Evidence {
		var ev []model.Evidence
		for i, x := range xs {
			if i == 5 {
				ev = append(ev, model.Evidence{Text: fmt.Sprintf("… and %d more", len(xs)-5)})
				break
			}
			ev = append(ev, model.Evidence{Source: x.RemovedSource, Ref: model.ExecutorRef(x.ID), Text: fmt.Sprintf("executor %s on %s: “%s”", x.ID, x.Host, x.RemovedReason)})
		}
		return ev
	}
	failedTasks := func(xs []*model.Executor) int64 {
		var n int64
		for _, x := range xs {
			n += x.Tasks.Failed
		}
		return n
	}
	if len(killed) > 0 {
		c.add(model.Finding{
			Rule: "executor-memory-kill", Severity: model.Critical, Section: "executors",
			Title: fmt.Sprintf("%s killed with exit code 137", model.Plural(len(killed), "executor was", "executors were")),
			Explanation: fmt.Sprintf("Exit code 137 means the executor process received SIGKILL. On YARN that almost always means the container used more memory than it was given, "+
				"either the Java heap plus overhead, or Python workers on top. Tasks running on %s had to start again elsewhere (%s failed on these executors).",
				pronoun(len(killed)), model.Plural(int(failedTasks(killed)), "task", "tasks")),
			Evidence: evidence(killed),
			Fix:      "Check the container's stderr for “exceeding physical memory limits”. If so, raise spark.executor.memoryOverhead (PySpark jobs often need 20–40% of the heap) or run fewer cores per executor so fewer tasks share the memory.",
		})
	}
	if len(lost) > 0 {
		c.add(model.Finding{
			Rule: "executor-lost", Severity: model.Warning, Section: "executors",
			Title:       fmt.Sprintf("%s lost during the run", model.Plural(len(lost), "executor was", "executors were")),
			Explanation: "Spark lost contact with these executors (the process crashed, stopped sending heartbeats, or its node went away). Their running tasks and any shuffle data they held had to be recomputed.",
			Evidence:    evidence(lost),
			Fix:         "Look at the executors' container logs around the removal time for the cause: run sparkplain with -cluster-id (or -from with a copy of the cluster's logs) and it adds each executor's last error here.",
		})
	}
	if len(decom) > 0 {
		c.add(model.Finding{
			Rule: "executor-decommissioned", Severity: model.Warning, Section: "executors",
			Title:       fmt.Sprintf("%s decommissioned", model.Plural(len(decom), "executor was", "executors were")),
			Explanation: "Their nodes were taken away during the run. On EMR this is usually a spot interruption or the cluster scaling in.",
			Evidence:    evidence(decom),
			Fix:         "If this happens often, run core work on on-demand instances, or enable spark.decommission.enabled so Spark moves data off leaving nodes.",
		})
	}
}

func pronoun(n int) string {
	if n == 1 {
		return "it"
	}
	return "them"
}

func analyzeNodes(c *ctx, r *model.Report) {
	s := &r.Nodes
	s.Missing = []string{"Instance ID, type, vCPU and memory, spot or on-demand, and instance group (needs the EMR API)", "Hosts in the cluster that ran no executors"}
	if !c.has() {
		s.Coverage = model.NeedsEventLog
		return
	}
	s.Coverage = model.Partial
	hosts := map[string]*model.Host{}
	get := func(name string) *model.Host {
		h := hosts[name]
		if h == nil {
			h = &model.Host{Name: name, Executors: []string{}}
			hosts[name] = h
		}
		return h
	}
	allocated := map[string]int64{}
	for _, x := range c.log.Executors {
		if x.Host == "" {
			continue
		}
		h := get(x.Host)
		h.Executors = append(h.Executors, x.ID)
		h.Cores += x.Cores
		h.Tasks.Add(x.Tasks)
		h.PeakHeap = max(h.PeakHeap, x.Peak.JVMHeap)
		if x.RemovalKind == model.RemovalLost || x.RemovalKind == model.RemovalMemoryKill || x.RemovalKind == model.RemovalDecommissioned {
			h.Lost++
		}
		start, end := c.lifetime(x)
		if h.FirstSeen.IsZero() || start.Before(h.FirstSeen) {
			h.FirstSeen = start
		}
		if end.After(h.LastSeen) {
			h.LastSeen = end
		}
		allocated[x.Host] += int64(x.Cores) * end.Sub(start).Milliseconds()
		if h.Source.IsZero() {
			h.Source = x.AddedSource
		}
	}
	driverHost := ""
	if d := c.log.Driver; d != nil && d.Host != "" {
		driverHost = d.Host
	} else if v := c.conf["spark.driver.host"]; v != "" {
		driverHost = v
	}
	if driverHost != "" {
		h := get(driverHost)
		h.Driver = true
		if d := c.log.Driver; d != nil && h.Source.IsZero() {
			h.Source = d.AddedSource
		}
	}
	for name, h := range hosts {
		h.CPUShare = share(h.Tasks.CPUTimeNs/1e6, h.Tasks.RunTimeMs)
		h.AllocatedCore = share(h.Tasks.RunTimeMs, allocated[name])
		s.Hosts = append(s.Hosts, *h)
	}
	sort.Slice(s.Hosts, func(i, j int) bool {
		if s.Hosts[i].Driver != s.Hosts[j].Driver {
			return s.Hosts[i].Driver
		}
		return s.Hosts[i].Name < s.Hosts[j].Name
	})
	workers := 0
	for _, h := range s.Hosts {
		if len(h.Executors) > 0 {
			workers++
		}
	}
	lede := fmt.Sprintf("%s ran executors.", model.Plural(workers, "host", "hosts"))
	switch {
	case driverHost == "":
	case hosts[driverHost] != nil && len(hosts[driverHost].Executors) > 0:
		lede += fmt.Sprintf(" The driver ran on %s, alongside executors.", driverHost)
	default:
		lede += fmt.Sprintf(" The driver ran on %s.", driverHost)
	}
	if c.log.Application.DeployMode == "client" {
		lede += " In client mode the driver runs where spark-submit ran (on EMR, usually the primary node)."
	}
	s.Lede = lede
}

func analyzeTimeline(c *ctx, r *model.Report) {
	t := &r.Timeline
	if !c.has() {
		t.Coverage = model.NeedsEventLog
		return
	}
	t.Coverage = model.Complete
	a := c.log.Application
	t.Start, t.End = a.Start, c.end
	t.ExecutorSeries = c.series()
	stagesByJob := map[int][]*model.Stage{}
	for _, s := range c.log.Stages {
		for _, j := range s.JobIDs {
			stagesByJob[j] = append(stagesByJob[j], s)
		}
	}
	for _, j := range c.log.Jobs {
		end := j.Completed
		if end.IsZero() {
			end = c.end
		}
		b := model.Bar{ID: j.ID, Label: jobLabel(j), Start: j.Submitted, End: end, Status: j.Status}
		for _, s := range stagesByJob[j.ID] {
			if s.Submitted.IsZero() {
				continue
			}
			se := s.Completed
			if se.IsZero() {
				se = end
			}
			b.Stages = append(b.Stages, model.Bar{ID: s.ID, Label: fmt.Sprintf("Stage %d: %s", s.ID, s.Name), Start: s.Submitted, End: se, Status: s.Status})
		}
		t.Jobs = append(t.Jobs, b)
	}
	var ev []model.TimelineEvent
	if !a.Start.IsZero() {
		ev = append(ev, model.TimelineEvent{Time: a.Start, Kind: "start", Text: fmt.Sprintf("Application %s started as %s", a.Name, a.User), Source: a.Source})
	}
	// Group executor starts that happen close together.
	var batch []*model.Executor
	flush := func() {
		if len(batch) == 0 {
			return
		}
		hosts := map[string]bool{}
		for _, x := range batch {
			hosts[x.Host] = true
		}
		text := fmt.Sprintf("%s started on %s", model.Plural(len(batch), "executor", "executors"), model.Plural(len(hosts), "host", "hosts"))
		if len(batch) == 1 {
			text = fmt.Sprintf("Executor %s started on %s", batch[0].ID, batch[0].Host)
		}
		ev = append(ev, model.TimelineEvent{Time: batch[0].Added, Kind: "executors", Text: text, Source: batch[0].AddedSource})
		batch = nil
	}
	added := append([]*model.Executor(nil), c.log.Executors...)
	sort.SliceStable(added, func(i, j int) bool { return added[i].Added.Before(added[j].Added) })
	for _, x := range added {
		if x.Added.IsZero() {
			continue
		}
		if len(batch) > 0 && x.Added.Sub(batch[0].Added) > time.Minute {
			flush()
		}
		batch = append(batch, x)
	}
	flush()
	for _, x := range c.log.Executors {
		if x.Removed.IsZero() {
			continue
		}
		kind := "executor-removed"
		switch x.RemovalKind {
		case model.RemovalMemoryKill:
			kind = "executor-killed"
		case model.RemovalLost, model.RemovalDecommissioned:
			kind = "executor-lost"
		}
		ev = append(ev, model.TimelineEvent{Time: x.Removed, Kind: kind, Text: fmt.Sprintf("Executor %s on %s ended: %s", x.ID, x.Host, x.RemovedReason), Source: x.RemovedSource})
	}
	for _, j := range c.log.Jobs {
		if j.Status == model.StatusFailed {
			ev = append(ev, model.TimelineEvent{Time: j.Completed, Kind: "job-failed", Text: fmt.Sprintf("Job %d (%s) failed: %s", j.ID, jobLabel(j), shortError(j.Failure)), Source: j.EndSource})
		}
	}
	for _, s := range c.log.Stages {
		if s.Attempt > 0 {
			ev = append(ev, model.TimelineEvent{Time: s.Submitted, Kind: "stage-retry", Text: fmt.Sprintf("Stage %d retried (attempt %d)", s.ID, s.Attempt+1), Source: s.Source})
		}
	}
	if !a.End.IsZero() {
		ev = append(ev, model.TimelineEvent{Time: a.End, Kind: "end", Text: "Application ended (" + a.Status + ")", Source: a.EndSource})
	} else if !c.end.IsZero() {
		ev = append(ev, model.TimelineEvent{Time: c.end, Kind: "end", Text: "The event log ends here; the application had not finished"})
	}
	sort.SliceStable(ev, func(i, j int) bool { return ev[i].Time.Before(ev[j].Time) })
	if len(ev) > 60 {
		ev = append(ev[:59], ev[len(ev)-1])
	}
	t.Events = ev
}

func jobLabel(j *model.Job) string {
	if j.Description != "" {
		return j.Description
	}
	if j.Name != "" {
		return j.Name
	}
	return fmt.Sprintf("job %d", j.ID)
}
