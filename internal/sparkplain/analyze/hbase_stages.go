package analyze

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// processOf names the process a log came from.
func processOf(h hit) string {
	switch x := h.f.Executor; x {
	case "driver":
		return "the driver"
	case "am":
		return "the application master"
	case "":
		return "container " + h.f.Container
	default:
		return "executor " + x
	}
}

// during reports whether a log line's time falls while a stage ran. Log
// times have whole seconds, so the stage's start is rounded down.
func during(s *model.Stage, t time.Time) bool {
	return !t.IsZero() && !s.Submitted.IsZero() && !t.Before(s.Submitted.Truncate(time.Second)) && (s.Completed.IsZero() || !t.After(s.Completed))
}

func stageHas(s *model.Stage, rdd, callsite string) bool {
	return slices.ContainsFunc(s.RDDs, func(r model.StageRDD) bool {
		return r.Name == rdd && strings.HasPrefix(r.Callsite, callsite)
	})
}

// hbaseStages finds the stages that read or wrote HBase and what the
// clients logged while they ran (phase 5 step 4): from the connector's
// scan RDD and its SQL queries' tables, and from the TableInputFormat
// split lines and TableOutputFormat lines logged during a stage.
func hbaseStages(c *ctx, r *model.Report, h *model.HBaseSection) {
	if c.log == nil {
		return
	}
	var uses, problems []hit
	if c.logs != nil {
		for _, x := range c.logs.hits {
			switch {
			case x.l.Kind == model.LogHBaseUse && x.l.Fields["server"] != "" || x.l.Kind == model.LogHBaseUse && x.l.Fields["api"] == "TableOutputFormat":
				uses = append(uses, x)
			case x.l.Kind == model.LogHBase && x.l.Fields["hbase"] != "":
				problems = append(problems, x)
			}
		}
	}
	jobs := map[int]*model.Job{}
	for _, j := range c.log.Jobs {
		jobs[j.ID] = j
	}
	queries := map[int64]*model.SQLQuery{}
	for _, q := range c.log.SQL {
		queries[q.ID] = q
	}
	// The connector writes in the result stage of its query's last job;
	// earlier jobs of the query only prepare its input.
	lastJob := map[int64]int{}
	for _, j := range c.log.Jobs {
		if j.SQLExecutionID != nil && j.ID >= lastJob[*j.SQLExecutionID] {
			lastJob[*j.SQLExecutionID] = j.ID
		}
	}
	// A log line goes to the stage that started last before it, of those
	// running then: log times have whole seconds, so a line at a stage's
	// boundary would otherwise go to both.
	latest := func(t time.Time, ok func(*model.Stage) bool) *model.Stage {
		var best *model.Stage
		for _, s := range c.log.Stages {
			if during(s, t) && ok(s) && (best == nil || s.Submitted.After(best.Submitted)) {
				best = s
			}
		}
		return best
	}
	isScan := func(s *model.Stage) bool { return stageHas(s, "NewHadoopRDD", "newAPIHadoopRDD") }
	anyStage := func(*model.Stage) bool { return true }
	if c.rebuilt { // a rebuilt run does not say which stages scan
		isScan = anyStage
	}
	var stages []*model.Stage
	for _, s := range c.log.Stages {
		hs := model.HBaseStage{StageID: s.ID, Attempt: s.Attempt, Description: s.Name, Tasks: s.NumTasks, DurationMs: s.DurationMs(),
			RunTimeMs: s.Totals.RunTimeMs, CPUTimeNs: s.Totals.CPUTimeNs, Slowest: s.Slowest, MedianMs: s.TaskDuration.P50, Source: s.Source}
		var q *model.SQLQuery
		finalJob := false
		for _, id := range s.JobIDs {
			if j := jobs[id]; j != nil {
				if j.Description != "" {
					hs.Description = j.Description
				}
				if j.SQLExecutionID != nil {
					q = queries[*j.SQLExecutionID]
					finalJob = finalJob || lastJob[*j.SQLExecutionID] == j.ID
				}
			}
		}
		var apis []string
		add := func(to *[]string, name, api string) {
			if name != "" && !slices.Contains(*to, name) {
				*to = append(*to, name)
			}
			if !slices.Contains(apis, api) {
				apis = append(apis, api)
			}
		}
		if q != nil {
			if stageHas(s, "HBaseTableScanRDD", "") {
				for _, d := range q.Reads {
					if d.Format == "hbase" {
						add(&hs.Reads, d.Name, "hbase-spark connector")
					}
				}
			}
			if s.TaskType == "ResultTask" && finalJob {
				for _, d := range q.Writes {
					if d.Format == "hbase" {
						add(&hs.Writes, d.Name, "hbase-spark connector")
					}
				}
			}
		}
		for _, x := range uses {
			switch x.l.Fields["api"] {
			case "TableInputFormat":
				if latest(x.l.Time, isScan) == s {
					add(&hs.Reads, x.l.Fields["table"], "TableInputFormat")
				}
			case "TableOutputFormat":
				if latest(x.l.Time, anyStage) == s {
					add(&hs.Writes, x.l.Fields["table"], "TableOutputFormat")
				}
			}
		}
		if len(hs.Reads)+len(hs.Writes) == 0 {
			continue
		}
		hs.API = strings.Join(apis, " and ")
		h.Stages = append(h.Stages, hs)
		stages = append(stages, s)
	}
	// What the clients logged, given to the HBase stage that started last
	// before it.
	for _, x := range problems {
		for i := len(stages) - 1; i >= 0; i-- {
			if s := stages[i]; during(s, x.l.Time) {
				hs := &h.Stages[i]
				if hs.Retries == nil {
					hs.Retries = map[string]int{}
				}
				hs.Retries[x.l.Fields["hbase"]] += x.l.Count
				firstSource(&hs.RetrySources, x.l.Fields["hbase"], x.l)
				if v := x.l.Fields["server"]; v != "" && !slices.Contains(hs.Servers, v) {
					hs.Servers = append(hs.Servers, v)
				}
				break
			}
		}
	}
	// Wall-clock time at least one HBase stage ran.
	type span struct{ a, b time.Time }
	var spans []span
	for _, s := range stages {
		h.RunTimeMs += s.Totals.RunTimeMs
		h.CPUTimeNs += s.Totals.CPUTimeNs
		if !s.Submitted.IsZero() && !s.Completed.IsZero() {
			spans = append(spans, span{s.Submitted, s.Completed})
		}
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].a.Before(spans[j].a) })
	var end time.Time
	for _, sp := range spans {
		if sp.a.Before(end) {
			if sp.b.After(end) {
				h.TimeMs += sp.b.Sub(end).Milliseconds()
				end = sp.b
			}
			continue
		}
		h.TimeMs += sp.b.Sub(sp.a).Milliseconds()
		end = sp.b
	}
	h.RunMs = c.log.Application.DurationMs
}

// hbaseLocality counts the TableInputFormat regions read on the region
// server's own node, and from another node.
func hbaseLocality(c *ctx, h *model.HBaseSection) {
	if c.logs == nil {
		return
	}
	short := func(host string) string { s, _, _ := strings.Cut(host, "."); return s }
	for _, x := range c.logs.hits {
		srv := x.l.Fields["server"]
		if x.l.Kind != model.LogHBaseUse || srv == "" || x.f.Host == "" {
			continue
		}
		if short(srv) == short(x.f.Host) {
			h.LocalRegions += x.l.Count
		} else {
			h.RemoteRegions += x.l.Count
		}
	}
}

func stageName(s model.HBaseStage) string {
	n := fmt.Sprintf("stage %d", s.StageID)
	if s.Attempt > 0 {
		n = fmt.Sprintf("stage %d.%d", s.StageID, s.Attempt)
	}
	if s.Description != "" {
		n += " (" + clip(s.Description, 80) + ")"
	}
	return n
}

// hbaseSlowFindings says how much of the run HBase took and why it was
// slow, how many ZooKeeper connections one process opened, which region
// server held most of a table's regions, and how many regions were read
// from another node (phase 5 step 4).
func hbaseSlowFindings(c *ctx, h *model.HBaseSection) {
	t := c.t
	all := map[string]int{}
	for _, s := range h.Stages {
		for k, n := range s.Retries {
			all[k] += n
		}
	}
	// The run's time in HBase stages, and whether they waited.
	if h.RunMs > 0 && h.RunMs >= t.MinRunTime.Milliseconds() && float64(h.TimeMs) >= t.HBaseTimeShare*float64(h.RunMs) {
		cpu := share(h.CPUTimeNs/1e6, h.RunTimeMs)
		sev := model.Info
		their := "Their"
		if len(h.Stages) == 1 {
			their = "Its"
		}
		expl := fmt.Sprintf("Spark spent that long in the %s that read or wrote HBase. %s tasks were on the CPU %s of their run time", model.Plural(len(h.Stages), "stage", "stages"), their, model.Percent(cpu))
		if !c.metrics() { // CPU time is only in the event log
			expl = fmt.Sprintf("Spark spent that long in the %s that read or wrote HBase", model.Plural(len(h.Stages), "stage", "stages"))
		}
		if !c.metrics() {
			expl += "."
		} else if cpu < t.LowCPUShare {
			sev = model.Warning
			expl += ", so they spent most of it waiting, usually on HBase."
		} else {
			expl += "."
		}
		if s := model.HBaseRetriesText(all); s != "" {
			expl += " While they ran, the HBase clients logged " + s + "."
		}
		longest := h.Stages[0]
		for _, s := range h.Stages {
			if s.DurationMs > longest.DurationMs {
				longest = s
			}
		}
		expl += fmt.Sprintf(" The longest was %s, %s", stageName(longest), model.Duration(longest.DurationMs))
		if sl := longest.Slowest; sl != nil && longest.MedianMs > 0 && sl.DurationMs >= 2*longest.MedianMs && sl.DurationMs >= 1000 {
			expl += fmt.Sprintf("; its slowest task took %s on %s, against a median of %s", model.Duration(sl.DurationMs), sl.Host, model.Duration(longest.MedianMs))
		}
		expl += "."
		fix := "HBase answered slowly without logging errors. Check the region servers' own logs for that time, how many regions each scan reads (one task per region), scanner caching for scans and the client's write buffer for writes."
		if len(all) > 0 {
			fix = "Start with the HBase findings below, which say what the clients retried and how to avoid it."
		}
		var ev []model.Evidence
		for _, s := range h.Stages {
			if len(ev) == 5 {
				break
			}
			text := fmt.Sprintf("%s: %s, CPU %s of task time", stageName(s), model.Duration(s.DurationMs), model.Percent(share(s.CPUTimeNs/1e6, s.RunTimeMs)))
			if !c.metrics() {
				text = fmt.Sprintf("%s: %s", stageName(s), model.Duration(s.DurationMs))
			}
			ev = append(ev, model.Evidence{Source: s.Source, Ref: model.StageRef(s.StageID, s.Attempt), Text: text})
		}
		c.add(model.Finding{Rule: "hbase-time", Severity: sev, Section: "stages",
			Title:       fmt.Sprintf("Stages reading or writing HBase took %s of the %s run (%s)", model.Duration(h.TimeMs), model.Duration(h.RunMs), model.Percent(share(h.TimeMs, h.RunMs))),
			Explanation: expl, Evidence: ev, Fix: fix})
	}
	// The stage each recovered problem slowed.
	for rule, problem := range map[string]string{"hbase-busy": "busy", "hbase-scanner-expired": "scanner", "hbase-region-moved": "moved"} {
		f := c.finding(rule)
		if f == nil {
			continue
		}
		var names []string
		for _, s := range h.Stages {
			if s.Retries[problem] > 0 {
				names = append(names, stageName(s))
				f.Evidence = append(f.Evidence, model.Evidence{Source: s.Source, Ref: model.StageRef(s.StageID, s.Attempt), Text: fmt.Sprintf("%s: %s, in %s of task time", stageName(s), model.Plural(s.Retries[problem], "time", "times"), model.Duration(s.RunTimeMs))})
			}
		}
		if len(names) > 0 {
			f.Explanation += " They happened while " + strings.Join(names, " and ") + " ran."
		}
	}
	// One process opening many ZooKeeper connections.
	if h.MostSessions > t.HBaseConnections {
		var ev []model.Evidence
		if c.logs != nil {
			for _, x := range c.logs.hits {
				if x.l.Kind == model.LogHBase && x.l.Fields["quorum"] != "" && processOf(x) == h.MostSessionsBy {
					ev = append(ev, x.evidence(fmt.Sprintf("%s: %s (%d times)", x.who(), x.l.Text, x.l.Count)))
					break
				}
			}
		}
		c.add(model.Finding{Rule: "hbase-zk-connections", Severity: model.Warning, Section: "stages",
			Title: fmt.Sprintf("%s opened %d ZooKeeper connections to reach HBase", upperFirst(h.MostSessionsBy), h.MostSessions),
			Explanation: fmt.Sprintf("Every new HBase connection asks ZooKeeper where HBase's meta table is, then looks each region up there again, because a new connection has nothing cached. %d in one process (%d in the run) means the code opened a connection for each task or batch instead of sharing one, which adds those round trips to every task and load on ZooKeeper and the server holding the meta table.",
				h.MostSessions, h.Sessions),
			Evidence: ev,
			Fix:      "Open one HBase Connection per executor and reuse it, for example a lazily created one per JVM, rather than one per partition. A Connection is thread-safe and meant to be shared."})
	}
	// One region server holding most of a table's regions.
	servers := map[string]bool{}
	for _, tb := range h.Tables {
		for _, g := range tb.Regions {
			servers[g.Server] = true
		}
	}
	for _, s := range h.Stages {
		for _, v := range s.Servers {
			servers[v] = true
		}
	}
	for _, tb := range h.Tables {
		total := 0
		for _, g := range tb.Regions {
			total += g.Regions
		}
		if total < 4 || len(servers) < 2 || float64(tb.Regions[0].Regions) < t.HBaseHotspot*float64(total) {
			continue
		}
		top := tb.Regions[0]
		c.add(model.Finding{Rule: "hbase-hotspot", Severity: model.Warning, Section: "stages",
			Title:       fmt.Sprintf("Region server %s held %d of the %d regions of %s the run read", top.Server, top.Regions, total, tb.Name),
			Explanation: fmt.Sprintf("Each task of a TableInputFormat scan reads one region from the region server holding it, so %s answered %s of the scan while the other region servers the run used (%d in all) did little.", top.Server, model.Percent(share(int64(top.Regions), int64(total))), len(servers)),
			Evidence: []model.Evidence{{Source: tb.Source, Text: fmt.Sprintf("%s: regions read per region server, from the executors' split lines", tb.Name)},
				{Ref: model.NodeRef(top.Server), Text: fmt.Sprintf("region server %s: %d of %d regions", top.Server, top.Regions, total)}},
			Fix: "Spread the table's regions across region servers (the HBase balancer, or move regions by hand), and split large regions; a table with few regions should be pre-split."})
		break
	}
	// Regions read from another node.
	if n := h.LocalRegions + h.RemoteRegions; n >= 4 && float64(h.RemoteRegions) > t.LocalityAnyShare*float64(n) {
		c.add(model.Finding{Rule: "hbase-remote-regions", Severity: model.Info, Section: "stages",
			Title:       fmt.Sprintf("%d of %d HBase regions were read from another node", h.RemoteRegions, n),
			Explanation: "TableInputFormat tells Spark which node holds each region, and Spark prefers to run the task there. These tasks ran on other nodes, so every row they read crossed the network between nodes. Spark does this when no executor on the region's node is free within spark.locality.wait.",
			Evidence:    remoteReaders(c),
			Fix:         "Run executors on the nodes that serve regions (with HBase on the same cluster, the core nodes). If tasks are short, a longer spark.locality.wait lets Spark wait for a local slot."})
	}
}

// remoteReaders cites each executor that read regions held on another
// node, so the report can point at it.
func remoteReaders(c *ctx) []model.Evidence {
	type reader struct {
		h       hit
		n, from int
	}
	var order []string
	by := map[string]*reader{}
	for _, x := range c.logs.hits {
		srv := x.l.Fields["server"]
		if x.l.Kind != model.LogHBaseUse || srv == "" || x.f.Host == "" {
			continue
		}
		k := x.f.Location
		if by[k] == nil {
			by[k] = &reader{h: x}
			order = append(order, k)
		}
		by[k].n += x.l.Count
		if shortHost(srv) != shortHost(x.f.Host) {
			by[k].from += x.l.Count
		}
	}
	var ev []model.Evidence
	for _, k := range order {
		r := by[k]
		if r.from > 0 {
			ev = append(ev, r.h.evidence(fmt.Sprintf("%s on %s read %d of its %d regions from another node", processOf(r.h), r.h.f.Host, r.from, r.n)))
		}
	}
	return ev
}

func upperFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// firstSource keeps, per key, the source of the first line behind a
// count, in the order the logs were read.
func firstSource(m *map[string]model.Source, key string, l *model.LogLine) {
	if *m == nil {
		*m = map[string]model.Source{}
	}
	if _, ok := (*m)[key]; !ok {
		(*m)[key] = l.Source
	}
}
