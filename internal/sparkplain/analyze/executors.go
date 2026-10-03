package analyze

import (
	"fmt"
	"sort"
	"strings"
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
	if c.rebuilt {
		s.Coverage = model.Partial
		s.Missing = []string{rebuiltNote}
	}
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
			Explanation: fmt.Sprintf("Exit code 137 means that the executor process got SIGKILL. On YARN, this almost always means that the container used more memory than YARN gave it. "+
				"The memory includes the Java heap, the overhead and the Python workers. The tasks on %s started again on other executors (%s failed on these executors).",
				pronoun(len(killed)), model.Plural(int(failedTasks(killed)), "task", "tasks")),
			Evidence: evidence(killed),
			Fix:      "Look for “exceeding physical memory limits” in the stderr of the container. If you find it, do one of these:\n- Increase spark.executor.memoryOverhead. PySpark jobs often use 20–40% of the heap for it.\n- Use fewer cores for each executor, so that fewer tasks share the memory.",
		})
	}
	if len(lost) > 0 {
		c.add(model.Finding{
			Rule: "executor-lost", Severity: model.Warning, Section: "executors",
			Title:       fmt.Sprintf("Spark lost %s during the run", model.Plural(len(lost), "executor", "executors")),
			Explanation: "Spark lost contact with these executors. The process stopped, it stopped its heartbeats, or its node stopped. Spark calculated their tasks and their shuffle data again.",
			Evidence:    evidence(lost),
			Fix:         "Find the cause in the container logs of the executors, near the time that Spark removed them.\nRun sparkplain with -cluster-id, or with -from and a copy of the cluster logs. Then this finding shows the last error of each executor.",
		})
	}
	if len(decom) > 0 {
		c.add(model.Finding{
			Rule: "executor-decommissioned", Severity: model.Warning, Section: "executors",
			Title:       fmt.Sprintf("Spark decommissioned %s", model.Plural(len(decom), "executor", "executors")),
			Explanation: "Their nodes stopped during the run. On EMR, the cause is usually a spot interruption, or the cluster removed nodes when it scaled in.",
			Evidence:    evidence(decom),
			Fix:         "If this occurs frequently, do one of these:\n- Run the core work on on-demand instances.\n- Set spark.decommission.enabled=true. Then Spark moves the data off a node before the node stops.",
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
	s.Missing = []string{"Instance ID, type, vCPU and memory, spot or on-demand, and instance group (needs -cluster-id)", "Hosts in the cluster that ran no executors (needs -cluster-id)"}
	if r.Cluster != nil && len(r.Cluster.Instances) > 0 {
		s.Missing = nil
	}
	if !c.has() {
		s.Coverage = model.NeedsEventLog
		if r.Cluster != nil && len(r.Cluster.Instances) > 0 {
			s.Coverage = model.Partial
			s.Missing = []string{"Which nodes ran executors and what they did (the event log)"}
			// EMR lists every instance the cluster has had, so on a
			// long-lived cluster that scales most were not there for this
			// application: keep those up at some point while it ran.
			start, end := c.in.RunStart, c.in.RunEnd
			others := 0
			for _, in := range r.Cluster.Instances {
				in := in
				if !start.IsZero() && ((!in.Ended.IsZero() && in.Ended.Before(start)) || (!end.IsZero() && in.Created.After(end))) {
					others++
					continue
				}
				s.Hosts = append(s.Hosts, model.Host{Name: orID(in.PrivateDNS, in.ID), Instance: &in, Executors: []string{}})
			}
			switch {
			case start.IsZero():
				s.Lede = fmt.Sprintf("EMR lists %s for the cluster. Nothing says when the application ran (no event log, YARN summary or step), so they are all shown, including any that ended before it or joined after it.", model.Plural(len(s.Hosts), "node", "nodes"))
			case others > 0:
				s.Lede = fmt.Sprintf("The cluster had %s up while the application ran; %s that ended before it or joined after it %s left out. Without the event log, which of them ran this application is not known.",
					model.Plural(len(s.Hosts), "node", "nodes"), model.Plural(others, "other", "others"), map[bool]string{true: "is", false: "are"}[others == 1])
			default:
				s.Lede = fmt.Sprintf("The cluster had %s up while the application ran. Without the event log, which of them ran this application is not known.", model.Plural(len(s.Hosts), "node", "nodes"))
			}
		}
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
	idle := joinInstances(c, r, hosts, get)
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
	if r.Cluster != nil && len(r.Cluster.Instances) > 0 {
		up := 0
		for _, h := range s.Hosts {
			if h.Instance != nil {
				up++
			}
		}
		lede += fmt.Sprintf(" The cluster had %s up while it ran.", model.Plural(up, "node", "nodes"))
		if len(idle) > 0 {
			lede += fmt.Sprintf(" %s ran no executors.", model.Plural(len(idle), "worker node", "worker nodes"))
		}
		if !hasUnjoined(s.Hosts) {
			s.Coverage = model.Complete
		}
	}
	s.Lede = lede
	nodeFindings(c, r, idle)
}

func orID(dns, id string) string {
	if dns != "" {
		return dns
	}
	return id
}

// hostKey is a host name without its domain, or an IP address as the name
// EMR gives it, so the event log's and the EMR API's names match.
func hostKey(h string) string {
	h = strings.ToLower(strings.TrimSpace(h))
	if strings.Count(h, ".") == 3 && strings.Trim(h, "0123456789.") == "" {
		return "ip-" + strings.ReplaceAll(h, ".", "-")
	}
	if i := strings.IndexByte(h, '.'); i > 0 {
		return h[:i]
	}
	return h
}

func hasUnjoined(hs []model.Host) bool {
	for _, h := range hs {
		if h.Instance == nil {
			return true
		}
	}
	return false
}

// joinInstances attaches each host's EC2 instance and adds the instances
// that were up during the run but ran nothing for it. It returns the
// worker (core and task) nodes that ran no executors.
func joinInstances(c *ctx, r *model.Report, hosts map[string]*model.Host, get func(string) *model.Host) []*model.Host {
	if r.Cluster == nil {
		return nil
	}
	start, end := c.log.Application.Start, c.end
	byKey := map[string]string{}
	for name := range hosts {
		byKey[hostKey(name)] = name
	}
	var idle []*model.Host
	for _, in := range r.Cluster.Instances {
		in := in
		name, ok := byKey[hostKey(in.PrivateDNS)]
		if !ok {
			name, ok = byKey[hostKey(in.PrivateIP)]
		}
		if !ok {
			// Up at some point while the application ran?
			if (!in.Ended.IsZero() && !start.IsZero() && in.Ended.Before(start)) || (!end.IsZero() && in.Created.After(end)) {
				continue
			}
			name = orID(in.PrivateDNS, in.ID)
		}
		h := get(name)
		h.Instance = &in
		if len(h.Executors) == 0 && !in.Primary && in.Role != "MASTER" && upForMostOf(in, start, end) {
			idle = append(idle, h)
		}
	}
	return idle
}

// upForMostOf reports whether a node was ready for at least half the run
// and did not end before it: a node that joined near the end, or one that
// went away mid-run (see spot-interrupted), is not one the application
// left idle.
func upForMostOf(in model.Instance, start, end time.Time) bool {
	if start.IsZero() || end.IsZero() || !end.After(start) {
		return true
	}
	if !in.Ended.IsZero() && in.Ended.Before(end) {
		return false
	}
	from := in.Ready
	if from.IsZero() {
		from = in.Created
	}
	return !from.After(start.Add(end.Sub(start) / 2))
}

// nodeFindings reports worker nodes that ran no executors, and spot nodes
// that went away while the application ran.
func nodeFindings(c *ctx, r *model.Report, idle []*model.Host) {
	ran := 0
	for _, h := range r.Nodes.Hosts {
		ran += len(h.Executors)
	}
	// An application that never got an executor failed before it could use
	// the nodes; its failure is the finding, not the idle nodes.
	if len(idle) > 0 && ran > 0 {
		workers := 0
		for _, h := range r.Nodes.Hosts {
			if in := h.Instance; in != nil && !in.Primary && in.Role != "MASTER" && (len(h.Executors) > 0 || upForMostOf(*in, c.log.Application.Start, c.end)) {
				workers++
			}
		}
		var ev []model.Evidence
		driverOnly := 0
		for _, h := range idle {
			in := h.Instance
			what := "ran nothing for this application"
			if h.Driver {
				what = "ran only the driver"
				driverOnly++
			}
			ev = append(ev, model.Evidence{Text: fmt.Sprintf("%s (%s, %s %s, %s): %s", in.ID, h.Name, strings.ToLower(in.Role), in.Type, strings.ToLower(strings.ReplaceAll(in.Market, "_", "-")), what)})
		}
		expl := fmt.Sprintf("The cluster had %s for most of the run. %s ran no executors. As a result, the application used less of the cluster than you paid for.", model.Plural(workers, "worker node", "worker nodes"), model.Plural(len(idle), "worker node", "worker nodes"))
		if driverOnly > 0 {
			expl += "\nA node that ran only the driver had memory left, but no executor fitted into it. The executors have the size of a full node, and the driver container used a part of the node."
		}
		c.add(model.Finding{Rule: "idle-nodes", Severity: model.Warning, Section: "nodes",
			Title: fmt.Sprintf("%s of %s ran no executors", model.Plural(len(idle), "worker node", "worker nodes"), fmt.Sprint(workers)), Explanation: expl, Evidence: ev,
			Fix: "Do one of these:\n- Make the executors smaller (spark.executor.memory and spark.executor.cores), so that two or more fit on a node next to the driver.\n- Let dynamic allocation ask for more executors.\n- Use fewer nodes.\nIt is possible that other applications on the cluster used these nodes."})
	}
}

// spotFindings reports spot nodes that went away while the application
// ran, including while an earlier attempt ran, and what each took with
// it: executors, or an earlier attempt's driver. It runs after the logs
// are read, since only they say where an earlier attempt ran.
func spotFindings(c *ctx, r *model.Report) {
	if !c.has() {
		return
	}
	start, end := c.log.Application.Start, c.end
	drivers := map[string][]hit{} // host key → earlier attempts' driver-host lines
	notices := map[string]hit{}   // host key → YARN's first notice for it
	if c.logs != nil {
		for _, h := range c.logs.hits {
			if h.f.EarlierAttempt > 0 && !h.l.Time.IsZero() && h.l.Time.Before(start) {
				start = h.l.Time
			}
			switch h.l.Kind {
			case model.LogDriverHost:
				if h.f.EarlierAttempt > 0 {
					k := hostKey(h.l.Fields["host"])
					drivers[k] = append(drivers[k], h)
				}
			case model.LogNodeState:
				if k := hostKey(h.l.Fields["host"]); notices[k].l == nil {
					notices[k] = h
				}
			}
		}
	}
	for _, h := range r.Nodes.Hosts {
		in := h.Instance
		if in == nil || in.Market != "SPOT" || in.Ended.IsZero() || in.Ended.Before(start) || (!end.IsZero() && in.Ended.After(end)) {
			continue
		}
		k := hostKey(h.Name)
		ev := []model.Evidence{{Text: fmt.Sprintf("EMR ListInstances: spot instance %s (%s) ended at %s: %s", in.ID, h.Name, in.Ended.UTC().Format("15:04:05 UTC"), orNone(in.StateReason))}}
		if n := notices[k]; n.l != nil {
			ev = append(ev, n.evidence(fmt.Sprintf("%s: YARN reported the node %s at %s", n.who(), n.l.Fields["state"], c.clock(n.l.Time))))
		}
		for _, x := range c.log.Executors {
			if hostKey(x.Host) == k && x.RemovalKind != model.RemovalNone {
				ev = append(ev, model.Evidence{Source: x.RemovedSource, Ref: model.ExecutorRef(x.ID), Text: fmt.Sprintf("executor %s removed: %s", x.ID, x.RemovedReason)})
			}
		}
		var took []string
		sev := model.Warning
		if n := len(h.Executors); n > 0 {
			took = append(took, fmt.Sprintf("%s and the shuffle data on the node, and Spark did that work again", model.Plural(n, "executor", "executors")))
		}
		var lost []string
		for _, d := range drivers[k] {
			took = append(took, fmt.Sprintf("the driver of attempt %d", d.f.EarlierAttempt))
			lost = append(lost, fmt.Sprintf(" Attempt %d failed without its driver, and YARN started the application again.", d.f.EarlierAttempt))
			ev = append(ev, d.evidence(fmt.Sprintf("%s: the driver ran on %s", d.who(), h.Name)))
		}
		what := "It ran no executors of this application, so the application lost no work."
		if len(took) == 0 {
			sev = model.Info
		} else {
			what = "The application lost " + joinAnd(took) + "." + strings.Join(lost, "")
		}
		why := "EMR reports all instances that stop in the same way. As a result, sparkplain finds the spot interruption from the market and the time."
		if strings.Contains(strings.ToLower(in.StateReason), "spot") {
			why = "The reason from EMR: " + strings.TrimSuffix(in.StateReason, ".") + "."
		}
		c.add(model.Finding{Rule: "spot-interrupted", Severity: sev, Section: "nodes",
			Title:       fmt.Sprintf("Spot node %s stopped while the application ran", in.ID),
			Explanation: fmt.Sprintf("The node was a spot instance. It stopped at %s, before the application ended. %s\n%s", c.clock(in.Ended), what, why),
			Evidence:    ev,
			Fix:         "Do one of these:\n- Run the core work and the stages with much shuffle data on on-demand nodes. Use spot only for task nodes.\n- Set spark.decommission.enabled=true. Then Spark moves the shuffle data off a node when the node gets a notice."})
	}
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
