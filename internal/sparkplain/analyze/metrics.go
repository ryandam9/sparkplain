package analyze

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// Thresholds for the metric rules. Kept here rather than in the config
// file until real runs show teams need to tune them.
const (
	pendingMinMinutes = 3    // containers waiting at least this long
	pendingMinShare   = 0.20 // and for at least this share of the run
	freeMemoryShare   = 25.0 // YARN memory free (%) above which waiting means "did not fit"
	hotCPU            = 85.0 // a node's average CPU (%) over the run
	hotMemory         = 95.0 // a node's highest memory use (%), from the agent
)

// spacing is the smallest gap between a series' points, or 0 when it has
// fewer than two.
func spacing(s *model.Series) time.Duration {
	var gap time.Duration
	for i := 1; i < len(s.Points); i++ {
		if g := s.Points[i].T.Sub(s.Points[i-1].T); g > 0 && (gap == 0 || g < gap) {
			gap = g
		}
	}
	return gap
}

// analyzeMetrics reads the CloudWatch metrics for the run: each node's CPU
// for the Nodes table, and findings for containers that waited, other
// applications sharing the cluster, and busy nodes.
func analyzeMetrics(c *ctx, r *model.Report) {
	m := r.Metrics
	if m == nil || m.Coverage == model.NoData {
		return
	}
	start, end := r.Application.Start, r.Application.End
	if start.IsZero() {
		// Without the application's own times, its step's are the next
		// best; the padded metrics window would count other work as its.
		for _, st := range r.Steps {
			if st.AppID != "" && !st.Started.IsZero() {
				start, end = st.Started, st.Ended
				if end.IsZero() && r.Cluster != nil {
					end = r.Cluster.Ended // cancelled with its cluster
				}
			}
		}
	}
	timed := !start.IsZero()
	if !timed {
		start, end = m.From, m.To
	}
	if end.IsZero() || end.Before(start) {
		end = m.To
	}
	// A point stands for the period that starts at its time: a 5-minute EC2
	// point at 08:00 covers a run that started at 08:02. That period is the
	// one asked for, or the spacing of the points when wider: EC2 without
	// detailed monitoring has only 5-minute CPU points, and CloudWatch
	// returns those when asked for 60-second ones (as on the phase 4 test
	// cluster). A series of one point, from a node that lived minutes,
	// takes the spacing of the same metric on the other nodes.
	metricGap := map[string]time.Duration{}
	for _, list := range [][]model.Series{m.Cluster, m.Hosts} {
		for i := range list {
			k := list[i].Namespace + "/" + list[i].Name
			if g := spacing(&list[i]); g > 0 && (metricGap[k] == 0 || g < metricGap[k]) {
				metricGap[k] = g
			}
		}
	}
	in := func(s *model.Series, p model.Point) bool {
		gap := spacing(s)
		if gap == 0 {
			gap = metricGap[s.Namespace+"/"+s.Name]
		}
		return p.T.Add(max(time.Duration(max(s.PeriodS, 60))*time.Second, gap)).After(start) && !p.T.After(end)
	}
	find := func(list []model.Series, name, stat, scope string) *model.Series {
		for i := range list {
			s := &list[i]
			if s.Name == name && s.Stat == stat && (scope == "" || s.Scope == scope) {
				return s
			}
		}
		return nil
	}

	// Each node's CPU over the run.
	for i := range r.Nodes.Hosts {
		h := &r.Nodes.Hosts[i]
		if h.Instance == nil {
			continue
		}
		avg, mx := find(m.Hosts, "CPUUtilization", "Average", h.Instance.ID), find(m.Hosts, "CPUUtilization", "Maximum", h.Instance.ID)
		if avg == nil {
			continue
		}
		var sum, peak float64
		n := 0
		for _, p := range avg.Points {
			if in(avg, p) {
				sum += p.V
				n++
			}
		}
		if mx != nil {
			for _, p := range mx.Points {
				if in(mx, p) {
					peak = max(peak, p.V)
				}
			}
		}
		if n > 0 {
			h.HostCPU = &model.HostCPU{Average: sum / float64(n), Peak: peak, Source: avg.Source}
		}
	}

	pending, free, apps := find(m.Cluster, "ContainerPending", "Maximum", ""), find(m.Cluster, "YARNMemoryAvailablePercentage", "Minimum", ""), find(m.Cluster, "AppsRunning", "Maximum", "")
	// A series with no point while the application ran says nothing, not
	// zero: EMR's cluster metrics can be missing for a short run.
	var unrecorded []string
	for _, x := range []struct {
		s    **model.Series
		what string
	}{{&pending, "containers waiting"}, {&free, "YARN memory free"}, {&apps, "applications at once"}} {
		if *x.s != nil && !slices.ContainsFunc((*x.s).Points, func(p model.Point) bool { return in(*x.s, p) }) {
			*x.s = nil
			unrecorded = append(unrecorded, x.what)
		}
	}
	if len(unrecorded) > 0 {
		list := unrecorded[0]
		if n := len(unrecorded); n > 1 {
			list = strings.Join(unrecorded[:n-1], ", ") + " and " + unrecorded[n-1]
		}
		m.Missing = append(m.Missing, strings.ToUpper(list[:1])+list[1:]+": CloudWatch returned no EMR values for them while the application ran.")
	}
	src := func(s *model.Series) model.Source { return model.Source{File: s.Source} }
	maxApps := 0.0
	if apps != nil {
		for _, p := range apps.Points {
			if in(apps, p) {
				maxApps = max(maxApps, p.V)
			}
		}
	}
	if pending != nil {
		var waitMin, peak, freeSum float64
		freeN := 0
		freeAt := map[time.Time]float64{}
		if free != nil {
			for _, p := range free.Points {
				freeAt[p.T] = p.V
			}
		}
		for _, p := range pending.Points {
			if !in(pending, p) || p.V <= 0 {
				continue
			}
			waitMin += float64(pending.PeriodS) / 60
			peak = max(peak, p.V)
			if v, ok := freeAt[p.T]; ok {
				freeSum += v
				freeN++
			}
		}
		// Each point covers a whole period, so a run that starts and ends
		// inside minutes can count more waiting than it lasted.
		runMin := end.Sub(start).Minutes()
		waitMin = min(waitMin, runMin)
		m.Summary = append(m.Summary, model.Fact{Label: "Containers waiting", Value: fmt.Sprintf("%.0f at most, for %s", peak, model.Duration(int64(waitMin*60000))),
			Explain: "Containers YARN had been asked for but had not placed, across the whole cluster.", Source: src(pending)})
		// Minutes of waiting over a fair share of the run, or most of a
		// short run spent waiting.
		long := waitMin >= pendingMinMinutes && runMin > 0 && waitMin/runMin >= pendingMinShare
		most := waitMin >= 1 && runMin > 0 && waitMin/runMin >= 0.5
		if timed && (long || most) {
			ev := []model.Evidence{{Source: src(pending), Text: fmt.Sprintf("CloudWatch: up to %.0f containers waiting, for %s of the %s run", peak, model.Duration(int64(waitMin*60000)), model.Duration(int64(runMin*60000)))}}
			f := model.Finding{Rule: "waited-for-capacity", Severity: model.Warning, Section: "nodes"}
			avgFree := -1.0
			if freeN > 0 {
				avgFree = freeSum / float64(freeN)
				ev = append(ev, model.Evidence{Source: src(free), Text: fmt.Sprintf("CloudWatch: %.0f%% of YARN memory was free on average while they waited", avgFree)})
			}
			if avgFree >= freeMemoryShare {
				f.Title = fmt.Sprintf("Containers waited %s while %.0f%% of YARN memory was free", model.Duration(int64(waitMin*60000)), avgFree)
				f.Explanation = "YARN had free memory, but not in pieces large enough for the containers. Each container must fit on one node. As a result, free memory on many nodes cannot hold a large executor."
				f.Fix = "Use smaller executors, so that they fit in the memory that remains on each node. Refer to the executor size finding, if there is one."
			} else {
				f.Title = fmt.Sprintf("Containers waited %s for room on the cluster", model.Duration(int64(waitMin*60000)))
				f.Explanation = "The YARN memory was in use, so the containers waited until other containers finished. The application ran with fewer executors than it asked for."
				f.Fix = "Do one of these:\n- Add nodes, or let EMR managed scaling add them.\n- Run fewer applications at the same time."
			}
			if maxApps > 1 {
				f.Explanation += fmt.Sprintf("\n%.0f applications ran at the same time. It is possible that some of the containers that waited were theirs.", maxApps)
			}
			f.Evidence = ev
			c.add(f)
		}
	}
	if free != nil {
		low := 100.0
		for _, p := range free.Points {
			if in(free, p) {
				low = min(low, p.V)
			}
		}
		m.Summary = append(m.Summary, model.Fact{Label: "YARN memory free", Value: fmt.Sprintf("%.0f%% at the lowest", low),
			Explain: "The share of the cluster's YARN memory no container was using.", Source: src(free)})
	}
	if apps != nil {
		m.Summary = append(m.Summary, model.Fact{Label: "Applications at once", Value: fmt.Sprintf("%.0f at most", maxApps),
			Explain: "Applications YARN was running on the cluster at the same time, this one included.", Source: src(apps)})
		if maxApps > 1 {
			f := model.Finding{Rule: "shared-cluster", Severity: model.Info, Section: "nodes",
				Title:       fmt.Sprintf("%.0f applications shared the cluster", maxApps),
				Explanation: "Other applications ran on the cluster at the same time. As a result, the node metrics (CPU, network, containers that wait) include their work too. The other applications also wanted the same executors as this one.",
				Evidence:    []model.Evidence{{Source: src(apps), Text: fmt.Sprintf("CloudWatch AppsRunning reached %.0f during the run", maxApps)}},
				Fix:         "If the time of this run is important, do one of these:\n- Give it its own cluster.\n- Give it a YARN queue with guaranteed capacity."}
			// A node this application left alone may have been busy with
			// the others, so it is not called idle.
			if idle := c.drop("idle-nodes"); idle != nil {
				others := "the other applications"
				if maxApps == 2 {
					others = "the other application"
				}
				f.Explanation += fmt.Sprintf("\n%s ran nothing for this application. It is possible that %s used %s.",
					model.Plural(len(idle.Evidence), "worker node", "worker nodes"), others, map[bool]string{true: "it", false: "them"}[len(idle.Evidence) == 1])
				f.Evidence = append(f.Evidence, idle.Evidence...)
			}
			c.add(f)
		}
	}

	var hot []string
	var hotEv []model.Evidence
	var memEv []model.Evidence
	for _, h := range r.Nodes.Hosts {
		if h.HostCPU != nil && h.HostCPU.Average >= hotCPU {
			hot = append(hot, h.Instance.ID)
			hotEv = append(hotEv, model.Evidence{Source: model.Source{File: h.HostCPU.Source}, Text: fmt.Sprintf("%s (%s): %.0f%% CPU on average, %.0f%% at peak", h.Instance.ID, h.Name, h.HostCPU.Average, h.HostCPU.Peak)})
		}
		if h.Instance == nil {
			continue
		}
		if s := find(m.Hosts, "mem_used_percent", "Maximum", h.Instance.ID); s != nil {
			if v, ok := s.Max(); ok && v >= hotMemory {
				memEv = append(memEv, model.Evidence{Source: src(s), Text: fmt.Sprintf("%s (%s): %.0f%% of memory in use at peak", h.Instance.ID, h.Name, v)})
			}
		}
	}
	if len(hot) > 0 {
		c.add(model.Finding{Rule: "host-cpu-saturated", Severity: model.Warning, Section: "cpu",
			Title:       fmt.Sprintf("%s ran near full CPU", model.Plural(len(hot), "node", "nodes")),
			Explanation: fmt.Sprintf("These nodes used more than %.0f%% CPU on average while the application ran. As a result, tasks waited for processor time. More cores for each executor do not help.", hotCPU),
			Evidence:    hotEv,
			Fix:         "Do one of these:\n- Use instance types with more vCPUs.\n- Use more nodes.\nMake sure that spark.executor.cores is not more than the vCPUs that each executor gets."})
	}
	if len(memEv) > 0 {
		c.add(model.Finding{Rule: "host-memory-pressure", Severity: model.Warning, Section: "memory",
			Title:       fmt.Sprintf("%s ran out of free memory", model.Plural(len(memEv), "node", "nodes")),
			Explanation: "The CloudWatch agent found that the memory of these nodes was almost full. Then the operating system stops processes or uses swap. This can stop executors without a Java error.",
			Evidence:    memEv,
			Fix:         "Keep memory free for the operating system and the daemons. Do one of these:\n- Decrease yarn.nodemanager.resource.memory-mb.\n- Use instances with more memory."})
	}
	if len(m.Summary) > 0 && !strings.Contains(r.Nodes.Lede, "CloudWatch") {
		r.Nodes.Lede += " CloudWatch shows how busy the cluster was while it ran."
	}
}
