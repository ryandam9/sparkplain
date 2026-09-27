package analyze

import (
	"fmt"
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
	in := func(p model.Point) bool { return !p.T.Before(start.Truncate(time.Minute)) && !p.T.After(end) }
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
			if in(p) {
				sum += p.V
				n++
			}
		}
		if mx != nil {
			for _, p := range mx.Points {
				if in(p) {
					peak = max(peak, p.V)
				}
			}
		}
		if n > 0 {
			h.HostCPU = &model.HostCPU{Average: sum / float64(n), Peak: peak, Source: avg.Source}
		}
	}

	pending, free, apps := find(m.Cluster, "ContainerPending", "Maximum", ""), find(m.Cluster, "YARNMemoryAvailablePercentage", "Minimum", ""), find(m.Cluster, "AppsRunning", "Maximum", "")
	src := func(s *model.Series) model.Source { return model.Source{File: s.Source} }
	maxApps := 0.0
	if apps != nil {
		for _, p := range apps.Points {
			if in(p) {
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
			if !in(p) || p.V <= 0 {
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
				f.Explanation = "YARN had memory to spare, but not in pieces big enough for the containers asked for: each container must fit on one node, so free memory spread across nodes cannot hold a large executor."
				f.Fix = "Use smaller executors (see the executor size finding, when there is one), so they fit in the memory each node has left."
			} else {
				f.Title = fmt.Sprintf("Containers waited %s for room on the cluster", model.Duration(int64(waitMin*60000)))
				f.Explanation = "YARN's memory was in use, so requested containers queued until something finished. The application ran with fewer executors than it asked for."
				f.Fix = "Add nodes (or let EMR managed scaling add them), or run fewer applications at the same time."
			}
			if maxApps > 1 {
				f.Explanation += fmt.Sprintf(" %.0f applications were running at once, so some of the waiting containers may have been theirs.", maxApps)
			}
			f.Evidence = ev
			c.add(f)
		}
	}
	if free != nil {
		low := 100.0
		for _, p := range free.Points {
			if in(p) {
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
				Explanation: "Other applications ran on the cluster at the same time, so node metrics (CPU, network, containers waiting) include their work too, and they competed with this one for executors.",
				Evidence:    []model.Evidence{{Source: src(apps), Text: fmt.Sprintf("CloudWatch AppsRunning reached %.0f during the run", maxApps)}},
				Fix:         "If this run's timing matters, give it its own cluster or a YARN queue with guaranteed capacity."}
			// A node this application left alone may have been busy with
			// the others, so it is not called idle.
			if idle := c.drop("idle-nodes"); idle != nil {
				f.Explanation += fmt.Sprintf(" %s ran nothing for this application; the other applications may have been using %s.",
					model.Plural(len(idle.Evidence), "worker node", "worker nodes"), map[bool]string{true: "it", false: "them"}[len(idle.Evidence) == 1])
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
			Explanation: fmt.Sprintf("These nodes averaged over %.0f%% CPU while the application ran, so tasks queued for processor time; more cores per executor would not have helped.", hotCPU),
			Evidence:    hotEv,
			Fix:         "Use instance types with more vCPUs, or more nodes. Check that each executor's spark.executor.cores does not exceed the vCPUs it gets."})
	}
	if len(memEv) > 0 {
		c.add(model.Finding{Rule: "host-memory-pressure", Severity: model.Warning, Section: "memory",
			Title:       fmt.Sprintf("%s ran out of free memory", model.Plural(len(memEv), "node", "nodes")),
			Explanation: "The CloudWatch agent saw these nodes' memory almost full. The operating system then kills processes or swaps, which can take executors down with no Java error.",
			Evidence:    memEv,
			Fix:         "Leave memory for the operating system and daemons: lower yarn.nodemanager.resource.memory-mb, or use instances with more memory."})
	}
	if len(m.Summary) > 0 && !strings.Contains(r.Nodes.Lede, "CloudWatch") {
		r.Nodes.Lede += " CloudWatch shows how busy the cluster was while it ran."
	}
}
