package analyze

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// series makes a per-minute series from start with the given values.
func series(name, stat, scope string, start time.Time, vs ...float64) model.Series {
	s := model.Series{Namespace: "AWS/ElasticMapReduce", Name: name, Stat: stat, Scope: scope, PeriodS: 60, Source: "CloudWatch " + name}
	for i, v := range vs {
		s.Points = append(s.Points, model.Point{T: start.Add(time.Duration(i) * time.Minute), V: v})
	}
	return s
}

func metricsRun(t *testing.T, pending, free, apps []float64, cpu, mem float64) *model.Report {
	return metricsRunFor(t, 10*time.Minute, pending, free, apps, cpu, mem)
}

func metricsRunFor(t *testing.T, run time.Duration, pending, free, apps []float64, cpu, mem float64) *model.Report {
	t.Helper()
	l := synthetic(nil, &model.Executor{ID: "1", Host: "ip-10-0-0-2.ec2.internal", Cores: 4})
	start := l.Application.Start
	l.Application.End = start.Add(run)
	l.Application.DurationMs = run.Milliseconds()
	cl := &model.Cluster{ID: "j-1", Instances: []model.Instance{{ID: "i-2", PrivateDNS: "ip-10-0-0-2.ec2.internal", Role: "CORE", Created: start.Add(-time.Hour)}}}
	m := &model.MetricsSection{Coverage: model.Complete, From: start.Add(-5 * time.Minute), To: start.Add(15 * time.Minute),
		Cluster: []model.Series{series("ContainerPending", "Maximum", "j-1", start, pending...), series("YARNMemoryAvailablePercentage", "Minimum", "j-1", start, free...),
			series("AppsRunning", "Maximum", "j-1", start, apps...)},
		Hosts: []model.Series{series("CPUUtilization", "Average", "i-2", start, cpu, cpu), series("CPUUtilization", "Maximum", "i-2", start, cpu+5, cpu+5),
			series("mem_used_percent", "Maximum", "i-2", start, mem)}}
	return Run(Input{Tool: "t", EventLog: l, EventSource: model.SourceStatus{Name: "Spark event log", Status: "read"}, Cluster: cl, Metrics: m})
}

func TestMetricFindings(t *testing.T) {
	t.Parallel()
	// Waiting 6 of 10 minutes with most memory free: containers too big.
	r := metricsRun(t, []float64{9, 9, 9, 9, 9, 9, 0, 0, 0, 0}, []float64{40, 40, 40, 40, 40, 40, 100, 100, 100, 100}, []float64{2, 2, 1}, 90, 97)
	got := rules(r)
	if f := got["waited-for-capacity"]; f.Title != "Containers waited 6 min 0 s while 40% of YARN memory was free" || !strings.Contains(f.Explanation, "2 applications ran at the same time") {
		t.Errorf("waited = %+v", f)
	}
	if f := got["shared-cluster"]; f.Title != "2 applications shared the cluster" || f.Severity != model.Info {
		t.Errorf("shared = %+v", f)
	}
	if f := got["host-cpu-saturated"]; f.Title != "1 node ran near full CPU" || !strings.Contains(f.Evidence[0].Text, "90% CPU on average, 95% at peak") {
		t.Errorf("cpu = %+v", f)
	}
	if f := got["host-memory-pressure"]; f.Title != "1 node ran out of free memory" {
		t.Errorf("memory = %+v", f)
	}
	h := r.Nodes.Hosts[0]
	if h.HostCPU == nil || h.HostCPU.Average != 90 || h.HostCPU.Peak != 95 {
		t.Errorf("host cpu = %+v", h.HostCPU)
	}
	if len(r.Metrics.Summary) != 3 || r.Metrics.Summary[0].Value != "9 at most, for 6 min 0 s" {
		t.Errorf("summary = %+v", r.Metrics.Summary)
	}

	// Waiting with the cluster full: too small a cluster.
	full := rules(metricsRun(t, []float64{4, 4, 4, 4, 0}, []float64{5, 5, 5, 5, 60}, []float64{1}, 30, 50))
	if f := full["waited-for-capacity"]; f.Title != "Containers waited 4 min 0 s for room on the cluster" || !strings.Contains(f.Fix, "Add nodes") {
		t.Errorf("full = %+v", f)
	}
	for _, rule := range []string{"shared-cluster", "host-cpu-saturated", "host-memory-pressure"} {
		if _, ok := full[rule]; ok {
			t.Errorf("%s should not fire", rule)
		}
	}
	// A short run that spent most of its time waiting counts too (the
	// NOAA run: 2 min 50 s, containers waiting throughout).
	short := rules(metricsRunFor(t, 170*time.Second, []float64{49, 8, 37}, []float64{44, 44, 44}, []float64{1}, 30, 50))
	if f := short["waited-for-capacity"]; f.Title != "Containers waited 2 min 50 s while 44% of YARN memory was free" {
		t.Errorf("short run = %+v", f)
	}
	// A minute of waiting is normal start-up.
	if _, ok := rules(metricsRun(t, []float64{20, 0, 0}, []float64{40}, []float64{1}, 30, 50))["waited-for-capacity"]; ok {
		t.Error("a brief wait raised waited-for-capacity")
	}
}

// EC2 publishes a node's CPU every 5 minutes, each point stamped with the
// start of its period. On the phase 4 test cluster a run from 08:02:19 to
// 08:06:36 fell mostly in the 08:00 point, which was left out, and a node
// reclaimed at 08:03 had no CPU at all.
func TestHostCPUCountsPeriodsTheRunOverlaps(t *testing.T) {
	t.Parallel()
	l := synthetic(nil, &model.Executor{ID: "1", Host: "ip-10-0-0-2.ec2.internal", Cores: 4})
	start := l.Application.Start
	l.Application.End = start.Add(4 * time.Minute)
	l.Application.DurationMs = 240_000
	cl := &model.Cluster{ID: "j-1", Instances: []model.Instance{{ID: "i-2", PrivateDNS: "ip-10-0-0-2.ec2.internal", Role: "CORE", Created: start.Add(-time.Hour)}}}
	cpu := func(stat string, vs ...float64) model.Series {
		// Asked for every minute, answered every 5 minutes.
		s := model.Series{Namespace: "AWS/EC2", Name: "CPUUtilization", Stat: stat, Scope: "i-2", PeriodS: 60, Source: "CloudWatch CPUUtilization"}
		for i, v := range vs {
			s.Points = append(s.Points, model.Point{T: start.Add(-2*time.Minute + time.Duration(i)*5*time.Minute), V: v})
		}
		return s
	}
	// i-4 was reclaimed within minutes: one point, spaced like i-2's.
	cl.Instances = append(cl.Instances, model.Instance{ID: "i-4", PrivateDNS: "ip-10-0-0-4.ec2.internal", Role: "TASK", Market: "SPOT", Created: start.Add(-time.Hour), Ended: start.Add(time.Minute)})
	lone := cpu("Average", 14)
	lone.Scope = "i-4"
	m := &model.MetricsSection{Coverage: model.Complete, From: start.Add(-5 * time.Minute), To: start.Add(10 * time.Minute),
		Hosts: []model.Series{cpu("Average", 40, 20), cpu("Maximum", 90, 60), lone}}
	r := Run(Input{Tool: "t", EventLog: l, EventSource: model.SourceStatus{Name: "Spark event log", Status: "read"}, Cluster: cl, Metrics: m})
	for _, h := range r.Nodes.Hosts {
		switch h.Instance.ID {
		case "i-2":
			if c := h.HostCPU; c == nil || c.Average != 30 || c.Peak != 90 {
				t.Errorf("i-2 cpu = %+v, want both periods: 30%% average, 90%% peak", c)
			}
		case "i-4":
			if c := h.HostCPU; c == nil || c.Average != 14 {
				t.Errorf("i-4 cpu = %+v, want its one point", c)
			}
		}
	}
	if k := r.Summary.KPIs[1]; k.Label != "Hosts" || k.Explain != "Machines that ran executors, of 2 nodes up during the run; the Nodes section describes each one." {
		t.Errorf("hosts card = %+v", k)
	}
}

// EMR's cluster series can come back with no point while a short run ran
// (a 31 s run on the phase 6 cluster). That is not zero containers waiting
// and 100% memory free: the facts are left out and the gap is said.
func TestClusterMetricsWithNoPointsAreNotZero(t *testing.T) {
	t.Parallel()
	r := metricsRun(t, nil, nil, nil, 30, 50)
	for _, f := range r.Metrics.Summary {
		t.Errorf("fact from an empty series: %+v", f)
	}
	want := "Containers waiting, YARN memory free and applications at once: CloudWatch returned no EMR values for them while the application ran."
	if !slices.Contains(r.Metrics.Missing, want) {
		t.Errorf("missing = %q", r.Metrics.Missing)
	}
	for _, rule := range []string{"waited-for-capacity", "shared-cluster"} {
		if _, ok := rules(r)[rule]; ok {
			t.Errorf("%s fired from an empty series", rule)
		}
	}
	// Only the one without points is left out.
	r = metricsRun(t, []float64{0, 0}, nil, []float64{1}, 30, 50)
	if len(r.Metrics.Summary) != 2 || !slices.Contains(r.Metrics.Missing, "YARN memory free: CloudWatch returned no EMR values for them while the application ran.") {
		t.Errorf("summary = %+v, missing = %q", r.Metrics.Summary, r.Metrics.Missing)
	}
}
