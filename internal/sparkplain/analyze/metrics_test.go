package analyze

import (
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
	t.Helper()
	l := synthetic(nil, &model.Executor{ID: "1", Host: "ip-10-0-0-2.ec2.internal", Cores: 4})
	start := l.Application.Start
	l.Application.End = start.Add(10 * time.Minute)
	l.Application.DurationMs = 600_000
	cl := &model.Cluster{ID: "j-1", Instances: []model.Instance{{ID: "i-2", PrivateDNS: "ip-10-0-0-2.ec2.internal", Role: "CORE", Created: start.Add(-time.Hour)}}}
	m := &model.MetricsSection{Coverage: model.Complete, From: start.Add(-5 * time.Minute), To: start.Add(15 * time.Minute),
		Cluster: []model.Series{series("ContainerPending", "Maximum", "j-1", start, pending...), series("YARNMemoryAvailablePercentage", "Minimum", "j-1", start, free...),
			series("AppsRunning", "Maximum", "j-1", start, apps...)},
		Hosts: []model.Series{series("CPUUtilization", "Average", "i-2", start, cpu, cpu), series("CPUUtilization", "Maximum", "i-2", start, cpu+5, cpu+5),
			series("mem_used_percent", "Maximum", "i-2", start, mem)}}
	return Run(Input{Tool: "t", EventLog: l, EventSource: model.SourceStatus{Name: "Spark event log", Status: "read"}, Cluster: cl, Metrics: m})
}

func TestMetricFindings(t *testing.T) {
	// Waiting 6 of 10 minutes with most memory free: containers too big.
	r := metricsRun(t, []float64{9, 9, 9, 9, 9, 9, 0, 0, 0, 0}, []float64{40, 40, 40, 40, 40, 40, 100, 100, 100, 100}, []float64{2, 2, 1}, 90, 97)
	got := rules(r)
	if f := got["waited-for-capacity"]; f.Title != "Containers waited 6 min 0 s while 40% of YARN memory was free" || !strings.Contains(f.Explanation, "2 applications were running at once") {
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
	// A minute of waiting is normal start-up.
	if _, ok := rules(metricsRun(t, []float64{20, 0, 0}, []float64{40}, []float64{1}, 30, 50))["waited-for-capacity"]; ok {
		t.Error("a brief wait raised waited-for-capacity")
	}
}
