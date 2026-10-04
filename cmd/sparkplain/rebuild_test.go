package main

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// With no event log, a run's jobs, stages, tasks and executors are rebuilt
// from its driver's log, labelled as rebuilt; what only the event log
// holds (task metrics) says so instead of reading zeros.
func TestRebuiltFromDriverLog(t *testing.T) {
	dir := t.TempDir()
	app := "application_1790380000000_0049"
	if code, _, errs := runCLI(t, "-app-id", app, "-from", filepath.Join(emrlogs, "j-FIXTURE0049CLUSTER"), "-out", dir, "-format", "json,html,explorer"); code != exitPartial {
		t.Fatalf("exit %d, want %d (no event log): %s", code, exitPartial, errs)
	}
	r := readReport(t, dir)
	if r.EventLog == nil || r.EventLog.Layout != model.LayoutRebuilt {
		t.Fatalf("event log stats %+v, want layout %q", r.EventLog, model.LayoutRebuilt)
	}
	if len(r.Jobs.Jobs) != 19 || len(r.Jobs.Stages) != 29 || r.Jobs.Coverage != model.Partial {
		t.Errorf("%d jobs, %d stages, coverage %q; want 19, 29, partial", len(r.Jobs.Jobs), len(r.Jobs.Stages), r.Jobs.Coverage)
	}
	for _, j := range r.Jobs.Jobs {
		if j.ID == 12 && (j.Status != model.StatusFailed || !strings.Contains(j.Source.File, "_01_000001")) {
			t.Errorf("job 12: status %q from %s, want failed, from the driver's log", j.Status, j.Source.File)
		}
	}
	if r.Application.Name != "sparkplain_emr_test" || r.Application.User != "hadoop" || r.Application.DeployMode != "cluster" {
		t.Errorf("application %q by %q in %q mode", r.Application.Name, r.Application.User, r.Application.DeployMode)
	}
	i := slices.IndexFunc(r.Sources, func(s model.SourceStatus) bool { return s.Name == rebuiltSource })
	if i < 0 || r.Sources[i].Status != "read" || !strings.Contains(r.Sources[i].Location, "container_1790380000000_0049_01_000001") {
		t.Errorf("no %q source read from the driver's container: %+v", rebuiltSource, r.Sources)
	}
	cov := map[string]model.Coverage{}
	for _, c := range r.Coverage {
		cov[c.ID] = c.Coverage
	}
	for _, id := range []string{"cpu", "memory", "io"} {
		if cov[id] != model.NeedsEventLog {
			t.Errorf("section %s: coverage %q, want needs-event-log", id, cov[id])
		}
	}
	got := findingRules(t, dir)
	if _, ok := got["job-failed"]; !ok {
		t.Errorf("no job-failed finding: %v", got)
	}
	for _, rule := range []string{"cpu-low", "memory-over-provisioned", "scheduler-delay", "large-results", "memory-spill"} {
		if _, ok := got[rule]; ok {
			t.Errorf("%s raised on a rebuilt run, which has no task metrics", rule)
		}
	}
	summary := strings.Join(r.Summary.Sentences, " ")
	if !strings.Contains(summary, "made this run again from the driver log") || strings.Contains(summary, "of CPU time") {
		t.Errorf("summary: %s", summary)
	}
	html, err := os.ReadFile(filepath.Join(dir, app+"-report.html"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(html), "Event log: not supplied. sparkplain made the run again from the driver log.") {
		t.Error("the report's banner does not say the run was rebuilt")
	}
}

// A rebuilt run ties its HBase scans' regions to the driver's tasks as the
// event log does, with their times, and no rows.
func TestRebuiltHBaseScan(t *testing.T) {
	dir := t.TempDir()
	if code, _, errs := runCLI(t, "-app-id", "application_1790380000000_0088", "-from", filepath.Join(emrlogs, hbaseCluster), "-out", dir, "-format", "json"); code != exitPartial {
		t.Fatalf("exit %d: %s", code, errs)
	}
	r := readReport(t, dir)
	if r.HBase == nil || len(r.HBase.Scans) != 1 {
		t.Fatalf("HBase scans: %+v", r.HBase)
	}
	sc := r.HBase.Scans[0]
	var ms []int64
	for _, g := range sc.Regions {
		if g.Task != nil {
			ms = append(ms, g.Task.DurationMs)
		}
	}
	// The event log's own task durations.
	if !sc.Rebuilt || !sc.Tied || sc.Table != "sp_events" || !slices.Equal(ms, []int64{269470, 267172, 269446}) {
		t.Errorf("scan: rebuilt %v tied %v table %q times %v", sc.Rebuilt, sc.Tied, sc.Table, ms)
	}
	for _, tk := range r.HBase.Tasks {
		if tk.RowsKnown || tk.Timed && tk.TimeFrom != "driver log" && tk.TimeFrom != "executor log" {
			t.Errorf("task %d: rows known %v, time from %q", tk.TaskID, tk.RowsKnown, tk.TimeFrom)
		}
	}
	got := findingRules(t, dir)
	if !strings.HasPrefix(got["hbase-time"], "Stages reading or writing HBase took 9 min 0 s") {
		t.Errorf("hbase-time: %q", got["hbase-time"])
	}
	if _, ok := got["cpu-low"]; ok {
		t.Error("cpu-low raised on a rebuilt run")
	}
}
