package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Each task's story comes from its executor's log, with or without the
// event log: on 0049 every task the event log records has one, and the
// shuffle blocks read add up to the event log's count. The report and the
// explorer show them.
func TestTaskStories(t *testing.T) {
	for _, withLog := range []bool{false, true} {
		dir := t.TempDir()
		app := "application_1790380000000_0049"
		args := []string{"-app-id", app, "-from", filepath.Join(emrlogs, "j-FIXTURE0049CLUSTER"), "-out", dir, "-format", "json,html,explorer"}
		if withLog {
			args = append(args, "-eventlog", filepath.Join(fx, app))
		}
		if code, _, errs := runCLI(t, args...); code != exitPartial && code != exitOK {
			t.Fatalf("exit %d: %s", code, errs)
		}
		r := readReport(t, dir)
		s := r.TaskStories
		if s == nil {
			t.Fatal("no task stories")
		}
		if len(s.Tasks) != 245 || s.ByTID != 245 || len(s.Executors) != 1 || s.Totals.ShuffleBlocks != 733 || s.Totals.Commits != 11 || s.Totals.Broadcasts != 20 {
			t.Errorf("event log %v: %d tasks (%d by TID), %d executors, totals %+v", withLog, len(s.Tasks), s.ByTID, len(s.Executors), s.Totals)
		}
		failed := 0
		for _, x := range s.Tasks {
			if x.Outcome == "failed" {
				failed++
				if x.Error == "" || x.Executor != "1" || x.Host == "" || len(x.Steps) < 2 {
					t.Errorf("failed task %d: %+v", x.TaskID, x)
				}
			}
		}
		if failed != 7 {
			t.Errorf("%d failed tasks, want 7", failed)
		}
		for _, m := range s.Missing {
			if strings.Contains(m, "were not read") {
				t.Errorf("every task has a story, but: %s", m)
			}
		}
		html, _ := os.ReadFile(filepath.Join(dir, app+"-report.html"))
		x, _ := os.ReadFile(filepath.Join(dir, app+"-explorer.html"))
		if !strings.Contains(string(html), `id="tasklogs"`) || !strings.Contains(string(x), `"taskStories"`) {
			t.Error("the report or the explorer has no task stories")
		}
	}
}

// On 0050 the executors read most shuffle data over the network: the
// flows say so, and with network-min lowered in the config file (a size
// such as 1MiB) shuffle-network notes it. A size that is not one is
// refused.
func TestFlows(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfg, []byte("thresholds:\n  network-min: 1MiB\n  broadcast-large: 1GiB\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	app := "application_1790380000000_0050"
	if code, _, errs := runCLI(t, "-config", cfg, "-app-id", app, "-from", filepath.Join(emrlogs, "j-FIXTURE0050CLUSTER"), "-out", dir, "-format", "json,html,explorer"); code != exitPartial {
		t.Fatalf("exit %d: %s", code, errs)
	}
	r := readReport(t, dir)
	f := r.Flows
	if f == nil || len(f.Executors) != 4 || f.RemoteBytes <= f.LocalBytes || len(f.Broadcasts) < 3 || len(f.Remote) == 0 {
		t.Fatalf("flows: %+v", f)
	}
	got := findingRules(t, dir)
	if !strings.Contains(got["shuffle-network"], "crossed the network between nodes") {
		t.Errorf("shuffle-network: %q", got["shuffle-network"])
	}
	if _, ok := got["broadcast-large"]; ok {
		t.Error("broadcast-large fired under broadcast-large: 1GiB")
	}
	html, _ := os.ReadFile(filepath.Join(dir, app+"-report.html"))
	if !strings.Contains(string(html), "Data moved over time") {
		t.Error("the report has no flows chart")
	}
	if err := os.WriteFile(cfg, []byte("thresholds:\n  network-min: lots\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code, _, errs := runCLI(t, "-config", cfg, "-app-id", app, "-from", filepath.Join(emrlogs, "j-FIXTURE0050CLUSTER"), "-out", dir); code != exitFatal || !strings.Contains(errs, `"lots" is not a size`) {
		t.Errorf("a bad size: exit %d: %s", code, errs)
	}
}
