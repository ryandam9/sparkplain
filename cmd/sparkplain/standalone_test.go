package main

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

const standalone = "../../testdata/standalone"

// -from finds a Spark Standalone run's executor logs in a worker's work
// folder, the folder that holds it ($SPARK_HOME), or one folder per
// machine; a copy of an EMR cluster's log folder still wins.
func TestFromLayoutStandalone(t *testing.T) {
	t.Parallel()
	const app = "app-20261025032316-0000"
	mk := func(t *testing.T, dirs ...string) string {
		root := t.TempDir()
		for _, d := range dirs {
			if err := os.MkdirAll(filepath.Join(root, filepath.FromSlash(d)), 0o755); err != nil {
				t.Fatal(err)
			}
		}
		return root
	}
	for name, c := range map[string]struct {
		dirs []string
		want []string
	}{
		"work folder":     {[]string{app + "/0"}, []string{app + "/"}},
		"SPARK_HOME":      {[]string{"work/" + app + "/0", "conf", "jars"}, []string{"work/" + app + "/"}},
		"one per machine": {[]string{"a/work/" + app + "/0", "b/" + app + "/1", "c/work/app-other/0"}, []string{"a/work/" + app + "/", "b/" + app + "/"}},
		"EMR copy wins":   {[]string{"containers", "work/" + app + "/0"}, nil},
	} {
		lay, err := fromLayout(mk(t, c.dirs...), app)
		if err != nil || !slices.Equal(lay.standalone, c.want) {
			t.Errorf("%s: %+v, %v; want %q", name, lay, err, c.want)
		}
	}
	if _, err := fromLayout(mk(t, "work/app-other/0"), app); err == nil || !strings.Contains(err.Error(), "no Spark Standalone work folder for "+app) {
		t.Errorf("a folder without the application: %v", err)
	}
}

// Both fixtures, Standalone and local-cluster, read every executor's log,
// each tied to its executor by its folder and to its host by the event
// log, and give every task its story. A secret planted in an executor's
// log never reaches the output. (runCLI sets HOME, so not parallel.)
func TestStandaloneExecutorLogs(t *testing.T) {
	for _, c := range []struct{ app, from string }{
		{"app-20261025032316-0000", "cluster"},
		{"app-20261025032411-0000", "local-cluster"},
	} {
		from := filepath.Join(t.TempDir(), c.from)
		copyTree(t, filepath.Join(standalone, c.from), from)
		planted := filepath.Join(from, "work", c.app, "2", "stderr")
		f, err := os.OpenFile(planted, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		f.WriteString("26/10/25 03:23:59 ERROR Executor: connect failed with spark.hadoop.fs.s3a.secret.key=FAKESECRETabc123 password=hunter2fake\n")
		f.Close()
		out := t.TempDir()
		code, stdout, errs := runCLI(t, "-app-id", c.app, "-eventlog", filepath.Join(fx, c.app), "-from", from, "-out", out, "-format", "json,html,explorer")
		if code != exitOK && code != exitPartial {
			t.Fatalf("%s: exit %d: %s", c.from, code, errs)
		}
		r := readReport(t, out)
		var execs []string
		for _, lf := range r.Logs.Files {
			if lf.Kind != "container-stderr" {
				continue
			}
			execs = append(execs, lf.Executor)
			if lf.Container != "" || lf.Host != "localhost" {
				t.Errorf("%s: %s: container %q, host %q", c.from, lf.Location, lf.Container, lf.Host)
			}
		}
		slices.Sort(execs)
		if !slices.Equal(execs, []string{"0", "1", "2", "3"}) {
			t.Errorf("%s: executors %q", c.from, execs)
		}
		s := r.TaskStories
		if s == nil || len(s.Tasks) != 52 || s.ByTID != 52 || len(s.Executors) != 4 { // the event log's 52 tasks
			t.Errorf("%s: task stories %+v", c.from, s)
		}
		src := false
		for _, x := range r.Sources {
			src = src || x.Name == "Executor logs" && x.Status == "read"
		}
		if !src {
			t.Errorf("%s: no Executor logs source read: %+v", c.from, r.Sources)
		}
		for _, name := range []string{c.app + "-report.json", c.app + "-report.html", c.app + "-explorer.html"} {
			b, _ := os.ReadFile(filepath.Join(out, name))
			if len(b) == 0 || strings.Contains(string(b), "FAKESECRET") || strings.Contains(string(b), "hunter2fake") {
				t.Errorf("%s: %s is empty or holds the planted secret", c.from, name)
			}
		}
		if b, _ := os.ReadFile(filepath.Join(out, c.app+"-report.json")); !strings.Contains(string(b), "secret.key=[redacted] password=[redacted]") {
			t.Errorf("%s: the planted line was not read, so the secret check proves nothing", c.from)
		}
		if strings.Contains(stdout+errs, "FAKESECRET") || strings.Contains(stdout+errs, "hunter2fake") {
			t.Errorf("%s: the console shows the planted secret", c.from)
		}
	}
}

// The event log names a Standalone cluster-mode driver's folder in its
// driver log links; YARN's and client mode's do not.
func TestStandaloneDriverID(t *testing.T) {
	t.Parallel()
	for links, want := range map[[2]string]string{
		{"http://localhost:8081/logPage/?driverId=driver-20261025043052-0000&logType=stderr", ""}:                "driver-20261025043052-0000",
		{"http://ip-10-0-2-10:8042/node/containerlogs/container_1_0001_01_000001/hadoop/stderr?start=-4096", ""}: "",
		{"http://w:8081/logPage/?driverId=../../etc&logType=stderr", ""}:                                         "",
		{"", ""}: "",
	} {
		if got := standaloneDriverID(map[string]string{"stderr": links[0]}); got != want {
			t.Errorf("%q: %q, want %q", links[0], got, want)
		}
	}
}

// In cluster deploy mode the driver runs in a worker's work/driver-…/
// folder. With the event log its ID picks the folder; without, the
// driver's own "Connected to Spark cluster with app ID" line does, and
// the run is rebuilt from it. Another application's driver is never read.
func TestStandaloneClusterModeDriver(t *testing.T) {
	const app = "app-20261025043058-0000"
	from := filepath.Join(t.TempDir(), "cluster-mode")
	copyTree(t, filepath.Join(standalone, "cluster-mode"), from)
	decoy := filepath.Join(from, "work", "driver-20261025040000-0000")
	if err := os.MkdirAll(decoy, 0o755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(decoy, "stderr"), []byte("26/10/25 04:00:01 INFO StandaloneSchedulerBackend: Connected to Spark cluster with app ID app-20261025040000-0000\n26/10/25 04:00:02 INFO DAGScheduler: Job 0 finished: decoy at Decoy.scala:1, took 1.0 s\n"), 0o644)
	for _, withLog := range []bool{true, false} {
		out := t.TempDir()
		args := []string{"-app-id", app, "-from", from, "-out", out, "-format", "json"}
		if withLog {
			args = append(args, "-eventlog", filepath.Join(fx, app))
		}
		if code, _, errs := runCLI(t, args...); code != exitOK && code != exitPartial {
			t.Fatalf("event log %v: exit %d: %s", withLog, code, errs)
		}
		r := readReport(t, out)
		var drivers []string
		for _, f := range r.Logs.Files {
			if f.Executor == "driver" {
				drivers = append(drivers, filepath.Base(filepath.Dir(f.Location))+"/"+filepath.Base(f.Location))
			}
		}
		slices.Sort(drivers)
		if !slices.Equal(drivers, []string{"driver-20261025043052-0000/stderr", "driver-20261025043052-0000/stdout"}) {
			t.Errorf("event log %v: driver files %q", withLog, drivers)
		}
		if len(r.Jobs.Jobs) != 1 || r.TaskStories == nil || len(r.TaskStories.Tasks) != 20 {
			t.Errorf("event log %v: %d jobs, task stories %+v", withLog, len(r.Jobs.Jobs), r.TaskStories)
		}
	}
}

// -driver-log reads a client-mode driver's console saved to a file as the
// driver's log: with the executors' logs and no event log, the run is
// rebuilt from it. A secret planted in it is redacted, and a missing file
// is a source in error.
func TestDriverLogFlag(t *testing.T) {
	const app = "app-20261025032316-0000"
	dir := t.TempDir()
	console := filepath.Join(dir, "driver.txt")
	b, err := os.ReadFile(filepath.Join(standalone, "cluster", "driver.txt"))
	if err != nil {
		t.Fatal(err)
	}
	b = append(b, "26/10/25 03:23:53 ERROR SparkContext: connect failed with spark.hadoop.fs.s3a.secret.key=FAKESECRETdriver9\n"...)
	os.WriteFile(console, b, 0o644)
	out := t.TempDir()
	if code, _, errs := runCLI(t, "-app-id", app, "-from", filepath.Join(standalone, "cluster"), "-driver-log", console, "-out", out); code != exitPartial {
		t.Fatalf("exit %d (want 3, no event log): %s", code, errs)
	}
	r := readReport(t, out)
	if len(r.Jobs.Jobs) != 16 || r.TaskStories == nil || len(r.TaskStories.Tasks) != 52 || r.Application.Status != "failed" {
		t.Errorf("rebuilt: %d jobs, status %q, stories %v", len(r.Jobs.Jobs), r.Application.Status, r.TaskStories != nil)
	}
	read := false
	for _, s := range r.Sources {
		read = read || s.Name == "Driver log" && s.Status == "read"
	}
	if !read {
		t.Errorf("no Driver log source read: %+v", r.Sources)
	}
	for _, name := range []string{app + "-report.json", app + "-report.html", app + "-explorer.html"} {
		if b, _ := os.ReadFile(filepath.Join(out, name)); len(b) == 0 || strings.Contains(string(b), "FAKESECRETdriver9") {
			t.Errorf("%s is empty or holds the planted secret", name)
		}
	}
	if b, _ := os.ReadFile(filepath.Join(out, app+"-report.json")); !strings.Contains(string(b), "connect failed with spark.hadoop.fs.s3a.secret.key=[redacted]") {
		t.Error("the planted line was not read, so the secret check proves nothing")
	}
	out = t.TempDir()
	if code, _, errs := runCLI(t, "-app-id", app, "-eventlog", filepath.Join(fx, app), "-driver-log", filepath.Join(dir, "missing.txt"), "-out", out, "-format", "json"); code != exitPartial {
		t.Fatalf("missing driver log: exit %d: %s", code, errs)
	}
	for _, s := range readReport(t, out).Sources {
		if s.Name == "Driver log" && s.Status != "error" {
			t.Errorf("missing driver log: %+v", s)
		}
	}
}

// On the lost-executors fixture one executor was killed with SIGKILL (137)
// and four ran out of heap (52). The worker's log adds those deaths as
// evidence, with or without the event log, and the 137 finding says what
// it means without YARN: no memory limit, the machine's out-of-memory
// killer or a person.
func TestStandaloneLostExecutors(t *testing.T) {
	const app = "app-20261025054247-0000"
	for _, withLog := range []bool{true, false} {
		out := t.TempDir()
		args := []string{"-app-id", app, "-from", filepath.Join(standalone, "lost-executors"), "-out", out, "-format", "json"}
		if withLog {
			args = append(args, "-eventlog", filepath.Join(fx, app))
		}
		if code, _, errs := runCLI(t, args...); code != exitOK && code != exitPartial {
			t.Fatalf("event log %v: exit %d: %s", withLog, code, errs)
		}
		r := readReport(t, out)
		daemons := false
		for _, s := range r.Sources {
			daemons = daemons || s.Name == "Worker and master logs" && s.Status == "read"
		}
		if !daemons {
			t.Errorf("event log %v: worker and master logs not read: %+v", withLog, r.Sources)
		}
		var kill *model.Finding
		for i, f := range r.Findings {
			if f.Rule == "executor-memory-kill" {
				kill = &r.Findings[i]
			}
		}
		if kill == nil {
			t.Fatalf("event log %v: no finding for the executor killed with 137", withLog)
		}
		text := kill.Title + " " + kill.Explanation + " " + kill.Fix
		if !strings.Contains(kill.Explanation, "did not use YARN") || strings.Contains(text, "memoryOverhead") || strings.Contains(kill.Title, "YARN") {
			t.Errorf("event log %v: the 137 finding reads as YARN's: %q", withLog, text)
		}
		fromWorker := false
		for _, e := range kill.Evidence {
			fromWorker = fromWorker || strings.Contains(e.Source.File, "Worker-1-vm.out") && strings.Contains(e.Text, "137")
		}
		if !fromWorker {
			t.Errorf("event log %v: the worker's line is not evidence: %+v", withLog, kill.Evidence)
		}
	}
}
