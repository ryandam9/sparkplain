package main

import (
	"html"
	"os"
	"path/filepath"
	"regexp"
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

// A run not on YARN is not asked for what only an EMR cluster gives: no
// coverage line wants -cluster-id, the EMR API, CloudWatch or CloudTrail,
// those sources say why they were not read, and the memory section does
// not say an executor asked YARN for a container. An EMR run keeps all
// of that. Either way, read logs are not asked for again (the memory
// section used to look before the logs were analyzed).
func TestStandaloneWording(t *testing.T) {
	const app = "app-20261025054247-0000"
	out := t.TempDir()
	if code, _, errs := runCLI(t, "-app-id", app, "-eventlog", filepath.Join(fx, app), "-from", filepath.Join(standalone, "lost-executors"), "-out", out, "-format", "json"); code != exitOK {
		t.Fatalf("exit %d (want 0: nothing a Standalone run can have is missing): %s", code, errs)
	}
	r := readReport(t, out)
	for _, row := range r.Coverage {
		for _, m := range row.Missing {
			for _, w := range []string{"-cluster-id", "EMR", "CloudWatch", "CloudTrail"} {
				if strings.Contains(m, w) {
					t.Errorf("coverage %s asks for %s: %q", row.ID, w, m)
				}
			}
		}
	}
	for _, s := range r.Sources {
		if (s.Name == "EMR API" || s.Name == "CloudWatch" || s.Name == "CloudTrail") && (s.Status != "none" || !strings.Contains(s.Detail, "did not use YARN")) {
			t.Errorf("source %s: %s %q", s.Name, s.Status, s.Detail)
		}
	}
	if strings.Contains(r.Memory.Lede, "YARN for") || !strings.Contains(r.Memory.Lede, "did not add overhead") {
		t.Errorf("memory lede: %q", r.Memory.Lede)
	}
	for _, m := range r.Memory.Missing {
		if strings.Contains(m, "needs the") && strings.Contains(m, "logs") {
			t.Errorf("the logs were read, but memory still asks for them: %q", m)
		}
	}

	emr := t.TempDir()
	if code, _, errs := runCLI(t, "-app-id", "application_1790380000000_0049", "-eventlog", filepath.Join(fx, "application_1790380000000_0049"), "-from", filepath.Join(emrlogs, "j-FIXTURE0049CLUSTER"), "-out", emr, "-format", "json"); code != exitOK && code != exitPartial {
		t.Fatalf("EMR run: exit %d: %s", code, errs)
	}
	e := readReport(t, emr)
	if !strings.Contains(e.Memory.Lede, "asked YARN for") {
		t.Errorf("EMR memory lede lost YARN: %q", e.Memory.Lede)
	}
	asks := false
	for _, row := range e.Coverage {
		for _, m := range row.Missing {
			asks = asks || strings.Contains(m, "-cluster-id")
		}
	}
	if !asks {
		t.Error("an EMR run read offline no longer says what -cluster-id adds")
	}
	for _, m := range e.Memory.Missing {
		if strings.Contains(m, "needs the container logs") {
			t.Errorf("the EMR run read its container logs, but memory asks for them: %q", m)
		}
	}
}

// What the workers offered Spark: from the worker's log on a Standalone
// run, from the master URL on a local-cluster run (4 workers of 1 core and
// 1 GiB). Without the worker's log the nodes section says where it is; an
// EMR run has no workers.
func TestWorkerCapacity(t *testing.T) {
	for _, c := range []struct{ app, from string }{
		{"app-20261025032316-0000", "cluster"},
		{"app-20261025032411-0000", "local-cluster"},
		{"app-20261025054247-0000", "lost-executors"},
	} {
		out := t.TempDir()
		if code, _, errs := runCLI(t, "-app-id", c.app, "-eventlog", filepath.Join(fx, c.app), "-from", filepath.Join(standalone, c.from), "-out", out, "-format", "json"); code != exitOK {
			t.Fatalf("%s: exit %d: %s", c.from, code, errs)
		}
		h := readReport(t, out).Nodes.Hosts
		workers := 1
		if c.from == "local-cluster" {
			workers = 4
		}
		if len(h) != 1 || h[0].Workers != workers || h[0].WorkerCores != 4 || h[0].WorkerMemoryBytes != 4<<30 || h[0].WorkerSource.File == "" {
			t.Errorf("%s: hosts %+v", c.from, h)
		}
	}
	out := t.TempDir()
	const app = "app-20261025032316-0000"
	if code, _, errs := runCLI(t, "-app-id", app, "-eventlog", filepath.Join(fx, app), "-out", out, "-format", "json"); code != exitOK && code != exitPartial {
		t.Fatalf("event log only: exit %d: %s", code, errs)
	}
	r := readReport(t, out)
	if r.Nodes.Hosts[0].Workers != 0 || !slices.ContainsFunc(r.Nodes.Missing, func(m string) bool { return strings.Contains(m, "worker's log") }) {
		t.Errorf("without the worker's log: hosts %+v, missing %q", r.Nodes.Hosts, r.Nodes.Missing)
	}
	emr := t.TempDir()
	if code, _, errs := runCLI(t, "-app-id", "application_1790380000000_0049", "-eventlog", filepath.Join(fx, "application_1790380000000_0049"), "-from", filepath.Join(emrlogs, "j-FIXTURE0049CLUSTER"), "-out", emr, "-format", "json"); code != exitOK && code != exitPartial {
		t.Fatalf("EMR run: exit %d: %s", code, errs)
	}
	for _, h := range readReport(t, emr).Nodes.Hosts {
		if h.Workers != 0 || h.WorkerCores != 0 {
			t.Errorf("EMR host %s has Standalone workers: %+v", h.Name, h)
		}
	}
}

// At a glance draws a run not on YARN as it ran: a master, workers with
// what each offered Spark, and executors (and a cluster-mode driver) sized
// by their heap, with nothing about YARN, containers or EMR. The busiest
// time counts cores as well as memory, as a Standalone worker does.
func TestStandaloneAtAGlance(t *testing.T) {
	text := func(svg string) string {
		var b strings.Builder
		for _, m := range regexp.MustCompile(`>([^<]+)<`).FindAllStringSubmatch(svg, -1) {
			b.WriteString(html.UnescapeString(m[1]) + "\n")
		}
		return b.String()
	}
	for _, c := range []struct{ app, from, worker string }{
		{"app-20261025032316-0000", "cluster", "Worker · localhost"},
		{"app-20261025032411-0000", "local-cluster", "4 workers · localhost"},
		{"app-20261025043058-0000", "cluster-mode", "Worker · localhost"},
		{"app-20261025054247-0000", "lost-executors", "Worker · localhost"},
	} {
		out := t.TempDir()
		if code, _, errs := runCLI(t, "-app-id", c.app, "-eventlog", filepath.Join(fx, c.app), "-from", filepath.Join(standalone, c.from), "-out", out); code != exitOK {
			t.Fatalf("%s: exit %d: %s", c.from, code, errs)
		}
		page, _ := os.ReadFile(filepath.Join(out, c.app+"-report.html"))
		s := string(page)
		i := strings.Index(s, `class="anat"`)
		if i < 0 {
			t.Fatalf("%s: no diagram", c.from)
		}
		svg := text(s[i : i+strings.Index(s[i:], "</svg>")])
		for _, bad := range []string{"YARN", "ResourceManager", "container", "Container", "EMR", "Primary", "-cluster-id"} {
			if strings.Contains(svg, bad) {
				t.Errorf("%s: the diagram says %q", c.from, bad)
			}
		}
		for _, want := range []string{c.worker, "The workers offered Spark 4.0 GiB of memory and 4 cores", "heap"} {
			if !strings.Contains(svg, want) {
				t.Errorf("%s: the diagram lacks %q", c.from, want)
			}
		}
		if !strings.Contains(s, "The memory that the worker offered Spark, to scale.") {
			t.Errorf("%s: the guide under the diagram is not the Standalone one", c.from)
		}
		switch c.from {
		case "cluster-mode": // the driver's heap and core are the worker's too
			if !strings.Contains(svg, "Driver's heap") || !strings.Contains(svg, "4 of the 4 cores") {
				t.Errorf("cluster-mode: driver not counted: %s", svg)
			}
		case "lost-executors": // 4 of 512 MiB at once, not the 9 started; cores are the limit
			if !strings.Contains(svg, "used 2.0 GiB (50%) and 4 of the 4 cores") || !strings.Contains(svg, "all cores in use") || !strings.Contains(svg, "killed (137)") {
				t.Errorf("lost-executors: busiest time: %s", svg)
			}
		}
	}
}

// A local-mode run (local[4], made with PySpark from PyPI) starts no
// executor: the driver runs every task. The report says so instead of "0
// executors across 0 hosts", the tasks' stories come from the saved
// console, and there is no cluster diagram to draw.
func TestLocalMode(t *testing.T) {
	const app = "local-1792911433877"
	out := t.TempDir()
	code, stdout, errs := runCLI(t, "-app-id", app, "-eventlog", filepath.Join(fx, app), "-driver-log", "../../testdata/local/driver.txt", "-out", out)
	if code != exitOK {
		t.Fatalf("exit %d: %s", code, errs)
	}
	r := readReport(t, out)
	if r.Nodes.Manager != "local" {
		t.Errorf("manager %q, want local", r.Nodes.Manager)
	}
	sum := strings.Join(r.Summary.Sentences, " ")
	if !strings.Contains(sum, "in local mode. The driver ran every task, in 4 task slots.") || strings.Contains(sum, "0 executors") || strings.Contains(sum, "between executors") {
		t.Errorf("summary: %q", sum)
	}
	if strings.Contains(stdout, "0 executors across 0 hosts") {
		t.Error("the console says 0 executors across 0 hosts")
	}
	labels := map[string]string{}
	for _, k := range r.Summary.KPIs {
		labels[k.Label] = k.Value + " " + k.Unit
	}
	if labels["Task slots"] != "4 in the driver · 1.0 GiB heap" || labels["Executors"] != "0 local mode" || labels["Hosts"] != "1 ran the driver" {
		t.Errorf("cards: %q", labels)
	}
	if !strings.Contains(r.Memory.Lede, "spark.executor.memory has no effect") {
		t.Errorf("memory lede: %q", r.Memory.Lede)
	}
	if h := r.Nodes.Hosts; len(h) != 1 || h[0].Cores != 4 || h[0].Tasks.Tasks != 59 {
		t.Errorf("hosts: %+v", h)
	}
	if s := r.TaskStories; s == nil || len(s.Tasks) != 59 {
		t.Errorf("task stories from the console: %v", s != nil)
	}
	if b, _ := os.ReadFile(filepath.Join(out, app+"-report.html")); strings.Contains(string(b), `class="anat"`) {
		t.Error("a local-mode run has no cluster to draw, but the report draws one")
	}
}
