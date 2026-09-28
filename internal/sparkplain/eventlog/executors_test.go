package eventlog

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/source"
)

// rawEvents decodes every event of a plain fixture generically.
func rawEvents(t *testing.T, name string) []map[string]any {
	t.Helper()
	f, err := os.Open(filepath.Join(fixtures, name))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []map[string]any
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for sc.Scan() {
		var e map[string]any
		if json.Unmarshal(sc.Bytes(), &e) == nil {
			out = append(out, e)
		}
	}
	return out
}

// Spark writes every exclusion twice, under its new and its old name; the
// parser keeps one of each, and notes when an application-level one lifted.
func TestExclusionsMerged(t *testing.T) {
	t.Parallel()
	const name = "application_1790380000000_0046"
	unique := map[string]bool{}
	lifted := map[string]bool{}
	for _, e := range rawEvents(t, name) {
		ev, _ := e["Event"].(string)
		short := strings.TrimPrefix(ev, exclusionPrefix)
		if short == ev {
			continue
		}
		switch {
		case strings.Contains(short, "Unexcluded") || strings.Contains(short, "Unblacklisted"):
			lifted[e["executorId"].(string)] = true
		case strings.Contains(short, "Excluded") || strings.Contains(short, "Blacklisted"):
			scope := "app"
			if strings.HasSuffix(short, "ForStage") {
				scope = "stage"
			}
			// The same exclusion under both names shares scope, executor and time.
			unique[scope+"|"+e["executorId"].(string)+"|"+strconv.FormatInt(num(e["time"]), 10)] = true
		}
	}
	l := parseFixture(t, name, name)
	if len(l.Exclusions) != len(unique) || len(unique) == 0 {
		t.Fatalf("%d exclusions, want %d distinct: %+v", len(l.Exclusions), len(unique), l.Exclusions)
	}
	var app, gotLifted int
	for _, x := range l.Exclusions {
		if x.Kind != "executor" || x.Failures < 1 || x.Time.IsZero() {
			t.Errorf("bad exclusion %+v", x)
		}
		if x.Scope == "application" {
			app++
			if !x.Lifted.IsZero() {
				gotLifted++
				if !lifted[x.Target] {
					t.Errorf("executor %s marked lifted without an Unexcluded event", x.Target)
				}
			}
		}
	}
	if app == 0 || gotLifted != len(lifted) {
		t.Errorf("%d application exclusions, %d lifted, want %d lifted", app, gotLifted, len(lifted))
	}
}

func TestNodeExcludedForStage(t *testing.T) {
	t.Parallel()
	l := parseFixture(t, "application_1790380000000_0048", "application_1790380000000_0048")
	var node bool
	for _, x := range l.Exclusions {
		if x.Kind == "node" && x.Scope == "stage" && x.Target != "" && x.Failures > 0 {
			node = true
		}
	}
	if !node {
		t.Errorf("no node exclusion for a stage: %+v", l.Exclusions)
	}
}

// Tasks that started but never ended in an in-progress log are listed.
func TestRunningTasksAtLogEnd(t *testing.T) {
	t.Parallel()
	const name = "application_1790380000000_0047.inprogress"
	started := map[int64]bool{}
	for _, e := range rawEvents(t, name) {
		switch e["Event"] {
		case "SparkListenerTaskStart":
			started[num(obj(e, "Task Info")["Task ID"])] = true
		case "SparkListenerTaskEnd":
			delete(started, num(obj(e, "Task Info")["Task ID"]))
		}
	}
	l := parseFixture(t, name, "application_1790380000000_0047")
	if len(l.RunningTasks) != len(started) || len(started) == 0 {
		t.Fatalf("%d running tasks, want %d", len(l.RunningTasks), len(started))
	}
	for _, r := range l.RunningTasks {
		if !started[r.TaskID] || r.Launched.IsZero() || r.ExecutorID == "" {
			t.Errorf("unexpected running task %+v", r)
		}
	}
	if done := parseFixture(t, mainApp, mainApp); len(done.RunningTasks) != 0 {
		t.Errorf("a finished log lists %d running tasks", len(done.RunningTasks))
	}
}

// Executor launch detail and driver links from the real EMR log.
func TestExecutorLaunchDetail(t *testing.T) {
	t.Parallel()
	const name = "application_1790380000000_0049"
	startup := map[string]int64{}
	for _, e := range rawEvents(t, name) {
		if e["Event"] == "SparkListenerExecutorAdded" {
			info := obj(e, "Executor Info")
			startup[e["Executor ID"].(string)] = num(info["Registration Time"]) - num(info["Request Time"])
		}
	}
	l := parseFixture(t, name, name)
	for _, x := range l.Executors {
		want, ok := startup[x.ID]
		if !ok {
			continue
		}
		if x.StartupMs != want || x.LogURLs["stderr"] == "" || x.Attributes["CONTAINER_ID"] == "" {
			t.Errorf("executor %s: startup %d (want %d), logs %v, attributes %v", x.ID, x.StartupMs, want, x.LogURLs, x.Attributes)
		}
	}
	if l.Application.DriverLogs["stdout"] == "" || l.Application.DriverAttributes["CONTAINER_ID"] == "" {
		t.Errorf("driver logs %v, attributes %v", l.Application.DriverLogs, l.Application.DriverAttributes)
	}
}

// The driver's heartbeat samples carry stage -1 (they are not tied to a
// stage), so they feed the driver's overall peak memory.
func TestDriverHeartbeatSamples(t *testing.T) {
	t.Parallel()
	const name = "application_1790380000000_0046"
	var want int64
	for _, e := range rawEvents(t, name) {
		if e["Event"] != "SparkListenerExecutorMetricsUpdate" {
			continue
		}
		if e["Executor ID"] != "driver" {
			t.Errorf("heartbeat logged for executor %v; Spark 3.5 logs only the driver's", e["Executor ID"])
		}
		for _, u := range e["Executor Metrics Updated"].([]any) {
			m := u.(map[string]any)
			if num(m["Stage ID"]) != -1 {
				t.Errorf("driver sample for stage %v", m["Stage ID"])
			}
			want = max(want, num(obj(m, "Executor Metrics")["JVMHeapMemory"]))
		}
	}
	l := parseFixture(t, name, name)
	if want == 0 || l.Driver == nil || l.Driver.Peak.JVMHeap < want {
		t.Errorf("driver peak heap %v, want at least %d", l.Driver, want)
	}
}

// Where cached partitions were adds up to each cached RDD's size, and block
// kinds are counted.
func TestCachedPlacement(t *testing.T) {
	t.Parallel()
	const name = "application_1790380000000_0046"
	l := parseFixture(t, name, name)
	placed := 0
	for _, r := range l.RDDs {
		var mem, disk int64
		for _, pl := range r.Executors {
			mem, disk = mem+pl.MemoryBytes, disk+pl.DiskBytes
			if pl.Host == "" || pl.StorageLevel == "" || pl.Blocks == 0 {
				t.Errorf("rdd %d: incomplete placement %+v", r.ID, pl)
			}
		}
		if len(r.Executors) > 0 {
			placed++
			if mem != r.MemoryBytes || disk != r.DiskBytes {
				t.Errorf("rdd %d: placement holds %d+%d bytes, RDD %d+%d", r.ID, mem, disk, r.MemoryBytes, r.DiskBytes)
			}
		}
	}
	kinds := map[string]bool{}
	for _, k := range l.BlockKinds {
		kinds[k.Kind] = k.Updates > 0
	}
	if placed == 0 || !kinds["rdd"] || !kinds["broadcast"] {
		t.Errorf("placed %d RDDs; block kinds %v", placed, kinds)
	}
}

// Each stage keeps the RDDs it computes, with the operation that made each,
// as the raw StageSubmitted event lists them.
func TestStageOperations(t *testing.T) {
	t.Parallel()
	const name = "application_1790380000000_0046"
	type rdd struct {
		op      string
		parents int
	}
	want := map[int]map[int64]rdd{}
	for _, e := range rawEvents(t, name) {
		if e["Event"] != "SparkListenerStageSubmitted" {
			continue
		}
		si := obj(e, "Stage Info")
		m := map[int64]rdd{}
		for _, r := range si["RDD Info"].([]any) {
			ri := r.(map[string]any)
			var sc struct{ Name string }
			if s, ok := ri["Scope"].(string); ok {
				json.Unmarshal([]byte(s), &sc)
			}
			m[num(ri["RDD ID"])] = rdd{sc.Name, len(ri["Parent IDs"].([]any))}
		}
		want[int(num(si["Stage ID"]))] = m
	}
	l := parseFixture(t, name, name)
	for _, st := range l.Stages {
		w, ok := want[st.ID]
		if !ok {
			continue
		}
		if len(st.RDDs) != len(w) || st.Details == "" {
			t.Errorf("stage %d: %d RDDs (want %d), details %d bytes", st.ID, len(st.RDDs), len(w), len(st.Details))
			continue
		}
		for _, r := range st.RDDs {
			if got := (rdd{r.Operation, len(r.Parents)}); got != w[int64(r.ID)] {
				t.Errorf("stage %d rdd %d: %+v, want %+v", st.ID, r.ID, got, w[int64(r.ID)])
			}
		}
	}
}

// A job keeps exactly the properties that differ from the app's settings,
// redacted.
func TestJobLocalProperties(t *testing.T) {
	t.Parallel()
	const name = "application_1790380000000_0046"
	var env map[string]any
	want := map[int64]map[string]bool{}
	for _, e := range rawEvents(t, name) {
		switch e["Event"] {
		case "SparkListenerEnvironmentUpdate":
			env = obj(e, "Spark Properties")
		case "SparkListenerJobStart":
			keys := map[string]bool{}
			for k, v := range obj(e, "Properties") {
				if env[k] != v {
					keys[k] = true
				}
			}
			want[num(e["Job ID"])] = keys
		}
	}
	l := parseFixture(t, name, name)
	var pools, tokens int
	for _, j := range l.Jobs {
		if len(j.Properties) != len(want[int64(j.ID)]) {
			t.Errorf("job %d: %d local properties, want %d", j.ID, len(j.Properties), len(want[int64(j.ID)]))
		}
		for k, v := range j.Properties {
			if !want[int64(j.ID)][k] {
				t.Errorf("job %d: %s kept although it matches the app's setting", j.ID, k)
			}
			if k == "spark.scheduler.pool" && v == "etl" {
				pools++
			}
			if k == "spark.myapp.session.token" {
				tokens++
				if v != "[redacted]" {
					t.Errorf("job %d: session token shown as %q", j.ID, v)
				}
			}
		}
	}
	if pools == 0 || tokens == 0 {
		t.Errorf("scheduler pool seen on %d jobs, session token on %d", pools, tokens)
	}
}

// Queries keep their parent (for sub-queries and nested commands), session
// settings (redacted) and long call site.
func TestSQLQueryDetail(t *testing.T) {
	t.Parallel()
	const name = "application_1790380000000_0046"
	roots := map[int64]int64{}
	for _, e := range rawEvents(t, name) {
		if strings.HasSuffix(e["Event"].(string), "SQLExecutionStart") {
			if r := num(e["rootExecutionId"]); r != num(e["executionId"]) {
				roots[num(e["executionId"])] = r
			}
		}
	}
	l := parseFixture(t, name, name)
	nested := 0
	for _, q := range l.SQL {
		want, isSub := roots[q.ID]
		if isSub != (q.RootID != nil) || isSub && *q.RootID != want {
			t.Errorf("query %d: root %v, want %v", q.ID, q.RootID, want)
		}
		if isSub {
			nested++
		}
		if q.Details == "" {
			t.Errorf("query %d: no call stack", q.ID)
		}
		if v, ok := q.ModifiedConfigs["spark.myapp.session.token"]; ok && v != "[redacted]" {
			t.Errorf("query %d: token shown as %q", q.ID, v)
		}
	}
	if nested == 0 || l.SQL[1].ModifiedConfigs["spark.sql.shuffle.partitions"] != "7" {
		t.Errorf("%d nested queries; query 1 settings %v", nested, l.SQL[1].ModifiedConfigs)
	}
}

// Metrics adaptive execution added after planning are resolved.
func TestAdaptiveMetricsResolved(t *testing.T) {
	t.Parallel()
	const name = "application_1790380000000_0046"
	l := parseExplorer(t, name, name, model.ExplorerLimits{})
	var registered, known int
	for _, q := range l.SQL {
		registered += len(q.AdaptiveMetrics)
	}
	for _, g := range l.Explorer.SQL {
		for _, m := range g.Adaptive {
			if m.Known {
				known++
			}
		}
	}
	if registered == 0 || known == 0 {
		t.Errorf("%d adaptive metrics registered, %d with values", registered, known)
	}
}

// EMR's optimizer report: totals and rule counts match the raw event.
func TestOptimizerStats(t *testing.T) {
	t.Parallel()
	const name = "application_1790380000000_0049"
	type want struct{ total, rules, useful int64 }
	exp := map[int64]want{}
	for _, e := range rawEvents(t, name) {
		if !strings.HasSuffix(e["Event"].(string), "SparkListenerQueryExecutionMetrics") {
			continue
		}
		var w want
		for rule, ns := range e["timePerRule"].(map[string]any) {
			w.total += num(ns)
			w.rules++
			if num(e["numEffectiveRunsPerRule"].(map[string]any)[rule]) > 0 {
				w.useful++
			}
		}
		exp[num(e["executionId"])] = w
	}
	l := parseFixture(t, name, name)
	seen := 0
	for _, q := range l.SQL {
		w, ok := exp[q.ID]
		if !ok {
			continue
		}
		o := q.Optimizer
		if o == nil || (want{o.TotalNs, int64(o.RulesRun), int64(o.RulesUseful)}) != w {
			t.Errorf("query %d: optimizer %+v, want %+v", q.ID, o, w)
			continue
		}
		if len(o.Rules) > 1 && o.Rules[0].TimeNs < o.Rules[1].TimeNs {
			t.Errorf("query %d: rules not slowest first", q.ID)
		}
		seen++
	}
	if seen == 0 {
		t.Error("no optimizer reports parsed")
	}
}

// -show prints the event behind a cited file:line, redacted.
func TestShowEvent(t *testing.T) {
	t.Parallel()
	in, err := Resolve(filepath.Join(fixtures, mainApp+".zip"), mainApp, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	b, err := ShowEvent(t.Context(), in, mainApp+".zip!"+mainApp+".lz4", 4)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"SparkListenerEnvironmentUpdate"`) || !strings.Contains(string(b), "[redacted]") {
		t.Errorf("unexpected event: %.200s", b)
	}
	assertNoSecrets(t, string(b))
	in2, _ := Resolve(filepath.Join(fixtures, mainApp), mainApp, Limits{})
	defer in2.Close()
	if _, err := ShowEvent(t.Context(), in2, "other-file", 4); err == nil || !strings.Contains(err.Error(), mainApp) {
		t.Errorf("a miss should name the log's files: %v", err)
	}
	for _, bad := range []string{"nope", "file:", "file:0", ":3"} {
		if _, _, err := ParseLocation(bad); err == nil {
			t.Errorf("ParseLocation(%q) accepted", bad)
		}
	}
}

// Code locations: a JVM app's stages and jobs carry its own frames; a
// PySpark app's carry a Python line only for some actions.
func TestCodeLocations(t *testing.T) {
	t.Parallel()
	l := parseFixture(t, "application_1790380000000_0051", "application_1790380000000_0051")
	for _, j := range l.Jobs {
		if len(j.Code) == 0 || j.Code[0].File != "ClaimsJob.java" || j.Code[0].Line == 0 {
			t.Errorf("java job %d: code %+v", j.ID, j.Code)
		}
	}
	last := l.Jobs[len(l.Jobs)-1].Code
	if len(last) != 2 || last[0].Line != 29 || last[0].Function != "ClaimsJob.writeTotals" || last[1].Line != 20 {
		t.Errorf("write job's frames %+v, want writeTotals:29 called from main:20", last)
	}
	py := parseFixture(t, "application_1790380000000_0049", "application_1790380000000_0049")
	var with, without int
	for _, j := range py.Jobs {
		if len(j.Code) > 0 {
			with++
			if !strings.HasSuffix(j.Code[0].File, "emr_job.py") || j.Code[0].Action != "collect" {
				t.Errorf("pyspark job %d: %+v", j.ID, j.Code)
			}
		} else {
			without++
		}
	}
	if with == 0 || without == 0 {
		t.Errorf("pyspark: %d jobs with code, %d without; expect both", with, without)
	}
	for _, site := range []string{"count at NativeMethodAccessorImpl.java:0", "collect at <stdin>:36", "my job", ""} {
		if loc, ok := shortSite(site); ok {
			t.Errorf("shortSite(%q) = %+v, want none", site, loc)
		}
	}
}

// ResolveStore finds an application's event log under a prefix, as it would
// in S3 (a local folder stands in for the bucket).
func TestResolveStore(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	copyFile := func(src, dst string) {
		b, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		os.MkdirAll(filepath.Dir(filepath.Join(root, dst)), 0o755)
		os.WriteFile(filepath.Join(root, dst), b, 0o644)
	}
	copyFile(filepath.Join(fixtures, mainApp+".zstd"), "spark-events/"+mainApp+".zstd")
	copyFile(filepath.Join(fixtures, "application_1790380000000_0044"), "spark-events/application_1790380000000_0044")
	rolling := "eventlog_v2_application_1790380000000_0043"
	entries, _ := os.ReadDir(filepath.Join(fixtures, rolling))
	for _, e := range entries {
		copyFile(filepath.Join(fixtures, rolling, e.Name()), "spark-events/"+rolling+"/"+e.Name())
	}
	st := source.NewLocalStore(root)
	ctx := t.Context()
	events := func(in *Input) int64 {
		l, err := Parse(ctx, in, Options{})
		if err != nil {
			t.Fatal(err)
		}
		return l.Stats.Events
	}
	local := events(mustResolve(t, filepath.Join(fixtures, mainApp+".zstd"), mainApp))
	for _, loc := range []string{"spark-events", "spark-events/", "spark-events/" + mainApp + ".zstd"} {
		in, err := ResolveStore(ctx, st, loc, mainApp, Limits{})
		if err != nil {
			t.Fatalf("%s: %v", loc, err)
		}
		if in.Layout != "single" || events(in) != local {
			t.Errorf("%s: layout %s", loc, in.Layout)
		}
	}
	in, err := ResolveStore(ctx, st, "spark-events/", "application_1790380000000_0043", Limits{})
	if err != nil || in.Layout != "rolling" || len(in.PartNames()) < 2 {
		t.Fatalf("rolling: %v %v", in, err)
	}
	if events(in) != events(mustResolve(t, filepath.Join(fixtures, rolling), "application_1790380000000_0043")) {
		t.Error("rolling log read from the store differs from the local read")
	}
	if _, err := ResolveStore(ctx, st, "spark-events/", "application_1_9999", Limits{}); ErrorClass(err) != ClassNotFound {
		t.Errorf("missing app: %v", err)
	}
}

func mustResolve(t *testing.T, p, app string) *Input {
	t.Helper()
	in, err := Resolve(p, app, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return in
}
