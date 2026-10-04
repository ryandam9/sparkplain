package analyze

import (
	"context"
	"encoding/json"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/eventlog"
	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

const fixtures = "../../../testdata/eventlog"

func fixtureReport(t *testing.T, name, app string, th Thresholds) *model.Report {
	t.Helper()
	in, err := eventlog.Resolve(filepath.Join(fixtures, name), app, eventlog.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	l, err := eventlog.Parse(context.Background(), in, eventlog.Options{})
	if err != nil {
		t.Fatal(err)
	}
	status := "read"
	if l.Stats.Truncated {
		status = "partial"
	}
	return Run(Input{Tool: "sparkplain test", GeneratedAt: time.Unix(0, 0), TimeZone: "UTC", EventLog: l,
		EventSource: model.SourceStatus{Name: "Spark event log", Status: status}, Thresholds: th})
}

func rules(r *model.Report) map[string]model.Finding {
	m := map[string]model.Finding{}
	for _, f := range r.Findings {
		if _, ok := m[f.Rule]; !ok {
			m[f.Rule] = f
		}
	}
	return m
}

func TestMainFixtureFindings(t *testing.T) {
	t.Parallel()
	r := fixtureReport(t, "application_1790380000000_0042", "application_1790380000000_0042", DefaultThresholds())
	got := rules(r)
	for rule, sev := range map[string]model.Severity{
		"executor-memory-kill": model.Critical,
		"job-failed":           model.Warning, // the app carried on after it
		"stage-skew":           model.Warning,
		"memory-spill":         model.Warning,
		"access-static-keys":   model.Warning,
		"task-retries":         model.Info,
	} {
		f, ok := got[rule]
		if !ok {
			t.Errorf("missing finding %s; have %v", rule, keys(got))
			continue
		}
		if f.Severity != sev {
			t.Errorf("%s severity %s, want %s", rule, f.Severity, sev)
		}
		if f.Explanation == "" || len(f.Evidence) == 0 {
			t.Errorf("%s lacks explanation or evidence", rule)
		}
		for _, e := range f.Evidence {
			if e.Text == "" {
				t.Errorf("%s has empty evidence", rule)
			}
		}
	}
	if tr := got["task-retries"].Title; !regexp.MustCompile(`^(1 task attempt failed, and its retry succeeded|\d+ task attempts failed, and their retries succeeded)$`).MatchString(tr) {
		t.Errorf("task-retries title = %q", tr)
	}
	if !strings.Contains(got["stage-skew"].Title, "stage 18") || got["stage-skew"].Evidence[0].Source.Line == 0 {
		t.Errorf("skew finding should point at stage 18's slowest task: %+v", got["stage-skew"])
	}
	if !strings.Contains(got["job-failed"].Explanation, "ValueError: bad row 13") {
		t.Errorf("job failure should name the Python error: %s", got["job-failed"].Explanation)
	}
	for i := 1; i < len(r.Findings); i++ {
		if r.Findings[i-1].Severity.Rank() > r.Findings[i].Severity.Rank() {
			t.Fatal("findings not ranked by severity")
		}
	}
	if r.ExitCode != 0 {
		t.Errorf("exit code %d, want 0 for a fully read log", r.ExitCode)
	}
	if n := len(r.Summary.Sentences); n < 2 || n > 4 {
		t.Errorf("summary has %d sentences", n)
	}
	if len(r.Summary.KPIs) != 9 || len(r.Coverage) != 10 || len(r.Sources) != 7 {
		t.Errorf("kpis %d coverage %d sources %d", len(r.Summary.KPIs), len(r.Coverage), len(r.Sources))
	}
	if len(r.Nodes.Hosts) != 3 || !r.Nodes.Hosts[0].Driver {
		t.Errorf("hosts %+v", r.Nodes.Hosts)
	}
	if r.Memory.Config.HeapBytes != 1<<30 || r.Memory.Config.OverheadBytes != 384<<20 || !r.Memory.RSSKnown {
		t.Errorf("memory config %+v", r.Memory.Config)
	}
	if r.Executors.Started != 2 || r.Executors.Peak != 2 {
		t.Errorf("executors started %d peak %d", r.Executors.Started, r.Executors.Peak)
	}
	var sawIO bool
	for _, d := range r.IO.Data {
		if d.Access == "write" && d.Name == "spark_catalog.claims.region_totals" {
			sawIO = true
		}
	}
	if !sawIO || r.IO.Totals.InputBytes != 2713968999 {
		t.Errorf("io data %+v totals %d", r.IO.Data, r.IO.Totals.InputBytes)
	}
}

func keys(m map[string]model.Finding) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestThresholdsAreTunable(t *testing.T) {
	t.Parallel()
	th := DefaultThresholds()
	th.SkewRatio = 50
	th.SpillShare = 1e9
	r := fixtureReport(t, "application_1790380000000_0042", "application_1790380000000_0042", th)
	got := rules(r)
	if _, ok := got["stage-skew"]; ok {
		t.Error("skew should not fire at ratio 50")
	}
	if f, ok := got["memory-spill"]; ok && !strings.Contains(f.Evidence[0].Text, "no shuffle") {
		t.Error("spill against shuffle write should not fire at a huge threshold")
	}
}

func TestFailedAndInProgressApps(t *testing.T) {
	t.Parallel()
	r := fixtureReport(t, "application_1790380000000_0044", "application_1790380000000_0044", DefaultThresholds())
	if f := rules(r)["job-failed"]; f.Severity != model.Critical || !strings.Contains(f.Title, "application ended") {
		t.Errorf("failed app: %+v", f)
	}
	if !strings.Contains(r.Summary.Sentences[0], "failed") {
		t.Errorf("summary: %s", r.Summary.Sentences[0])
	}
	p := fixtureReport(t, "application_1790380000000_0045.inprogress", "application_1790380000000_0045", DefaultThresholds())
	if p.ExitCode != 3 || !strings.Contains(p.Summary.Sentences[0], "did not finish") {
		t.Errorf("in-progress: exit %d, %s", p.ExitCode, p.Summary.Sentences[0])
	}
}

func TestNoEventLog(t *testing.T) {
	t.Parallel()
	r := Run(Input{Tool: "t", EventSource: model.SourceStatus{Name: "Spark event log", Status: "error", Class: "corrupt", Detail: "bad"}})
	if r.ExitCode != 3 || r.Jobs.Coverage != model.NeedsEventLog || r.Memory.Coverage != model.NeedsEventLog || len(r.Summary.Sentences) == 0 {
		t.Errorf("exit %d jobs %s", r.ExitCode, r.Jobs.Coverage)
	}
	if _, err := json.Marshal(r); err != nil {
		t.Fatal(err)
	}
}

func TestReportHasNoPlantedSecrets(t *testing.T) {
	t.Parallel()
	for _, n := range []string{"application_1790380000000_0042", "application_1790380000000_0044"} {
		r := fixtureReport(t, n, n, DefaultThresholds())
		b, _ := json.Marshal(r)
		if m := regexp.MustCompile(`FAKE-[A-Z0-9-]+|AKIAIOSFODNN7EXAMPLE`).FindAllString(string(b), 3); m != nil {
			t.Errorf("%s: secrets leaked: %v", n, m)
		}
	}
}

// synthetic builds a small event log for rules the fixtures do not trigger.
func synthetic(conf map[string]string, execs ...*model.Executor) *model.EventLog {
	start := time.Unix(1_790_000_000, 0).UTC()
	l := &model.EventLog{Application: model.Application{ID: "application_1_1", Name: "synthetic", User: "hadoop", Start: start, End: start.Add(time.Hour), DurationMs: 3_600_000, Status: model.StatusSucceeded}}
	for k, v := range conf {
		l.Config = append(l.Config, model.ConfigEntry{Key: k, Value: v, Group: "Spark", Origin: "Spark Properties", Source: model.Source{File: "f", Line: 4}})
	}
	for _, x := range execs {
		if x.Added.IsZero() {
			x.Added = start
		}
		l.Executors = append(l.Executors, x)
	}
	return l
}

func runSynthetic(l *model.EventLog) map[string]model.Finding {
	return rules(Run(Input{Tool: "t", EventLog: l, EventSource: model.SourceStatus{Name: "Spark event log", Status: "read"}}))
}

func TestRuleGCPressureLowCPUAndIdle(t *testing.T) {
	t.Parallel()
	x := &model.Executor{ID: "1", Host: "h", Cores: 4, Tasks: model.TaskTotals{RunTimeMs: 600_000, GCTimeMs: 120_000, CPUTimeNs: 60_000 * 1e6}}
	got := runSynthetic(synthetic(nil, x))
	for _, r := range []string{"memory-gc-pressure", "cpu-low", "cpu-idle-executors"} {
		if _, ok := got[r]; !ok {
			t.Errorf("missing %s; have %v", r, keys(got))
		}
	}
	x2 := &model.Executor{ID: "1", Host: "h", Cores: 1, Tasks: model.TaskTotals{RunTimeMs: 3_000_000, GCTimeMs: 1000, CPUTimeNs: 2_500_000 * 1e6}}
	got = runSynthetic(synthetic(nil, x2))
	for _, r := range []string{"memory-gc-pressure", "cpu-low", "cpu-idle-executors"} {
		if _, ok := got[r]; ok {
			t.Errorf("%s fired on a healthy executor", r)
		}
	}
}

func TestRuleMemoryOverAndNearLimit(t *testing.T) {
	t.Parallel()
	conf := map[string]string{"spark.executor.memory": "8g"}
	low := &model.Executor{ID: "1", Host: "h", Cores: 2, Tasks: model.TaskTotals{RunTimeMs: 600_000, CPUTimeNs: 500_000 * 1e6}, Peak: model.PeakMemory{JVMHeap: 1 << 30}}
	if f, ok := runSynthetic(synthetic(conf, low))["memory-over-provisioned"]; !ok || !strings.Contains(f.Title, "8.0 GiB") {
		t.Errorf("over-provisioned: %+v", f)
	}
	high := &model.Executor{ID: "1", Host: "h", Cores: 2, Tasks: model.TaskTotals{RunTimeMs: 600_000, CPUTimeNs: 500_000 * 1e6}, Peak: model.PeakMemory{JVMHeap: 7900 << 20}}
	if _, ok := runSynthetic(synthetic(conf, high))["memory-heap-near-limit"]; !ok {
		t.Error("near-limit should fire")
	}
}

// Stock Spark leaves spark.eventLog.logStageExecutorMetrics off, so a log can
// carry no memory samples at all. Peak heap must then read "not recorded",
// never 0 B, and no memory rule may fire on the missing numbers.
func TestNoMemorySamples(t *testing.T) {
	t.Parallel()
	conf := map[string]string{"spark.executor.memory": "8g"}
	x := &model.Executor{ID: "1", Host: "h", Cores: 2, Tasks: model.TaskTotals{RunTimeMs: 600_000, CPUTimeNs: 500_000 * 1e6}}
	r := Run(Input{Tool: "t", EventLog: synthetic(conf, x), EventSource: model.SourceStatus{Name: "Spark event log", Status: "read"}})
	if r.Memory.HeapKnown {
		t.Error("HeapKnown with no samples")
	}
	if !strings.Contains(strings.Join(r.Memory.Missing, " "), "spark.eventLog.logStageExecutorMetrics") {
		t.Errorf("Missing does not explain the gap: %q", r.Memory.Missing)
	}
	for _, k := range r.Summary.KPIs {
		if k.Label == "Peak heap" && (k.Value != "—" || !strings.Contains(k.Unit, "not recorded")) {
			t.Errorf("Peak heap KPI = %+v", k)
		}
	}
	for _, rule := range []string{"memory-over-provisioned", "memory-heap-near-limit"} {
		if _, ok := rules(r)[rule]; ok {
			t.Errorf("%s fired without memory samples", rule)
		}
	}
	x.Peak.JVMHeap = 1 << 30
	if r := Run(Input{Tool: "t", EventLog: synthetic(conf, x), EventSource: model.SourceStatus{Name: "Spark event log", Status: "read"}}); !r.Memory.HeapKnown {
		t.Error("HeapKnown false with a sample")
	}
}

// Shapes taken from a real EMR run: stage 0's first task was slow only
// because its executor was warming up (every task read 1.25M rows); stage 27's
// slowest task read 24M rows against a 640K median.
func TestRuleSkewNeedsData(t *testing.T) {
	t.Parallel()
	stage := func(id int, recs, recsP50, shuffle, shuffleP50 int64) *model.Stage {
		return &model.Stage{ID: id, Name: "count at job.py:1", Status: model.StatusSucceeded,
			TaskDuration: model.Dist{Count: 32, Max: 2300, P50: 94},
			TaskRecords:  model.Dist{Count: 32, Max: recs, P50: recsP50},
			TaskShuffle:  model.Dist{Count: 32, Max: shuffle, P50: shuffleP50},
			Slowest:      &model.TaskRef{TaskID: 7, DurationMs: 2300, RecordsRead: recs, ShuffleReadBytes: shuffle, Source: model.Source{File: "f", Line: 9}}}
	}
	for _, tc := range []struct {
		name string
		st   *model.Stage
		want bool
		text string
	}{
		{"even rows, slow first task", stage(0, 1_250_000, 1_250_000, 0, 0), false, ""},
		{"hot key", stage(27, 24_003_657, 639_470, 1_500<<20, 44<<20), true, "24,003,657 rows, compared with a median of 639,470"},
		{"bytes only", stage(5, 0, 0, 900<<20, 10<<20), true, "900 MiB of shuffle data"},
	} {
		l := synthetic(nil)
		l.Stages = []*model.Stage{tc.st}
		f, ok := runSynthetic(l)["stage-skew"]
		if ok != tc.want {
			t.Errorf("%s: stage-skew fired = %v, want %v", tc.name, ok, tc.want)
		}
		if ok && !strings.Contains(f.Explanation, tc.text) {
			t.Errorf("%s: explanation %q lacks %q", tc.name, f.Explanation, tc.text)
		}
	}
}

func TestRuleLostAndDecommissioned(t *testing.T) {
	t.Parallel()
	lost := &model.Executor{ID: "1", Host: "h", Removed: time.Unix(1_790_000_100, 0), RemovedReason: "Executor heartbeat timed out", RemovalKind: model.RemovalLost}
	dec := &model.Executor{ID: "2", Host: "h", Removed: time.Unix(1_790_000_100, 0), RemovedReason: "Executor decommission", RemovalKind: model.RemovalDecommissioned}
	got := runSynthetic(synthetic(nil, lost, dec))
	if _, ok := got["executor-lost"]; !ok {
		t.Error("executor-lost missing")
	}
	if _, ok := got["executor-decommissioned"]; !ok {
		t.Error("executor-decommissioned missing")
	}
}

func TestRuleConfigRisks(t *testing.T) {
	t.Parallel()
	got := runSynthetic(synthetic(map[string]string{
		"spark.driver.maxResultSize": "0", "spark.dynamicAllocation.enabled": "true", "spark.sql.adaptive.enabled": "false",
	}))
	for _, r := range []string{"config-unlimited-result", "config-dynalloc-no-shuffle", "config-aqe-off"} {
		if _, ok := got[r]; !ok {
			t.Errorf("missing %s", r)
		}
	}
	got = runSynthetic(synthetic(map[string]string{"spark.dynamicAllocation.enabled": "true", "spark.shuffle.service.enabled": "true"}))
	if _, ok := got["config-dynalloc-no-shuffle"]; ok {
		t.Error("shuffle service on should silence the rule")
	}
}

func TestShortError(t *testing.T) {
	t.Parallel()
	msg := "Job aborted due to stage failure: Task 1 in stage 30.0 failed 3 times, most recent failure: Lost task 1.2 (TID 1): org.apache.spark.api.python.PythonException: Traceback (most recent call last):\n  File \"x.py\", line 3, in f\n    raise ValueError(\"bad\")\nValueError: bad\n\n\tat org.apache.X"
	if got := shortError(msg); got != "Job aborted due to stage failure: Task 1 in stage 30.0 failed 3 times — ValueError: bad" {
		t.Errorf("got %q", got)
	}
	if got := shortError("java.lang.OutOfMemoryError: Java heap space\n\tat x"); got != "java.lang.OutOfMemoryError: Java heap space" {
		t.Errorf("got %q", got)
	}
}

func TestSettingsCompare(t *testing.T) {
	t.Parallel()
	if !sameValue(settingByKey["spark.executor.memory"], "1024m") || sameValue(settingByKey["spark.executor.memory"], "2g") {
		t.Error("size compare")
	}
	if !sameValue(settingByKey["spark.sql.autoBroadcastJoinThreshold"], "10485760") {
		t.Error("10MB default should equal 10485760 bytes")
	}
	if parseSize("512", 1<<20) != 512<<20 || parseSize("1.5g", 1) != 3<<29 || parseSize("abc", 1) != -1 {
		t.Error("parseSize")
	}
}

func TestRuntimeTable(t *testing.T) {
	t.Parallel()
	r := fixtureReport(t, "application_1790380000000_0042", "application_1790380000000_0042", DefaultThresholds())
	rows := map[string]model.RuntimeRow{}
	groups := []string{}
	for _, row := range r.Config.Runtime {
		rows[row.Label] = row
		if len(groups) == 0 || groups[len(groups)-1] != row.Group {
			groups = append(groups, row.Group)
		}
		if row.Explain == "" || row.From == "" {
			t.Errorf("%s lacks explanation or origin", row.Label)
		}
		if !row.Missing && row.Source.Line == 0 {
			t.Errorf("%s has no source line", row.Label)
		}
	}
	if strings.Join(groups, ",") != "Versions,Runtime,Locations" {
		t.Errorf("groups out of order: %v", groups)
	}
	for label, want := range map[string]string{
		"Spark": "3.5.1", "Scala": "2.12.18", "Hadoop": "3.3.4", "Java": "21.0.10 (Ubuntu), OpenJDK 64-Bit Server VM",
		"Master": "local-cluster[2,2,1024]", "Deploy mode": "client", "Scheduler mode": "FIFO", "Operating system": "Linux 6.18.44-fc-v37 amd64",
		"Java home": "/usr/lib/jvm/java-21-openjdk-amd64", "Spark home": "/usr/lib/spark", "SQL warehouse": "/mnt/fixture/main/warehouse",
		"Event log directory": "file:///var/log/spark/apps/main", "Driver host": "ip-10-0-1-10.ec2.internal", "PySpark": "yes, Py4J 0.10.9.7",
		"Default filesystem": "file:///", "Hadoop authentication": "simple",
	} {
		if got := rows[label].Value; got != want {
			t.Errorf("%s = %q, want %q", label, got, want)
		}
	}
	for _, label := range []string{"EMR release", "Local scratch directories"} {
		if !rows[label].Missing || rows[label].Value != "not recorded" {
			t.Errorf("%s should be marked not recorded: %+v", label, rows[label])
		}
	}
	if rows["Spark"].Source.Line != 1 {
		t.Errorf("Spark version should cite line 1, got %v", rows["Spark"].Source)
	}
}

func TestRuntimeTableWithSparseEnvironment(t *testing.T) {
	t.Parallel()
	l := synthetic(map[string]string{"spark.master": "yarn"})
	rows := Run(Input{Tool: "t", EventLog: l, EventSource: model.SourceStatus{Name: "Spark event log", Status: "read"}}).Config.Runtime
	missing := 0
	for _, r := range rows {
		if r.Missing {
			missing++
			if r.Source.File != "" {
				t.Errorf("%s is missing but cites a source", r.Label)
			}
		}
	}
	if missing < 8 {
		t.Errorf("expected most rows to be not recorded, got %d missing", missing)
	}
}

// The phase 1c rules fire on the fixtures that exercise them, and not on the
// main fixture.
func TestInvestigateRulesOnFixtures(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name string
		want map[string]model.Severity
	}{
		{"application_1790380000000_0046", map[string]model.Severity{"executors-excluded": model.Warning}},
		{"application_1790380000000_0047.inprogress", map[string]model.Severity{"tasks-running-at-end": model.Warning}},
		{"application_1790380000000_0048", map[string]model.Severity{"executors-excluded": model.Warning, "tasks-running-at-end": model.Warning}},
		{"application_1790380000000_0050", map[string]model.Severity{"speculation": model.Info}},
	} {
		app := strings.TrimSuffix(c.name, ".inprogress")
		got := rules(fixtureReport(t, c.name, app, DefaultThresholds()))
		for rule, sev := range c.want {
			f, ok := got[rule]
			if !ok || f.Severity != sev || len(f.Evidence) == 0 || f.Evidence[0].Ref == "" && rule != "executors-excluded" {
				t.Errorf("%s: %s = %+v, want severity %s with linked evidence", c.name, rule, f, sev)
			}
		}
	}
	got := rules(fixtureReport(t, "application_1790380000000_0042", "application_1790380000000_0042", DefaultThresholds()))
	for _, rule := range []string{"executors-excluded", "tasks-running-at-end", "speculation", "scheduler-delay", "large-results", "slow-executor-startup"} {
		if _, ok := got[rule]; ok {
			t.Errorf("main fixture raised %s", rule)
		}
	}
}

func TestSchedulerDelayResultsAndStartup(t *testing.T) {
	t.Parallel()
	stage := func(delay, dur, result int64) *model.Stage {
		return &model.Stage{ID: 1, Name: "collect at job.py:3", TaskType: "ResultTask",
			Totals: model.TaskTotals{Tasks: 100, DurationMs: dur, SchedulerDelayMs: delay, ResultSizeBytes: result}}
	}
	l := synthetic(map[string]string{"spark.driver.maxResultSize": "1g"},
		&model.Executor{ID: "1", Host: "h", Cores: 2, StartupMs: 95_000}, &model.Executor{ID: "2", Host: "h", Cores: 2, StartupMs: 3_000})
	l.Stages = []*model.Stage{stage(40_000, 120_000, 700<<20)}
	got := runSynthetic(l)
	for _, r := range []string{"scheduler-delay", "large-results", "slow-executor-startup"} {
		if f, ok := got[r]; !ok || f.Evidence[0].Ref == "" {
			t.Errorf("%s missing or unlinked: %+v", r, f)
		}
	}
	if f := got["slow-executor-startup"]; !strings.Contains(f.Title, "1 executor") {
		t.Errorf("startup title %q", f.Title)
	}
	l.Stages = []*model.Stage{stage(10_000, 120_000, 100<<20)}
	l.Executors = []*model.Executor{{ID: "2", Host: "h", Cores: 2, StartupMs: 3_000}}
	got = runSynthetic(l)
	for _, r := range []string{"scheduler-delay", "large-results", "slow-executor-startup"} {
		if _, ok := got[r]; ok {
			t.Errorf("%s fired below its threshold", r)
		}
	}
}
