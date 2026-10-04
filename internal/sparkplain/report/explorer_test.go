package report

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/analyze"
	"github.com/ryandam9/sparkplain/internal/sparkplain/eventlog"
	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

func buildWithExplorer(t *testing.T, name string) (*model.Report, *model.Explorer) {
	t.Helper()
	in, err := eventlog.Resolve(filepath.Join("../../../testdata/eventlog", name), name, eventlog.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	l, err := eventlog.Parse(context.Background(), in, eventlog.Options{Explorer: &model.ExplorerLimits{}})
	if err != nil {
		t.Fatal(err)
	}
	r := analyze.Run(analyze.Input{Tool: "sparkplain test", GeneratedAt: time.Date(2026, 9, 26, 1, 0, 0, 0, time.UTC), TimeZone: "UTC",
		EventLog: l, EventSource: model.SourceStatus{Name: "Spark event log", Status: "read"}, Thresholds: analyze.DefaultThresholds()})
	return r, l.Explorer
}

func renderExplorer(t *testing.T, r *model.Report, x *model.Explorer) string {
	t.Helper()
	var b bytes.Buffer
	if err := WriteExplorer(&b, r, x, ExplorerOptions{ReportHref: "report.html"}); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

var dataRE = regexp.MustCompile(`(?s)<script type="application/json" id="sp-data">(.*?)</script>`)

func embedded(t *testing.T, page string) map[string]any {
	t.Helper()
	m := dataRE.FindStringSubmatch(page)
	if m == nil {
		t.Fatal("no embedded data")
	}
	var d map[string]any
	if err := json.Unmarshal([]byte(m[1]), &d); err != nil {
		t.Fatal(err)
	}
	return d
}

func rows(d map[string]any, key string) []any { return d[key].(map[string]any)["rows"].([]any) }

func TestExplorerDataMatchesReport(t *testing.T) {
	t.Parallel()
	r, x := buildWithExplorer(t, "application_1790380000000_0042")
	d := embedded(t, renderExplorer(t, r, x))
	if got, want := len(rows(d, "jobs")), len(r.Jobs.Jobs); got != want {
		t.Errorf("%d jobs, want %d", got, want)
	}
	if got, want := len(rows(d, "stages")), len(r.Jobs.Stages); got != want {
		t.Errorf("%d stages, want %d", got, want)
	}
	if got, want := len(rows(d, "sql")), len(r.Jobs.SQL); got != want {
		t.Errorf("%d queries, want %d", got, want)
	}
	if got, want := len(rows(d, "executors")), len(r.Executors.Executors)+1; got != want { // plus the driver
		t.Errorf("%d executors, want %d", got, want)
	}
	nTask := len(d["taskCols"].([]any))
	detail := d["detail"].(map[string]any)
	for _, st := range r.Jobs.Stages {
		key := fmt.Sprintf("%d.%d", st.ID, st.Attempt)
		sd, ok := detail[key].(map[string]any)
		if !ok {
			t.Fatalf("no detail for stage %s", key)
		}
		if int64(sd["from"].(float64)) != st.Totals.Tasks {
			t.Errorf("stage %s: sampled from %v, has %d tasks", key, sd["from"], st.Totals.Tasks)
		}
		for _, row := range sd["sample"].([]any) {
			if len(row.([]any)) != nTask {
				t.Fatalf("stage %s: task row has %d values for %d columns", key, len(row.([]any)), nTask)
			}
		}
	}
	dags := d["jobDags"].(map[string]any)
	for _, j := range r.Jobs.Jobs {
		if _, ok := dags[fmt.Sprint(j.ID)]; ok != (len(j.StageIDs) >= 2) {
			t.Errorf("job %d with %d stages: DAG present = %v", j.ID, len(j.StageIDs), ok)
		}
	}
	if got, want := len(d["planLayouts"].(map[string]any)), len(d["graphs"].(map[string]any)); got != want {
		t.Errorf("%d plan layouts for %d plan graphs", got, want)
	}
	if len(d["graphs"].(map[string]any)) == 0 || d["running"] == nil {
		t.Error("plan graphs or running tasks missing")
	}
	if d["reportHref"] != "report.html" || d["collected"] != true {
		t.Errorf("reportHref %v, collected %v", d["reportHref"], d["collected"])
	}
}

// The explorer makes no network requests: its charts are drawn by the
// embedded D3, so it works on machines without internet access
// (HISTORY.md, phase 1d). Only the SVG namespace, an identifier browsers never fetch,
// may appear as a URL in the page's own markup and script.
func TestExplorerLoadsNothingExternal(t *testing.T) {
	t.Parallel()
	r, x := buildWithExplorer(t, "application_1790380000000_0042")
	page := dataRE.ReplaceAllString(renderExplorer(t, r, x), "") // log values may contain URLs; they are data, not loads
	// The vendored D3 holds XML namespace names and its licence URL, never
	// loaded; TestVendoredD3 pins its content.
	if !strings.Contains(page, d3JS) {
		t.Fatal("D3 is not embedded")
	}
	page = strings.Replace(page, d3JS, "", 1)
	for _, re := range []string{`<link\b`, `<script[^>]+src=`, `@import`, `url\(\s*['"]?https?:`, `<iframe`, `<img\b`, `fonts\.googleapis`} {
		if m := regexp.MustCompile(re).FindString(page); m != "" {
			t.Errorf("explorer loads something outside itself: %q", m)
		}
	}
	for _, u := range regexp.MustCompile(`https?://[^\s"'<>)]+`).FindAllString(page, -1) {
		if u != "http://www.w3.org/2000/svg" {
			t.Errorf("explorer references %s; it must load nothing", u)
		}
	}
	for _, bad := range []string{"gstatic", "google.", "createElement(\"script\")", "fetch(", "XMLHttpRequest", "sendBeacon", "WebSocket"} {
		if strings.Contains(explorerJS, bad) {
			t.Errorf("explorer.js mentions %q; the page must not load or send anything", bad)
		}
	}
}

func TestExplorerHasNoPlantedSecrets(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"application_1790380000000_0042", "application_1790380000000_0044"} {
		r, x := buildWithExplorer(t, name)
		if m := regexp.MustCompile(`FAKE-[A-Z0-9-]+|AKIAIOSFODNN7EXAMPLE`).FindString(renderExplorer(t, r, x)); m != "" {
			t.Errorf("%s: planted secret %q in explorer.html", name, m)
		}
	}
}

// A value from the log must not be able to end the data block and run code.
func TestExplorerEscapesHostileText(t *testing.T) {
	t.Parallel()
	r, x := buildWithExplorer(t, "application_1790380000000_0042")
	evil := `</script><script>alert(1)</script><!--`
	r.Jobs.Jobs[0].Description = evil
	r.Application.Name = evil
	page := renderExplorer(t, r, x)
	if strings.Contains(page, "<script>alert(1)") {
		t.Fatal("hostile text reached the page unescaped")
	}
	d := embedded(t, page)
	if got := rows(d, "jobs")[0].([]any)[2]; got != evil {
		t.Errorf("description round-tripped as %q", got)
	}
}

// SPEC §6: at the default limits the page stays under 25 MB. This builds the
// shape the 1 GB benchmark log produces (490 stages of 500 tasks, 200
// executors), with the samples shrunk the way the parser shrinks them, and
// fills every value with a large number.
func TestExplorerSizeBudget(t *testing.T) {
	t.Parallel()
	r := &model.Report{Application: model.Application{ID: "application_1_1", Start: time.UnixMilli(1_790_000_000_000)}}
	lim := model.DefaultExplorerLimits()
	sample, slowest, shrinks := lim.SamplePerStage, lim.SlowestPerStage, 0
	for 490*(min(sample, 500)+min(slowest, 500)) > lim.MaxSampledTasks {
		sample, slowest, shrinks = sample/2, slowest/2, shrinks+1
	}
	x := &model.Explorer{Limits: lim, SampleShrinks: shrinks}
	src := model.Source{File: "events_1_application_1_1.zstd", Line: 12_345_678}
	for s := range 490 {
		st := &model.Stage{ID: s, Name: "save at job.py:42", Status: model.StatusSucceeded}
		r.Jobs.Stages = append(r.Jobs.Stages, st)
		sd := model.StageDetail{ID: s, Metrics: map[string]model.Quartiles{}, SampledFrom: 500}
		for _, m := range []string{model.MetricDuration, model.MetricRunTime, model.MetricGCTime, model.MetricRecordsRead, model.MetricInputBytes, model.MetricShuffleRead, model.MetricShuffleWrite} {
			sd.Metrics[m] = model.Quartiles{Count: 500, Sum: 9_876_543_210, Min: 12_345, P25: 234_567, P50: 345_678, P75: 456_789, Max: 98_765_432}
		}
		for i := range 40 {
			sd.Duration = append(sd.Duration, model.HistBin{Lo: int64(i) * 1000, Hi: int64(i)*1000 + 999, Count: 12})
		}
		task := func(i int) model.TaskSample {
			return model.TaskSample{TaskID: int64(s*500 + i + 1_000_000), Index: i, ExecutorID: fmt.Sprint(i % 200), Status: model.StatusSucceeded,
				LaunchMs: 1_790_000_000_000 + int64(s)*60_000 + int64(i)*97, DurationMs: 123_456, RunTimeMs: 120_000, GCTimeMs: 4_567,
				DeserializeMs: 123, FetchWaitMs: 2_345, RecordsRead: 12_345_678, InputBytes: 1_234_567_890, ShuffleRead: 234_567_890,
				ShuffleWrite: 345_678_901, Spill: 456_789_012, PeakExec: 567_890_123, Source: src}
		}
		for i := range min(slowest, 500) {
			sd.Slowest = append(sd.Slowest, task(i))
		}
		for i := range min(sample, 500) {
			sd.Sample = append(sd.Sample, task(i))
		}
		for e := range 200 {
			sd.Executors = append(sd.Executors, model.StageExecutorCell{ExecutorID: fmt.Sprint(e),
				Tasks: model.TaskTotals{Tasks: 3, Succeeded: 3, DurationMs: 370_368, GCTimeMs: 13_701, InputBytes: 3_703_703_670, InputRecords: 37_037_034,
					OutputBytes: 1_234_567_890, ShuffleReadBytes: 703_703_670, ShuffleReadRecords: 7_037_034, ShuffleWriteBytes: 1_037_036_703,
					MemorySpillBytes: 1_370_367_036, DiskSpillBytes: 370_367_036},
				Peak: model.PeakMemory{JVMHeap: 7_123_456_789, OnHeapExecution: 3_123_456_789, OnHeapStorage: 1_123_456_789}})
		}
		x.Stages = append(x.Stages, sd)
	}
	x.Running = model.RunningTasks{Start: r.Application.Start, BucketMs: 12_800, BusyMs: make([]int64, 2000)}
	var b bytes.Buffer
	if err := WriteExplorer(&b, r, x, ExplorerOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Logf("explorer.html for the worst benchmark shape: %.1f MB", float64(b.Len())/1e6)
	if b.Len() > 25_000_000 {
		t.Errorf("explorer.html is %.1f MB, over the 25 MB budget", float64(b.Len())/1e6)
	}
}

func TestExplorerWithoutCollectedData(t *testing.T) {
	t.Parallel()
	r, _ := buildWithExplorer(t, "application_1790380000000_0042")
	d := embedded(t, renderExplorer(t, r, nil))
	if d["collected"] != false || len(d["notes"].([]any)) == 0 {
		t.Errorf("collected %v, notes %v", d["collected"], d["notes"])
	}
	var b bytes.Buffer
	if err := WriteExplorer(&b, &model.Report{}, nil, ExplorerOptions{}); err != nil {
		t.Fatalf("empty report: %v", err)
	}
}

func TestReportLinksToExplorer(t *testing.T) {
	t.Parallel()
	r, _ := buildWithExplorer(t, "application_1790380000000_0042")
	var b bytes.Buffer
	if err := WriteHTML(&b, r, Options{ExplorerHref: "explorer.html"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), `href="explorer.html"`) || !regexp.MustCompile(`href="explorer.html#stage/\d+\.\d+"`).MatchString(b.String()) {
		t.Error("report.html should link to the explorer and to the stage its skew finding names")
	}
	if strings.Contains(render(t, r, nil), "explorer.html") {
		t.Error("without an explorer page the report must not link to one")
	}
}

// -source: the log's file names find the local files, and the embedded code
// is redacted (the fixture workload plants secrets in its own source).
func TestLoadSources(t *testing.T) {
	t.Parallel()
	r, x := buildWithExplorer(t, "application_1790380000000_0046")
	srcs, notes, err := LoadSources(r, []string{"../../../scripts/fixtures"})
	if err != nil {
		t.Fatal(err)
	}
	if len(srcs) != 1 || !strings.HasSuffix(srcs[0].Path, "workload.py") || srcs[0].Logged[0] != "/home/hadoop/jobs/workload.py" {
		t.Fatalf("sources %+v, notes %v", srcs, notes)
	}
	var b bytes.Buffer
	if err := WriteExplorer(&b, r, x, ExplorerOptions{Sources: srcs}); err != nil {
		t.Fatal(err)
	}
	if m := regexp.MustCompile(`FAKE-[A-Z0-9-]+|AKIAIOSFODNN7EXAMPLE`).FindString(b.String()); m != "" {
		t.Errorf("planted secret %q reached the page through the source", m)
	}
	if !strings.Contains(b.String(), `spark.myapp.db.password\", \"[redacted]\"`) {
		t.Error("the redacted config line should still show its key")
	}
	if _, notes, _ := LoadSources(r, []string{"../../../scripts/fixtures/java"}); len(notes) == 0 {
		t.Error("an unmatched log file name should be noted")
	}
	if _, _, err := LoadSources(r, []string{"/no/such/dir"}); err == nil {
		t.Error("a missing -source path is an error")
	}
	for _, c := range []struct {
		a, b string
		n    int
	}{{"/mnt/yarn/x/jobs/etl.py", "src/jobs/etl.py", 2}, {"ClaimsJob.java", "a/b/ClaimsJob.java", 1}, {"etl.py", "other.py", 0}} {
		if got := tailMatch(c.a, c.b); got != c.n {
			t.Errorf("tailMatch(%q, %q) = %d, want %d", c.a, c.b, got, c.n)
		}
	}
}

// Every chart the explorer places has a function to draw it; a misspelt
// name would only show as an empty box in the browser.
func TestExplorerChartsHaveDrawers(t *testing.T) {
	t.Parallel()
	slots := regexp.MustCompile(`chartSlot\("[^"]*", "([A-Za-z]+)`).FindAllStringSubmatch(explorerJS, -1)
	if len(slots) < 15 {
		t.Fatalf("found %d chart slots; the pattern no longer matches explorer.js", len(slots))
	}
	for _, s := range slots {
		if !regexp.MustCompile(`\n    ` + s[1] + `: function \(c(, [a-z]+)?\)`).MatchString(explorerJS) {
			t.Errorf("chart %q has no DRAW entry", s[1])
		}
	}
}

// D3 is embedded exactly as released (7.9.0, checked against npm's
// integrity hash when vendored), and the page never calls the parts of it
// that fetch data.
func TestVendoredD3(t *testing.T) {
	t.Parallel()
	sum := sha256.Sum256([]byte(d3JS))
	if got := hex.EncodeToString(sum[:]); got != "f2094bbf6141b359722c4fe454eb6c4b0f0e42cc10cc7af921fc158fceb86539" {
		t.Errorf("assets/vendor/d3-7.9.0.min.js changed: sha256 %s", got)
	}
	if m := regexp.MustCompile(`d3\.(json|csv|tsv|dsv|text|xml|html|svg|image|blob|buffer)\(`).FindString(explorerJS); m != "" {
		t.Errorf("explorer.js calls D3's data loader %q", m)
	}
}

// SP-006: a source file cut short by an over-long line is marked cut and
// noted, not embedded as if it were complete.
func TestLoadSourcesLongLineMarkedCut(t *testing.T) {
	t.Parallel()
	r, _ := buildWithExplorer(t, "application_1790380000000_0046")
	orig, err := os.ReadFile("../../../scripts/fixtures/workload.py")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.SplitAfter(string(orig), "\n")
	long := strings.Repeat("x", maxSourceLine+10) + "\n"
	body := strings.Join(lines[:10], "") + long + strings.Join(lines[10:], "")
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "workload.py"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	srcs, notes, err := LoadSources(r, []string{dir})
	if err != nil || len(srcs) != 1 {
		t.Fatalf("%v %v", srcs, err)
	}
	if !srcs[0].Cut || len(srcs[0].Lines) != 10 || !strings.Contains(strings.Join(notes, " "), "longer than") {
		t.Fatalf("cut=%v lines=%d notes=%v", srcs[0].Cut, len(srcs[0].Lines), notes)
	}
}

// The run timeline shades the same driver gaps the report lists.
func TestExplorerCarriesDriverGaps(t *testing.T) {
	t.Parallel()
	r, x := buildWithExplorer(t, "application_1790380000000_0046")
	if len(r.Jobs.DriverGaps) == 0 {
		t.Fatal("fixture has no driver gaps")
	}
	d := embedded(t, renderExplorer(t, r, x))
	gaps, _ := d["gaps"].([]any)
	if len(gaps) != len(r.Jobs.DriverGaps) {
		t.Fatalf("explorer has %d gaps, report %d", len(gaps), len(r.Jobs.DriverGaps))
	}
	for i, g := range r.Jobs.DriverGaps {
		row := gaps[i].([]any)
		if int64(row[0].(float64)) != g.Start.UnixMilli() || int64(row[1].(float64)) != g.End.UnixMilli() {
			t.Errorf("gap %d: explorer %v, report %v–%v", i, row, g.Start, g.End)
		}
	}
	if !strings.Contains(explorerJS, `["timeline", "Timeline"]`) {
		t.Error("no Timeline tab")
	}
}

// The page gets the whole run path for its totals and a drawing of it:
// every stage step, gaps worth showing, and context parents beside.
func TestExplorerRunPath(t *testing.T) {
	t.Parallel()
	r, x := buildWithExplorer(t, "application_1790380000000_0046")
	if len(r.Jobs.RunPath) == 0 {
		t.Fatal("no run path")
	}
	var total int64
	for _, s := range r.Jobs.RunPath {
		total += s.DurationMs()
	}
	if want := r.Application.End.Sub(r.Application.Start).Milliseconds(); total != want {
		t.Errorf("path adds up to %d ms, the run took %d", total, want)
	}
	g := runPathGraph(r)
	if len(g.Steps) != len(r.Jobs.RunPath) || g.Graph == nil || len(g.Graph.Pos) != len(g.Drawn)+len(g.Extra) {
		t.Fatalf("steps %d, drawn %d, extra %d, graph %+v", len(g.Steps), len(g.Drawn), len(g.Extra), g.Graph)
	}
	for _, i := range g.Drawn {
		if s := r.Jobs.RunPath[i]; s.Kind != model.PathStage && s.DurationMs() < 1000 {
			t.Errorf("drew a %d ms %s gap", s.DurationMs(), s.Kind)
		}
	}
	stagesOnPath := 0
	for _, s := range r.Jobs.RunPath {
		if s.Kind == model.PathStage {
			stagesOnPath++
		}
	}
	drawnStages := 0
	for _, i := range g.Drawn {
		if r.Jobs.RunPath[i].Kind == model.PathStage {
			drawnStages++
		}
	}
	if drawnStages != stagesOnPath {
		t.Errorf("drew %d of %d stages on the path", drawnStages, stagesOnPath)
	}
	for _, e := range g.Graph.Edges {
		if a, b := g.Graph.Pos[e[0]], g.Graph.Pos[e[1]]; a[1] >= b[1] {
			t.Errorf("edge %v does not point down: %v → %v", e, a, b)
		}
	}
	if d := embedded(t, renderExplorer(t, r, x)); d["runPath"] == nil {
		t.Error("page has no runPath")
	}
}

// The Overview's utilisation panel restates what the analysis found, one
// line each, with no combined score.
func TestResourceUse(t *testing.T) {
	t.Parallel()
	r, _ := buildWithExplorer(t, "application_1790380000000_0042")
	got := map[string]xUse{}
	for _, u := range resourceUse(r, buildAnatomy(r)) {
		got[u.Label] = u
		if u.Explain == "" || u.Share > 1 || u.Group == "" || u.Tone != "" && u.Verdict == "" ||
			len(u.Band) > 0 && (len(u.Band) != 2 || u.Band[0] >= u.Band[1] || u.Share < 0) {
			t.Errorf("%s: %+v", u.Label, u)
		}
	}
	for _, c := range []struct{ label, value, tone, verdict string }{
		{"Task slots busy", "69%", "ok", "Healthy"},
		{"JVM CPU share", "55%", "ok", "Healthy"},
		{"Peak heap", "74% 757 MiB of 1.0 GiB", "ok", "Healthy"},
		{"Executors lost", "1 of 2 started", "crit", "Lost"},
		{"Disk spill", "337 MiB", "warn", "Spilled"},
	} {
		u, ok := got[c.label]
		if !ok || !strings.Contains(u.Value+" "+u.Detail, c.value) || u.Tone != c.tone || u.Verdict != c.verdict {
			t.Errorf("%s = %+v, want %q (%s, %s)", c.label, u, c.value, c.tone, c.verdict)
		}
	}
	if _, ok := got["Containers waiting"]; ok {
		t.Error("no CloudWatch, so no line for waiting containers")
	}
	// With CloudWatch, the count leads and the rest explains it.
	a := buildAnatomy(r)
	for _, c := range []struct{ waiting, value, detail, verdict string }{
		{"53 at most, for 2 min 53 s", "53", "at most, for 2 min 53 s, from CloudWatch", "Waited"},
		{"0 at most, for 0 ms", "0", "from CloudWatch", "None"},
	} {
		a.RM.Waiting = c.waiting
		found := false
		for _, u := range resourceUse(r, a) {
			found = found || u.Label == "Containers waiting"
			if u.Label == "Containers waiting" && (u.Value != c.value || u.Detail != c.detail || u.Verdict != c.verdict || u.Group != "stability") {
				t.Errorf("waiting %q: %+v", c.waiting, u)
			}
		}
		if !found {
			t.Errorf("waiting %q: no card", c.waiting)
		}
	}
}

// The Logs tab lists the nodes up during the run (the Nodes section's,
// as At a glance counts them), not every instance the cluster ever had:
// primary first, then core and task nodes, each numbered within its role.
func TestClusterNodesUpDuringTheRun(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC)
	inst := func(id, role string, ready time.Duration) model.Instance {
		return model.Instance{ID: id, Role: role, Primary: role == "MASTER", PrivateDNS: id + ".example.internal", Type: "m5.xlarge", Ready: t0.Add(ready)}
	}
	all := []model.Instance{inst("i-p", "MASTER", 0), inst("i-c2", "CORE", 2*time.Minute), inst("i-c1", "CORE", time.Minute), inst("i-t1", "TASK", 3*time.Minute),
		inst("i-old1", "TASK", -48*time.Hour), inst("i-old2", "TASK", -47*time.Hour)} // ended long before the run
	r := &model.Report{Cluster: &model.Cluster{ID: "j-1", Instances: all}}
	for _, k := range []int{3, 0, 1, 2} {
		in := all[k]
		h := &model.Host{Name: in.PrivateDNS, Instance: &in}
		if k == 1 {
			h.Executors = []string{"1", "2"}
		}
		if k == 0 {
			h.Driver = true
		}
		r.Nodes.Hosts = append(r.Nodes.Hosts, *h)
	}
	d := embedded(t, renderExplorer(t, r, nil))
	c, _ := d["cluster"].(map[string]any)
	nodes, _ := c["nodes"].([]any)
	var got []string
	for _, n := range nodes {
		m := n.(map[string]any)
		got = append(got, fmt.Sprintf("%v %v %v", m["kind"], m["seq"], m["id"]))
	}
	if strings.Join(got, ", ") != "primary 1 i-p, core 1 i-c1, core 2 i-c2, task 1 i-t1" || c["allInstances"] != float64(6) {
		t.Errorf("nodes = %v, all = %v", got, c["allInstances"])
	}
}

// The replay is a tab of its own when the executors' logs told the tasks'
// stories, and it plays only when asked (it starts paused). A legend says
// which stage each colour is, and what happened is a numbered list whose
// tasks are grouped by stage and second. Each executor's card names its
// cores but not its node.
func TestExplorerReplay(t *testing.T) {
	t.Parallel()
	for _, want := range []string{
		`if (D.taskStories && D.taskStories.tasks.length) TABS.splice(`,
		`views.replay = function () {`,
		`paused at the start`,
		// the colours say what they are, and the list is numbered,
		// grouped, and can be cut down to jobs and stages
		`"Task colour = its stage:"`,
		`"Jobs and stages only"`,
		`el("span", { cls: "rp-n", text: num(n) })`,
		`Math.floor(t / 1000) + "|" + kind + "|" + x.task.stage`,
	} {
		if !strings.Contains(explorerJS, want) {
			t.Errorf("explorer.js has no %q", want)
		}
	}
	if strings.Contains(explorerJS, "setInterval(") {
		t.Error("the replay should animate with requestAnimationFrame, which stops with the tab")
	}
	// An executor's card names its cores, not its node (the user asked).
	card := explorerJS[strings.Index(explorerJS, `el("div", { cls: "rp-exhead" }`):]
	card = card[:strings.Index(card, "\n")]
	if strings.Contains(card, "host") || !strings.Contains(card, `plural(e.cores, "core", "cores")`) {
		t.Errorf("replay card header: %s", card)
	}
}

// In the explorer an executor's card opens At a glance with that executor
// drawn in full: the diagram links to #anatomy/<id>, and the page carries
// a panel per executor.
func TestExplorerAnatomyOpensExecutors(t *testing.T) {
	t.Parallel()
	r, x := buildWithExplorer(t, "application_1790380000000_0046")
	d := embedded(t, renderExplorer(t, r, x))
	anat, _ := d["anatomy"].(string)
	execs, _ := d["anatomyExecutors"].(map[string]any)
	if len(execs) == 0 || len(execs) != len(r.Executors.Executors) {
		t.Fatalf("%d executor panels for %d executors", len(execs), len(r.Executors.Executors))
	}
	for id := range execs {
		if !strings.Contains(anat, `href="#anatomy/`+id+`"`) {
			t.Errorf("the diagram does not open executor %s", id)
		}
	}
	if !strings.Contains(explorerJS, `location.hash = "#anatomy/" + encodeURIComponent(this.getAttribute("data-exec"))`) {
		t.Error("the whole executor card should open it")
	}
}

// The Stages tab and the Overview lead with the stages worth a look: a
// ranked list with its reasons in words, ranked failed, critical path,
// flagged, then by duration, and the critical-path reason dropped when
// most stages are on the path. The time-against-data scatter is offered
// only when at least two stages handled data.
func TestExplorerStagesWorthALook(t *testing.T) {
	t.Parallel()
	for _, want := range []string{
		`var STAGE_VIEW = "stageAttention";`,
		`chartSlot("", "stageAttention:compact")`,
		`return st.status === "failed" ? 0 : crit[st.key] ? 1 : stageSkewed(st) || st.diskSpill > 0 || st.attempt > 0 ? 2 : 3;`,
		`pathAll = onPath > all.length / 2`,
		`"Slowest task " + (st.max / st.p50).toFixed(1) + "× the median"`,
		`return stages.filter(function (st) { return stageMoved(st) > 0; }).length >= 2;`,
	} {
		if !strings.Contains(explorerJS, want) {
			t.Errorf("explorer.js has no %q", want)
		}
	}
}

// The node chart's memory axis steps in whole units (1 GiB, 2 GiB), never
// halves.
func TestExplorerNodeChartWholeTicks(t *testing.T) {
	t.Parallel()
	for _, want := range []string{`note: D.aws.nodeMemNote, wholeTicks: true }, "bytes");`, `if (kind === "count" || whole) ticks = ticks.filter(Number.isInteger);`} {
		if !strings.Contains(explorerJS, want) {
			t.Errorf("explorer.js has no %q", want)
		}
	}
}

// At a glance shows the inside of an executor and the driver as sections
// of their own, under the diagram; the report keeps them in its diagram.
func TestExplorerAnatomyPanelsApart(t *testing.T) {
	t.Parallel()
	r, x := buildWithExplorer(t, "application_1790380000000_0046")
	d := embedded(t, renderExplorer(t, r, x))
	anat, _ := d["anatomy"].(string)
	if anat == "" || strings.Contains(anat, ">The driver<") || strings.Contains(anat, ">Inside executor ") {
		t.Errorf("the diagram still draws the executor or the driver inside it")
	}
	panels, _ := d["anatomyPanels"].([]any)
	var titles []string
	for _, p := range panels {
		m, _ := p.(map[string]any)
		titles = append(titles, fmt.Sprint(m["title"]))
		if svg, _ := m["svg"].(string); !strings.HasPrefix(svg, `<svg class="anat"`) || strings.Contains(svg, ">"+fmt.Sprint(m["title"])+"<") {
			t.Errorf("panel %q: its drawing is missing or repeats its title", m["title"])
		}
	}
	if len(titles) != 2 || !strings.HasPrefix(titles[0], "Inside executor ") || titles[1] != "The driver" {
		t.Errorf("panels = %q", titles)
	}
	if d["anatomyPanelGuide"] == nil {
		t.Error("the panels have no guide")
	}
	page := html(t, r, Options{})
	if !strings.Contains(page, ">The driver<") {
		t.Error("the report's diagram lost the driver")
	}
	for _, want := range []string{`(D.anatomyPanels || []).forEach(`, `guideNodes(D.anatomyPanelGuide || {})`} {
		if !strings.Contains(explorerJS, want) {
			t.Errorf("explorer.js has no %q", want)
		}
	}
}
