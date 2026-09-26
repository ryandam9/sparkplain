package report

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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

// The page's only network request is the Google Charts loader, pinned to a
// frozen release, asking only for chart packages that render in the browser
// (CLAUDE.md: never GeoChart or Map, which send data to Google).
func TestExplorerOnlyLoadsGoogleCharts(t *testing.T) {
	r, x := buildWithExplorer(t, "application_1790380000000_0042")
	page := dataRE.ReplaceAllString(renderExplorer(t, r, x), "") // log values may contain URLs; they are data, not loads
	for _, re := range []string{`<link\b`, `<script[^>]+src=`, `@import`, `url\(\s*['"]?https?:`, `<iframe`, `<img\b`} {
		if m := regexp.MustCompile(re).FindString(page); m != "" {
			t.Errorf("explorer loads something outside its script: %q", m)
		}
	}
	// The SVG namespace is an identifier browsers never fetch.
	allowed := map[string]bool{"https://www.gstatic.com/charts/loader.js": true, "http://www.w3.org/2000/svg": true}
	loader := false
	for _, u := range regexp.MustCompile(`https?://[^\s"'<>)]+`).FindAllString(page, -1) {
		if !allowed[u] {
			t.Errorf("explorer references %s; only the Google Charts loader is allowed", u)
		}
		loader = loader || u == "https://www.gstatic.com/charts/loader.js"
	}
	if !loader {
		t.Error("the Google Charts loader is missing")
	}
	if !strings.Contains(explorerJS, `GC_VERSION = "52"`) || strings.Contains(explorerJS, `load("current"`) {
		t.Error("Google Charts must be pinned to a frozen release")
	}
	pk := regexp.MustCompile(`packages:\s*\[([^\]]*)\]`).FindAllStringSubmatch(explorerJS, -1)
	if len(pk) != 1 || strings.TrimSpace(pk[0][1]) != `"corechart", "timeline"` {
		t.Errorf("chart packages %v; only corechart and timeline are allowed", pk)
	}
	if regexp.MustCompile(`(?i)geochart|visualization\.map\b|"map"`).MatchString(explorerJS) {
		t.Error("GeoChart and Map send data to Google and must not be used")
	}
}

func TestExplorerHasNoPlantedSecrets(t *testing.T) {
	for _, name := range []string{"application_1790380000000_0042", "application_1790380000000_0044"} {
		r, x := buildWithExplorer(t, name)
		if m := regexp.MustCompile(`FAKE-[A-Z0-9-]+|AKIAIOSFODNN7EXAMPLE`).FindString(renderExplorer(t, r, x)); m != "" {
			t.Errorf("%s: planted secret %q in explorer.html", name, m)
		}
	}
}

// A value from the log must not be able to end the data block and run code.
func TestExplorerEscapesHostileText(t *testing.T) {
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
				ShuffleWrite: 345_678_901, Spill: 456_789_012, Source: src}
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
