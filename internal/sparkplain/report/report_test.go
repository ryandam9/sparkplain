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

func build(t *testing.T, name string) *model.Report {
	t.Helper()
	in, err := eventlog.Resolve(filepath.Join("../../../testdata/eventlog", name), name, eventlog.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	l, err := eventlog.Parse(context.Background(), in, eventlog.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return analyze.Run(analyze.Input{Tool: "sparkplain test", GeneratedAt: time.Date(2026, 9, 26, 1, 0, 0, 0, time.UTC), TimeZone: "UTC",
		EventLog: l, EventSource: model.SourceStatus{Name: "Spark event log", Status: "read"}, Thresholds: analyze.DefaultThresholds()})
}

func render(t *testing.T, r *model.Report, loc *time.Location) string {
	t.Helper()
	var b bytes.Buffer
	if err := WriteHTML(&b, r, Options{Location: loc}); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func TestHTMLIsSelfContained(t *testing.T) {
	t.Parallel()
	html := render(t, build(t, "application_1790380000000_0042"), nil)
	for _, re := range []string{`<link\b`, `<script[^>]+src=`, `<img[^>]+src="?https?:`, `@import`, `url\(\s*['"]?https?:`, `<iframe`, `fonts\.googleapis`} {
		if m := regexp.MustCompile(re).FindString(html); m != "" {
			t.Errorf("report loads something external: %q", m)
		}
	}
}

func TestHTMLHasEverySection(t *testing.T) {
	t.Parallel()
	html := render(t, build(t, "application_1790380000000_0042"), nil)
	for _, id := range []string{"summary", "coverage", "findings", "timeline", "nodes", "executors", "memory", "cpu", "io", "stages", "sql", "config", "access", "sources"} {
		if !strings.Contains(html, `<section id="`+id+`"`) {
			t.Errorf("missing section %s", id)
		}
	}
	for _, want := range []string{"claims_enrich_fixture", "Stage 18 is skewed", "<svg", "ip-10-0-1-23.ec2.internal", "spark_catalog.claims.region_totals", "Set · value hidden"} {
		if !strings.Contains(html, want) {
			t.Errorf("report lacks %q", want)
		}
	}
}

func TestNoPlantedSecretsInOutputs(t *testing.T) {
	t.Parallel()
	for _, n := range []string{"application_1790380000000_0042", "application_1790380000000_0044"} {
		r := build(t, n)
		html := render(t, r, nil)
		var js bytes.Buffer
		if err := WriteJSON(&js, r); err != nil {
			t.Fatal(err)
		}
		for _, out := range []string{html, js.String()} {
			if m := regexp.MustCompile(`FAKE-[A-Z0-9-]+|AKIAIOSFODNN7EXAMPLE`).FindAllString(out, 3); m != nil {
				t.Errorf("%s: secrets leaked: %v", n, m)
			}
		}
	}
}

func TestTimesRenderInConfiguredZoneWithLabel(t *testing.T) {
	t.Parallel()
	r := build(t, "application_1790380000000_0042")
	syd, err := time.LoadLocation("Australia/Sydney")
	if err != nil {
		t.Skip("no tzdata")
	}
	html := render(t, r, syd)
	want := r.Application.Start.In(syd).Format("15:04:05")
	if !strings.Contains(html, want) || !strings.Contains(html, "Australia/Sydney") {
		t.Errorf("start time %s or zone label missing", want)
	}
	if !strings.Contains(html, `data-time="`+r.Application.Start.UTC().Format(time.RFC3339Nano)+`"`) {
		t.Error("times must carry data-time for the viewer's zone")
	}
}

func TestJSONRoundTrips(t *testing.T) {
	t.Parallel()
	r := build(t, "application_1790380000000_0042")
	var b bytes.Buffer
	if err := WriteJSON(&b, r); err != nil {
		t.Fatal(err)
	}
	var back model.Report
	if err := json.Unmarshal(b.Bytes(), &back); err != nil {
		t.Fatal(err)
	}
	if back.SchemaVersion != model.SchemaVersion || back.Application.ID != r.Application.ID || len(back.Findings) != len(r.Findings) {
		t.Errorf("round trip lost data")
	}
}

func TestRendersWithoutEventLog(t *testing.T) {
	t.Parallel()
	r := analyze.Run(analyze.Input{Tool: "t", EventSource: model.SourceStatus{Name: "Spark event log", Status: "error", Class: "corrupt", Detail: "bad magic"}})
	html := render(t, r, nil)
	if !strings.Contains(html, "Event log: not read") || !strings.Contains(html, "bad magic") {
		t.Error("degraded report should say the event log was not read and why")
	}
}

func TestHostileTextIsEscaped(t *testing.T) {
	t.Parallel()
	r := build(t, "application_1790380000000_0044")
	r.Application.Name = `<script>alert(1)</script>`
	r.Findings = append(r.Findings, model.Finding{Title: `<img src=x onerror=alert(1)>`, Severity: model.Info})
	html := render(t, r, nil)
	if strings.Contains(html, "<script>alert(1)") || strings.Contains(html, "<img src=x") {
		t.Error("log text must be escaped")
	}
}

func TestRuntimeEnvironmentTable(t *testing.T) {
	t.Parallel()
	html := render(t, build(t, "application_1790380000000_0042"), nil)
	for _, want := range []string{`id="runtime"`, `href="#runtime"`, "Runtime environment", "Spark 3.5.1</b> · Java 21.0.10 · Hadoop 3.3.4",
		"/usr/lib/jvm/java-21-openjdk-amd64", `<tr class="grp"><th colspan="4">Locations</th></tr>`, `class="missingrow"`} {
		if !strings.Contains(html, want) {
			t.Errorf("report lacks %q", want)
		}
	}
}

// A scan built from the executors' logs, with no event log, says rows
// need the event log and how its times were found, and shows no rows.
func TestScanFromLogsRenders(t *testing.T) {
	t.Parallel()
	r := scanFromLogsReport()
	html := render(t, r, nil)
	for _, want := range []string{"Stage 172: TableInputFormat scan of", "needs the event log", "Built without the event log", "4 min 10 s", "task 6429"} {
		if !strings.Contains(html, want) {
			t.Errorf("report lacks %q", want)
		}
	}
	if x := renderExplorer(t, r, nil); !strings.Contains(x, `"fromLogs":true`) {
		t.Error("explorer lacks the logs-only scan")
	}
}

func scanFromLogsReport() *model.Report {
	r := analyze.Run(analyze.Input{Tool: "t", EventSource: model.SourceStatus{Name: "Spark event log", Status: "not-supplied"}})
	task := &model.ScanTask{Index: 0, TaskID: 6429, ExecutorID: "3", DurationMs: 250_000, Rows: 0, Source: model.Source{File: "e1", Line: 40}}
	r.HBase = &model.HBaseSection{
		Tables: []model.HBaseTable{{Name: "orders", Read: true, APIs: []string{"TableInputFormat"}}},
		Scans: []model.HBaseScanRead{{StageID: 172, Table: "orders", Rows: "[k1, k2)", Tasks: 1, Tied: true, TiedBy: "task", FromLogs: true,
			Regions: []model.HBaseRegionRead{{Region: "aaa949cedc", StartRow: "k1", EndRow: "k2", Server: "rs-1", Task: task, Source: model.Source{File: "e1", Line: 12}}},
			Servers: []model.HBaseServerRead{{Server: "rs-1", Regions: 1, TaskMs: 250_000}}}},
	}
	return r
}

// The HBase tasks table lists every task attempt; the report keeps the
// first 500 in stage order and the explorer all of them.
func TestHBaseTasksTable(t *testing.T) {
	t.Parallel()
	r := scanFromLogsReport()
	t0 := time.Date(2026, 10, 2, 9, 17, 0, 0, time.UTC)
	for i := range 600 {
		k := model.HBaseTaskRead{Stage: 172 + i/300, Partition: i % 300, TaskID: int64(6000 + i), Table: "ns:orders", StartRow: fmt.Sprintf("k%03d", i), EndRow: fmt.Sprintf("k%03d", i+1),
			Region: fmt.Sprintf("r%03d", i), Server: "rs-1.example.internal", ExecutorID: "3", Host: "ip-3.example.internal", Start: t0, End: t0.Add(time.Minute),
			DurationMs: 60_000, Timed: true, TimeFrom: "executor log", Outcome: "finished", Source: model.Source{File: "e1", Line: int64(10 + i)}}
		if i == 1 {
			k.Outcome, k.EndSource = "failed", model.Source{File: "e1", Line: 9001}
		}
		r.HBase.Tasks = append(r.HBase.Tasks, k)
	}
	r.HBase.TaskStages = []model.HBaseTaskStage{{Stage: 172, Tables: []string{"ns:orders"}, Tasks: 300, Failed: 1, Regions: 300, Servers: 1, Start: t0, End: t0.Add(time.Minute)},
		{Stage: 173, Tables: []string{"ns:orders"}, Tasks: 300, Regions: 300, Servers: 1, Start: t0, End: t0.Add(time.Minute)}}
	html := render(t, r, nil)
	for _, want := range []string{"Every HBase task, all stages", "The first 500 of 600 task attempts", "k499", "e1:9001", "failed", "executor log",
		"Rows are shown only for tasks the event log records"} {
		if !strings.Contains(html, want) {
			t.Errorf("report lacks %q", want)
		}
	}
	if strings.Contains(html, ">r500<") || !strings.Contains(html, ">r499<") {
		t.Error("report goes past 500 tasks")
	}
	d := embedded(t, renderExplorer(t, r, nil))
	if ts, _ := d["hbaseTasks"].([]any); len(ts) != 600 {
		t.Errorf("explorer has %d tasks, want 600", len(ts))
	}
	if ss, _ := d["hbaseTaskStages"].([]any); len(ss) != 2 {
		t.Errorf("explorer has %d task stages", len(ss))
	}
}

// loadReport is a run with no event log where one region server serves 8
// of the 10 scan tasks running for 3 minutes.
func loadReport() *model.Report {
	t0 := time.Date(2026, 10, 2, 9, 17, 0, 0, time.UTC)
	f := model.LogFile{Location: "e1", Kind: "container-stderr", Container: "container_1_0001_01_000002"}
	for i := range 10 {
		srv, end := "rs-hot.example.internal", t0.Add(3*time.Minute)
		if i >= 8 {
			srv, end = fmt.Sprintf("rs-%d.example.internal", i), t0.Add(10*time.Minute)
		}
		sp := model.HBaseSplit{Table: "orders", StartRow: fmt.Sprintf("k%d", i), EndRow: fmt.Sprintf("k%d", i+1), Server: srv, Region: fmt.Sprintf("r%d", i),
			Time: t0, Source: model.Source{File: "e1", Line: int64(10 + i)},
			Task: &model.SplitTask{TaskID: int64(100 + i), Partition: i, Stage: 5, Start: t0, End: end, EndSource: model.Source{File: "e1", Line: int64(100 + i)}}}
		f.HBaseSplits = append(f.HBaseSplits, sp)
		f.Found = append(f.Found, model.LogLine{Kind: model.LogHBaseUse, Time: t0, Count: 1, Source: sp.Source,
			Fields: map[string]string{"table": "orders", "access": "read", "api": "TableInputFormat", "server": srv}})
	}
	return analyze.Run(analyze.Input{AppID: "application_1_1", Tool: "t", TimeZone: "UTC", Thresholds: analyze.DefaultThresholds(),
		EventSource: model.SourceStatus{Name: "Spark event log", Status: "not-supplied"}, Logs: []model.LogFile{f}, LogsRead: true,
		LogSources: []model.SourceStatus{{Name: "Container logs", Status: "read"}}})
}

// Region server load over time: a chart that explains itself and links
// the finding, a table per server with its hot stretch, and the explorer's
// copy.
func TestHBaseLoadRenders(t *testing.T) {
	t.Parallel()
	r := loadReport()
	page := render(t, r, time.UTC)
	i := strings.Index(page, "<h4>Region server load over time</h4>")
	if i < 0 {
		t.Fatal("no load chart")
	}
	chart := page[i:]
	chart = chart[:strings.Index(chart, `<div class="tbl">`)]
	for _, want := range []string{`<dl class="axes">`, "How to read it", "rs-hot did the most scan work: 24 min 0 s of task time, up to 8 tasks at once", "See finding", "<svg"} {
		if !strings.Contains(chart, want) {
			t.Errorf("load chart lacks %q", want)
		}
	}
	for _, want := range []string{">rs-hot<", "up to 8 of 10", "On average while busy"} {
		if !strings.Contains(page, want) {
			t.Errorf("load table lacks %q", want)
		}
	}
	d := embedded(t, renderExplorer(t, r, nil))
	if l, _ := d["hbaseLoad"].([]any); len(l) != 3 {
		t.Errorf("explorer load: %d servers", len(l))
	}
}
