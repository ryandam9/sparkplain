package report

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

func html(t *testing.T, r *model.Report, opt Options) string {
	t.Helper()
	var b bytes.Buffer
	if err := WriteHTML(&b, r, opt); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

// Each chart appears when its data does, and not otherwise.
func TestReportCharts(t *testing.T) {
	t.Parallel()
	r, _ := buildWithExplorer(t, "application_1790380000000_0042")
	page := html(t, r, Options{ExplorerHref: "app-explorer.html"})
	for _, title := range []string{"The longest stages", "Task time spread", "Data each stage moved", "Data over time", "Spill by stage", "Where executor time went", "The longest queries"} {
		if !strings.Contains(page, "<h4>"+title+"</h4>") {
			t.Errorf("chart %q missing", title)
		}
	}
	for _, title := range []string{nodeTitle, "Node CPU"} {
		if strings.Contains(page, "<h4>"+title+"</h4>") {
			t.Errorf("chart %q drawn without the cluster's data", title)
		}
	}
	if !strings.Contains(page, `href="app-explorer.html#stage/`) {
		t.Error("stage rows should link to the explorer")
	}

	// With the EMR API's and CloudWatch's data, the node charts appear.
	at := r.Application.Start
	r.Nodes.Hosts = append(r.Nodes.Hosts, model.Host{Name: "ip-10-0-0-9.ec2.internal", Executors: []string{"1"}, PeakExecutors: 1,
		YARNMemoryBytes: 12 << 30, DriverContainerBytes: 2 << 30, ExecutorContainerBytes: 11 << 30, Instance: &model.Instance{ID: "i-9", Role: "CORE"}})
	r.Metrics = &model.MetricsSection{From: at, To: at.Add(time.Hour), Hosts: []model.Series{{Name: "CPUUtilization", Stat: "Average", Scope: "i-9",
		Points: []model.Point{{T: at, V: 20}, {T: at.Add(5 * time.Minute), V: 80}}}}}
	page = html(t, r, Options{})
	for _, title := range []string{nodeTitle, "Node CPU"} {
		if !strings.Contains(page, "<h4>"+title+"</h4>") {
			t.Errorf("chart %q missing", title)
		}
	}
	if !strings.Contains(page, "i-9 (1 executors)") {
		t.Error("the CPU chart should name each node by what it ran")
	}

	// A stage name cannot break out of the SVG.
	r.Jobs.Stages[0].Name = `</text><script>alert(4)</script>`
	if strings.Contains(html(t, r, Options{}), "<script>alert(4)") {
		t.Fatal("a stage name reached the chart unescaped")
	}
}

// Without the event log there is nothing to chart.
func TestNoChartsWithoutData(t *testing.T) {
	t.Parallel()
	page := html(t, &model.Report{Application: model.Application{ID: "application_1_1"}}, Options{})
	if strings.Contains(page, "<h4>") {
		t.Error("charts drawn for an empty report")
	}
}

// Long tables scroll inside a box no taller than most of the screen, with
// the header in view, and print in full. Every table sits in such a box.
func TestTablesScrollInTheirBox(t *testing.T) {
	t.Parallel()
	for _, rule := range []string{".tbl{overflow:auto;max-height:75vh;", ".tbl thead th{position:sticky;top:0;", "@media print{", ".tbl{max-height:none;overflow:visible}"} {
		if !strings.Contains(css, rule) {
			t.Errorf("report.css lacks %q", rule)
		}
	}
	r, _ := buildWithExplorer(t, "application_1790380000000_0042")
	page := html(t, r, Options{})
	if tables, boxes := strings.Count(page, "<table"), strings.Count(page, `<div class="tbl"><table`); tables != boxes {
		t.Errorf("%d tables but %d in a scrolling box", tables, boxes)
	}
}

// A legend lists only the colours the bars use.
func TestLegendShowsUsedColours(t *testing.T) {
	t.Parallel()
	rows := []hbar{{segs: []seg{{v: 5, color: cInput}, {v: 0, color: cSpill}}}}
	got := shown(rows, []legendItem{{cInput, "Read"}, {cSpill, "Spilled to disk"}})
	if len(got) != 1 || got[0].label != "Read" {
		t.Errorf("legend = %v", got)
	}
}

// Findings read in labelled bands, and "What happened" is a bullet list.
func TestFindingBandsAndSummaryBullets(t *testing.T) {
	t.Parallel()
	r, _ := buildWithExplorer(t, "application_1790380000000_0044") // a failed run: critical findings
	page := html(t, r, Options{})
	for _, want := range []string{`<div class="fpart what"><span class="k">Error</span>`, `<div class="fpart evid"><span class="k">Evidence</span>`, `<div class="fpart try"><span class="k">Try</span>`, `<h2>What happened</h2>
          <ul><li>`} {
		if !strings.Contains(page, want) {
			t.Errorf("report lacks %q", want)
		}
	}
	for _, want := range []string{`"fpart what"`, `"fpart evid"`, `"fpart try"`, `el("ul", null, (D.summary`} {
		if !strings.Contains(explorerJS, want) {
			t.Errorf("explorer lacks %s", want)
		}
	}
}

// Every chart, in both pages, has a title, says what it shows and says how
// to read it.
func TestEveryChartExplainsItself(t *testing.T) {
	t.Parallel()
	r, _ := buildWithExplorer(t, "application_1790380000000_0042")
	page := html(t, r, Options{ExplorerHref: "x.html"})
	charts := strings.Split(page, `<div class="chart">`)[1:]
	if len(charts) < 6 {
		t.Fatalf("only %d charts on the fixture's report", len(charts))
	}
	for _, c := range charts {
		head := strings.TrimSpace(c)[:min(len(strings.TrimSpace(c)), 80)]
		if !strings.HasPrefix(strings.TrimSpace(c), "<h4>") || !strings.Contains(c, `<dl class="axes"><div><dt>`) ||
			!regexp.MustCompile(`class="read"><b>How to read it</b>`).MatchString(c) {
			t.Errorf("report chart lacks a title, what its axes are, or how to read it: %q", head)
		}
	}
	guides := regexp.MustCompile(`\{ t: [^}]*?\}`).FindAllString(explorerJS, -1)
	if len(guides) < 20 {
		t.Fatalf("found %d chart guides in explorer.js", len(guides))
	}
	for _, g := range guides {
		if strings.HasPrefix(g, "{ t: g.t,") {
			continue // the timeline helper passing its caller's guide on
		}
		if !strings.Contains(g, "axes: [[") || !strings.Contains(g, "read: ") || strings.Contains(g, "shows: ") {
			t.Errorf("explorer chart guide lacks axes or read, or still has a caption: %.90s", g)
		}
	}
	if strings.Contains(explorerJS, "guideNodes({ shows") {
		t.Error("a guide in explorer.js still passes a caption instead of axes")
	}
	if n := regexp.MustCompile(`frame\(c, "|drawGraph\([^;]*\}, "|hbarChart\([^;]*\], "`).FindAllString(explorerJS, -1); len(n) > 0 {
		t.Errorf("charts still passed a bare caption: %v", n)
	}
}

// The stage time split explains every task's time: each bar's parts add up
// to the stage's task time, which its note shows.
func TestSplitChart(t *testing.T) {
	t.Parallel()
	r, _ := buildWithExplorer(t, "application_1790380000000_0042")
	out := string(splitChart(r, "x.html"))
	if !strings.Contains(out, "Where stage time went") || !strings.Contains(out, `href="x.html#stage/`) {
		t.Fatal("split chart missing, or its bars do not link to the explorer")
	}
	for _, s := range r.Jobs.Stages {
		p := s.Totals.TimeSplit()
		if want := s.Totals.SchedulerDelayMs + s.Totals.DeserializeMs + s.Totals.RunTimeMs + s.Totals.ResultSerializationMs + s.Totals.GettingResultMs; p.Total() != want {
			t.Errorf("stage %d: split adds up to %d ms, task time is %d ms", s.ID, p.Total(), want)
		}
	}
}

// A node that ran the driver or executors is never left out of the node
// chart silently: when its YARN capacity is unknown, the chart names it,
// and when no node can be drawn the chart still says why.
func TestNodeChartNamesNodesItCannotDraw(t *testing.T) {
	t.Parallel()
	gib := int64(1 << 30)
	r := &model.Report{Logs: &model.LogsSection{}}
	r.Nodes.Hosts = []model.Host{
		{Name: "ip-10-0-2-10.ec2.internal", YARNMemoryBytes: 12 * gib, ExecutorContainerBytes: 3 * gib, Executors: []string{"1"}},
		{Name: "ip-10-0-2-13.ec2.internal", Driver: true, Executors: []string{"2", "3"}},
		{Name: "ip-10-0-2-11.ec2.internal"}, // ran nothing: not named
	}
	out := string(nodeMemoryChart(r))
	if !strings.Contains(out, "ip-10-0-2-13 (the driver and 2 executors)") || strings.Contains(out, "ip-10-0-2-11") {
		t.Errorf("chart note = %s", out)
	}
	r.Nodes.Hosts[0].YARNMemoryBytes = 0
	if out := string(nodeMemoryChart(r)); !strings.Contains(out, "<h4>"+nodeTitle+"</h4>") || !strings.Contains(out, "ip-10-0-2-10 (1 executor); ip-10-0-2-13") {
		t.Errorf("with no node drawable, chart = %s", out)
	}
	// Without the cluster's logs, nothing is said: no node has a capacity.
	r.Logs = nil
	if out := string(nodeMemoryChart(r)); out != "" {
		t.Errorf("without logs, chart = %s", out)
	}
}

// Each executor container on a node is its own piece, in its own colour,
// saying which executor it is and its vCPUs (shortened when narrow), and
// the row ends with the memory and vCPUs taken of what the node offered.
// When more executors ran on a node than at once, none is named.
func TestNodeChartNamesEachExecutor(t *testing.T) {
	t.Parallel()
	r := &model.Report{Logs: &model.LogsSection{}}
	r.Executors.Executors = []*model.Executor{{ID: "1", Cores: 4}, {ID: "2", Cores: 4}, {ID: "10", Cores: 4}, {ID: "3", Cores: 4}, {ID: "4", Cores: 4}, {ID: "5", Cores: 4}}
	r.Nodes.Hosts = []model.Host{
		{Name: "ip-10-0-2-10.ec2.internal", Executors: []string{"10", "2", "1"}, PeakExecutors: 3, YARNMemoryBytes: 48 << 30, YARNVCores: 16,
			DriverContainerBytes: 2 << 30, ExecutorContainerBytes: 12 << 30},
		{Name: "ip-10-0-2-11.ec2.internal", Executors: []string{"3", "4", "5"}, PeakExecutors: 2, YARNMemoryBytes: 48 << 30, YARNVCores: 16,
			ExecutorContainerBytes: 12 << 30},
	}
	out := string(nodeMemoryChart(r))
	// narrow pieces say the short form: "E1 · 4 vCPU", and "D" for the driver
	for _, want := range []string{">E1 · 4 vCPU<", ">E2 · 4 vCPU<", ">E10 · 4 vCPU<", "executor 10&#39;s container: 12.0 GiB, 4 vCPU",
		"38.0 GiB of 48.0 GiB · 12 of 16 vCPU", ">Executor · 4 vCPU<", "24.0 GiB of 48.0 GiB · 8 of 16 vCPU", ">D<",
		`fill="var(--viz-1)"`, `fill="var(--viz-3)"`, `fill="var(--viz-4)"`} {
		if !strings.Contains(out, want) {
			t.Errorf("chart lacks %q", want)
		}
	}
	if strings.Contains(out, ">E3 ·") || strings.Contains(out, ">Executor 3 ·") {
		t.Error("a node where executors replaced others named which ran together")
	}
}
