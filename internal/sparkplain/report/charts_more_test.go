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
	r, _ := buildWithExplorer(t, "application_1790380000000_0042")
	page := html(t, r, Options{ExplorerHref: "app-explorer.html"})
	for _, title := range []string{"The longest stages", "Task time spread", "Data each stage moved", "Data over time", "Spill by stage", "Where executor time went", "The longest queries"} {
		if !strings.Contains(page, "<h4>"+title+"</h4>") {
			t.Errorf("chart %q missing", title)
		}
	}
	for _, title := range []string{"What YARN placed on each node", "Node CPU"} {
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
	for _, title := range []string{"What YARN placed on each node", "Node CPU"} {
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
	page := html(t, &model.Report{Application: model.Application{ID: "application_1_1"}}, Options{})
	if strings.Contains(page, "<h4>") {
		t.Error("charts drawn for an empty report")
	}
}

// Long tables scroll inside a box no taller than most of the screen, with
// the header in view, and print in full. Every table sits in such a box.
func TestTablesScrollInTheirBox(t *testing.T) {
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
	rows := []hbar{{segs: []seg{{v: 5, color: cInput}, {v: 0, color: cSpill}}}}
	got := shown(rows, []legendItem{{cInput, "Read"}, {cSpill, "Spilled to disk"}})
	if len(got) != 1 || got[0].label != "Read" {
		t.Errorf("legend = %v", got)
	}
}

// Findings read in labelled bands, and "What happened" is a bullet list.
func TestFindingBandsAndSummaryBullets(t *testing.T) {
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
	r, _ := buildWithExplorer(t, "application_1790380000000_0042")
	page := html(t, r, Options{ExplorerHref: "x.html"})
	charts := strings.Split(page, `<div class="chart">`)[1:]
	if len(charts) < 6 {
		t.Fatalf("only %d charts on the fixture's report", len(charts))
	}
	for _, c := range charts {
		head := strings.TrimSpace(c)[:min(len(strings.TrimSpace(c)), 80)]
		if !strings.HasPrefix(strings.TrimSpace(c), "<h4>") || !strings.Contains(c, `<p class="read"><b>How to read it</b>`) {
			t.Errorf("report chart lacks a title or reading guide: %q", head)
		}
	}
	guides := regexp.MustCompile(`\{ t: [^}]*?\}`).FindAllString(explorerJS, -1)
	if len(guides) < 20 {
		t.Fatalf("found %d chart guides in explorer.js", len(guides))
	}
	for _, g := range guides {
		if !strings.Contains(g, "shows: ") || !strings.Contains(g, "read: ") {
			t.Errorf("explorer chart guide lacks shows or read: %.90s", g)
		}
	}
	if n := regexp.MustCompile(`frame\(c, "|drawGraph\([^;]*\}, "|hbarChart\([^;]*\], "`).FindAllString(explorerJS, -1); len(n) > 0 {
		t.Errorf("charts still passed a bare caption: %v", n)
	}
}
