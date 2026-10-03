package report

import (
	"bytes"
	"regexp"
	"strings"
	"testing"

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

// The report is a summary: what happened, the run at a glance, findings,
// what it covers and its sources, each opening the explorer's matching
// tab; the detail sections live only in the explorer. Without an explorer
// page, nothing links to one.
func TestReportIsASummary(t *testing.T) {
	t.Parallel()
	r, _ := buildWithExplorer(t, "application_1790380000000_0042")
	page := html(t, r, Options{ExplorerHref: "app-explorer.html"})
	for _, id := range []string{"summary", "coverage", "findings", "sources"} {
		if !strings.Contains(page, `<section id="`+id+`">`) {
			t.Errorf("summary section %s missing", id)
		}
	}
	for _, id := range []string{"timeline", "nodes", "executors", "memory", "cpu", "io", "stages", "tasklogs", "sql", "config", "access"} {
		if strings.Contains(page, `<section id="`+id+`"`) {
			t.Errorf("detail section %s is still in the report", id)
		}
	}
	for _, want := range []string{`<a class="xlink" href="app-explorer.html#overview">Open in the explorer →</a>`, `href="app-explorer.html#stages">Jobs and stages →</a>`,
		`href="app-explorer.html#environment">Configuration, identity and access →</a>`, `<a href="app-explorer.html#executors">Memory →</a>`, `href="app-explorer.html#log">Open in the explorer →</a>`} {
		if !strings.Contains(page, want) {
			t.Errorf("report lacks %q", want)
		}
	}
	bare := html(t, r, Options{})
	if strings.Contains(bare, "explorer.html#") || strings.Contains(bare, "Open in the explorer") {
		t.Error("with no explorer page, the report links to one")
	}
	// No link points at a section the report no longer has.
	for _, p := range []string{page, bare} {
		ids := map[string]bool{}
		for _, m := range regexp.MustCompile(`id="([^"]+)"`).FindAllStringSubmatch(p, -1) {
			ids[m[1]] = true
		}
		for _, m := range regexp.MustCompile(`href="#([^"]+)"`).FindAllStringSubmatch(p, -1) {
			if !ids[m[1]] {
				t.Errorf("link to #%s, which the report does not have", m[1])
			}
		}
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

// Findings read in labelled bands, and "What happened" is a bullet list.
func TestFindingBandsAndSummaryBullets(t *testing.T) {
	t.Parallel()
	r, _ := buildWithExplorer(t, "application_1790380000000_0044") // a failed run: critical findings
	page := html(t, r, Options{})
	for _, want := range []string{`<div class="fpart what"><span class="k">Error</span>`, `<div class="fpart evid"><span class="k">Evidence</span>`, `<div class="fpart try"><span class="k">Try</span>`, `<h2>What happened</h2></div>
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

// Every explorer chart has a title, says what it shows and says how to
// read it.
func TestEveryChartExplainsItself(t *testing.T) {
	t.Parallel()
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

// A stage's time split explains every task's time: its parts add up to
// the stage's task time.
func TestStageTimeSplitAddsUp(t *testing.T) {
	t.Parallel()
	r, _ := buildWithExplorer(t, "application_1790380000000_0042")
	for _, s := range r.Jobs.Stages {
		p := s.Totals.TimeSplit()
		if want := s.Totals.SchedulerDelayMs + s.Totals.DeserializeMs + s.Totals.RunTimeMs + s.Totals.ResultSerializationMs + s.Totals.GettingResultMs; p.Total() != want {
			t.Errorf("stage %d: split adds up to %d ms, task time is %d ms", s.ID, p.Total(), want)
		}
	}
}

// A node that ran the driver or executors is never left out of the
// explorer's node chart silently: when its YARN capacity is unknown, the
// chart's note names it; without the cluster's logs nothing is said.
func TestNodeChartNamesNodesItCannotDraw(t *testing.T) {
	t.Parallel()
	gib := int64(1 << 30)
	r := &model.Report{Logs: &model.LogsSection{}}
	r.Nodes.Hosts = []model.Host{
		{Name: "ip-10-0-2-10.ec2.internal", YARNMemoryBytes: 12 * gib, ExecutorContainerBytes: 3 * gib, Executors: []string{"1"}},
		{Name: "ip-10-0-2-13.ec2.internal", Driver: true, Executors: []string{"2", "3"}},
		{Name: "ip-10-0-2-11.ec2.internal"}, // ran nothing: not named
	}
	if note := unknownNote(unknownCapacity(r)); !strings.Contains(note, "ip-10-0-2-13 (the driver and 2 executors)") || strings.Contains(note, "ip-10-0-2-11") || strings.Contains(note, "ip-10-0-2-10") {
		t.Errorf("note = %s", note)
	}
	r.Logs = nil
	if note := unknownNote(unknownCapacity(r)); note != "" {
		t.Errorf("without logs, note = %s", note)
	}
}

// Text inside a bar is dark or white by its contrast with the bar's
// colour, so it reads on the palette's light amber, green and pink too.
func TestInBarTextContrasts(t *testing.T) {
	t.Parallel()
	if !strings.Contains(explorerJS, `"inbar " + inkOn(piece.node())`) || !strings.Contains(css, "svg text.inbar.dark{fill:") {
		t.Error("in-bar text does not pick its colour by contrast")
	}
}
