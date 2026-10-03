package report

import (
	"strings"
	"testing"
)

// Every chart says what this run's version of it shows: a line from the
// analysis, a link to each finding it is evidence for, or "Nothing unusual
// here", never nothing. Checked on the main fixture, whose findings are
// known.
func TestRunNotes(t *testing.T) {
	t.Parallel()
	r, _ := buildWithExplorer(t, "application_1790380000000_0042")
	n := runNotes(r)
	for chart := range chartRules {
		if len(n[chart]) == 0 {
			t.Errorf("%s has no \"In this run\"", chart)
		}
	}
	// Findings are linked by their number, and only from charts that are
	// evidence for them.
	for chart, points := range n {
		for _, p := range points {
			if p.Finding == 0 {
				continue
			}
			f := r.Findings[p.Finding-1]
			if !strings.Contains(p.Text, f.Title) || !strings.Contains(strings.Join(chartRules[chart], " "), f.Rule) {
				t.Errorf("%s links finding %d (%s) as %q", chart, p.Finding, f.Rule, p.Text)
			}
		}
	}
	has := func(chart, want string) {
		t.Helper()
		for _, p := range n[chart] {
			if strings.Contains(p.Text, want) {
				return
			}
		}
		t.Errorf("%s: no line with %q in %+v", chart, want, n[chart])
	}
	has("spread", "Stage 18's slowest task took 16.9× its median task")
	has("spread", "Stage 18 is skewed")
	has("spill", "2 stages spilled 558 MiB (337 MiB on disk)")
	has("executors", "1 ended early")
	has("jobs", "(23% of the run) no job was running")
	has("stages", "1 stage failed.")
	has("split", "Stage 0 lost the most time to anything but computing")
}

// The explorer's charts show the lines, from its embedded data.
func TestRunNotesShown(t *testing.T) {
	t.Parallel()
	r, x := buildWithExplorer(t, "application_1790380000000_0042")
	if d := embedded(t, renderExplorer(t, r, x)); d["runNotes"] == nil {
		t.Error("explorer has no runNotes")
	}
	for _, chart := range []string{"stages", "running", "heatmap", "timeline", "nodeMemory"} {
		if !strings.Contains(explorerJS, `run: RUN("`+chart+`")`) {
			t.Errorf("explorer chart %s does not show its \"In this run\"", chart)
		}
	}
}
