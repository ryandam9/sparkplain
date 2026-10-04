package report

import (
	"regexp"
	"strings"
	"testing"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// Each finding folds under its number, severity and title: critical and
// warning findings start open, info closed. Buttons open or close them all,
// a link to a finding opens it, and printing opens every fold. The coverage
// table folds under a count of its statuses.
func TestFindingsFold(t *testing.T) {
	t.Parallel()
	r, _ := buildWithExplorer(t, "application_1790380000000_0049") // warning and info findings
	page := html(t, r, Options{})
	folds := regexp.MustCompile(`<article class="finding (crit|warn|info)" id="finding-\d+"><div class="stripe"></div><details class="body"( open)?>\s*<summary class="t">`).FindAllStringSubmatch(page, -1)
	if len(folds) != len(r.Findings) || len(folds) < 2 {
		t.Fatalf("%d findings fold, of %d", len(folds), len(r.Findings))
	}
	sevs := map[string]bool{}
	for _, f := range folds {
		sevs[f[1]] = true
		if open := f[2] != ""; open != (f[1] != "info") {
			t.Errorf("a %s finding starts open=%v", f[1], open)
		}
	}
	if !sevs["info"] || !(sevs["warn"] || sevs["crit"]) {
		t.Errorf("the fixture's findings do not test both defaults: %v", sevs)
	}
	for _, want := range []string{`<span class="foldall" data-fold="#findings" hidden>`} {
		if !strings.Contains(page, want) {
			t.Errorf("report lacks %q", want)
		}
	}
	if !regexp.MustCompile(`<details class="cover"><summary>\d+ parts: \d+ (complete|partial|need)`).MatchString(page) {
		t.Error("the coverage table does not fold under its counts")
	}
	for _, want := range []string{`"beforeprint"`, `"afterprint"`, `function openTarget(`, `addEventListener("hashchange", openTarget)`} {
		if !strings.Contains(js, want) {
			t.Errorf("report.js lacks %q", want)
		}
	}
	for _, want := range []string{`el("details", { cls: "body", open: sev !== "info" }`, `function foldAll(`, `"beforeprint"`, `var d = f.querySelector("details"); if (d) d.open = true;`, `el("details", { cls: "logsrc"`} {
		if !strings.Contains(explorerJS, want) {
			t.Errorf("explorer.js lacks %q", want)
		}
	}
}

func TestCovCounts(t *testing.T) {
	t.Parallel()
	got := covCounts([]model.SectionStatus{{Coverage: model.Complete}, {Coverage: model.NeedsEventLog}, {Coverage: model.Complete}, {Coverage: model.NeedsEventLog}, {Coverage: model.NoData}})
	if want := "2 complete, 2 need the event log, 1 has no data"; got != want {
		t.Errorf("covCounts = %q, want %q", got, want)
	}
}
