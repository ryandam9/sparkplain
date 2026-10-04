package report

import (
	"regexp"
	"strings"
	"testing"
)

// The report's headings make an outline with no gaps (an h4 straight under
// an h2 would confuse a screen reader), so the small labels in guides and
// findings are headings at the level below their section.
func TestReportHeadingsHaveNoGaps(t *testing.T) {
	t.Parallel()
	r, _ := buildWithExplorer(t, "application_1790380000000_0044") // findings, guides and charts
	page := html(t, r, Options{})
	last := 1
	for _, m := range regexp.MustCompile(`<h([1-6])[ >]`).FindAllStringSubmatch(page, -1) {
		lv := int(m[1][0] - '0')
		if lv > last+1 {
			t.Errorf("an h%d follows an h%d", lv, last)
		}
		last = lv
	}
	for _, want := range []string{`<h4 class="k">Try</h4>`, `<h3 class="glabel">How to read it</h3>`} {
		if !strings.Contains(page, want) {
			t.Errorf("report lacks %q", want)
		}
	}
}

// Each explorer tab closes gaps in its headings and lists its parts in
// "On this page", which never narrows the page: fixed in the margin when
// it is wide enough, else folded above the tab.
func TestExplorerTabContents(t *testing.T) {
	t.Parallel()
	for _, want := range []string{`function headingLevels(`, `function tocHeads(`, `"On this page"`, `matchMedia("(min-width: 1800px)")`, `scrollIntoView(`} {
		if !strings.Contains(explorerJS, want) {
			t.Errorf("explorer.js lacks %q", want)
		}
	}
	for _, want := range []string{`@media (min-width:1800px){`, `.pagetoc{position:fixed;`, `scroll-margin-top:`} {
		if !strings.Contains(explorerCSS, want) {
			t.Errorf("explorer.css lacks %q", want)
		}
	}
	if strings.Contains(explorerCSS, "withtoc") || strings.Contains(explorerJS, "withtoc") {
		t.Error("the contents take a column from the page")
	}
}
