package report

import (
	"fmt"
	"html/template"
	"strings"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// chartGuide explains a chart under it, the same way everywhere:
//   - Axes names what each axis or mark stands for ("Across", "Up", "Rows",
//     "Bar length", "Colour", ...), one line each;
//   - Read says how to read it: what a good picture looks like and what a
//     bad one means, one point each, shown as bullets when there are several;
//   - Note is fine print: sampling, what was left out, what clicking does.
type chartGuide struct {
	Axes [][2]string `json:"axes"`
	Read []string    `json:"read"`
	Note string      `json:"note,omitempty"`
	// Run says what this run's chart shows (runNotes), with links to the
	// findings it is evidence for.
	Run []runPoint `json:"run,omitempty"`
}

// html renders the guide. The explorer's guideNodes draws the same shape.
func (g chartGuide) html() template.HTML {
	var b strings.Builder
	if len(g.Axes) > 0 {
		b.WriteString(`<dl class="axes">`)
		for _, a := range g.Axes {
			fmt.Fprintf(&b, `<div><dt>%s</dt><dd>%s</dd></div>`, esc(a[0]), esc(a[1]))
		}
		b.WriteString(`</dl>`)
	}
	b.WriteString(string(pointList("read", "How to read it", g.Read)))
	b.WriteString(string(runList(g.Run)))
	if g.Note != "" {
		fmt.Fprintf(&b, `<p class="cap">%s</p>`, esc(g.Note))
	}
	return template.HTML(b.String())
}

// pointList is a titled paragraph for one point, or a titled bullet list
// for several.
func pointList(cls, title string, points []string) template.HTML {
	switch len(points) {
	case 0:
		return ""
	case 1:
		return template.HTML(fmt.Sprintf(`<p class="%s"><b>%s</b> %s</p>`, cls, esc(title), esc(points[0])))
	}
	var b strings.Builder
	fmt.Fprintf(&b, `<div class="%s"><b>%s</b><ul>`, cls, esc(title))
	for _, p := range points {
		fmt.Fprintf(&b, `<li>%s</li>`, esc(p))
	}
	b.WriteString(`</ul></div>`)
	return template.HTML(b.String())
}

// runList renders a chart's "In this run": a paragraph for one point, or a
// bullet list, with each finding linked to the report's findings list.
func runList(points []runPoint) template.HTML {
	item := func(p runPoint) string {
		if p.Finding > 0 {
			return fmt.Sprintf(`<a href="#finding-%d">%s</a>`, p.Finding, esc(p.Text))
		}
		return esc(p.Text)
	}
	switch len(points) {
	case 0:
		return ""
	case 1:
		return template.HTML(`<p class="run"><b>In this run</b> ` + item(points[0]) + `</p>`)
	}
	var b strings.Builder
	b.WriteString(`<div class="run"><b>In this run</b><ul>`)
	for _, p := range points {
		b.WriteString(`<li>` + item(p) + `</li>`)
	}
	b.WriteString(`</ul></div>`)
	return template.HTML(b.String())
}

func clipLabel(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}

// unknownCapacity lists the hosts that ran part of the application but
// have no YARN capacity to draw, as "ip-10-0-2-13 (the driver and 2
// executors)", so the node chart never leaves one out silently. Without
// the cluster's logs no node has a capacity, and the Sources panel already
// says the logs were not read, so it lists none.
func unknownCapacity(r *model.Report) []string {
	if r.Logs == nil {
		return nil
	}
	var out []string
	for _, h := range r.Nodes.Hosts {
		if ran := ranHere(h); h.YARNMemoryBytes <= 0 && ran != "" {
			short, _, _ := strings.Cut(h.Name, ".")
			out = append(out, short+" ("+ran+")")
		}
	}
	return out
}

// unknownNote names the nodes that ran part of the application but whose
// YARN capacity is not in the logs read, so they are not drawn.
func unknownNote(nodes []string) string {
	if len(nodes) == 0 {
		return ""
	}
	return "Not drawn, because the logs read do not say how much memory their NodeManager offered YARN (its registration and container placement lines may have rotated out of the ResourceManager's log): " + strings.Join(nodes, "; ") + "."
}

// chartFuncs are the report template's chart functions; explorer links
// chart rows to the explorer's pages when it was written.
func chartFuncs(loc *time.Location, explorer string) template.FuncMap {
	return template.FuncMap{
		"anatomy": func(r *model.Report) template.HTML { return anatomyHTML(r, explorer) },
	}
}

// ranHere says what of the application ran on a host: "the driver and 2
// executors", or "" when nothing did.
func ranHere(h model.Host) string {
	var parts []string
	if h.Driver {
		parts = append(parts, "the driver")
	}
	if n := len(h.Executors); n > 0 {
		parts = append(parts, model.Plural(n, "executor", "executors"))
	}
	return strings.Join(parts, " and ")
}
