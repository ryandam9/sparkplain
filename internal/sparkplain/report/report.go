// Package report renders a model.Report as one self-contained HTML file and
// as JSON. The HTML embeds its CSS and script and loads nothing from the
// network when opened.
package report

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

var (
	//go:embed assets/report.css
	css string
	//go:embed assets/report.js
	js string
	//go:embed templates/report.html.tmpl
	pageTmpl string
)

// WriteJSON writes the report as indented JSON.
func WriteJSON(w io.Writer, r *model.Report) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	return enc.Encode(r)
}

// Options control HTML rendering.
type Options struct {
	// Location is the time zone times are rendered in before the page's
	// script converts them to the viewer's zone. Nil means UTC.
	Location *time.Location
	// ExplorerHref links to explorer.html; empty when it was not written.
	ExplorerHref string
	// Sources are the application's files (-source, source:): with them,
	// each stage's line of the application's code is the one they match.
	Sources []SourceFile
}

type page struct {
	R        *model.Report
	CSS      template.CSS
	JS       template.JS
	Zone     string
	Complete int
	Partial  int
	Needs    int
	Crit     int
	Warn     int
	Info     int
	Tasks    int64
	Explorer string
}

// WriteHTML renders the report page.
func WriteHTML(w io.Writer, r *model.Report, opt Options) error {
	loc := opt.Location
	if loc == nil {
		loc = time.UTC
	}
	fm := funcs(loc)
	for name, fn := range chartFuncs(loc, opt.ExplorerHref) {
		fm[name] = fn
	}
	t, err := template.New("report").Funcs(fm).Parse(pageTmpl)
	if err != nil {
		return err
	}
	p := page{R: r, CSS: template.CSS(css), JS: template.JS(js), Zone: zoneLabel(loc, r.Application.Start), Explorer: opt.ExplorerHref}
	for _, c := range r.Coverage {
		switch c.Coverage {
		case model.Complete:
			p.Complete++
		case model.Partial:
			p.Partial++
		default:
			p.Needs++
		}
	}
	for _, f := range r.Findings {
		switch f.Severity {
		case model.Critical:
			p.Crit++
		case model.Warning:
			p.Warn++
		default:
			p.Info++
		}
	}
	for _, s := range r.Jobs.Stages {
		p.Tasks += s.Totals.Tasks
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, p); err != nil {
		return err
	}
	_, err = w.Write(buf.Bytes())
	return err
}

func zoneLabel(loc *time.Location, at time.Time) string {
	if at.IsZero() {
		at = time.Now()
	}
	abbr, off := at.In(loc).Zone()
	sign := "+"
	if off < 0 {
		sign, off = "−", -off
	}
	utc := fmt.Sprintf("UTC%s%d", sign, off/3600)
	if m := off % 3600 / 60; m != 0 {
		utc += fmt.Sprintf(":%02d", m)
	}
	if loc.String() == "UTC" {
		return "UTC"
	}
	return fmt.Sprintf("%s (%s, %s)", abbr, loc.String(), utc)
}

func funcs(loc *time.Location) template.FuncMap {
	timeTag := func(t time.Time, f string) template.HTML {
		if t.IsZero() {
			return "—"
		}
		layout := map[string]string{"hms": "15:04:05", "hm": "15:04", "dt": "2 Jan 2006, 15:04:05"}[f]
		if layout == "" {
			layout, f = "15:04:05", "hms"
		}
		iso := t.UTC().Format(time.RFC3339Nano)
		return template.HTML(fmt.Sprintf(`<time datetime="%s" data-time="%s" data-fmt="%s">%s</time>`, iso, iso, f, t.In(loc).Format(layout)))
	}
	return template.FuncMap{
		"bytes": model.Bytes,
		"num":   model.Num,
		"time":  timeTag,
		"src":   func(s model.Source) string { return s.String() },
		"cov":   covClass,
		"covLabel": func(c model.Coverage) string {
			return map[model.Coverage]string{model.Complete: "Complete", model.Partial: "Partial", model.NeedsEventLog: "Needs event log", model.NoData: "No data"}[c]
		},
		"sev": func(s model.Severity) string {
			return map[model.Severity]string{model.Critical: "crit", model.Warning: "warn", model.Info: "info"}[s]
		},
		"sevPill": func(s model.Severity) string {
			return map[model.Severity]string{model.Critical: "crit", model.Warning: "part", model.Info: "info"}[s]
		},
		"sevLabel": func(s model.Severity) string {
			return map[model.Severity]string{model.Critical: "Critical", model.Warning: "Warning", model.Info: "Info"}[s]
		},
		"statusClass": func(s string) string {
			switch s {
			case model.StatusSucceeded:
				return "ok"
			case model.StatusFailed:
				return "crit"
			case model.StatusIncomplete, model.StatusRunning:
				return "warn"
			}
			return "na"
		},
		"statusLabel": statusLabel,
		"srcPill": func(s string) string {
			return map[string]string{"read": "full", "partial": "part", "error": "crit", "not-supplied": "none", "not-yet": "none", "none": "none", "not-requested": "none"}[s]
		},
		"srcLabel": func(s string) string {
			return map[string]string{"read": "Read", "partial": "Partly read", "error": "Could not read", "not-supplied": "Not supplied", "not-yet": "Not in this version",
				"none": "Nothing for this app", "not-requested": "Not requested"}[s]
		},
		"fileCounts": func(fs []model.SourceFile) map[string]int {
			out := map[string]int{"read": 0, "skipped": 0, "error": 0}
			for _, f := range fs {
				out[f.Status]++
			}
			return out
		},
		"headFiles": func(fs []model.SourceFile, n int) []model.SourceFile {
			if len(fs) > n {
				return fs[:n]
			}
			return fs
		},
		"join":  strings.Join,
		"first": func(n int, v []*model.SQLQuery) []*model.SQLQuery { return v[:min(n, len(v))] },
		// explorerURL links an Evidence.Ref ("stage:27.0") to its view in
		// the explorer ("explorer.html#stage/27.0").
		"xTab":  explorerTab,
		"prose": proseHTML,
		"explorerURL": func(href, ref string) template.URL {
			kind, id, _ := strings.Cut(ref, ":")
			if kind == "node" {
				return template.URL(href + "#cluster") // the explorer's nodes
			}
			return template.URL(href + "#" + url.PathEscape(kind) + "/" + url.PathEscape(id))
		},
		"add":   func(a, b int) int { return a + b },
		"rt":    runtimeValue,
		"int64": func(n int) int64 { return int64(n) },
	}
}

func covClass(c model.Coverage) string {
	return map[model.Coverage]string{model.Complete: "full", model.Partial: "part", model.NeedsEventLog: "none", model.NoData: "none"}[c]
}

func statusLabel(s string) string {
	return map[string]string{model.StatusSucceeded: "Succeeded", model.StatusFailed: "Failed", model.StatusIncomplete: "Incomplete",
		model.StatusRunning: "Running", model.StatusSkipped: "Skipped", model.StatusUnknown: "Unknown", "": "Unknown"}[s]
}

func capText(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && s[cut]&0xc0 == 0x80 {
		cut--
	}
	return s[:cut] + "\n… (cut here; the JSON export has the full text)"
}

// runtimeValue returns a recorded value from the runtime table, or "".
func runtimeValue(r *model.Report, label string) string {
	for _, row := range r.Config.Runtime {
		if row.Label == label && !row.Missing {
			v, _, _ := strings.Cut(row.Value, " (") // "21.0.10 (Ubuntu), …" -> "21.0.10"
			return v
		}
	}
	return ""
}

// explorerTabs maps the report's sections (as the coverage table names
// them) to the explorer tab that has their detail; the report itself keeps
// only the summary.
// proseHTML renders a finding's text: each line a paragraph, and lines
// that start with "- " a bulleted list, so alternatives read as a list.
func proseHTML(text, cls string) template.HTML {
	var b strings.Builder
	inList := false
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		item, isItem := strings.CutPrefix(line, "- ")
		if isItem != inList {
			if isItem {
				b.WriteString(`<ul class="` + cls + `">`)
			} else {
				b.WriteString(`</ul>`)
			}
			inList = isItem
		}
		switch {
		case isItem:
			b.WriteString("<li>" + template.HTMLEscapeString(item) + "</li>")
		case line != "":
			b.WriteString(`<p class="` + cls + `">` + template.HTMLEscapeString(line) + "</p>")
		}
	}
	if inList {
		b.WriteString(`</ul>`)
	}
	return template.HTML(b.String())
}

var explorerTabs = map[string]string{
	"summary": "overview", "findings": "overview", "anatomy": "anatomy", "nodes": "anatomy",
	"timeline": "timeline", "executors": "executors", "memory": "executors", "cpu": "executors",
	"io": "stages", "stages": "stages", "tasklogs": "tasks", "sql": "sql",
	"config": "environment", "runtime": "environment", "access": "environment",
	"sources": "log", "hbase": "overview",
}

// explorerTab is the explorer link for a report section, or "" when there
// is no explorer page or no tab for it.
func explorerTab(href, id string) template.URL {
	tab, ok := explorerTabs[id]
	if href == "" || !ok {
		return ""
	}
	return template.URL(href + "#" + tab)
}
