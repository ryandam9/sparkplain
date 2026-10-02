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
	"sort"
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
		"dur":   model.Duration,
		"pct":   model.Percent,
		"num":   model.Num,
		"numi":  func(n int) string { return model.Num(int64(n)) },
		"time":  timeTag,
		"span": func(a, b time.Time) string {
			if a.IsZero() || b.IsZero() {
				return "—"
			}
			return model.Duration(b.Sub(a).Milliseconds())
		},
		"cpuDur": func(ns int64) string { return model.Duration(ns / 1e6) },
		"src":    func(s model.Source) string { return s.String() },
		"cov":    covClass,
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
		"clusterChart": func(r *model.Report) template.HTML { return clusterChart(r, loc) },
		"market": func(m string) string {
			return map[string]string{"SPOT": "spot", "ON_DEMAND": "on-demand"}[m]
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
		"share":  shareBar,
		"sharev": func(f float64) string { return fmt.Sprintf("%.4f", f) },
		"ratio": func(a, b int64) float64 {
			if b <= 0 {
				return 0
			}
			return float64(a) / float64(b)
		},
		"gcBad":        func(f float64) bool { return f > 0.10 },
		"join":         strings.Join,
		"hbaseRetries": model.HBaseRetriesText,
		"hbaseServer":  model.HBaseServerEventsText,
		"hbaseEvent":   model.HBaseServerEventName,
		"hbaseCPUMs":   func(ns int64) int64 { return ns / 1e6 },
		"hbaseRegions": func(a, b int) int { return a + b },
		"hbaseSrcs":    hbaseSrcs,
		"headRegions":  headRegions,
		"tasksWithRows": func(ts []model.HBaseTaskRead) int {
			n := 0
			for _, t := range ts {
				if t.RowsKnown {
					n++
				}
			}
			return n
		},
		"headEvents": func(es []model.HBaseRegionEvent) []model.HBaseRegionEvent {
			if len(es) > maxReportEvents {
				return es[:maxReportEvents]
			}
			return es
		},
		"headTasks": func(ts []model.HBaseTaskRead) []model.HBaseTaskRead {
			if len(ts) > maxReportTasks {
				return ts[:maxReportTasks]
			}
			return ts
		},
		"shortHost":    shortHost,
		"notableTasks": notableTasks,
		"maxNotable":   func() int { return maxNotable },
		"scanFacts": func(sc *model.HBaseScan) [][2]string { // all but the key range, which the card shows
			var out [][2]string
			for _, f := range sc.Facts() {
				if f[0] != "Rows" {
					out = append(out, f)
				}
			}
			return out
		},
		"removal": func(k string) string {
			return map[string]string{"": "running at end", model.RemovalMemoryKill: "killed (137)", model.RemovalLost: "lost", model.RemovalDecommissioned: "decommissioned", model.RemovalKilledByDriver: "removed by Spark", model.RemovalIdle: "idle", model.RemovalOther: "other"}[k]
		},
		"removalBad": func(k string) bool {
			return k == model.RemovalMemoryKill || k == model.RemovalLost || k == model.RemovalDecommissioned
		},
		"skewx": func(d model.Dist) string {
			if d.P50 <= 0 || d.Count < 2 {
				return "—"
			}
			return fmt.Sprintf("%.1f×", float64(d.Max)/float64(d.P50))
		},
		"skewv": func(d model.Dist) float64 {
			if d.P50 <= 0 {
				return 0
			}
			return float64(d.Max) / float64(d.P50)
		},
		"stageDur": func(s *model.Stage) int64 { return s.DurationMs() },
		"jobDur":   func(j *model.Job) int64 { return j.DurationMs() },
		"planCap":  func(s string) string { return capText(s, 20000) },
		"first":    func(n int, v []*model.SQLQuery) []*model.SQLQuery { return v[:min(n, len(v))] },
		"ints": func(v []int) string {
			s := make([]string, len(v))
			for i, x := range v {
				s[i] = fmt.Sprint(x)
			}
			return strings.Join(s, ", ")
		},
		"execChart": func(r *model.Report) template.HTML { return execChart(r, loc) },
		"gantt":     func(r *model.Report) template.HTML { return gantt(r, loc) },
		"memChart":  memChart,
		"hasPrefix": strings.HasPrefix,
		// explorerURL links an Evidence.Ref ("stage:27.0") to its view in
		// the explorer ("explorer.html#stage/27.0").
		"explorerURL": func(href, ref string) template.URL {
			kind, id, _ := strings.Cut(ref, ":")
			if kind == "node" {
				return template.URL(href + "#cluster") // the explorer's nodes
			}
			return template.URL(href + "#" + url.PathEscape(kind) + "/" + url.PathEscape(id))
		},
		"add":   func(a, b int) int { return a + b },
		"short": func(s string, n int) string { return capText(s, n) },
		"lower": strings.ToLower,
		"rt":    runtimeValue,
		"int64": func(n int) int64 { return int64(n) },
		"cpuShare": func(t model.TaskTotals) float64 {
			if t.RunTimeMs <= 0 {
				return 0
			}
			return float64(t.CPUTimeNs) / 1e6 / float64(t.RunTimeMs)
		},
		"divBytes": func(b int64, n int) int64 {
			if n <= 0 {
				return 0
			}
			return b / int64(n)
		},
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

func shareBar(f float64) template.HTML {
	w := min(100, max(0, f*100))
	return template.HTML(fmt.Sprintf(`<span class="mini"><span class="bar"><i style="width:%.1f%%"></i></span>%s</span>`, w, template.HTMLEscapeString(model.Percent(f))))
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

// hbaseSrcs names a line behind each count in an HBase stage's cell, so
// a reader can check it: "scanner: file:line; busy: file:line".
func hbaseSrcs(maps ...map[string]model.Source) string {
	var parts []string
	for _, m := range maps {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			parts = append(parts, k+": "+m[k].String())
		}
	}
	return strings.Join(parts, "; ")
}

// maxReportTasks caps the HBase tasks table in the report; the explorer
// and the JSON report list them all.
const maxReportTasks = 500

// maxReportEvents caps the HBase region events table in the report.
const maxReportEvents = 200

// scanRegion is a region of a scan with its place in key order.
type scanRegion struct {
	Index  int
	Region model.HBaseRegionRead
}

// headRegions keeps a scan's table short: the n regions that returned the
// most rows (then the longest) when they are tied to tasks, else the first
// n in key order; shown in key order.
func headRegions(rs []model.HBaseRegionRead, n int) []scanRegion {
	out := make([]scanRegion, len(rs))
	for i, g := range rs {
		out[i] = scanRegion{i, g}
	}
	if len(out) <= n {
		return out
	}
	// The most rows first, then (as when rows are not known) the longest.
	key := func(g model.HBaseRegionRead) [2]int64 {
		if g.Task == nil {
			return [2]int64{-1, -1}
		}
		return [2]int64{g.Task.Rows, g.Task.DurationMs}
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := key(out[i].Region), key(out[j].Region)
		return a[0] > b[0] || a[0] == b[0] && a[1] > b[1]
	})
	out = out[:n]
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out
}

// maxNotable caps the report's table of tasks worth a look.
const maxNotable = 50

// notableTasks are the task stories worth a look: failed and killed
// tasks, then those that read the most shuffle data over the network or
// spilled the most, at most maxNotable.
func notableTasks(ts []model.TaskLog) []model.TaskLog {
	var out []model.TaskLog
	for _, t := range ts {
		if t.Outcome == "failed" || t.Outcome == "killed" || t.ShuffleRemoteBytes > 0 || t.SpillBytes > 0 || t.NotCached > 0 {
			out = append(out, t)
		}
	}
	rank := func(t model.TaskLog) int {
		if t.Outcome == "failed" || t.Outcome == "killed" {
			return 0
		}
		return 1
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if rank(a) != rank(b) {
			return rank(a) < rank(b)
		}
		return a.ShuffleRemoteBytes+a.SpillBytes > b.ShuffleRemoteBytes+b.SpillBytes
	})
	if len(out) > maxNotable {
		out = out[:maxNotable]
	}
	return out
}
