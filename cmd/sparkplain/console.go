package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// console prints what sparkplain read and, at the end, what it found: a
// summary on stdout of the report's "What happened", its findings and the
// files written. On a terminal each source is listed on stderr as it is
// read, with a note of what is being read in the meantime, and markers
// are coloured (not with NO_COLOR or TERM=dumb). Piped, the summary lists
// the sources itself and notes keep their "sparkplain:" prefix on stderr.
type console struct {
	out, err     io.Writer
	live         bool // stdout and stderr are both a terminal
	colOut       bool // colour on stdout
	colErr       bool // colour on stderr
	width        int
	started      time.Time
	cluster      string          // "Cluster j-… (name, release, state)"
	shown        map[string]bool // sources listed live
	notAsked     []string        // sources this run was not asked to read, listed together
	readHeader   bool            // "Read" printed live
	working      bool            // a transient "reading …" line is on screen
	home, opener string
}

// isTerminal reports whether w is a terminal; tests replace it.
var isTerminal = func(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	st, err := f.Stat()
	return err == nil && st.Mode()&os.ModeCharDevice != 0
}

func newConsole(out, err io.Writer) *console {
	c := &console{out: out, err: err, started: time.Now(), width: 80, shown: map[string]bool{}, opener: "xdg-open"}
	c.live = isTerminal(out) && isTerminal(err)
	colour := os.Getenv("NO_COLOR") == "" && os.Getenv("TERM") != "dumb"
	c.colOut, c.colErr = colour && isTerminal(out), colour && isTerminal(err)
	if n, e := strconv.Atoi(os.Getenv("COLUMNS")); e == nil && n >= 40 {
		c.width = min(n, 120)
	}
	c.home, _ = os.UserHomeDir()
	if runtime.GOOS == "darwin" {
		c.opener = "open"
	}
	return c
}

const (
	bold  = "1"
	dim   = "2"
	red   = "31"
	green = "32"
	amber = "33"
	blue  = "34"
)

func paint(on bool, code, s string) string {
	if !on || s == "" {
		return s
	}
	return "\x1b[" + code + "m" + s + "\x1b[0m"
}

// clear removes the transient "reading …" line before anything else is
// printed on the terminal.
func (c *console) clear() {
	if c.working {
		fmt.Fprint(c.err, "\r\x1b[2K")
		c.working = false
	}
}

// begin names the run; on a terminal it heads the live listing.
func (c *console) begin(appID string) {
	if c.live {
		fmt.Fprintf(c.err, "%s %s\n", paint(c.colErr, bold, "sparkplain "+version), paint(c.colErr, dim, "· "+appID))
	}
}

// clusterFound names the cluster being read.
func (c *console) clusterFound(cl model.Cluster) {
	c.cluster = fmt.Sprintf("Cluster %s (%s, %s, %s)", cl.ID, cl.Name, cl.Release, strings.ToLower(cl.State))
	if c.live {
		c.clear()
		fmt.Fprintln(c.err, c.cluster)
	} else {
		fmt.Fprintf(c.err, "sparkplain: cluster %s (%s, %s, %s)\n", cl.ID, cl.Name, cl.Release, cl.State)
	}
}

// note says something about how the run is going, such as where the
// event log was looked for.
func (c *console) note(format string, a ...any) {
	msg := fmt.Sprintf(format, a...)
	if !c.live {
		fmt.Fprintln(c.err, "sparkplain: "+msg)
		return
	}
	c.clear()
	c.wrap(c.err, "  ", "  ", msg, func(s string) string { return paint(c.colErr, dim, s) })
}

// status shows what sparkplain is doing until the next line replaces it.
func (c *console) status(doing string) {
	if !c.live {
		return
	}
	c.clear()
	fmt.Fprint(c.err, paint(c.colErr, dim, "  "+doing+"…"))
	c.working = true
}

// sources lists sources as they finish, on a terminal.
func (c *console) sources(rows ...model.SourceStatus) {
	if !c.live {
		return
	}
	c.clear()
	for _, s := range rows {
		if c.shown[s.Name] {
			continue
		}
		if !c.readHeader {
			fmt.Fprintln(c.err, "\n"+paint(c.colErr, bold, "Read"))
			c.readHeader = true
		}
		c.shown[s.Name] = true
		if s.Status == "not-requested" {
			c.notAsked = append(c.notAsked, s.Name)
			continue
		}
		c.sourceLine(c.err, c.colErr, s)
	}
}

// notAskedLine lists together the sources the run was not asked to read,
// which each say the same thing: pass -cluster-id or -from.
func (c *console) notAskedLine(w io.Writer, col bool) {
	if len(c.notAsked) == 0 {
		return
	}
	c.sourceLine(w, col, model.SourceStatus{Name: "Not asked for", Status: "not-requested",
		Detail: strings.Join(c.notAsked, ", ") + ". The report's Sources panel says how to add each."})
	c.notAsked = nil
}

// sourceLine is one source: a marker, its name, and the first sentence of
// what was read, or all of why it was not.
func (c *console) sourceLine(w io.Writer, col bool, s model.SourceStatus) {
	mark, code := "·", dim
	switch s.Status {
	case "read":
		mark, code = "✓", green
	case "partial":
		mark, code = "!", amber
	case "error":
		mark, code = "✗", red
	case "not-supplied":
		mark, code = "–", amber
	}
	name := fmt.Sprintf("%-17s", s.Name)
	detail := s.Detail
	if s.Status == "read" {
		detail = parenRE.ReplaceAllString(firstSentence(detail), "")
	}
	lead := "  " + paint(col, code, mark) + " " + name + " "
	c.wrapLimited(w, lead, strings.Repeat(" ", 22), detail, s.Status == "read")
}

// parenRE is an aside in brackets, which a source read as expected can do
// without on the console: "Read 22 files (127 KiB compressed, …) from 7
// containers." The report keeps it all.
var parenRE = regexp.MustCompile(` \([^()]*\)`)

// firstSentence is the text up to the first ". ", which is enough for a
// source read as expected.
func firstSentence(s string) string {
	if i := strings.Index(s, ". "); i > 0 {
		return s[:i+1]
	}
	return s
}

// summary prints the run's outcome on stdout.
func (c *console) summary(r *model.Report, written map[string]string, order []string, exit int) {
	c.clear()
	w, col := c.out, c.colOut
	if !c.live {
		fmt.Fprintf(w, "%s %s\n", paint(col, bold, "sparkplain "+version), paint(col, dim, "· "+r.Application.ID))
		if c.cluster != "" {
			fmt.Fprintln(w, c.cluster)
		}
		fmt.Fprintln(w, "\n"+paint(col, bold, "Read"))
		for _, s := range r.Sources {
			if s.Status == "not-requested" {
				c.notAsked = append(c.notAsked, s.Name)
				continue
			}
			c.sourceLine(w, col, s)
		}
		c.notAskedLine(w, col)
	} else {
		// Sources added while the report was built, such as the
		// application's code, which were not listed as they were read.
		var late []model.SourceStatus
		for _, s := range r.Sources {
			if !c.shown[s.Name] {
				late = append(late, s)
			}
		}
		c.sources(late...)
		c.notAskedLine(c.err, c.colErr)
	}

	var said []string
	for _, s := range r.Summary.Sentences {
		if !strings.HasPrefix(s, "What needs attention") { // the findings below say it
			said = append(said, s)
		}
	}
	if len(said) > 0 {
		fmt.Fprintln(w, "\n"+paint(col, bold, "What happened"))
		for _, s := range said {
			c.wrap(w, "  ", "  ", s, nil)
		}
	}

	counts := map[model.Severity]int{}
	for _, f := range r.Findings {
		counts[f.Severity]++
	}
	var parts []string
	for _, sv := range []struct {
		s    model.Severity
		name string
	}{{model.Critical, "critical"}, {model.Warning, "warning"}, {model.Info, "info"}} {
		if n := counts[sv.s]; n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", n, sv.name))
		}
	}
	if len(parts) == 0 {
		fmt.Fprintln(w, "\n"+paint(col, bold, "Findings: none"))
	} else {
		fmt.Fprintln(w, "\n"+paint(col, bold, "Findings: "+strings.Join(parts, ", ")))
	}
	for _, f := range r.Findings {
		mark, code := "·", blue
		switch f.Severity {
		case model.Critical:
			mark, code = "✖", red
		case model.Warning:
			mark, code = "▲", amber
		}
		c.wrap(w, "  "+paint(col, code, mark)+" ", "    ", f.Title, nil)
	}

	// The first file with its folder, the rest by name when they sit
	// beside it.
	if len(order) > 0 {
		fmt.Fprintln(w)
		dir := filepath.Dir(written[order[0]])
		for i, k := range order {
			p := c.tilde(written[k])
			if i > 0 && filepath.Dir(written[k]) == dir {
				p = filepath.Base(p)
			}
			fmt.Fprintf(w, "%-9s %s\n", k, paint(col, dim, p))
		}
	}
	if p := written["Report"]; p != "" {
		fmt.Fprintf(w, "%s %s %s\n", paint(col, dim, "Open it:"), c.opener, shellQuote(p))
	}
	done := fmt.Sprintf("Done in %s · ", elapsed(time.Since(c.started)))
	switch exit {
	case exitOK:
		done += paint(col, green, "complete") + " (exit 0)"
	case exitPartial:
		missing := 0
		for _, s := range r.Sources {
			if s.Status == "partial" || s.Status == "error" || s.Status == "not-supplied" {
				missing++
			}
		}
		done += paint(col, amber, "partial") + fmt.Sprintf(" (exit 3): %s missing or incomplete, marked above and in the report's Sources panel", model.Plural(missing, "source", "sources"))
	default:
		done += fmt.Sprintf("exit %d", exit)
	}
	fmt.Fprintln(w, "\n"+done)
	if exit == exitPartial && !c.live {
		fmt.Fprintln(c.err, "sparkplain: partial report (exit 3): see the Sources panel for what is missing")
	}
}

// tilde shortens a path under the home folder for display.
func (c *console) tilde(p string) string {
	if c.home != "" && strings.HasPrefix(p, c.home+string(os.PathSeparator)) {
		return "~" + p[len(c.home):]
	}
	return p
}

func shellQuote(s string) string {
	if strings.Trim(s, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-./~") == "" {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func elapsed(d time.Duration) string {
	if d < 10*time.Second {
		return fmt.Sprintf("%.1f s", d.Seconds())
	}
	return model.Duration(d.Milliseconds())
}

// wrap prints text wrapped at the console's width, the first line after
// lead and the rest after indent. style, when set, colours each line.
func (c *console) wrap(w io.Writer, lead, indent, text string, style func(string) string) {
	for i, line := range wrapWords(text, c.width-visibleLen(lead)) {
		if style != nil {
			line = style(line)
		}
		if i == 0 {
			fmt.Fprintln(w, lead+line)
		} else {
			fmt.Fprintln(w, indent+line)
		}
	}
}

// wrapLimited is wrap for a source's line: up to two lines when the
// source was read as expected, three when it says what is missing.
func (c *console) wrapLimited(w io.Writer, lead, indent, text string, oneLine bool) {
	width := c.width - visibleLen(lead)
	lines := wrapWords(text, width)
	max := 3
	if oneLine {
		max = 2
	}
	if len(lines) > max {
		lines = lines[:max]
		last := lines[max-1]
		for utf8.RuneCountInString(last) > width-1 {
			_, n := utf8.DecodeLastRuneInString(last)
			last = last[:len(last)-n]
		}
		lines[max-1] = strings.TrimRight(last, " ,;:") + "…"
	}
	if len(lines) == 0 {
		lines = []string{""}
	}
	fmt.Fprintln(w, lead+lines[0])
	for _, l := range lines[1:] {
		fmt.Fprintln(w, indent+l)
	}
}

// wrapWords splits text into lines of at most width characters, breaking
// between words; a word longer than a line (a path) gets a line of its own.
func wrapWords(text string, width int) []string {
	width = max(width, 20)
	var lines []string
	var cur strings.Builder
	n := 0
	for _, word := range strings.Fields(text) {
		wl := utf8.RuneCountInString(word)
		if n > 0 && n+1+wl > width {
			lines = append(lines, cur.String())
			cur.Reset()
			n = 0
		}
		if n > 0 {
			cur.WriteByte(' ')
			n++
		}
		cur.WriteString(word)
		n += wl
	}
	if n > 0 {
		lines = append(lines, cur.String())
	}
	return lines
}

// visibleLen counts the characters a terminal shows, leaving out colour
// codes.
func visibleLen(s string) int {
	n := 0
	for i := 0; i < len(s); {
		if s[i] == 0x1b {
			for i < len(s) && s[i] != 'm' {
				i++
			}
			i++
			continue
		}
		_, sz := utf8.DecodeRuneInString(s[i:])
		i += sz
		n++
	}
	return n
}
