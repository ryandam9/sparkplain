package main

import (
	"cmp"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/source"
)

// console prints what sparkplain read and, at the end, what it found: a
// summary on stdout of the report's "What happened", how many findings (the
// report lists them) and the files written. On a terminal each source is listed on stderr as it is
// read, with a note of what is being read in the meantime, and markers
// are coloured (not with NO_COLOR or TERM=dumb). Piped, the summary lists
// the sources itself and notes keep their "sparkplain:" prefix on stderr.
//
// On a colour terminal the console is also animated: the note of what is
// being read spins and counts the seconds, each mark settles into its dot,
// sections arrive a beat apart and lines one after another. Piped output,
// NO_COLOR, a CI environment or SPARKPLAIN_NO_ANIMATION=1 turn it off, and
// the text is the same either way.
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
	headed       bool            // the access check printed the cluster and the sources checked
	working      bool            // a transient "reading …" line is on screen
	home, opener string

	animate  bool          // spin, settle and pace (see the type)
	spinStop chan struct{} // stops the status line's spinner
	spinDone chan struct{} // closed once the spinner has stopped writing

	verbose bool                            // -verbose: trace lines, no spinner (setVerbose)
	prog    atomic.Pointer[source.Progress] // how far the current step is, when it counts
	steps   []stepTime                      // how long each step took, for the closing line
	stepAt  time.Time                       // when the current step started
}

// stepTime is one step of the run and how long it took.
type stepTime struct {
	label string
	took  time.Duration
}

// heartbeat is how often a long step says it is still working when the
// console is not a terminal; tests shorten it.
var heartbeat = 30 * time.Second

// animPace scales every pause and frame of the animation; tests set it to
// 0, which turns the animation off.
var animPace = 1.0

// The animation's timing: a beat before each section, the frames a mark
// or a finding's dot spins through before it settles, and how often the
// status line redraws.
const (
	sectionGap = time.Second
	frameGap   = 45 * time.Millisecond
	spinTick   = 90 * time.Millisecond
)

var (
	spinFrames   = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
	settleFrames = []string{"⠋", "⠹", "⠼"}
	headerFrames = []string{"◇", "◈"}
)

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
	c.animate = c.live && c.colOut && c.colErr && animPace > 0 && os.Getenv("CI") == "" && os.Getenv("SPARKPLAIN_NO_ANIMATION") == ""
	return c
}

// pause waits d when animating.
func (c *console) pause(d time.Duration) {
	if c.animate {
		time.Sleep(time.Duration(float64(d) * animPace))
	}
}

// settle draws frames in place after prefix, one after another, then
// clears the line so the real one can be printed over it.
func (c *console) settle(w io.Writer, prefix string, frames []string) {
	if !c.animate {
		return
	}
	for _, f := range frames {
		fmt.Fprint(w, "\r\x1b[2K"+prefix+f)
		c.pause(frameGap)
	}
	fmt.Fprint(w, "\r\x1b[2K")
}

// section prints a section's heading, a beat after what came before.
func (c *console) section(w io.Writer, col bool, title, rest string) {
	c.pause(sectionGap)
	fmt.Fprintln(w, heading(col, title)+rest)
}

const (
	bold  = "1"
	dim   = "2"
	red   = "31"
	green = "32"
	amber = "33"
	blue  = "34"
	pink  = "35"
)

// numberColour is the colour numbers and their units are shown in. It
// resets only the foreground, so a number in a dim or bold line stays so.
const numberColour = "36"

// numbers colours each number in s, with its unit ("1.7 MiB", "39 s",
// "17×", "86%"), when on. Digits that are part of a name or a path, such
// as application_1790380000000_0042, ip-10-0-2-13 or /out/001, stay as
// they are: a number starts the text or follows a space, a bracket or a
// "·", and ends before a space or punctuation.
func numbers(on bool, s string) string {
	if !on {
		return s
	}
	digit := func(i int) bool { return i < len(s) && s[i] >= '0' && s[i] <= '9' }
	before := func(i int) bool {
		return i == 0 || strings.ContainsRune(" ([", rune(s[i-1])) || strings.HasSuffix(s[:i], "·")
	}
	after := func(i int) bool { return i >= len(s) || strings.ContainsRune(" ,.;:)]", rune(s[i])) }
	var b strings.Builder
	for i := 0; i < len(s); {
		if !digit(i) || !before(i) {
			b.WriteByte(s[i])
			i++
			continue
		}
		j := i
		for digit(j) || j+1 < len(s) && (s[j] == '.' || s[j] == ',') && digit(j+1) {
			j++
		}
		end := -1
		for _, u := range []string{"%", "×", " TiB", " GiB", " MiB", " KiB", " B", " ms", " min", " s", " h"} {
			if strings.HasPrefix(s[j:], u) && after(j+len(u)) {
				end = j + len(u)
				break
			}
		}
		if end < 0 && after(j) {
			end = j
		}
		if end < 0 { // part of a word, such as 3rd or 10-0
			b.WriteString(s[i:j])
			i = j
			continue
		}
		b.WriteString("\x1b[" + numberColour + "m" + s[i:end] + "\x1b[39m")
		i = end
	}
	return b.String()
}

func paint(on bool, code, s string) string {
	if !on || s == "" {
		return s
	}
	return "\x1b[" + code + "m" + s + "\x1b[0m"
}

// clear removes the transient "reading …" line before anything else is
// printed on the terminal, stopping its spinner first so nothing else
// writes to the terminal meanwhile.
func (c *console) clear() {
	if c.spinStop != nil {
		close(c.spinStop)
		<-c.spinDone
		c.spinStop, c.spinDone = nil, nil
	}
	if !c.working {
		return
	}
	fmt.Fprint(c.err, "\r\x1b[2K")
	c.working = false
}

// begin names the run, on stdout, before anything else; animated, its
// diamond fills in first.
func (c *console) begin(appID string) {
	for _, f := range headerFrames {
		if !c.animate {
			break
		}
		fmt.Fprint(c.out, "\r"+paint(true, blue, f)+" "+paint(true, bold, "sparkplain"))
		c.pause(3 * frameGap)
	}
	if c.animate {
		fmt.Fprint(c.out, "\r\x1b[2K")
	}
	fmt.Fprintf(c.out, "%s %s\n", paint(c.colOut, bold, "◆ sparkplain "+version), paint(c.colOut, dim, "· "+appID))
}

// clusterFound names the cluster being read, unless the access check's
// header already did.
func (c *console) clusterFound(cl model.Cluster) {
	c.cluster = strings.Join(nonEmpty(cl.ID, cl.Name, cl.Release, strings.ToLower(cl.State)), " · ")
	if c.headed {
		return
	}
	if c.live {
		c.clear()
		fmt.Fprintln(c.err, "  Cluster  "+c.cluster)
	} else {
		fmt.Fprintf(c.err, "sparkplain: cluster %s\n", c.cluster)
	}
}

// Marks, the same everywhere: a dot, green when read, red when refused or
// failed, half-filled amber when partial or empty, pink when the report
// needs it but this run was not given it (no AWS profile, no cluster logs,
// an event log on HDFS), and hollow when not asked for or not needed;
// without colour (piped, NO_COLOR) Y, N, !, ? and -, so logs and scripts
// stay plain.
const (
	markOK = iota
	markBad
	markPartial
	markOff
	markNotGiven
)

func (c *console) mark(col bool, m int) string {
	sym := [...]string{"●", "●", "◐", "○", "●"}[m]
	if !col {
		return [...]string{"Y", "N", "!", "-", "?"}[m]
	}
	return paint(true, [...]string{green, red, amber, dim, pink}[m], sym)
}

// heading is a section's title line.
func heading(col bool, title string) string { return "\n" + paint(col, bold, "▸ "+title) }

// nameCol is the width of a source's name, in both lists.
const nameCol = 22

// note says something about how the run is going, such as where the
// event log was looked for.
func (c *console) note(format string, a ...any) {
	msg := fmt.Sprintf(format, a...)
	if !c.live {
		fmt.Fprintln(c.err, "sparkplain: "+msg)
		return
	}
	c.clear()
	c.wrap(c.err, "  ", "  ", msg, func(s string) string { return paint(c.colErr, dim, numbers(c.colErr, s)) })
}

// step starts a step of the run: it ends the one before, for the closing
// "Took" line, and shows what sparkplain is doing.
func (c *console) step(label, doing string) {
	c.endStep()
	c.steps = append(c.steps, stepTime{label: label})
	c.stepAt = time.Now()
	c.status(doing)
}

// endStep records how long the current step took.
func (c *console) endStep() {
	if n := len(c.steps); n > 0 && c.steps[n-1].took == 0 {
		c.steps[n-1].took = max(time.Since(c.stepAt), time.Nanosecond)
	}
}

// setVerbose turns on trace lines (-verbose). The status line then
// prints once and says every heartbeat that it is still working, as when
// piped, so the trace lines that readers print from their own goroutines
// never meet a line being redrawn; one lock keeps lines whole.
func (c *console) setVerbose() {
	c.verbose, c.animate = true, false
	c.err = &lockedWriter{w: c.err}
}

// trace prints one line of what the run is doing (-verbose), after the
// time since it started. Readers call it from their own goroutines.
func (c *console) trace(msg string) {
	at := time.Since(c.started).Round(time.Second)
	line := fmt.Sprintf("[%d:%02d] %s", int(at.Minutes()), int(at.Seconds())%60, msg)
	if c.live {
		fmt.Fprintln(c.err, paint(c.colErr, dim, "  · "+line))
		return
	}
	fmt.Fprintln(c.err, "sparkplain: "+line)
}

// lockedWriter lets several goroutines write whole lines to one writer.
type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// track shows p's counts beside the current step until the next one.
func (c *console) track(p *source.Progress) { c.prog.Store(p) }

// status shows what sparkplain is doing until the next line replaces it;
// animated, a spinner turns beside it and the seconds count up, with how
// far the step is when it counts (track). When the console is not a
// terminal, a step that takes long says every heartbeat that it is still
// working, so a log shows the run is alive.
func (c *console) status(doing string) {
	c.clear()
	c.prog.Store(nil)
	stop, done := make(chan struct{}), make(chan struct{})
	start := time.Now()
	if c.verbose {
		c.trace(doing)
	}
	switch {
	case !c.live || c.verbose:
		c.spinStop, c.spinDone = stop, done
		go func() {
			defer close(done)
			tick := time.NewTicker(heartbeat)
			defer tick.Stop()
			for {
				select {
				case <-stop:
					return
				case <-tick.C:
					fmt.Fprintf(c.err, "sparkplain: still %s (%s%s)\n", doing, elapsed(time.Since(start)), progressText(c.prog.Load(), time.Since(start), ", "))
				}
			}
		}()
		return
	case !c.animate:
		// A terminal without the animation: the line redraws each second.
		c.working = true
		c.spinStop, c.spinDone = stop, done
		go func() {
			defer close(done)
			tick := time.NewTicker(time.Second)
			defer tick.Stop()
			for {
				fmt.Fprint(c.err, "\r\x1b[2K"+paint(c.colErr, dim, "  "+doing+"…"+progressText(c.prog.Load(), time.Since(start), "  ")))
				select {
				case <-stop:
					return
				case <-tick.C:
				}
			}
		}()
		return
	}
	c.working = true
	c.spinStop, c.spinDone = stop, done
	go func() {
		defer close(done)
		tick := time.NewTicker(time.Duration(float64(spinTick) * animPace))
		defer tick.Stop()
		for i := 0; ; i++ {
			fmt.Fprint(c.err, "\r\x1b[2K  "+paint(true, blue, spinFrames[i%len(spinFrames)])+" "+paint(true, dim, doing+"…"+progressText(c.prog.Load(), time.Since(start), "  ")+"  ")+numbers(true, elapsed(time.Since(start))))
			select {
			case <-stop:
				return
			case <-tick.C:
			}
		}
	}()
}

// progressText says how far a step is: the share of its bytes read with
// the time left, or the files read, after lead; "" when it does not count.
func progressText(p *source.Progress, took time.Duration, lead string) string {
	if p == nil {
		return ""
	}
	files, filesTotal := p.Files.Load(), p.FilesTotal.Load()
	done, total := p.Bytes.Load(), p.BytesTotal.Load()
	var parts []string
	if total > 0 {
		share := min(float64(done)/float64(total), 1)
		parts = append(parts, fmt.Sprintf("%.0f%% · %s of %s", 100*share, model.Bytes(done), model.Bytes(total)))
		if share > 0.02 && share < 1 && took > 5*time.Second {
			left := time.Duration(float64(took) * (1 - share) / share)
			parts = append(parts, "about "+elapsed(left)+" left")
		}
	}
	if filesTotal > 1 {
		parts = append(parts, fmt.Sprintf("%s of %s", model.Num(files), model.Plural(int(filesTotal), "file", "files")))
	}
	if len(parts) == 0 {
		return ""
	}
	return lead + strings.Join(parts, " · ")
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
		c.shown[s.Name] = true
		if !c.listed(s) {
			continue
		}
		if !c.readHeader {
			fmt.Fprintln(c.err, heading(c.colErr, "Read"))
			c.readHeader = true
		}
		if s.Status == "not-requested" {
			c.notAsked = append(c.notAsked, s.Name)
			continue
		}
		c.sourceLine(c.err, c.colErr, s)
	}
}

// listed reports whether a source gets a line of its own under Read: the
// EMR and EC2 APIs read as the access check found they could be say
// nothing new.
func (c *console) listed(s model.SourceStatus) bool {
	return !(c.headed && s.Status == "read" && (s.Name == "EMR API" || s.Name == "EC2 API"))
}

// notAskedLine lists together the sources the run was not asked to read,
// which each say the same thing: pass -cluster-id or -from.
func (c *console) notAskedLine(w io.Writer, col bool) {
	if len(c.notAsked) == 0 || c.headed {
		c.notAsked = nil // the access check said what was not asked for
		return
	}
	c.sourceLine(w, col, model.SourceStatus{Name: "Not asked for", Status: "not-requested",
		Detail: strings.Join(c.notAsked, ", ") + ". The report's Sources panel says how to add each."})
	c.notAsked = nil
}

// sourceLine is one source: a mark, its name, and what was read in a few
// words, or all of why it was not.
func (c *console) sourceLine(w io.Writer, col bool, s model.SourceStatus) {
	m := markOff
	switch s.Status {
	case "read":
		m = markOK
	case "partial":
		m = markPartial
	case "not-supplied":
		m = markNotGiven
	case "error":
		m = markBad
	}
	detail := s.Detail
	if s.Status == "read" {
		detail = firstNonEmpty(s.Brief, parenRE.ReplaceAllString(firstSentence(detail), ""))
	}
	lead := "  " + c.mark(col, m) + " " + fmt.Sprintf("%-*s", nameCol, s.Name) + " "
	indent := strings.Repeat(" ", nameCol+5)
	c.settle(w, "  ", settleFrames)
	c.wrapLimited(w, col, lead, indent, detail, s.Status == "read")
	// The event log may have been found in a folder, or inside a zip:
	// name the file itself, whole, on a line of its own.
	if loc := c.tilde(s.Location); s.Name == eventLogSource && loc != "" && !strings.Contains(detail, s.Location) {
		lines := []string{"from " + loc}
		if zip, inside, ok := strings.Cut(loc, " › "); ok && len(indent)+utf8.RuneCountInString(lines[0]) > c.width {
			lines = []string{"from " + zip, "  › " + inside}
		}
		for _, l := range lines {
			pad := indent
			if len(pad)+utf8.RuneCountInString(l) > c.width { // a long path: less indent, as in the access check
				pad = "    "
			}
			fmt.Fprintln(w, pad+paint(col, dim, l))
		}
	}
}

// eventLogSource is the event log's name in the Sources list.
const eventLogSource = "Spark event log"

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
		fmt.Fprintln(w, heading(col, "Read"))
		for _, s := range r.Sources {
			if !c.listed(s) {
				continue
			}
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
		c.section(w, col, "What happened", "")
		for _, s := range said {
			c.pause(4 * frameGap)
			c.wrap(w, "  ", "  ", s, func(l string) string { return numbers(col, l) })
		}
	}

	counts := map[model.Severity]int{}
	for _, f := range r.Findings {
		counts[f.Severity]++
	}
	var parts []string
	for _, sv := range []struct {
		s           model.Severity
		one, others string
	}{{model.Critical, "critical", "critical"}, {model.Warning, "warning", "warnings"}, {model.Info, "note", "notes"}} {
		if n := counts[sv.s]; n > 0 {
			parts = append(parts, model.Plural(n, sv.one, sv.others))
		}
	}
	if len(parts) == 0 {
		parts = []string{"none"}
	}
	// Only the count: the report and the explorer list each finding with
	// its evidence and fix.
	c.section(w, col, "Findings", "  "+numbers(col, strings.Join(parts, " · "))+paint(col, dim, " · listed in the report"))

	// The folder once, then the files in it.
	if len(order) > 0 {
		dir := filepath.Dir(written[order[0]])
		var names []string
		for _, k := range order {
			if filepath.Dir(written[k]) == dir {
				names = append(names, filepath.Base(written[k]))
			} else {
				names = append(names, c.tilde(written[k]))
			}
		}
		c.section(w, col, "Written", "  "+c.tilde(dir)+string(os.PathSeparator))
		c.wrap(w, "  ", "  ", strings.Join(names, " · "), func(s string) string { return paint(col, dim, s) })
	}
	if p := written["Report"]; p != "" {
		fmt.Fprintf(w, "  %s %s %s\n", paint(col, dim, "Open it"), c.opener, shellQuote(p))
	}
	c.endStep()
	if took := c.tookLine(col); took != "" {
		c.wrap(w, "  "+paint(col, dim, "Took")+"     ", "           ", took, nil)
	}
	done := fmt.Sprintf("Done in %s · ", numbers(col, elapsed(time.Since(c.started))))
	m := -1
	switch exit {
	case exitOK:
		done += paint(col, green, "complete") + " (exit 0)"
		m = markOK
	case exitPartial:
		missing := 0
		for _, s := range r.Sources {
			if s.Status == "partial" || s.Status == "error" || s.Status == "not-supplied" {
				missing++
			}
		}
		done += paint(col, amber, "partial") + fmt.Sprintf(" (exit 3): %s missing or incomplete, marked above and in the report's Sources panel", model.Plural(missing, "source", "sources"))
		m = markPartial
	default:
		done += fmt.Sprintf("exit %d", exit)
	}
	if col && m >= 0 {
		done = c.mark(true, m) + " " + done
	}
	fmt.Fprintln(w)
	if c.animate {
		// A rule sweeps across before the last line.
		c.pause(sectionGap / 2)
		n := min(c.width, 64)
		for i := 4; i <= n; i += 4 {
			fmt.Fprint(w, "\r"+paint(true, dim, strings.Repeat("─", i)))
			c.pause(frameGap / 3)
		}
		fmt.Fprintln(w)
	}
	fmt.Fprintln(w, done)
	if exit == exitPartial && !c.live {
		fmt.Fprintln(c.err, "sparkplain: partial report (exit 3): see the Sources panel for what is missing")
	}
}

// tookLine says where the run's time went: each step that took a second
// or more, slowest first, so a long run shows what to look at.
func (c *console) tookLine(col bool) string {
	var steps []stepTime // one per label, adding up its steps
	for _, s := range c.steps {
		if i := slices.IndexFunc(steps, func(t stepTime) bool { return t.label == s.label }); i >= 0 {
			steps[i].took += s.took
		} else {
			steps = append(steps, s)
		}
	}
	slices.SortStableFunc(steps, func(a, b stepTime) int { return cmp.Compare(b.took, a.took) })
	var parts []string
	for _, s := range steps {
		if s.took >= time.Second {
			parts = append(parts, s.label+" "+numbers(col, elapsed(s.took)))
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, " · ")
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
func (c *console) wrapLimited(w io.Writer, col bool, lead, indent, text string, oneLine bool) {
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
	fmt.Fprintln(w, lead+numbers(col, lines[0]))
	for _, l := range lines[1:] {
		fmt.Fprintln(w, indent+numbers(col, l))
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

// accessCheck prints what the run found it could read, before reading it
// (SPEC §2): a header naming the cluster, the credentials and the log
// folder once, then one line per source with a location relative to that
// folder. A source it cannot read says why and gives an aws command that
// repeats the call, on one line so it can be copied.
func (c *console) accessCheck(chk checked, profile, region string, online bool) {
	c.clear()
	c.headed = true
	w, col := c.out, c.colOut
	fmt.Fprintln(w)
	field := func(label, value string) {
		if value != "" {
			fmt.Fprintf(w, "  %-8s %s\n", label, value)
		}
	}
	if online {
		field("Cluster", chk.cluster)
		if chk.picked != "" {
			c.wrap(w, "           ", "           ", chk.picked, func(s string) string { return paint(col, dim, numbers(col, s)) })
		}
		field("As", strings.Join(nonEmpty(chk.who, "profile "+profile, region), " · "))
	} else {
		field("As", "offline: local files only, no AWS calls")
	}
	field("Logs", c.tilde(chk.logRoot))
	c.section(w, col, "Access check", "")
	for _, r := range chk.rows {
		c.settle(w, "  ", settleFrames)
		m := markOff
		switch r.Status {
		case "ok":
			m = markOK
		case "empty":
			m = markPartial
		case "denied", "error":
			m = markBad
		}
		if r.NotGiven {
			m = markNotGiven
		}
		lead := "  " + c.mark(col, m) + " " + fmt.Sprintf("%-*s", nameCol, r.Name) + " "
		indent := strings.Repeat(" ", nameCol+5)
		loc := r.Location
		if chk.logRoot != "" && strings.HasPrefix(loc, chk.logRoot) && loc != chk.logRoot {
			loc = strings.TrimPrefix(loc, chk.logRoot)
		}
		loc = c.tilde(loc)
		detail := strings.TrimSpace(r.Detail)
		if r.Status == "ok" {
			// What it proved, briefly: "list, read".
			var keep []string
			for _, part := range strings.Split(strings.TrimSuffix(detail, "."), ". ") {
				switch part {
				case "", "Readable", "Found", "Writable":
				case "List, read", "Read":
					keep = append(keep, strings.ToLower(part))
				default:
					keep = append(keep, part)
				}
			}
			detail = strings.Join(keep, " · ")
		}
		if loc != "" && strings.Contains(detail, loc) {
			loc = ""
		}
		if r.Status == "ok" && loc == "" && detail == "" {
			detail = "readable"
		}
		first, rest := loc, detail
		if first == "" {
			first, rest = detail, ""
		}
		switch {
		case first == "":
			fmt.Fprintln(w, strings.TrimRight(lead, " "))
		case !strings.Contains(first, " ") && visibleLen(lead)+len(first) > c.width:
			// A path too long to share the line: on its own line.
			fmt.Fprintln(w, strings.TrimRight(lead, " "))
			fmt.Fprintln(w, "    "+first)
		case r.Status == "ok" && rest != "" && visibleLen(lead)+len(first)+3+len(rest) <= c.width:
			fmt.Fprintln(w, lead+first+"  "+paint(col, dim, numbers(col, rest)))
			rest = ""
		default:
			c.wrap(w, lead, indent, first, nil)
		}
		if rest != "" {
			style := func(s string) string { return numbers(col, s) }
			if r.Status == "ok" {
				style = func(s string) string { return paint(col, dim, numbers(col, s)) }
			}
			c.wrap(w, indent, indent, rest, style)
		}
		if r.Try != "" && r.Status != "ok" && r.Status != "skipped" {
			fmt.Fprintln(w, indent+paint(col, dim, "try: "+r.Try))
		}
	}
}

func nonEmpty(v ...string) []string {
	var out []string
	for _, s := range v {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}
