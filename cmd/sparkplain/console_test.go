package main

import (
	"io"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"unicode/utf8"
)

var ansiRE = regexp.MustCompile(`\x1b\[[0-9;]*[A-Za-z]`)

// Piped, the summary on stdout carries everything: the sources read, what
// happened, each finding and the files, in plain text.
func TestConsoleSummaryPiped(t *testing.T) {
	dir := t.TempDir()
	code, out, errs := runCLI(t, "-app-id", "application_1790380000000_0042", "-eventlog", filepath.Join(fx, "application_1790380000000_0042"), "-out", dir)
	if code != exitOK {
		t.Fatalf("exit %d: %s", code, errs)
	}
	for _, want := range []string{
		"◆ sparkplain " + version + " · application_1790380000000_0042\n",
		"\n▸ Access check\n  ? AWS                    EMR API, CloudWatch and CloudTrail: not asked for.",
		"  ? Cluster logs           Container, step, node and HBase logs: not asked for.",
		"  Y Spark event log\n    ../../testdata/eventlog/application_1790380000000_0042\n",
		"\n▸ Read\n  Y Spark event log        ",
		"\n    from ../../testdata/eventlog/application_1790380000000_0042\n", // the file itself, not just its size
		"\n▸ What happened\n  ",
		"\n▸ Findings  1 critical · ",
		"\n  !! ",
		"\n▸ Written  " + dir + "/\n  application_1790380000000_0042-report.html ·",
		"application_1790380000000_0042-explorer.html ·",
		"  Open it " + map[bool]string{true: "open", false: "xdg-open"}[runtime.GOOS == "darwin"] + " " + outPath(dir, "report.html") + "\n",
		"\nDone in ",
		"· complete (exit 0)\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "Not asked for") {
		t.Error("the access check already said what was not asked for")
	}
	if strings.Contains(out, "What needs attention") {
		t.Error("the findings list replaces the summary's attention sentence")
	}
	if ansiRE.MatchString(out + errs) {
		t.Error("piped output must not be coloured")
	}
	for _, line := range strings.Split(out, "\n") {
		if n := utf8.RuneCountInString(line); n > 80 && !strings.Contains(line, dir) {
			t.Errorf("line of %d characters: %q", n, line)
		}
	}
}

// A partial run says so at the end, and keeps its stderr line for scripts.
func TestConsoleSummaryPartial(t *testing.T) {
	code, out, errs := runCLI(t, "-app-id", "application_1790380000000_0045", "-eventlog", fx, "-out", t.TempDir())
	if code != exitPartial || !strings.Contains(out, "· partial (exit 3): 1 source missing or incomplete") || !strings.Contains(out, "  ! Spark event log") ||
		!strings.Contains(errs, "sparkplain: partial report (exit 3)") {
		t.Errorf("exit %d\nstdout: %s\nstderr: %s", code, out, errs)
	}
}

// On a terminal, sources are listed on stderr as they are read, in colour
// unless NO_COLOR is set, and the summary does not list them again.
func TestConsoleLive(t *testing.T) {
	saved := isTerminal
	t.Cleanup(func() { isTerminal = saved })
	isTerminal = func(io.Writer) bool { return true }
	t.Setenv("TERM", "xterm")
	for _, noColor := range []string{"", "1"} {
		t.Setenv("NO_COLOR", noColor)
		code, out, errs := runCLI(t, "-app-id", "application_1790380000000_0042", "-eventlog", filepath.Join(fx, "application_1790380000000_0042"), "-out", t.TempDir())
		if code != exitOK {
			t.Fatalf("exit %d: %s", code, errs)
		}
		plainErr, plainOut := ansiRE.ReplaceAllString(errs, ""), ansiRE.ReplaceAllString(out, "")
		if !strings.Contains(plainErr, "\n▸ Read\n") || !strings.Contains(plainErr, map[bool]string{true: "●", false: "Y"}[noColor == ""]+" Spark event log") || !strings.Contains(errs, "reading the event log…") {
			t.Errorf("NO_COLOR=%q: stderr lacks the live listing:\n%s", noColor, errs)
		}
		if strings.Contains(plainOut, "\n▸ Read\n") || !strings.Contains(plainOut, "▸ Access check") || !strings.Contains(plainOut, "What happened") || !strings.Contains(plainOut, "● Done in ") && noColor == "" || !strings.Contains(plainOut, "complete (exit 0)") {
			t.Errorf("NO_COLOR=%q: stdout should have the summary without the sources:\n%s", noColor, out)
		}
		// AWS is needed for a full report but was not given: a pink dot,
		// not the grey of something turned off on purpose.
		if noColor == "" && !strings.Contains(out, "\x1b[35m●\x1b[0m AWS ") {
			t.Errorf("the AWS row lacks its pink not-given dot:\n%q", out)
		}
		coloured := regexp.MustCompile(`\x1b\[3\dm`).MatchString(out + errs)
		if coloured != (noColor == "") {
			t.Errorf("NO_COLOR=%q: coloured = %v", noColor, coloured)
		}
		if strings.Contains(errs, "sparkplain: ") {
			t.Errorf("NO_COLOR=%q: notes on a terminal need no prefix:\n%s", noColor, errs)
		}
	}
}

func TestWrapWords(t *testing.T) {
	got := wrapWords("It read 4.1 GiB, wrote 2.5 GiB and moved 1.5 GiB between executors in shuffles.", 30)
	for _, l := range got {
		if utf8.RuneCountInString(l) > 30 {
			t.Errorf("line over 30: %q", l)
		}
	}
	if strings.Join(got, " ") != "It read 4.1 GiB, wrote 2.5 GiB and moved 1.5 GiB between executors in shuffles." {
		t.Errorf("words lost: %q", got)
	}
	if n := visibleLen("\x1b[32m●\x1b[0m ab"); n != 4 {
		t.Errorf("visibleLen = %d, want 4", n)
	}
	if shellQuote("/tmp/a b/it's.html") != `'/tmp/a b/it'\''s.html'` || shellQuote("/tmp/r.html") != "/tmp/r.html" {
		t.Error("shellQuote")
	}
}

// Animated, the console spins, settles and paces its lines, and says the
// same things: with the frames drawn over (each ends at a carriage return)
// removed, the text is what the still console prints. Tests run the
// animation a thousand times faster; under -race this also checks the
// spinner's goroutine against the lines printed after it.
func TestConsoleAnimated(t *testing.T) {
	saved, savedPace := isTerminal, animPace
	t.Cleanup(func() { isTerminal, animPace = saved, savedPace })
	isTerminal = func(io.Writer) bool { return true }
	t.Setenv("TERM", "xterm")
	t.Setenv("NO_COLOR", "")
	t.Setenv("CI", "")
	t.Setenv("SPARKPLAIN_NO_ANIMATION", "")
	run := func(pace float64) (string, string) {
		animPace = pace
		code, out, errs := runCLI(t, "-app-id", "application_1790380000000_0042", "-eventlog", filepath.Join(fx, "application_1790380000000_0042"), "-out", t.TempDir())
		if code != exitOK {
			t.Fatalf("exit %d: %s", code, errs)
		}
		return out, errs
	}
	out, errs := run(0.001)
	if !regexp.MustCompile(`[⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏] \x1b\[2mreading the event log…  \d`).MatchString(ansiRE.ReplaceAllString(errs, "")+errs) && !strings.Contains(errs, "reading the event log…  ") {
		t.Errorf("no spinner with a clock on the status line:\n%q", errs)
	}
	if !strings.Contains(out, "◇") || !strings.Contains(ansiRE.ReplaceAllString(out, ""), "─────") || !strings.Contains(out, "∙") {
		t.Errorf("stdout lacks the header, the finding dots growing or the closing rule:\n%q", out)
	}
	// What is left once the frames are drawn over is the still console's
	// text (less its timings).
	landed := func(s string) string {
		s = ansiRE.ReplaceAllString(s, "")
		var keep []string
		for _, line := range strings.Split(s, "\n") {
			if i := strings.LastIndex(line, "\r"); i >= 0 {
				line = line[i+1:]
			}
			keep = append(keep, line)
		}
		text := regexp.MustCompile(`/\S*/TestConsoleAnimated\d+/\d+`).ReplaceAllString(strings.Join(keep, "\n"), "OUT")
		return regexp.MustCompile(`\b\d+(\.\d+)? (ms|s)\b|Done in [^·]*`).ReplaceAllString(text, "")
	}
	stillOut, _ := run(0)
	animated := strings.ReplaceAll(landed(out), strings.Repeat("─", 64)+"\n", "")
	if animated != landed(stillOut) {
		t.Errorf("animated stdout says something different:\n%s\n---- still ----\n%s", animated, landed(stillOut))
	}
}

// Tests run with the console still; TestConsoleAnimated turns it on.
func init() { animPace = 0 }

// Numbers and their units are coloured; digits in names and paths are not.
func TestNumbersColoured(t *testing.T) {
	c := func(s string) string { return "\x1b[" + numberColour + "m" + s + "\x1b[39m" }
	for in, want := range map[string]string{
		"527 events, 1.7 MiB":                            c("527") + " events, " + c("1.7 MiB"),
		"ran for 39 s as hadoop":                         "ran for " + c("39 s") + " as hadoop",
		"one task ran 17× longer (86%).":                 "one task ran " + c("17×") + " longer (" + c("86%") + ").",
		"1 critical · 4 warnings":                        c("1") + " critical · " + c("4") + " warnings",
		"2 stages spilled 337 MiB to disk":               c("2") + " stages spilled " + c("337 MiB") + " to disk",
		"application_1790380000000_0042 on ip-10-0-2-13": "application_1790380000000_0042 on ip-10-0-2-13",
		"/tmp/out/001 and 3rd":                           "/tmp/out/001 and 3rd",
		"2 min 15 s":                                     c("2 min") + " " + c("15 s"),
	} {
		if got := numbers(true, in); got != want {
			t.Errorf("numbers(%q) = %q, want %q", in, got, want)
		}
	}
	if numbers(false, "527 events") != "527 events" {
		t.Error("coloured without colour")
	}
}
