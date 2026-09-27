package main

import (
	"io"
	"path/filepath"
	"regexp"
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
		"sparkplain " + version + " · application_1790380000000_0042\n",
		"\nRead\n  ✓ Spark event log   ",
		"  · Not asked for     Container logs, Step logs, Node logs, EMR API, CloudWatch,",
		"\nWhat happened\n  ",
		"\nFindings: 1 critical, ",
		"  ✖ ",
		"\nReport    " + outPath(dir, "report.html") + "\n",
		"\nExplorer  application_1790380000000_0042-explorer.html\nJSON      application_1790380000000_0042-report.json\n",
		"Open it: xdg-open ",
		"· complete (exit 0)\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout lacks %q:\n%s", want, out)
		}
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
		if !strings.Contains(plainErr, "\nRead\n") || !strings.Contains(plainErr, "✓ Spark event log") || !strings.Contains(errs, "reading the event log…") ||
			!strings.Contains(plainErr, "Not asked for") {
			t.Errorf("NO_COLOR=%q: stderr lacks the live listing:\n%s", noColor, errs)
		}
		if strings.Contains(plainOut, "\nRead\n") || !strings.Contains(plainOut, "What happened") || !strings.Contains(plainOut, "complete (exit 0)") {
			t.Errorf("NO_COLOR=%q: stdout should have the summary without the sources:\n%s", noColor, out)
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
	if n := visibleLen("\x1b[32m✓\x1b[0m ab"); n != 4 {
		t.Errorf("visibleLen = %d, want 4", n)
	}
	if shellQuote("/tmp/a b/it's.html") != `'/tmp/a b/it'\''s.html'` || shellQuote("/tmp/r.html") != "/tmp/r.html" {
		t.Error("shellQuote")
	}
}
