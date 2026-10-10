package main

import (
	"slices"
	"strings"
	"testing"

	"github.com/spf13/pflag"
)

// Both dash styles ask for the help, which goes to stdout, lists every
// flag, and carries no colour when it is not written to a terminal.
func TestHelp(t *testing.T) {
	for _, arg := range []string{"--help", "-help", "-h"} {
		code, out, _ := runCLI(t, arg)
		if code != exitOK {
			t.Errorf("%s: exit %d", arg, code)
		}
		for _, want := range []string{"USAGE", "EXAMPLES", "FLAGS", "--app-id", "--eventlog", "--source-context", "Exit codes"} {
			if !strings.Contains(out, want) {
				t.Errorf("%s: help has no %q:\n%s", arg, want, out)
			}
		}
		if strings.Contains(out, "\x1b[") {
			t.Errorf("%s: help has ANSI codes off a terminal", arg)
		}
	}
}

func TestFlagMistakesAreFatal(t *testing.T) {
	for _, args := range [][]string{{"--no-such-flag"}, {"-no-such-flag"}, {"--app-id"}, {"-workers", "many"}} {
		if code, _, errs := runCLI(t, args...); code != exitFatal || errs == "" {
			t.Errorf("%q: exit %d, stderr %q", args, code, errs)
		}
	}
	if code, _, errs := runCLI(t, "-app-id", "application_1790380000000_0042", "stray"); code != exitFatal || !strings.Contains(errs, `"stray"`) {
		t.Errorf("stray value: exit %d, %s", code, errs)
	}
}

func TestLongFlagsTakeOneDash(t *testing.T) {
	fs := pflag.NewFlagSet("t", pflag.ContinueOnError)
	fs.String("app-id", "", "")
	fs.String("decode-scan", "", "")
	fs.String("eventlog", "", "")
	fs.Bool("check", false, "")
	for _, c := range []struct{ in, want []string }{
		{[]string{"-app-id", "a", "--eventlog=e", "-check"}, []string{"--app-id", "a", "--eventlog=e", "--check"}},
		{[]string{"-app-id=a", "-help"}, []string{"--app-id=a", "--help"}},
		// A value is never a flag, even when it looks like one.
		{[]string{"-eventlog", "-check", "--eventlog", "-app-id"}, []string{"--eventlog", "-check", "--eventlog", "-app-id"}},
		{[]string{"-decode-scan", "-"}, []string{"--decode-scan", "-"}},
		{[]string{"-h", "-x", "--", "-check"}, []string{"-h", "-x", "--", "-check"}},
	} {
		if got := longFlags(fs, c.in); !slices.Equal(got, c.want) {
			t.Errorf("longFlags(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestDoubleDashFlagsRun(t *testing.T) {
	code, out, _ := runCLI(t, "--version")
	if code != exitOK || !strings.HasPrefix(out, "sparkplain ") {
		t.Errorf("--version: exit %d, %q", code, out)
	}
}
