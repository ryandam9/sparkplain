package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const fx = "../../testdata/eventlog"

func runCLI(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	// Never pick up a real config file from the machine running the tests.
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("HOME", t.TempDir())
	code := run(args, &out, &errb)
	return code, out.String(), errb.String()
}

func TestRunWritesBothReports(t *testing.T) {
	dir := t.TempDir()
	code, out, errs := runCLI(t, "-app-id", "application_1790380000000_0042", "-eventlog", filepath.Join(fx, "application_1790380000000_0042.zip"), "-out", dir)
	if code != exitOK {
		t.Fatalf("exit %d: %s %s", code, out, errs)
	}
	html, err := os.ReadFile(filepath.Join(dir, "report.html"))
	if err != nil {
		t.Fatal(err)
	}
	js, err := os.ReadFile(filepath.Join(dir, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var r struct {
		SchemaVersion string
		ExitCode      int
		Application   struct{ ID string }
	}
	if err := json.Unmarshal(js, &r); err != nil || r.SchemaVersion == "" || r.Application.ID != "application_1790380000000_0042" {
		t.Fatalf("json %+v %v", r, err)
	}
	secret := regexp.MustCompile(`FAKE-[A-Z0-9-]+|AKIAIOSFODNN7EXAMPLE`)
	for name, b := range map[string][]byte{"html": html, "json": js, "stdout": []byte(out), "stderr": []byte(errs)} {
		if m := secret.Find(b); m != nil {
			t.Errorf("%s leaks %s", name, m)
		}
	}
}

func TestRunExitCodes(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name string
		args []string
		code int
		msg  string
	}{
		{"missing app id", []string{"-eventlog", fx}, exitFatal, "-app-id is required"},
		{"bad app id", []string{"-app-id", "../../etc", "-eventlog", fx}, exitFatal, "does not look like"},
		{"no event log", []string{"-app-id", "application_1_2"}, exitFatal, "needs an event log"},
		{"s3 not yet", []string{"-app-id", "application_1_2", "-eventlog", "s3://bucket/sparklogs/"}, exitFatal, "phase 2"},
		{"online flag", []string{"-app-id", "application_1_2", "-cluster-id", "j-1", "-eventlog", fx}, exitFatal, "online mode"},
		{"not found", []string{"-app-id", "application_1_2", "-eventlog", fx, "-out", dir}, exitFatal, "no event log for"},
		{"wrong app", []string{"-app-id", "application_1790380000000_0044", "-eventlog", filepath.Join(fx, "application_1790380000000_0042"), "-out", dir}, exitFatal, "belongs to application_1790380000000_0042"},
		{"bad format", []string{"-app-id", "application_1_2", "-eventlog", fx, "-format", "pdf"}, exitFatal, "-format"},
		{"in progress is partial", []string{"-app-id", "application_1790380000000_0045", "-eventlog", fx, "-out", dir}, exitPartial, "partial report"},
		{"version", []string{"-version"}, exitOK, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			code, out, errs := runCLI(t, c.args...)
			if code != c.code || !strings.Contains(errs+out, c.msg) {
				t.Errorf("exit %d (want %d), output %q", code, c.code, errs+out)
			}
		})
	}
}

func TestCorruptEventLogDegrades(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "application_1_2.lz4")
	os.WriteFile(bad, []byte("this is not lz4 at all"), 0o644)
	out := filepath.Join(dir, "out")
	code, _, errs := runCLI(t, "-app-id", "application_1_2", "-eventlog", bad, "-out", out)
	if code != exitPartial {
		t.Fatalf("exit %d: %s", code, errs)
	}
	html, err := os.ReadFile(filepath.Join(out, "report.html"))
	if err != nil || !bytes.Contains(html, []byte("Could not read")) {
		t.Fatalf("degraded report missing: %v", err)
	}
}

func TestConfigFileThresholdsAndFormat(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	os.WriteFile(cfg, []byte("format: json\ntimezone: Australia/Sydney\nthresholds:\n  skew-ratio: 100\n  skew-min-task: 1s\n"), 0o644)
	out := filepath.Join(dir, "out")
	code, _, errs := runCLI(t, "-config", cfg, "-app-id", "application_1790380000000_0042", "-eventlog", filepath.Join(fx, "application_1790380000000_0042"), "-out", out)
	if code != exitOK {
		t.Fatalf("exit %d: %s", code, errs)
	}
	if _, err := os.Stat(filepath.Join(out, "report.html")); err == nil {
		t.Error("format json should not write html")
	}
	js, _ := os.ReadFile(filepath.Join(out, "report.json"))
	if bytes.Contains(js, []byte(`"rule": "stage-skew"`)) {
		t.Error("skew-ratio 100 from the config file should silence the skew rule")
	}
	if !bytes.Contains(js, []byte(`"timeZone": "Australia/Sydney"`)) {
		t.Error("timezone from config not applied")
	}

	os.WriteFile(cfg, []byte("unknown-key: 1\n"), 0o644)
	if code, _, errs := runCLI(t, "-config", cfg, "-app-id", "application_1_2", "-eventlog", fx); code != exitFatal || !strings.Contains(errs, "unknown-key") {
		t.Errorf("unknown config keys should be rejected: %d %s", code, errs)
	}
	if code, _, _ := runCLI(t, "-config", filepath.Join(dir, "missing.yaml"), "-app-id", "application_1_2", "-eventlog", fx); code != exitFatal {
		t.Error("an explicit missing config file is an error")
	}
}

func TestParseSize(t *testing.T) {
	for in, want := range map[string]int64{"10GiB": 10 << 30, "500mb": 500 << 20, "2g": 2 << 30, "1024": 1024, "x": -1, "5 GB": 5 << 30} {
		if got := parseSize(in); got != want {
			t.Errorf("parseSize(%q) = %d, want %d", in, got, want)
		}
	}
}
