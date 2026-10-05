package main

import (
	"path/filepath"
	"strings"
	"testing"
)

// -verbose names each log folder listed and each file read, with its size
// and how long it took; without it, none of that is printed.
func TestVerboseTracesLogReads(t *testing.T) {
	from := filepath.Join(emrlogs, "j-FIXTURE0049CLUSTER")
	args := []string{"-app-id", "application_1790380000000_0049", "-from", from, "-format", "json"}
	code, _, errs := runCLI(t, append(args, "-out", t.TempDir(), "-verbose")...)
	if code != exitOK && code != exitPartial {
		t.Fatalf("exit %d: %s", code, errs)
	}
	for _, want := range []string{
		"sparkplain: [0:00] reading the logs in ",
		"listed " + filepath.Join(from, "containers", "application_1790380000000_0049") + ": 4 objects",
		"/container_1790380000000_0049_01_000001/stderr.gz (18.8 KiB)",
		"done ",
		"listing " + filepath.Join(from, "node"),
	} {
		if !strings.Contains(errs, want) {
			t.Errorf("-verbose output lacks %q:\n%s", want, errs)
		}
	}
	_, _, quiet := runCLI(t, append(args, "-out", t.TempDir())...)
	if strings.Contains(quiet, "listing ") || strings.Contains(quiet, "done ") {
		t.Errorf("trace lines without -verbose:\n%s", quiet)
	}
}
