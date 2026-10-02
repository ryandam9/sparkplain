package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The application's code can come from the config file (source:, one path
// or a list, per environment too) with source-context lines around each
// stage's line; -source and -source-context override them. The report
// gives each stage's line, the explorer the code around it.
func TestSourceFromConfig(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	repo, err := filepath.Abs("../../scripts/fixtures")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg, []byte("source: /nowhere\nsource-context: 7\nenvironments:\n  dev:\n    source: ["+repo+"]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	app := "application_1790380000000_0046"
	if code, _, errs := runCLI(t, "-config", cfg, "-env", "dev", "-app-id", app, "-eventlog", filepath.Join(fx, app), "-out", dir, "-format", "html,explorer"); code != exitOK && code != exitPartial {
		t.Fatalf("exit %d: %s", code, errs)
	}
	html, _ := os.ReadFile(filepath.Join(dir, app+"-report.html"))
	if !strings.Contains(string(html), ">Your code</th>") || !strings.Contains(string(html), "workload.py:154<span class=\"sub\">nearest line recorded before it") {
		t.Error("the report's stages lack each stage's line of code")
	}
	x, _ := os.ReadFile(filepath.Join(dir, app+"-explorer.html"))
	if !strings.Contains(string(x), `"sourceContext":7`) || !strings.Contains(string(x), filepath.Join(repo, "workload.py")) {
		t.Error("the explorer lacks the code from source: or its context")
	}
	// A flag wins over the file.
	if code, _, errs := runCLI(t, "-config", cfg, "-env", "dev", "-source-context", "3", "-app-id", app, "-eventlog", filepath.Join(fx, app), "-out", dir, "-format", "explorer"); code != exitOK && code != exitPartial {
		t.Fatalf("exit %d: %s", code, errs)
	}
	if x, _ = os.ReadFile(filepath.Join(dir, app+"-explorer.html")); !strings.Contains(string(x), `"sourceContext":3`) {
		t.Error("-source-context did not override source-context")
	}
}
