package main

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const standalone = "../../testdata/standalone"

// -from finds a Spark Standalone run's executor logs in a worker's work
// folder, the folder that holds it ($SPARK_HOME), or one folder per
// machine; a copy of an EMR cluster's log folder still wins.
func TestFromLayoutStandalone(t *testing.T) {
	t.Parallel()
	const app = "app-20261025032316-0000"
	mk := func(t *testing.T, dirs ...string) string {
		root := t.TempDir()
		for _, d := range dirs {
			if err := os.MkdirAll(filepath.Join(root, filepath.FromSlash(d)), 0o755); err != nil {
				t.Fatal(err)
			}
		}
		return root
	}
	for name, c := range map[string]struct {
		dirs []string
		want []string
	}{
		"work folder":     {[]string{app + "/0"}, []string{app + "/"}},
		"SPARK_HOME":      {[]string{"work/" + app + "/0", "conf", "jars"}, []string{"work/" + app + "/"}},
		"one per machine": {[]string{"a/work/" + app + "/0", "b/" + app + "/1", "c/work/app-other/0"}, []string{"a/work/" + app + "/", "b/" + app + "/"}},
		"EMR copy wins":   {[]string{"containers", "work/" + app + "/0"}, nil},
	} {
		lay, err := fromLayout(mk(t, c.dirs...), app)
		if err != nil || !slices.Equal(lay.standalone, c.want) {
			t.Errorf("%s: %+v, %v; want %q", name, lay, err, c.want)
		}
	}
	if _, err := fromLayout(mk(t, "work/app-other/0"), app); err == nil || !strings.Contains(err.Error(), "no Spark Standalone work folder for "+app) {
		t.Errorf("a folder without the application: %v", err)
	}
}

// Both fixtures, Standalone and local-cluster, read every executor's log,
// each tied to its executor by its folder and to its host by the event
// log, and give every task its story. A secret planted in an executor's
// log never reaches the output. (runCLI sets HOME, so not parallel.)
func TestStandaloneExecutorLogs(t *testing.T) {
	for _, c := range []struct{ app, from string }{
		{"app-20261025032316-0000", "cluster"},
		{"app-20261025032411-0000", "local-cluster"},
	} {
		from := filepath.Join(t.TempDir(), c.from)
		copyTree(t, filepath.Join(standalone, c.from), from)
		planted := filepath.Join(from, "work", c.app, "2", "stderr")
		f, err := os.OpenFile(planted, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		f.WriteString("26/10/25 03:23:59 ERROR Executor: connect failed with spark.hadoop.fs.s3a.secret.key=FAKESECRETabc123 password=hunter2fake\n")
		f.Close()
		out := t.TempDir()
		code, stdout, errs := runCLI(t, "-app-id", c.app, "-eventlog", filepath.Join(fx, c.app), "-from", from, "-out", out, "-format", "json,html,explorer")
		if code != exitOK && code != exitPartial {
			t.Fatalf("%s: exit %d: %s", c.from, code, errs)
		}
		r := readReport(t, out)
		var execs []string
		for _, lf := range r.Logs.Files {
			if lf.Kind != "container-stderr" {
				continue
			}
			execs = append(execs, lf.Executor)
			if lf.Container != "" || lf.Host != "localhost" {
				t.Errorf("%s: %s: container %q, host %q", c.from, lf.Location, lf.Container, lf.Host)
			}
		}
		slices.Sort(execs)
		if !slices.Equal(execs, []string{"0", "1", "2", "3"}) {
			t.Errorf("%s: executors %q", c.from, execs)
		}
		s := r.TaskStories
		if s == nil || len(s.Tasks) != 52 || s.ByTID != 52 || len(s.Executors) != 4 { // the event log's 52 tasks
			t.Errorf("%s: task stories %+v", c.from, s)
		}
		src := false
		for _, x := range r.Sources {
			src = src || x.Name == "Executor logs" && x.Status == "read"
		}
		if !src {
			t.Errorf("%s: no Executor logs source read: %+v", c.from, r.Sources)
		}
		for _, name := range []string{c.app + "-report.json", c.app + "-report.html", c.app + "-explorer.html"} {
			b, _ := os.ReadFile(filepath.Join(out, name))
			if len(b) == 0 || strings.Contains(string(b), "FAKESECRET") || strings.Contains(string(b), "hunter2fake") {
				t.Errorf("%s: %s is empty or holds the planted secret", c.from, name)
			}
		}
		if b, _ := os.ReadFile(filepath.Join(out, c.app+"-report.json")); !strings.Contains(string(b), "secret.key=[redacted] password=[redacted]") {
			t.Errorf("%s: the planted line was not read, so the secret check proves nothing", c.from)
		}
		if strings.Contains(stdout+errs, "FAKESECRET") || strings.Contains(stdout+errs, "hunter2fake") {
			t.Errorf("%s: the console shows the planted secret", c.from)
		}
	}
}
