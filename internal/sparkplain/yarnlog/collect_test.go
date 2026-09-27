package yarnlog

import (
	"archive/zip"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/source"
)

func sourceRow(t *testing.T, c Collection, name string) model.SourceStatus {
	t.Helper()
	for _, s := range c.Sources {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("no %s row in %+v", name, c.Sources)
	return model.SourceStatus{}
}

func kindCount(c Collection) map[string]int {
	out := map[string]int{}
	for _, f := range c.Files {
		out[f.Kind]++
	}
	return out
}

// A local copy of the log bucket: the cluster's root is a prefix, as on S3.
func TestCollectUnderPrefix(t *testing.T) {
	st := source.NewLocalStore(emrlogs)
	c := Collect(context.Background(), st, Plan{Root: "j-FIXTURE0049CLUSTER/", AppID: "application_1790380000000_0049"})
	if len(c.Steps) != 1 || c.Steps[0] != "s-FIXTURESTEP0001" {
		t.Errorf("steps = %v (the s3-dist-cp step submitted no application)", c.Steps)
	}
	got := kindCount(c)
	want := map[string]int{"container-stderr": 2, "container-stdout": 2, "step-stderr": 1, "step-controller": 1, "nodemanager": 1, "resourcemanager": 1, "bootstrap": 1}
	for k, n := range want {
		if got[k] != n {
			t.Errorf("%s files = %d, want %d (all %v)", k, got[k], n, got)
		}
	}
	for _, name := range []string{"Container logs", "Step logs", "Node logs"} {
		if s := sourceRow(t, c, name); s.Status != "read" || s.Class != "" || !strings.HasPrefix(s.Location, emrlogs) {
			t.Errorf("%s = %+v", name, s)
		}
	}
	steps := sourceRow(t, c, "Step logs")
	if !strings.Contains(steps.Detail, "Step s-FIXTURESTEP0001 submitted this application") {
		t.Errorf("step detail = %q", steps.Detail)
	}
	other := 0
	for _, f := range steps.Files {
		if strings.Contains(f.Location, "s-FIXTURESTEP0002") {
			other++
			if f.Detail != "another application's step" {
				t.Errorf("other step's file = %+v", f)
			}
		}
	}
	if other != 2 {
		t.Errorf("the other step's stderr and controller should be listed: %d", other)
	}
	for _, f := range c.Files {
		if !strings.HasPrefix(f.Location, filepath.Join(emrlogs, "j-FIXTURE0049CLUSTER")) || f.Lines == 0 {
			t.Errorf("file = %+v", f)
		}
		for _, l := range f.Found {
			if l.Source.File != f.Location {
				t.Errorf("line source %q is not its file %q", l.Source.File, f.Location)
			}
		}
		if f.Kind == "container-stderr" && f.Container == "" || f.Kind == "nodemanager" && f.Instance == "" {
			t.Errorf("file ids missing: %+v", f)
		}
	}
}

// The failed run from its own folder (Root ""), with the steps and nodes
// the EMR API would name.
func TestCollectNamedStepsAndNodes(t *testing.T) {
	st := source.NewLocalStore(filepath.Join(emrlogs, "j-FIXTURE0052CLUSTER"))
	c := Collect(context.Background(), st, Plan{AppID: "application_1790380000000_0052", Steps: []string{"s-FIXTURESTEP0001", "s-NOSUCHSTEP"},
		Instances: []string{"i-0fee0000000000003"}, Limits: source.Limits{Workers: 2}})
	got := kindCount(c)
	if got["nodemanager"] != 0 || got["resourcemanager"] != 1 || got["bootstrap"] != 1 || got["container-stdout"] != 2 {
		t.Errorf("files = %v (only the named node's logs)", got)
	}
	if len(c.Steps) != 1 {
		t.Errorf("steps = %v", c.Steps)
	}
}

func TestCollectSinceSkipsOldDaemonLogs(t *testing.T) {
	st := source.NewLocalStore(filepath.Join(emrlogs, "j-FIXTURE0050CLUSTER"))
	c := Collect(context.Background(), st, Plan{AppID: "application_1790380000000_0050", Since: time.Now().Add(24 * time.Hour)})
	if n := kindCount(c)["nodemanager"] + kindCount(c)["resourcemanager"]; n != 0 {
		t.Errorf("%d daemon logs read", n)
	}
	skipped := 0
	for _, f := range sourceRow(t, c, "Node logs").Files {
		if f.Status == "skipped" && f.Detail == "last written before the application started" {
			skipped++
		}
	}
	if skipped != 3 { // two NodeManagers and the ResourceManager
		t.Errorf("skipped %d daemon logs, want 3", skipped)
	}
}

// -from given one application's container folder.
func TestCollectAppFolder(t *testing.T) {
	st := source.NewLocalStore(filepath.Join(emrlogs, "j-FIXTURE0050CLUSTER", "containers", "application_1790380000000_0050"))
	c := Collect(context.Background(), st, Plan{AppFolder: true, AppID: "application_1790380000000_0050"})
	if n := kindCount(c)["container-stderr"]; n != 5 {
		t.Errorf("container stderr files = %d", n)
	}
	if s := sourceRow(t, c, "Step logs"); s.Status != "none" {
		t.Errorf("steps = %+v", s)
	}
}

func TestCollectNothingThere(t *testing.T) {
	c := Collect(context.Background(), source.NewLocalStore(t.TempDir()), Plan{AppID: "application_1790380000000_0099"})
	for name, want := range map[string]string{"Container logs": "not-supplied", "Step logs": "none", "Node logs": "not-supplied"} {
		if s := sourceRow(t, c, name); s.Status != want {
			t.Errorf("%s = %+v, want %s", name, s, want)
		}
	}
}

// failingStore lists like the fixture but cannot open stdout files.
type failingStore struct{ source.Store }

func (f failingStore) Open(ctx context.Context, o source.Object) (io.ReadCloser, error) {
	if strings.HasSuffix(o.Key, "stdout.gz") {
		return nil, &source.Error{Class: source.ClassAccessDenied, Key: o.Key, Err: errors.New("403")}
	}
	return f.Store.Open(ctx, o)
}

func TestCollectReportsUnreadable(t *testing.T) {
	st := failingStore{source.NewLocalStore(filepath.Join(emrlogs, "j-FIXTURE0052CLUSTER"))}
	c := Collect(context.Background(), st, Plan{AppID: "application_1790380000000_0052"})
	s := sourceRow(t, c, "Container logs")
	if s.Status != "partial" || s.Class != source.ClassAccessDenied || !strings.Contains(s.Detail, "2 of 4 files could not be read (2 accessDenied)") {
		t.Errorf("containers = %+v", s)
	}
	denied := 0
	for _, f := range s.Files {
		if f.Status == "error" && f.Class == source.ClassAccessDenied {
			denied++
		}
	}
	if denied != 2 {
		t.Errorf("denied files listed = %d", denied)
	}
}

func TestLogRoot(t *testing.T) {
	for _, tc := range []struct{ uri, bucket, prefix string }{
		{"s3n://logs/emr/", "logs", "emr/j-1/"},
		{"s3://logs", "logs", "j-1/"},
		{"s3a://logs/a/b", "logs", "a/b/j-1/"},
	} {
		b, p, ok := LogRoot(tc.uri, "j-1")
		if !ok || b != tc.bucket || p != tc.prefix {
			t.Errorf("LogRoot(%q) = %q %q %v", tc.uri, b, p, ok)
		}
	}
	if _, _, ok := LogRoot("hdfs:///logs", "j-1"); ok {
		t.Error("an HDFS log URI is not a log root sparkplain can read")
	}
}

func TestMain(m *testing.M) {
	if _, err := os.Stat(emrlogs); err != nil {
		panic("fixtures missing: " + err.Error())
	}
	os.Exit(m.Run())
}

// A zip holding several logs gives one log file per entry, each citing its
// own entry: results used to overwrite each other, keeping only the last
// entry's, and every line cited the zip rather than the entry.
func TestCollectZipKeepsEveryEntry(t *testing.T) {
	gzPath := filepath.Join(emrlogs, "j-FIXTURE0049CLUSTER/node/i-0fee0000000000001/applications/hadoop-yarn/hadoop-yarn-nodemanager-ip-10-0-2-10.us-east-1.compute.internal.log.gz")
	f, err := os.Open(gzPath)
	if err != nil {
		t.Fatal(err)
	}
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	logText, _ := io.ReadAll(zr)
	f.Close()

	dir := filepath.Join(t.TempDir(), "node/i-0fee0000000000001/applications/hadoop-yarn")
	os.MkdirAll(dir, 0o755)
	out, _ := os.Create(filepath.Join(dir, "hadoop-yarn-nodemanager-ip-10-0-2-10.log.zip"))
	zw := zip.NewWriter(out)
	for _, name := range []string{"part-1.log", "part-2.log"} {
		w, _ := zw.Create(name)
		w.Write(logText)
	}
	zw.Close()
	out.Close()

	root := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(dir))))
	c := Collect(context.Background(), source.NewLocalStore(root), Plan{AppID: "application_1790380000000_0049"})
	var nm []model.LogFile
	for _, lf := range c.Files {
		if lf.Kind == "nodemanager" {
			nm = append(nm, lf)
		}
	}
	if len(nm) != 2 {
		t.Fatalf("%d nodemanager files from a two-entry zip, want 2: %+v", len(nm), nm)
	}
	for i, lf := range nm {
		entry := []string{"!part-1.log", "!part-2.log"}[i]
		if !strings.HasSuffix(lf.Location, entry) || lf.Lines == 0 || lf.Bytes != int64(len(logText)) {
			t.Errorf("entry %d: location %q lines %d bytes %d", i, lf.Location, lf.Lines, lf.Bytes)
		}
		for _, l := range lf.Found {
			if l.Source.File != lf.Location {
				t.Errorf("line cites %q, want its entry %q", l.Source.File, lf.Location)
				break
			}
		}
	}
	if nm[0].Lines != nm[1].Lines || len(nm[0].Found) != len(nm[1].Found) {
		t.Errorf("identical entries read differently: %d/%d lines, %d/%d found", nm[0].Lines, nm[1].Lines, len(nm[0].Found), len(nm[1].Found))
	}
}
