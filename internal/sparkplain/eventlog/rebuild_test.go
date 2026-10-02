package eventlog_test

import (
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/eventlog"
	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/yarnlog"
)

const testdata = "../../../testdata"

// rebuildFixture rebuilds a fixture run from its driver's log, and reads
// its real event log beside it.
func rebuildFixture(t *testing.T, app string) (rebuilt, real *model.EventLog) {
	t.Helper()
	id := "application_1790380000000_" + app
	matches, _ := filepath.Glob(filepath.Join(testdata, "emrlogs/*/containers", id, "container_*_01_000001/stderr.gz"))
	if len(matches) != 1 {
		t.Fatalf("driver log of %s: %v", app, matches)
	}
	fh, err := os.Open(matches[0])
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()
	zr, err := gzip.NewReader(fh)
	if err != nil {
		t.Fatal(err)
	}
	res, err := yarnlog.Classify(zr, matches[0], yarnlog.File{Kind: yarnlog.ContainerStderr}, yarnlog.Options{})
	if err != nil {
		t.Fatal(err)
	}
	rebuilt = eventlog.Rebuild(context.Background(), res.DriverEvents, matches[0], id, "1", eventlog.Options{})
	in, err := eventlog.Resolve(filepath.Join(testdata, "eventlog", id), id, eventlog.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	if real, err = eventlog.Parse(context.Background(), in, eventlog.Options{}); err != nil {
		t.Fatal(err)
	}
	return rebuilt, real
}

// A run rebuilt from its driver's log has the jobs, the stages that ran,
// the tasks, the failures and the executors of its real event log, to the
// millisecond where the log says ("finished in 3.956 s"); stages Spark
// skipped are named only when the log names them, and skipped stages
// further up are not in the log at all.
func TestRebuildMatchesEventLog(t *testing.T) {
	t.Parallel()
	for _, app := range []string{"0049", "0050", "0084"} {
		rb, real := rebuildFixture(t, app)
		if rb.Stats.Layout != eventlog.LayoutLogs || rb.Stats.Malformed != 0 {
			t.Errorf("%s: layout %q, %d malformed", app, rb.Stats.Layout, rb.Stats.Malformed)
		}
		a, b := rb.Application, real.Application
		near := func(x, y time.Time) bool { return x.Sub(y).Abs() < time.Second } // log times here have whole seconds
		if a.Name != b.Name || a.SparkVersion != b.SparkVersion || !near(a.Start, b.Start) || !near(a.End, b.End) {
			t.Errorf("%s: app %q %s %s–%s, want %q %s %s–%s", app, a.Name, a.SparkVersion, a.Start, a.End, b.Name, b.SparkVersion, b.Start, b.End)
		}
		jobs := func(l *model.EventLog) string {
			var out []string
			for _, j := range l.Jobs {
				out = append(out, fmt.Sprintf("%d:%s", j.ID, j.Status))
			}
			return strings.Join(out, " ")
		}
		if jobs(rb) != jobs(real) {
			t.Errorf("%s jobs:\n%s\nwant\n%s", app, jobs(rb), jobs(real))
		}
		want := map[string]*model.Stage{}
		for _, s := range real.Stages {
			want[fmt.Sprintf("%d.%d", s.ID, s.Attempt)] = s
		}
		ran := 0
		for _, s := range rb.Stages {
			w := want[fmt.Sprintf("%d.%d", s.ID, s.Attempt)]
			if w == nil {
				t.Errorf("%s: stage %d.%d not in the event log", app, s.ID, s.Attempt)
				continue
			}
			if s.Status != w.Status || s.Totals.Tasks != w.Totals.Tasks || s.Totals.Succeeded != w.Totals.Succeeded || s.Totals.Failed != w.Totals.Failed {
				t.Errorf("%s stage %d: %s %d tasks (%d ok, %d failed), want %s %d (%d, %d)", app, s.ID, s.Status, s.Totals.Tasks, s.Totals.Succeeded, s.Totals.Failed,
					w.Status, w.Totals.Tasks, w.Totals.Succeeded, w.Totals.Failed)
			}
			if s.Status == "skipped" {
				continue
			}
			ran++
			if s.Name != w.Name {
				t.Errorf("%s stage %d name %q, want %q", app, s.ID, s.Name, w.Name)
			}
			d, wd := s.Completed.Sub(s.Submitted).Milliseconds(), w.Completed.Sub(w.Submitted).Milliseconds()
			if d < wd-2 || d > wd+2 {
				t.Errorf("%s stage %d took %d ms, want %d", app, s.ID, d, wd)
			}
		}
		realRan := 0
		for _, s := range real.Stages {
			if s.Status != "skipped" {
				realRan++
			}
		}
		if ran != realRan {
			t.Errorf("%s: %d stages ran, want %d", app, ran, realRan)
		}
		execs := func(l *model.EventLog) string {
			var out []string
			for _, x := range l.Executors {
				out = append(out, fmt.Sprintf("%s@%s/%d tasks %d", x.ID, x.Host, x.Cores, x.Tasks.Tasks))
			}
			return strings.Join(out, " ")
		}
		if execs(rb) != execs(real) {
			t.Errorf("%s executors:\n%s\nwant\n%s", app, execs(rb), execs(real))
		}
	}
}

// A failed task keeps its error, as the event log has it.
func TestRebuildKeepsFailures(t *testing.T) {
	t.Parallel()
	rb, real := rebuildFixture(t, "0049")
	var got, want string
	for _, s := range rb.Stages {
		if s.ID == 22 && len(s.Failures) > 0 {
			got = s.Failures[0].Kind + " " + s.Failures[0].Message[:min(80, len(s.Failures[0].Message))]
		}
	}
	for _, s := range real.Stages {
		if s.ID == 22 && len(s.Failures) > 0 {
			want = s.Failures[0].Kind + " " + s.Failures[0].Message[:min(80, len(s.Failures[0].Message))]
		}
	}
	if got == "" || got != want {
		t.Errorf("failure %q, want %q", got, want)
	}
	if !strings.HasSuffix(rb.Stages[0].Source.File, "stderr.gz") || rb.Stages[0].Source.Line == 0 {
		t.Errorf("a stage's source should be the driver's log line: %+v", rb.Stages[0].Source)
	}
}
