package report

import (
	"testing"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// Each stage's line of the application's code: its own call site when that
// is the application's; its job's action when it is Spark's own
// (PythonRDD.scala); the nearest recorded before it, said so, when Spark
// recorded none; and, with the application's files at hand, only lines in
// them.
func TestYourCode(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(1_790_000_000, 0)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	etl := func(n int) []model.CodeLocation {
		return []model.CodeLocation{{File: "/mnt/yarn/etl.py", Line: n, Action: "count"}}
	}
	r := &model.Report{}
	r.Jobs.Jobs = []*model.Job{{ID: 0, Code: etl(10)}, {ID: 1, Code: etl(20)}, {ID: 2, Code: []model.CodeLocation{{File: "NativeMethodAccessorImpl.java", Line: 0}}}}
	r.Jobs.Stages = []*model.Stage{
		{ID: 0, JobIDs: []int{0}, Code: etl(10), Submitted: at(1)},
		{ID: 1, JobIDs: []int{1}, Code: []model.CodeLocation{{File: "PythonRDD.scala", Line: 160}}, Submitted: at(2)},
		{ID: 2, JobIDs: []int{2}, Submitted: at(3)},
		{ID: 3, JobIDs: []int{2}}, // skipped: never submitted
		// Spark's background work, run by the JVM's thread pool
		{ID: 4, JobIDs: []int{2}, Code: []model.CodeLocation{{File: "FutureTask.java", Line: 264}}, Submitted: at(4)},
	}
	got := yourCode(r, nil)
	for key, want := range map[string]string{"0.0": "etl.py:10|", "1.0": "etl.py:20|the action of job 1", "2.0": "etl.py:20|nearest line recorded before it; Spark recorded none for this stage",
		"4.0": "etl.py:20|nearest line recorded before it; Spark recorded none for this stage"} {
		y := got[key]
		if y == nil || y.Label()+"|"+y.Via != want {
			t.Errorf("stage %s: %+v, want %s", key, y, want)
		}
	}
	if got["3.0"] != nil {
		t.Errorf("a stage never submitted has no nearest line: %+v", got["3.0"])
	}
	// With the application's files given, only lines in them count.
	got = yourCode(r, []SourceFile{{Path: "jobs/other.py", Logged: []string{"other.py"}}})
	if len(got) != 0 {
		t.Errorf("lines outside the files given: %+v", got)
	}
}
