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

// PySpark's DataFrame actions record no line of the application's, so a
// stage is tied to the line that set its job's description: f-string
// placeholders match what they filled in, a description two lines could
// have set ties to neither, and a string that is mostly placeholders ties
// to nothing. Later stages with no line take the nearest before them.
func TestYourCodeFromDescriptions(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(1_790_000_000, 0)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	native := []model.CodeLocation{{File: "NativeMethodAccessorImpl.java", Line: 0}}
	r := &model.Report{}
	r.Jobs.Jobs = []*model.Job{
		{ID: 0, Description: "generate 1,000,000 orders and write lab_orders", Code: native},
		{ID: 1, Description: "join orders with stores", Code: native},
		{ID: 2, Description: "load day 2026-09-01", Code: native},
		{ID: 3, Code: native},
		{ID: 4, Description: "x=1", Code: native},
	}
	r.Jobs.Stages = []*model.Stage{
		{ID: 0, JobIDs: []int{0}, Code: native, Submitted: at(1)},
		{ID: 1, JobIDs: []int{1}, Code: native, Submitted: at(2)},
		{ID: 2, JobIDs: []int{2}, Code: native, Submitted: at(3)},
		{ID: 3, JobIDs: []int{3}, Code: native, Submitted: at(4)},
		{ID: 4, JobIDs: []int{4}, Code: native, Submitted: at(5)},
	}
	src := []SourceFile{{Path: "app/orders.py", Logged: []string{"orders.py"}, Lines: []string{
		`rows = int(sys.argv[1])`,
		`sc.setJobDescription(f"generate {rows:,} orders and write lab_orders")`,
		`spark.sparkContext.setJobDescription('join orders with stores')`,
		`sc.setLocalProperty("spark.job.description", "load day %s" % day)`,
		`sc.setJobDescription("load day {}".format(other))`,
		`sc.setJobDescription(f"{k}={v}")`,
	}}}
	got := yourCode(r, src)
	for key, want := range map[string]string{
		"0.0": "orders.py:2|the line that set job 0's description",
		"1.0": "orders.py:3|the line that set job 1's description",
		// lines 4 and 5 could both have set it
		"2.0": "orders.py:3|nearest line recorded before it; Spark recorded none for this stage",
		"3.0": "orders.py:3|nearest line recorded before it; Spark recorded none for this stage",
		"4.0": "orders.py:3|nearest line recorded before it; Spark recorded none for this stage",
	} {
		y := got[key]
		if y == nil || y.Label()+"|"+y.Via != want {
			t.Errorf("stage %s: %+v, want %s", key, y, want)
		}
	}
	// Without the code there is nothing to match.
	if got := yourCode(r, nil); len(got) != 0 {
		t.Errorf("descriptions matched with no code at hand: %+v", got)
	}
}
