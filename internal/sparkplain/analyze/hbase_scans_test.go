package analyze

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// scanRun is a three-region scan of orders on two executors, as the user's
// example: the region of 2026-10-01 to 2026-10-10 holds most rows and
// took 4 min 10 s, the others under a minute.
func scanRun(swap bool) Input {
	l := synthetic(nil,
		&model.Executor{ID: "1", Host: "ip-1", Cores: 2, Attributes: map[string]string{"CONTAINER_ID": "container_1_0001_01_000002"}},
		&model.Executor{ID: "2", Host: "ip-2", Cores: 2, Attributes: map[string]string{"CONTAINER_ID": "container_1_0001_01_000003"}})
	start := l.Application.Start.Add(time.Minute)
	st := &model.Stage{ID: 3, NumTasks: 3, Submitted: start, Completed: start.Add(4*time.Minute + 15*time.Second), Status: "succeeded",
		RDDs:   []model.StageRDD{{Name: "NewHadoopRDD", Callsite: "newAPIHadoopRDD at NativeMethodAccessorImpl.java:0"}},
		Totals: model.TaskTotals{InputRecords: 9_500_000}, Source: model.Source{File: "ev", Line: 10}}
	st.ScanTasks = []model.ScanTask{
		{Index: 0, TaskID: 30, ExecutorID: "1", DurationMs: 40_000, Rows: 1_100_000, Source: model.Source{File: "ev", Line: 31}},
		{Index: 1, TaskID: 31, ExecutorID: "1", DurationMs: 55_000, Rows: 1_600_000, Source: model.Source{File: "ev", Line: 32}},
		{Index: 2, TaskID: 32, ExecutorID: "2", DurationMs: 250_000, Rows: 6_800_000, Source: model.Source{File: "ev", Line: 33}},
	}
	l.Stages = []*model.Stage{st}
	at := start.Add(time.Second)
	split := func(file string, line int64, a, b, srv, region string) model.HBaseSplit {
		return model.HBaseSplit{Table: "orders", StartRow: a, EndRow: b, Server: srv, Region: region, SizeBytes: 3 << 30, Time: at, Source: model.Source{File: file, Line: line}}
	}
	e1 := model.LogFile{Location: "e1", Kind: "container-stderr", Container: "container_1_0001_01_000002",
		HBaseSplits: []model.HBaseSplit{split("e1", 60, "2026-09", "2026-10-01", "rs-3", "r3"), split("e1", 61, "2026-08-15", "2026-09", "rs-2", "r2")}}
	e2 := model.LogFile{Location: "e2", Kind: "container-stderr", Container: "container_1_0001_01_000003",
		HBaseSplits: []model.HBaseSplit{split("e2", 70, "2026-10-01", "2026-10-10", "rs-1", "r4")}}
	// Each split line is also an hbase-use entry, as the classifier keeps it.
	for _, f := range []*model.LogFile{&e1, &e2} {
		for _, sp := range f.HBaseSplits {
			f.Found = append(f.Found, model.LogLine{Kind: model.LogHBaseUse, Time: at, Count: 1, Source: sp.Source,
				Fields: map[string]string{"table": "orders", "access": "read", "api": "TableInputFormat", "server": sp.Server}})
		}
	}
	if swap { // a split logged by the wrong executor: the order cannot be trusted
		e1.Container, e2.Container = e2.Container, e1.Container
	}
	return Input{Tool: "t", EventLog: l, EventSource: model.SourceStatus{Name: "Spark event log", Status: "read"}, LogsRead: true, Logs: []model.LogFile{e1, e2}}
}

func TestHBaseScanTiesAndSkew(t *testing.T) {
	t.Parallel()
	r := Run(scanRun(false))
	if r.HBase == nil || len(r.HBase.Scans) != 1 {
		t.Fatalf("scans: %+v", r.HBase)
	}
	sc := r.HBase.Scans[0]
	var order []string
	for _, g := range sc.Regions {
		order = append(order, g.Region)
	}
	if !sc.Tied || strings.Join(order, ",") != "r2,r3,r4" || sc.Regions[2].Task.Rows != 6_800_000 || sc.Rows != "[2026-08-15, 2026-10-10)" ||
		sc.Servers[0].Server != "rs-1" || sc.Servers[0].Rows != 6_800_000 || sc.SizeBytes != 9<<30 {
		t.Errorf("scan = %+v", sc)
	}
	f, ok := rules(r)["hbase-scan-skew"]
	if !ok || f.Title != "Stage 3's scan of orders waited on one region: rs-1 took 4 min 10 s, the median region 55 s" ||
		!strings.Contains(f.Explanation, "returned 6,800,000 of the scan's 9,500,000 rows (72%)") || !strings.Contains(f.Fix, "split 'orders'") ||
		len(f.Evidence) != 2 || f.Evidence[0].Source.Line != 70 || f.Evidence[1].Source.Line != 33 {
		t.Errorf("finding = %+v", f)
	}
}

// A split logged by an executor that did not run its partition means the
// key order cannot be trusted: no region is tied, none guessed, and the
// scan says why.
func TestHBaseScanUntied(t *testing.T) {
	t.Parallel()
	r := Run(scanRun(true))
	sc := r.HBase.Scans[0]
	if sc.Tied || !strings.Contains(sc.Untied, "did not match the executors") || sc.Servers[0].Rows != 0 {
		t.Errorf("scan = %+v", sc)
	}
	for _, g := range sc.Regions {
		if g.Task != nil {
			t.Errorf("region %s tied", g.Region)
		}
	}
	if _, ok := rules(r)["hbase-scan-skew"]; ok {
		t.Error("no skew finding without tied regions")
	}
}

// When each split line names its task (the log layout prints the thread),
// regions are tied by it, exactly: even when the executors would not match
// the key order, and even for a split logged outside the stage's time.
func TestHBaseScanTiedByTask(t *testing.T) {
	t.Parallel()
	in := scanRun(true)
	part := map[string]int{"r2": 0, "r3": 1, "r4": 2}
	for i := range in.Logs {
		for j := range in.Logs[i].HBaseSplits {
			sp := &in.Logs[i].HBaseSplits[j]
			p := part[sp.Region]
			sp.Task = &model.SplitTask{TaskID: int64(30 + p), Partition: p, Stage: 3}
			sp.Time = time.Time{} // no time to place it by
		}
	}
	r := Run(in)
	if r.HBase == nil || len(r.HBase.Scans) != 1 {
		t.Fatalf("scans: %+v", r.HBase)
	}
	sc := r.HBase.Scans[0]
	if !sc.Tied || sc.TiedBy != "task" || sc.Untied != "" {
		t.Fatalf("scan = %+v", sc)
	}
	for _, g := range sc.Regions {
		if g.Task == nil || g.Task.Index != part[g.Region] {
			t.Errorf("region %s task %+v", g.Region, g.Task)
		}
	}
	if _, ok := rules(r)["hbase-scan-skew"]; !ok {
		t.Error("skew finding expected once regions are tied")
	}
	if sc := Run(scanRun(false)).HBase.Scans[0]; sc.TiedBy != "key order" {
		t.Errorf("key order tie: %q", sc.TiedBy)
	}
}

// Without the event log, a scan is built from split lines that name their
// tasks: each region's time runs from its task's Running line to its
// Finished line, a retried partition keeps the attempt that finished, a
// task with no end shows no time, and rows are not claimed.
func TestHBaseScanFromLogs(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 10, 2, 9, 17, 0, 0, time.UTC)
	split := func(file string, line int64, a, b, srv, region string, gib int64, part, attempt int, tid int64, start, end time.Duration, failed bool) model.HBaseSplit {
		sp := model.HBaseSplit{Table: "orders", StartRow: a, EndRow: b, Server: srv, Region: region, SizeBytes: gib << 30, Time: t0.Add(start),
			Source: model.Source{File: file, Line: line},
			Task:   &model.SplitTask{TaskID: tid, Partition: part, Attempt: attempt, Stage: 172, Start: t0.Add(start), Failed: failed}}
		if end > 0 {
			sp.Task.End, sp.Task.EndSource = t0.Add(end), model.Source{File: file, Line: line + 100}
		}
		return sp
	}
	e1 := model.LogFile{Location: "e1", Kind: "container-stderr", Container: "container_1_0001_01_000002",
		HBaseSplits: []model.HBaseSplit{
			split("e1", 10, "2026-08-15", "2026-09", "rs-2", "r2", 1, 0, 0, 30, 0, 40*time.Second, false),
			split("e1", 11, "2026-09", "2026-10-01", "rs-3", "r3", 1, 1, 0, 31, 0, 5*time.Second, true),
			split("e1", 12, "2026-09", "2026-10-01", "rs-3", "r3", 1, 1, 1, 33, 6*time.Second, 61*time.Second, false),
			{Table: "orders", StartRow: "x", EndRow: "y", Server: "rs-2", Region: "rx", Time: t0, Source: model.Source{File: "e1", Line: 13}}, // no thread
		}}
	e2 := model.LogFile{Location: "e2", Kind: "container-stderr", Container: "container_1_0001_01_000003",
		HBaseSplits: []model.HBaseSplit{
			split("e2", 20, "2026-10-01", "2026-10-10", "rs-1", "r4", 6, 2, 0, 32, 0, 250*time.Second, false),
			split("e2", 21, "2026-10-10", "", "rs-1", "r5", 1, 3, 0, 34, 0, 0, false), // the log ends before it does
		}}
	for i, f := range []*model.LogFile{&e1, &e2} {
		f.Found = append(f.Found, model.LogLine{Kind: model.LogExecutorHost, Time: t0, Count: 1, Source: model.Source{File: f.Location, Line: 1},
			Fields: map[string]string{"executor": strconv.Itoa(i + 1), "host": "ip-" + strconv.Itoa(i+1)}})
		for _, sp := range f.HBaseSplits {
			f.Found = append(f.Found, model.LogLine{Kind: model.LogHBaseUse, Time: sp.Time, Count: 1, Source: sp.Source,
				Fields: map[string]string{"table": "orders", "access": "read", "api": "TableInputFormat", "server": sp.Server}})
		}
	}
	r := runWithLogs(nil, nil, e1, e2)
	if r.HBase == nil || len(r.HBase.Scans) != 1 {
		t.Fatalf("scans: %+v", r.HBase)
	}
	sc := r.HBase.Scans[0]
	var got []string
	for _, g := range sc.Regions {
		s := g.Region + " none"
		if g.Task != nil {
			s = fmt.Sprintf("%s TID %d %s on %s", g.Region, g.Task.TaskID, model.Duration(g.Task.DurationMs), g.Task.ExecutorID)
		}
		got = append(got, s)
	}
	want := "r2 TID 30 40 s on 1, r3 TID 33 55 s on 1, r4 TID 32 4 min 10 s on 2, r5 none"
	if strings.Join(got, ", ") != want || !sc.FromLogs || !sc.Tied || sc.TiedBy != "task" || sc.StageID != 172 || sc.Tasks != 4 || sc.TotalRows != 0 ||
		sc.Servers[0].Server != "rs-1" || sc.Servers[0].TaskMs != 250_000 || sc.Rows != "[2026-08-15, last row]" {
		t.Errorf("scan = %+v\nregions: %s", sc, strings.Join(got, ", "))
	}
	f, ok := rules(r)["hbase-scan-skew"]
	if !ok || !strings.Contains(f.Title, "rs-1 took 4 min 10 s, the median region 55 s") || !strings.Contains(f.Explanation, "holds 6.0 GiB of the scan's 9.0 GiB (67%") ||
		strings.Contains(f.Explanation, "returned") || f.Evidence[1].Ref != "" || f.Evidence[1].Source.Line != 120 {
		t.Errorf("finding = %+v", f)
	}
	missing := strings.Join(r.HBase.Missing, "\n")
	if !strings.Contains(missing, "Rows each scan region returned") || !strings.Contains(missing, "Scans for 1 split logged with no task") {
		t.Errorf("missing = %s", missing)
	}
}
