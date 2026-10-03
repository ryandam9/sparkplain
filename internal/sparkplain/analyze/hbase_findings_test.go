package analyze

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// scanTask is one synthetic split line in the thread layout: its task,
// region and size, run from t0+at for d.
type scanTask struct {
	stage, part, attempt int
	table, start, end    string
	mib                  int64
	at, d                time.Duration
	failed               bool
}

// scanLogs makes one executor's log of the given split lines, with the
// hbase-use entries the classifier keeps beside them.
func scanLogs(ts ...scanTask) []model.LogFile {
	t0 := time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC)
	f := model.LogFile{Location: "e1", Kind: "container-stderr", Container: "container_1_0001_01_000002"}
	for i, s := range ts {
		region := fmt.Sprintf("%032x", i+1)
		sp := model.HBaseSplit{Table: s.table, StartRow: s.start, EndRow: s.end, Server: fmt.Sprintf("rs-%d.example.internal", i%3), Region: region,
			SizeBytes: s.mib << 20, Time: t0.Add(s.at), Source: model.Source{File: "e1", Line: int64(10 + i)},
			Task: &model.SplitTask{TaskID: int64(1000 + i), Partition: s.part, Attempt: s.attempt, Stage: s.stage, Start: t0.Add(s.at), End: t0.Add(s.at + s.d),
				Failed: s.failed, EndSource: model.Source{File: "e1", Line: int64(5000 + i)}}}
		f.HBaseSplits = append(f.HBaseSplits, sp)
		f.Found = append(f.Found, model.LogLine{Kind: model.LogHBaseUse, Time: sp.Time, Count: 1, Source: sp.Source,
			Fields: map[string]string{"table": s.table, "access": "read", "api": "TableInputFormat", "server": sp.Server}})
	}
	return []model.LogFile{f}
}

// The same four regions of orders read by stages 3 and 9 (and another
// table by stage 12): an uncached RDD read twice.
func TestHBaseRepeatedScan(t *testing.T) {
	t.Parallel()
	var ts []scanTask
	for _, st := range []int{3, 9} {
		for p := range 4 {
			ts = append(ts, scanTask{stage: st, part: p, table: "orders", start: fmt.Sprintf("k%d", p), end: fmt.Sprintf("k%d", p+1), mib: 100, at: time.Duration(st) * time.Minute, d: 30 * time.Second})
		}
	}
	ts = append(ts, scanTask{stage: 12, table: "customers", start: "a", end: "b", mib: 100, at: 20 * time.Minute, d: 30 * time.Second})
	f, ok := rules(runWithLogs(nil, nil, scanLogs(ts...)...))["hbase-repeated-scan"]
	if !ok || f.Title != "Stages 3 and 9 each read the same regions of orders: the table was scanned 2 times" || f.Severity != model.Warning ||
		!strings.Contains(f.Explanation, "cost 2 min 0 s of task time after the first read") || len(f.Evidence) != 2 || f.Evidence[1].Source.Line != 14 ||
		!strings.Contains(f.Fix, "persist") {
		t.Errorf("finding = %+v", f)
	}
}

// PySpark's newAPIHadoopRDD takes one record first to check it can be sent
// to Python ("take at SerDeUtil.scala", stage 1), reading the first region
// again before the scan proper (stage 2); that is not a repeated scan.
func TestHBaseRepeatedScanSkipsPySparkProbe(t *testing.T) {
	t.Parallel()
	ts := []scanTask{{stage: 1, table: "orders", start: "", end: "k1", mib: 21, d: time.Second}}
	for p := range 4 {
		start := fmt.Sprintf("k%d", p)
		if p == 0 {
			start = ""
		}
		ts = append(ts, scanTask{stage: 2, part: p, table: "orders", start: start, end: fmt.Sprintf("k%d", p+1), mib: 21, at: 2 * time.Second, d: 8 * time.Second})
	}
	l := &model.EventLog{Stages: []*model.Stage{
		{ID: 1, Name: "take at SerDeUtil.scala:173", NumTasks: 1},
		{ID: 2, Name: "count at NativeMethodAccessorImpl.java:0", NumTasks: 4},
	}}
	r := runWithLogs(l, nil, scanLogs(ts...)...)
	if f, ok := rules(r)["hbase-repeated-scan"]; ok {
		t.Errorf("the probe was taken for a second scan: %+v", f)
	}
	if len(r.HBase.Tasks) != 5 {
		t.Fatalf("%d HBase tasks, want 5 (the probe's and the scan's)", len(r.HBase.Tasks))
	}
	// Without the probe's name, the same reads are a repeated scan.
	l.Stages[0].Name = "count at etl.py:12"
	if _, ok := rules(runWithLogs(l, nil, scanLogs(ts...)...))["hbase-repeated-scan"]; !ok {
		t.Error("two stages reading the same first region should still be a repeated scan")
	}
}

// Twelve regions from the table's first row to its last: no start or
// stop row.
func TestHBaseFullScan(t *testing.T) {
	t.Parallel()
	var ts []scanTask
	for p := range 12 {
		start, end := fmt.Sprintf("k%02d", p), fmt.Sprintf("k%02d", p+1)
		if p == 0 {
			start = ""
		}
		if p == 11 {
			end = ""
		}
		ts = append(ts, scanTask{stage: 4, part: p, table: "orders", start: start, end: end, mib: 200, d: 10 * time.Second})
	}
	r := runWithLogs(nil, nil, scanLogs(ts...)...)
	f, ok := rules(r)["hbase-full-scan"]
	if !ok || f.Title != "Stage 4 scanned all of orders: 12 regions from the first row to the last" || !strings.Contains(f.Explanation, "(about 2.3 GiB, HBase's estimate)") ||
		len(f.Evidence) != 2 || f.Evidence[0].Source.Line != 10 || f.Evidence[1].Source.Line != 21 || !strings.Contains(f.Fix, "withStartRow") {
		t.Errorf("finding = %+v", f)
	}
	if _, ok := rules(r)["hbase-tiny-regions"]; ok {
		t.Error("200 MiB regions are not tiny")
	}
	// A short range is not reported.
	if _, ok := rules(runWithLogs(nil, nil, scanLogs(ts[0], ts[11])...))["hbase-full-scan"]; ok {
		t.Error("two regions should not be reported")
	}
}

// 25 regions of 4 MiB: a task and a scanner per small piece.
func TestHBaseTinyRegions(t *testing.T) {
	t.Parallel()
	var ts []scanTask
	for p := range 25 {
		ts = append(ts, scanTask{stage: 6, part: p, table: "events", start: fmt.Sprintf("e%02d", p), end: fmt.Sprintf("e%02d", p+1), mib: 4, d: 800 * time.Millisecond})
	}
	f, ok := rules(runWithLogs(nil, nil, scanLogs(ts...)...))["hbase-tiny-regions"]
	if !ok || f.Title != "Stage 6 read 25 small regions of events: the median region holds 4.0 MiB" || !strings.Contains(f.Explanation, "25 of the 25 regions with a known size are under 32 MiB") ||
		!strings.Contains(f.Explanation, "The median region took 800 ms to read.") || !strings.Contains(f.Fix, "merge_region") ||
		len(f.Evidence) != 3 || f.Evidence[0].Source.Line != 10 || f.Evidence[0].Text != "region 00000000000000000000000000000001 of events: 4.0 MiB" {
		t.Errorf("finding = %+v", f)
	}
}

// Partition 2 of stage 3 failed once and was read again.
func TestHBaseRetriedRegions(t *testing.T) {
	t.Parallel()
	ts := []scanTask{
		{stage: 3, part: 0, table: "orders", start: "a", end: "b", mib: 10, d: 10 * time.Second},
		{stage: 3, part: 2, table: "orders", start: "c", end: "d", mib: 10, d: 5 * time.Second, failed: true},
		{stage: 3, part: 2, attempt: 1, table: "orders", start: "c", end: "d", mib: 10, at: 6 * time.Second, d: 10 * time.Second},
	}
	f, ok := rules(runWithLogs(nil, nil, scanLogs(ts...)...))["hbase-retried-regions"]
	if !ok || f.Title != "1 region was read from HBase more than once because its task failed" ||
		!strings.Contains(f.Explanation, "partition 2 of stage 3 read region 00000000000000000000000000000002 of orders on rs-1 2 times") ||
		len(f.Evidence) != 1 || f.Evidence[0].Source.Line != 5001 || !strings.Contains(f.Fix, "scanner lease") {
		t.Errorf("finding = %+v", f)
	}
}
