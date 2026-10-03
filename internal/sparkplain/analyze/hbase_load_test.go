package analyze

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// loadRun is a scan of 10 regions where rs-hot holds 8: all 10 tasks run
// from t0 for 3 minutes, so rs-hot serves 8 of the 10 running.
func loadRun(hotFor time.Duration) []model.LogFile {
	t0 := time.Date(2026, 10, 2, 9, 17, 0, 0, time.UTC)
	f := model.LogFile{Location: "e1", Kind: "container-stderr", Container: "container_1_0001_01_000002"}
	for i := range 10 {
		srv := "rs-hot.example.internal"
		if i >= 8 {
			srv = fmt.Sprintf("rs-%d.example.internal", i)
		}
		end := t0.Add(hotFor)
		if i >= 8 {
			end = t0.Add(10 * time.Minute)
		}
		sp := model.HBaseSplit{Table: "orders", StartRow: fmt.Sprintf("k%d", i), EndRow: fmt.Sprintf("k%d", i+1), Server: srv, Region: fmt.Sprintf("r%d", i),
			Time: t0, Source: model.Source{File: "e1", Line: int64(10 + i)},
			Task: &model.SplitTask{TaskID: int64(100 + i), Partition: i, Stage: 5, Start: t0, End: end, EndSource: model.Source{File: "e1", Line: int64(100 + i)}}}
		f.HBaseSplits = append(f.HBaseSplits, sp)
		f.Found = append(f.Found, model.LogLine{Kind: model.LogHBaseUse, Time: t0, Count: 1, Source: sp.Source,
			Fields: map[string]string{"table": "orders", "access": "read", "api": "TableInputFormat", "server": srv}})
	}
	return []model.LogFile{f}
}

func TestHBaseServerLoad(t *testing.T) {
	t.Parallel()
	r := runWithLogs(nil, nil, loadRun(3*time.Minute)...)
	h := r.HBase
	if h == nil || len(h.Load) != 3 {
		t.Fatalf("load: %+v", h)
	}
	l := h.Load[0]
	if l.Server != "rs-hot.example.internal" || l.Tasks != 8 || l.Peak != 8 || l.BusyMs != 180_000 || l.TaskMs != 8*180_000 || l.Hot == nil ||
		l.Hot.Tasks != 8 || l.Hot.All != 10 || l.Hot.To.Sub(l.Hot.From) != 3*time.Minute {
		t.Errorf("hot server = %+v hot %+v", l, l.Hot)
	}
	var peak float64
	for _, p := range h.LoadTotal {
		peak = max(peak, p.V)
	}
	if peak != 10 || h.LoadStepMs != 3000 || len(l.Points) != len(h.LoadTotal) || l.Points[len(l.Points)-1].V != 0 {
		t.Errorf("total peak %v, step %d ms, %d points, last %v", peak, h.LoadStepMs, len(l.Points), l.Points[len(l.Points)-1])
	}
	f, ok := rules(r)["hbase-server-load"]
	if !ok || f.Title != "Region server rs-hot.example.internal served 8 of the 10 HBase scan tasks that ran at 09:17:00 UTC" ||
		!strings.Contains(f.Explanation, "From 09:17:00 UTC to 09:20:00 UTC (3 min 0 s)") || !strings.Contains(f.Explanation, "2 other region servers had the remaining tasks") ||
		len(f.Evidence) != 3 || f.Evidence[0].Source.Line != 10 || !strings.Contains(f.Evidence[0].Text, "task 0.0 in stage 5.0 (TID 100)") {
		t.Errorf("finding = %+v", f)
	}
	// Under a minute is not a finding.
	if _, ok := rules(runWithLogs(nil, nil, loadRun(50*time.Second)...))["hbase-server-load"]; ok {
		t.Error("a 50 s stretch should not be reported")
	}
}

// regionRun is a stage of six tasks, each reading one region, five in
// about 20 s and one (on region hot) in 4 min, while that region's server
// compacted it and refused writes to it three times; another region went
// offline under a fast task, and a region no task read was flushed.
func regionRun() []model.LogFile {
	t0 := time.Date(2026, 10, 2, 9, 17, 0, 0, time.UTC)
	enc := func(s string) string { return strings.Repeat(s, 32)[:32] }
	f := model.LogFile{Location: "e1", Kind: "container-stderr", Container: "container_1_0001_01_000002"}
	for i := range 6 {
		d := 20*time.Second + time.Duration(i)*time.Second
		if i == 5 {
			d = 4 * time.Minute
		}
		sp := model.HBaseSplit{Table: "orders", StartRow: fmt.Sprintf("k%d", i), EndRow: fmt.Sprintf("k%d", i+1), Server: "rs-1.example.internal",
			Region: enc(fmt.Sprint(i)), Time: t0, Source: model.Source{File: "e1", Line: int64(10 + i)},
			Task: &model.SplitTask{TaskID: int64(200 + i), Partition: i, Stage: 7, Start: t0, End: t0.Add(d), EndSource: model.Source{File: "e1", Line: int64(100 + i)}}}
		f.HBaseSplits = append(f.HBaseSplits, sp)
		f.Found = append(f.Found, model.LogLine{Kind: model.LogHBaseUse, Time: t0, Count: 1, Source: sp.Source,
			Fields: map[string]string{"table": "orders", "access": "read", "api": "TableInputFormat", "server": sp.Server}})
	}
	ev := func(at time.Duration, event, region, detail string, ms int64, line int64) model.HBaseRegionEvent {
		return model.HBaseRegionEvent{Time: t0.Add(at), Event: event, Region: region, Host: "rs-1.example.internal", Detail: detail, DurationMs: ms,
			Source: model.Source{File: "rs.log", Line: line}}
	}
	rs := model.LogFile{Location: "rs.log", Kind: "hbase-regionserver", Instance: "i-1", HBaseRegionEvents: []model.HBaseRegionEvent{
		ev(30*time.Second, "busy", enc("5"), "refused writes: its memstore was over 2.0 M", 0, 1),
		ev(31*time.Second, "busy", enc("5"), "refused writes: its memstore was over 2.0 M", 0, 2),
		ev(32*time.Second, "busy", enc("5"), "refused writes: its memstore was over 2.0 M", 0, 3),
		ev(2*time.Minute, "compaction", enc("5"), "rewrote 6 store files into one of 1.2 G", 42_000, 4),
		ev(3*time.Minute, "flush", enc("5"), "wrote 64 MB of memstore to disk", 900, 5),
		ev(5*time.Second, "closed", enc("1"), "closed the region: it stopped serving it", 0, 6),
		ev(8*time.Second, "opened", enc("1"), "opened the region: it serves it from now", 0, 7),
		ev(10*time.Second, "flush", enc("x"), "wrote 1 MB of memstore to disk", 100, 8), // no task read it
		ev(10*time.Minute, "compaction", enc("2"), "rewrote 2 store files", 1000, 9),    // after its task
	}}
	return []model.LogFile{f, rs}
}

func TestHBaseRegionEvents(t *testing.T) {
	t.Parallel()
	r := runWithLogs(nil, nil, regionRun()...)
	h := r.HBase
	var got []string
	for _, e := range h.RegionEvents {
		got = append(got, fmt.Sprintf("%s %s x%d %s slow=%d", e.Event, e.Region[:2], max(e.Count, 1), model.Duration(e.DurationMs), len(e.Slow)))
	}
	want := "busy 55 x3 0 ms slow=1, compaction 55 x1 42 s slow=1, flush 55 x1 900 ms slow=1, offline 11 x1 3.0 s slow=0"
	if strings.Join(got, ", ") != want {
		t.Errorf("events:\n%s\nwant\n%s", strings.Join(got, ", "), want)
	}
	slow := h.Tasks[5]
	if !slow.Slow || len(slow.Events) != 3 || h.Tasks[1].Slow || len(h.Tasks[1].Events) != 1 {
		t.Errorf("slow task %+v, task 1 %+v", slow, h.Tasks[1])
	}
	f, ok := rules(r)["hbase-region-events"]
	if !ok || f.Title != "Task 5.0 in stage 7.0 (TID 205) took 4 min 0 s, and its region on rs-1 had write refusals and a compaction (42 s) at that time" ||
		!strings.Contains(f.Explanation, "region 55555555555555555555555555555555 of orders on rs-1") || strings.Contains(f.Explanation, "a flush") ||
		!strings.Contains(f.Fix, "major compactions outside the hours of the job") || !strings.Contains(f.Fix, "Do not write to the same regions") ||
		len(f.Evidence) != 3 || f.Evidence[0].Source.Line != 15 || f.Evidence[1].Source.Line != 1 || f.Evidence[1].Text != "rs-1 logged 3 times from 09:17:30 UTC to 09:17:32 UTC: refused writes: its memstore was over 2.0 M" || f.Evidence[2].Source.Line != 4 {
		t.Errorf("finding = %+v", f)
	}
}
