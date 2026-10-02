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
	if !ok || f.Title != "Region server rs-hot.example.internal served 8 of the 10 HBase scan tasks running at 09:17:00 UTC" ||
		!strings.Contains(f.Explanation, "From 09:17:00 UTC to 09:20:00 UTC (3 min 0 s)") || !strings.Contains(f.Explanation, "while 2 other region servers had the rest") ||
		len(f.Evidence) != 3 || f.Evidence[0].Source.Line != 10 || !strings.Contains(f.Evidence[0].Text, "task 0.0 in stage 5.0 (TID 100)") {
		t.Errorf("finding = %+v", f)
	}
	// Under a minute is not a finding.
	if _, ok := rules(runWithLogs(nil, nil, loadRun(50*time.Second)...))["hbase-server-load"]; ok {
		t.Error("a 50 s stretch should not be reported")
	}
}
