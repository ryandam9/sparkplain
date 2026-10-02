package analyze

import (
	"strings"
	"testing"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// A plan's scan goes to the stages whose RDDs ran it, never a skipped one;
// a write with no WriteFiles stage goes to the final stage of the query's
// last job; the logs' folders merge with the plan's, an earlier attempt's
// logs are left out, and files no stage could be found for are noted.
func TestStageDataPlacement(t *testing.T) {
	scan := []model.StageRDD{{Name: "FileScanRDD", Operation: "Scan parquet "}}
	stages := []*model.Stage{
		{ID: 0, Status: model.StatusSucceeded, RDDs: scan},
		{ID: 1, Status: model.StatusSkipped, RDDs: scan},
		{ID: 2, Status: model.StatusSucceeded, RDDs: []model.StageRDD{{Name: "ShuffledRowRDD", Operation: "Exchange"}}},
	}
	c := &ctx{t: DefaultThresholds(), log: &model.EventLog{
		Stages: stages,
		Jobs:   []*model.Job{{ID: 0, StageIDs: []int{0}}, {ID: 1, StageIDs: []int{1, 2}}},
		SQL: []*model.SQLQuery{{ID: 0, JobIDs: []int{0, 1},
			Reads:  []model.DataRef{{Kind: "path", Access: "read", Name: "s3://b/a/", Format: "parquet", Node: "Scan parquet "}},
			Writes: []model.DataRef{{Kind: "table", Access: "write", Name: "db.t"}}}},
	}}
	r := &model.Report{
		Logs: &model.LogsSection{Files: []model.LogFile{
			{Location: "exec1.log", StageData: []model.StageData{{Stage: 0, Access: model.DataRead, Kind: model.DataPath, Name: "s3://b/a", Parts: 3, Sized: 3, Bytes: 300, Source: model.Source{Line: 7}}}, DataUntied: 2},
			{Location: "old.log", EarlierAttempt: 1, StageData: []model.StageData{{Stage: 0, Access: model.DataRead, Kind: model.DataPath, Name: "s3://b/old", Parts: 1}}},
		}},
		HBase: &model.HBaseSection{Stages: []model.HBaseStage{{StageID: 2, Reads: []string{"t1"}}}},
	}
	analyzeStageData(c, r)
	got := map[int][]string{}
	for _, st := range stages {
		for _, d := range st.Data {
			got[st.ID] = append(got[st.ID], d.Access+" "+d.Kind+" "+d.Name+" "+d.Format+" "+strings.Join(d.From, "+")+" "+d.Source.File)
		}
	}
	want := map[int][]string{
		0: {"read path s3://b/a parquet executor logs+SQL plan exec1.log"},
		2: {"read hbase t1  HBase ", "write table db.t  SQL plan "},
	}
	for id := 0; id < 3; id++ {
		if strings.Join(got[id], "|") != strings.Join(want[id], "|") {
			t.Errorf("stage %d: %q, want %q", id, got[id], want[id])
		}
	}
	if d := stages[0].Data[0]; d.Parts != 3 || d.Bytes != 300 {
		t.Errorf("merged read: %+v", d)
	}
	if len(r.Jobs.Missing) != 1 || !strings.Contains(r.Jobs.Missing[0], "2 files or file ranges") {
		t.Errorf("missing: %q", r.Jobs.Missing)
	}
}
