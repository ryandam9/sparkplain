package main

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// stageData runs sparkplain and returns the report and each stage
// attempt's folders and tables, keyed "id.attempt access name".
func stageData(t *testing.T, args ...string) (*model.Report, map[string]model.StageData, string) {
	t.Helper()
	dir := t.TempDir()
	if code, _, errs := runCLI(t, append(args, "-out", dir, "-format", "json,html,explorer")...); code != exitPartial && code != exitOK {
		t.Fatalf("exit %d: %s", code, errs)
	}
	r := readReport(t, dir)
	out := map[string]model.StageData{}
	for _, st := range r.Jobs.Stages {
		for _, d := range st.Data {
			if st.Status == model.StatusSkipped {
				t.Errorf("skipped stage %d has %s %s", st.ID, d.Access, d.Name)
			}
			out[stageKey(st.ID, st.Attempt)+" "+d.Access+" "+d.Name] = d
		}
	}
	return &r, out, dir
}

func stageKey(id, attempt int) string { return strconv.Itoa(id) + "." + strconv.Itoa(attempt) }

// On 0049 the executors' log says which files the write stage wrote and
// the next query read back, with or without the event log; the bytes it
// logged uploading are the event log's output bytes for that stage, and
// the SQL plan names the same folder. The explorer shows them.
func TestStageDataFromLogs(t *testing.T) {
	app := "application_1790380000000_0049"
	for _, withLog := range []bool{false, true} {
		args := []string{"-app-id", app, "-from", filepath.Join(emrlogs, "j-FIXTURE0049CLUSTER")}
		if withLog {
			args = append(args, "-eventlog", filepath.Join(fx, app))
		}
		r, got, dir := stageData(t, args...)
		out := "s3://sparkplain-fixtures/output"
		w, ok := got[stageKey(27, 0)+" write "+out]
		if !ok || w.Parts != 11 || w.Sized != 11 || w.Bytes != 2110707723 || w.Source.Line == 0 {
			t.Errorf("event log %v: stage 27 wrote %+v (all %v)", withLog, w, keys(got))
		}
		rd, ok := got[stageKey(29, 0)+" read "+out]
		if !ok || rd.Parts != 23 || rd.Bytes != 2110707723 {
			t.Errorf("event log %v: stage 29 read %+v", withLog, rd)
		}
		if withLog {
			for _, st := range r.Jobs.Stages {
				if st.ID == 27 && st.Totals.OutputBytes != w.Bytes {
					t.Errorf("stage 27 wrote %d bytes, logged %d", st.Totals.OutputBytes, w.Bytes)
				}
			}
			if !slices.Equal(w.From, []string{"executor logs", "SQL plan"}) || w.Format != "parquet" {
				t.Errorf("write known from %v, format %q", w.From, w.Format)
			}
		}
		x, _ := os.ReadFile(filepath.Join(dir, app+"-explorer.html"))
		if !strings.Contains(string(x), `"executor logs"`) {
			t.Errorf("event log %v: the explorer has no stage data", withLog)
		}
	}
}

// On 0072 a partitioned write's folder is the table's, not each year's,
// with its format; the plan's files read join the folder the logs name.
func TestStageDataPartitionedAndFiles(t *testing.T) {
	app := "application_1790380000000_0072"
	_, got, _ := stageData(t, "-app-id", app, "-from", filepath.Join(emrlogs, "j-FIXTURE0071CLUSTER"), "-eventlog", filepath.Join(fx, app+".zstd"))
	w := got[stageKey(25, 0)+" write s3://sparkplain-fixtures/p4-output/noaa/monthly_by_country"]
	if w.Parts != 6 || w.Format != "parquet" || len(w.From) != 2 {
		t.Errorf("partitioned write: %+v (all %v)", w, keys(got))
	}
	rd := got[stageKey(1, 0)+" read s3://noaa-ghcn-pds/csv/by_year"]
	if rd.Parts != 20 || len(rd.Paths) != 2 || !strings.HasSuffix(rd.Paths[1], "2025.csv") {
		t.Errorf("files read: %+v", rd)
	}
}

// With only the event log, the SQL plans place each table on the stages
// that scanned it (their RDDs carry the scan's name); with the HBase
// cluster's logs, the HBase tables on the stages that read and wrote them.
func TestStageDataFromPlansAndHBase(t *testing.T) {
	_, got, _ := stageData(t, "-app-id", "application_1790380000000_0042", "-eventlog", filepath.Join(fx, "application_1790380000000_0042"))
	if d, ok := got[stageKey(14, 0)+" read spark_catalog.claims.provider_dim"]; !ok || d.Kind != model.DataTable || !slices.Equal(d.From, []string{"SQL plan"}) {
		t.Errorf("table read: %+v (all %v)", d, keys(got))
	}
	if _, ok := got[stageKey(13, 0)+" write file:/mnt/fixture/main/lake/claims_sorted"]; !ok {
		t.Errorf("no write on stage 13: %v", keys(got))
	}
	_, got, _ = stageData(t, "-app-id", "application_1790380000000_0084", "-from", filepath.Join(emrlogs, hbaseCluster))
	if d := got[stageKey(0, 0)+" read sp_orders"]; d.Kind != model.DataHBase || d.Parts != 5 {
		t.Errorf("HBase read: %+v (all %v)", d, keys(got))
	}
	if d := got[stageKey(1, 0)+" write sp_totals"]; d.Kind != model.DataHBase {
		t.Errorf("HBase write: %+v", d)
	}
}

func keys(m map[string]model.StageData) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
