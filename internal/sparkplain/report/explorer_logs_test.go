package report

import (
	"strings"
	"testing"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

func TestExplorerCarriesLogs(t *testing.T) {
	r, x := buildWithExplorer(t, "application_1790380000000_0042")
	evil := `</script><script>alert(2)</script>`
	var many []model.LogLine
	for i := 0; i < maxLogLinesPerFile+5; i++ {
		many = append(many, model.LogLine{Kind: model.LogError, Severity: model.Info, Source: model.Source{File: "local/stderr", Line: int64(i + 1)}, Text: "x", Count: 1})
	}
	r.Logs = &model.LogsSection{Files: []model.LogFile{
		{Location: "s3://logs/emr/j-1/containers/application_1_1/container_1_1_01_000002/stderr.gz", Kind: "container-stderr", Executor: "1", Lines: 10,
			Found: []model.LogLine{{Kind: model.LogException, Severity: model.Critical, Source: model.Source{File: "s3://…", Line: 7, EndLine: 9}, Text: evil,
				Detail: []string{"java.lang.IllegalStateException: boom"}, Fields: map[string]string{"root": "java.lang.IllegalStateException"}, Count: 2, LastLine: 20}}},
		{Location: "local/stderr", Kind: "container-stderr", Found: many},
	}}
	r.Sources = append(r.Sources, model.SourceStatus{Name: "Container logs", Status: "partial", Files: []model.SourceFile{
		{Location: "s3://logs/x/directory.info.gz", Status: "skipped", Detail: "not a log sparkplain reads"}, {Location: "s3://logs/y/stderr.gz", Status: "read"}}})
	r.Cluster = &model.Cluster{ID: "j-1", Release: "emr-7.3.0", Instances: []model.Instance{{ID: "i-1", Primary: true}}}
	page := renderExplorer(t, r, x)
	if strings.Contains(page, "<script>alert(2)") {
		t.Fatal("a log line reached the page unescaped")
	}
	d := embedded(t, page)
	logs := d["logs"].([]any)
	first := logs[0].(map[string]any)
	if first["href"] != "https://s3.console.aws.amazon.com/s3/object/logs?prefix=emr%2Fj-1%2Fcontainers%2Fapplication_1_1%2Fcontainer_1_1_01_000002%2Fstderr.gz" || first["exec"] != "1" {
		t.Errorf("file = %v", first)
	}
	row := first["found"].([]any)[0].([]any)
	if row[0] != "exception" || row[5] != evil || row[7].(float64) != 2 {
		t.Errorf("row = %v", row)
	}
	second := logs[1].(map[string]any)
	if len(second["found"].([]any)) != maxLogLinesPerFile || second["cut"].(float64) != 5 || second["href"] != nil {
		t.Errorf("cap: %d found, cut %v", len(second["found"].([]any)), second["cut"])
	}
	srcs := d["logSources"].([]any)
	skipped := srcs[len(srcs)-1].(map[string]any)["skipped"].([]any)
	if len(skipped) != 1 {
		t.Errorf("only skipped and unreadable objects are listed: %v", skipped)
	}
	if d["cluster"].(map[string]any)["id"] != "j-1" {
		t.Errorf("cluster = %v", d["cluster"])
	}
}
