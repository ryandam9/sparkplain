package report

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

func TestExplorerCarriesClusterData(t *testing.T) {
	r, x := buildWithExplorer(t, "application_1790380000000_0042")
	at := time.Unix(1_790_000_000, 0).UTC()
	avg, peak := 42.0, 90.0
	r.Cluster = &model.Cluster{ID: "j-1", Groups: []model.InstanceGroup{{ID: "ig-1", Role: "CORE", InstanceTypes: []string{"m5.xlarge"}, Requested: 2}}}
	r.Nodes.Hosts = append(r.Nodes.Hosts, model.Host{Name: "ip-10-0-0-9", Instance: &model.Instance{ID: "i-9", Role: "CORE"}, YARNMemoryBytes: 12 << 30,
		ExecutorContainerBytes: 3 << 30, PeakExecutors: 2, DriverContainerBytes: 1 << 30, HostCPU: &model.HostCPU{Average: avg, Peak: peak}})
	r.Metrics = &model.MetricsSection{From: at, To: at.Add(time.Hour), Cluster: []model.Series{{Name: "ContainerPending", Stat: "Maximum", Scope: "j-1", Points: []model.Point{{T: at, V: 3}}},
		{Name: "AppsRunning", Stat: "Maximum", Scope: "j-1"}}}
	r.AWSCalls = &model.AWSCallsSection{Users: []string{"i-9"}, Events: 1, Calls: []model.AWSCall{{Service: "sts.amazonaws.com", Action: "AssumeRole", Count: 1, Errors: 1}},
		Denied: []model.AWSEvent{{Service: "sts.amazonaws.com", Action: "AssumeRole", ErrorCode: "AccessDenied", Message: "</script><script>alert(3)</script>"}}}
	page := renderExplorer(t, r, x)
	if strings.Contains(page, "<script>alert(3)") {
		t.Fatal("a CloudTrail message reached the page unescaped")
	}
	a := embedded(t, page)["aws"].(map[string]any)
	nodes := a["nodes"].([]any)
	last := nodes[len(nodes)-1].(map[string]any)
	// The node memory chart stacks these against yarnMem.
	if last["cpuAvg"].(float64) != 42 || last["yarnMem"].(float64) != float64(12<<30) ||
		last["execMem"].(float64) != float64(3<<30) || last["peakExecs"].(float64) != 2 || last["driverMem"].(float64) != float64(1<<30) {
		t.Errorf("node = %v", last)
	}
	if ms := a["metrics"].([]any); len(ms) != 1 || ms[0].(map[string]any)["name"] != "ContainerPending" {
		t.Errorf("metrics (empty series dropped) = %v", ms)
	}
	if len(a["denied"].([]any)) != 1 || len(a["groups"].([]any)) != 1 {
		t.Errorf("aws = %v", a)
	}
}

// A -source path sparkplain may not read becomes a note, not an error.
func TestLoadSourcesWithoutPermission(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root reads every folder")
	}
	parent := filepath.Join(t.TempDir(), "locked")
	if err := os.Mkdir(parent, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(parent, 0o755) })
	r, _ := buildWithExplorer(t, "application_1790380000000_0042")
	_, notes, err := LoadSources(r, []string{filepath.Join(parent, "job.py")})
	if err != nil || len(notes) == 0 || !strings.Contains(notes[0], "No permission to read") {
		t.Errorf("err %v, notes %v", err, notes)
	}
}
