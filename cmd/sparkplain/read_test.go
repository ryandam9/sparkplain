package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/ryandam9/sparkplain/internal/sparkplain/awsmeta"
	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/source"
)

// The read: block turns sources off with yes or no (true, false, on and
// off too); an environment's block overrides the top level's, a flag wins
// over both, and each source says what turned it off.
func TestReadSwitchesInConfig(t *testing.T) {
	cfg := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(cfg, []byte(`read:
  cloudtrail: no
  cloudwatch: yes
  node-logs: off
environments:
  prod:
    read:
      step-logs: no
      node-logs: yes
`), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := loadConfig(cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	if c, err = c.withEnv("prod"); err != nil {
		t.Fatal(err)
	}
	o := options{noHBaseLogs: true}
	o.applyReads(c.Read)
	want := map[string]string{
		"Step logs":         "turned off in the config file's prod environment (read: step-logs: no)",
		"HBase server logs": "turned off with -no-hbase-logs",
		"CloudTrail":        "turned off in the config file (read: cloudtrail: no)",
	}
	if len(o.off) != len(want) || !o.noStepLogs || o.noNodeLogs || !o.noCloudTrail || o.noCloudWatch {
		t.Errorf("off = %v (steps %v nodes %v cloudtrail %v cloudwatch %v)", o.off, o.noStepLogs, o.noNodeLogs, o.noCloudTrail, o.noCloudWatch)
	}
	for k, v := range want {
		if o.off[k] != v {
			t.Errorf("%s: %q, want %q", k, o.off[k], v)
		}
	}
	// Anything but yes or no stops the run.
	if err := os.WriteFile(cfg, []byte("read:\n  step-logs: maybe\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(cfg, true); err == nil || !strings.Contains(err.Error(), `"maybe" is not yes or no`) {
		t.Errorf("bad value: %v", err)
	}
}

// Sources turned off are neither checked nor read: the access check marks
// them grey with the reason, the Sources list says "not requested", and
// no step or node log is read.
func TestReadSwitchesOnline(t *testing.T) {
	bucket, stub := hbaseCluster0083(t, true)
	fakeAWS(t, map[string]string{"logs": bucket}, stub.clusters)
	awsDeps.emr = func(aws.Config) awsmeta.EMRAPI { return stub }
	cfg := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(cfg, []byte("read:\n  step-logs: no\n  node-logs: no\n  cloudtrail: no\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	_, stdout, errs := runCLI(t, "-app-id", "application_1790380000000_0092", "-profile", "test", "-cluster-id", hbaseCluster, "-config", cfg,
		"-no-hbase-logs", "-no-cloudwatch", "-eventlog", filepath.Join(fx, "application_1790380000000_0092.zstd"), "-format", "json", "-out", out)
	for _, w := range []string{
		"- Step logs Not asked for: turned off in the config file (read: step-logs: no).",
		"- Node logs Not asked for: turned off in the config file (read: node-logs: no).",
		"- HBase server logs Not asked for: turned off with -no-hbase-logs.",
		"- CloudTrail Not asked for: turned off in the config file (read: cloudtrail: no).",
		"- CloudWatch Not asked for: turned off with -no-cloudwatch.",
	} {
		if !strings.Contains(flat(stdout), w) {
			t.Errorf("access check lacks %q:\n%s%s", w, stdout, errs)
		}
	}
	r := readReport(t, out)
	got := map[string]model.SourceStatus{}
	for _, s := range r.Sources {
		got[s.Name] = s
	}
	for name, why := range map[string]string{"Step logs": "read: step-logs: no", "Node logs": "read: node-logs: no", "HBase server logs": "-no-hbase-logs", "CloudTrail": "read: cloudtrail: no"} {
		if s := got[name]; s.Status != "not-requested" || !strings.Contains(s.Detail, why) {
			t.Errorf("%s: %s %q", name, s.Status, s.Detail)
		}
	}
	if s := got["Container logs"]; s.Status != "read" {
		t.Errorf("container logs: %s %q", s.Status, s.Detail)
	}
	for _, f := range r.Logs.Files {
		if f.Step != "" || f.Instance != "" {
			t.Errorf("read %s, which was turned off", f.Location)
		}
	}
}

// With node logs and CloudWatch off, the nodes that ran the application
// are still read: their NodeManager logs (what YARN offered on each) and
// their own CloudWatch metrics, and nothing of any other node. Without the
// event log, its container logs say which nodes those are.
func TestReadSwitchesKeepAppNodes(t *testing.T) {
	for _, withLog := range []bool{true, false} {
		t.Run(map[bool]string{true: "with the event log", false: "without"}[withLog], func(t *testing.T) { keepAppNodes(t, withLog) })
	}
}

func keepAppNodes(t *testing.T, withLog bool) {
	rec := replayLive(t, "j-FIXTURE0062CLUSTER")
	if !withLog {
		bucket, _, _ := source.ParseS3(aws.ToString(rec.Cluster.LogUri))
		store := routeStore{bucket: bucket, routes: map[string]string{"emr-logs": emrlogs}} // no spark-events: no event log
		awsDeps.s3 = func(_ context.Context, _ aws.Config, b string) (source.Store, error) { return store, nil }
	}
	cfg := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(cfg, []byte("read:\n  node-logs: no\n  cloudwatch: no\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	runCLI(t, "-app-id", "application_1790380000000_0062", "-cluster-id", "j-FIXTURE0062CLUSTER", "-profile", "test", "-config", cfg, "-format", "json", "-out", out)
	r := readReport(t, out)
	if rebuilt := r.EventLog != nil && r.EventLog.Layout == model.LayoutRebuilt; rebuilt == withLog {
		t.Fatalf("event log %v: layout %+v", withLog, r.EventLog)
	}
	got := map[string]model.SourceStatus{}
	for _, s := range r.Sources {
		got[s.Name] = s
	}
	if s := got["Node logs"]; s.Status != "read" || !strings.Contains(s.Detail, "that ran this application were read anyway") {
		t.Errorf("event log %v: Node logs: %s %q", withLog, s.Status, s.Detail)
	}
	if s := got["CloudWatch"]; s.Status == "not-requested" || !strings.Contains(s.Detail, "were read anyway; nothing else") {
		t.Errorf("event log %v: CloudWatch: %s %q", withLog, s.Status, s.Detail)
	}
	ran := map[string]bool{}
	for _, h := range r.Nodes.Hosts {
		if h.Instance != nil && (len(h.Executors) > 0 || h.DriverContainerBytes > 0) {
			ran[h.Instance.ID] = true
		}
	}
	if len(ran) == 0 {
		t.Fatalf("event log %v: no node ran the application", withLog)
	}
	nodeLogs := 0
	for _, f := range r.Logs.Files {
		if f.Instance != "" && (f.Kind == "nodemanager" || f.Kind == "resourcemanager") {
			nodeLogs++
			if !ran[f.Instance] {
				t.Errorf("event log %v: read %s, of a node that did not run the application", withLog, f.Location)
			}
		}
	}
	if nodeLogs == 0 {
		t.Errorf("event log %v: read no NodeManager log of the application's nodes", withLog)
	}
	if r.Metrics == nil || len(r.Metrics.Cluster) != 0 || len(r.Metrics.Hosts) == 0 {
		t.Fatalf("event log %v: metrics: %+v", withLog, r.Metrics)
	}
	for _, s := range r.Metrics.Hosts {
		if !ran[s.Scope] {
			t.Errorf("event log %v: CloudWatch series of %s, which did not run the application", withLog, s.Scope)
		}
	}
}
