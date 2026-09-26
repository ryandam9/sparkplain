package main

import (
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emr/types"

	"github.com/ryandam9/sparkplain/internal/sparkplain/awsmeta"
	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

const emrlogs = "../../testdata/emrlogs"

func readReport(t *testing.T, dir string) model.Report {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var r model.Report
	if err := json.Unmarshal(b, &r); err != nil {
		t.Fatal(err)
	}
	return r
}

func sourceOf(r model.Report, name string) model.SourceStatus {
	for _, s := range r.Sources {
		if s.Name == name {
			return s
		}
	}
	return model.SourceStatus{}
}

func TestFromFolder(t *testing.T) {
	for _, tc := range []struct {
		name, from, app, eventlog string
		code                      int
	}{
		{"folder of cluster folders", emrlogs, "application_1790380000000_0049", filepath.Join(fx, "application_1790380000000_0049"), exitOK},
		{"one cluster's folder", filepath.Join(emrlogs, "j-FIXTURE0050CLUSTER"), "application_1790380000000_0050", filepath.Join(fx, "application_1790380000000_0050"), exitOK},
		{"no event log", emrlogs, "application_1790380000000_0052", "", exitPartial},
		{"one application's containers", filepath.Join(emrlogs, "j-FIXTURE0050CLUSTER", "containers", "application_1790380000000_0050"), "application_1790380000000_0050", filepath.Join(fx, "application_1790380000000_0050"), exitOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			args := []string{"-app-id", tc.app, "-from", tc.from, "-out", dir, "-format", "json,html,explorer"}
			if tc.eventlog != "" {
				args = append(args, "-eventlog", tc.eventlog)
			}
			code, _, errs := runCLI(t, args...)
			if code != tc.code {
				t.Fatalf("exit %d, want %d: %s", code, tc.code, errs)
			}
			r := readReport(t, dir)
			if r.Mode != "offline-logs" || r.Logs == nil || len(r.Logs.Files) == 0 {
				t.Fatalf("mode %q, logs %+v", r.Mode, r.Logs)
			}
			if s := sourceOf(r, "Container logs"); s.Status != "read" || len(s.Files) == 0 {
				t.Errorf("containers = %+v", s)
			}
			if s := sourceOf(r, "EMR API"); s.Status != "not-requested" {
				t.Errorf("EMR API = %+v", s)
			}
			// The 0049 step's command carries a planted password.
			for _, name := range []string{"report.json", "report.html", "explorer.html"} {
				if b, _ := os.ReadFile(filepath.Join(dir, name)); strings.Contains(string(b), "FAKE-EMR-PASSWORD") {
					t.Errorf("%s holds the planted password", name)
				}
			}
		})
	}
	if code, _, errs := runCLI(t, "-app-id", "application_1790380000000_0049", "-from", t.TempDir()); code != exitFatal || !strings.Contains(errs, "no cluster log folder") {
		t.Errorf("empty -from: exit %d, %s", code, errs)
	}
	if code, _, errs := runCLI(t, "-app-id", "application_1790380000000_0049", "-from", emrlogs, "-cluster-id", "j-1", "-profile", "x"); code != exitFatal || !strings.Contains(errs, "one or the other") {
		t.Errorf("-from with -cluster-id: exit %d, %s", code, errs)
	}
}

// copyTree copies a fixture tree under dst, so a local "bucket" holds it
// under the cluster's log prefix.
func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		out := filepath.Join(dst, rel)
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			return err
		}
		return os.WriteFile(out, b, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// An online run reads the logs under the cluster's log URI, finds the
// application's step among the cluster's steps, and reads only the nodes
// the event log names plus the primary node.
func TestOnlineReadsClusterLogs(t *testing.T) {
	bucket := t.TempDir()
	copyTree(t, filepath.Join(emrlogs, "j-FIXTURE0049CLUSTER"), filepath.Join(bucket, "emr", "j-FIXTURE0049CLUSTER"))
	// A node that did not run the application: its logs must not be read.
	copyTree(t, filepath.Join(emrlogs, "j-FIXTURE0050CLUSTER", "node", "i-0fee0000000000002"), filepath.Join(bucket, "emr", "j-FIXTURE0049CLUSTER", "node", "i-0fee0000000000009"))
	// The job set spark.eventLog.dir in its own arguments, not in the
	// cluster's configuration.
	ev, _ := os.ReadFile(filepath.Join(fx, "application_1790380000000_0049"))
	os.MkdirAll(filepath.Join(bucket, "job-events"), 0o755)
	os.WriteFile(filepath.Join(bucket, "job-events", "application_1790380000000_0049"), ev, 0o644)
	cl := cluster("j-FIXTURE0049CLUSTER", "")
	cl.MasterPublicDnsName = aws.String("ip-10-0-2-11.us-east-1.compute.internal")
	fakeAWS(t, map[string]string{"logs": bucket}, map[string]*emrtypes.Cluster{"j-FIXTURE0049CLUSTER": cl})
	t0 := time.Date(2026, 9, 26, 7, 40, 0, 0, time.UTC)
	step := func(id string, start, end time.Time, args ...string) emrtypes.StepSummary {
		return emrtypes.StepSummary{Id: aws.String(id), Name: aws.String(id), Config: &emrtypes.HadoopStepConfig{Jar: aws.String("command-runner.jar"), Args: args},
			Status: &emrtypes.StepStatus{State: emrtypes.StepStateCompleted, Timeline: &emrtypes.StepTimeline{StartDateTime: aws.Time(start), EndDateTime: aws.Time(end)}}}
	}
	inst := func(id, dns string) emrtypes.Instance {
		return emrtypes.Instance{Ec2InstanceId: aws.String(id), PrivateDnsName: aws.String(dns), PrivateIpAddress: aws.String("10.0.2.99"),
			Status: &emrtypes.InstanceStatus{Timeline: &emrtypes.InstanceTimeline{CreationDateTime: aws.Time(t0.Add(-time.Hour))}}}
	}
	saved := awsDeps.emr
	awsDeps.emr = func(c aws.Config) awsmeta.EMRAPI {
		s := saved(c).(stubEMR)
		s.steps = []emrtypes.StepSummary{
			step("s-FIXTURESTEP0002", t0.Add(3*time.Hour), t0.Add(3*time.Hour+time.Minute)), // after the app: not searched
			step("s-FIXTURESTEP0001", t0, t0.Add(5*time.Minute), "spark-submit", "--conf", "spark.eventLog.dir=s3://logs/job-events/", "s3://code/emr_job.py"),
		}
		s.instances = []emrtypes.Instance{inst("i-0fee0000000000001", "ip-10-0-2-10.us-east-1.compute.internal"),
			inst("i-0fee0000000000002", "ip-10-0-2-11.us-east-1.compute.internal"), inst("i-0fee0000000000009", "ip-10-0-2-77.us-east-1.compute.internal")}
		return s
	}
	dir := t.TempDir()
	code, _, errs := runCLI(t, "-app-id", "application_1790380000000_0049", "-cluster-id", "j-FIXTURE0049CLUSTER", "-profile", "test",
		"-out", dir, "-format", "json")
	if code != exitOK {
		t.Fatalf("exit %d: %s", code, errs)
	}
	if !strings.Contains(errs, "using the spark.eventLog.dir a step set, s3://logs/job-events/") {
		t.Errorf("the event log should come from the step's arguments: %s", errs)
	}
	r := readReport(t, dir)
	if r.Mode != "online" || r.Cluster == nil || len(r.Cluster.Instances) != 3 || !r.Cluster.Instances[1].Primary {
		t.Fatalf("cluster = %+v", r.Cluster)
	}
	if s := sourceOf(r, "EMR API"); s.Status != "read" || !strings.Contains(s.Detail, "ListSteps (2 steps), ListInstances (3 instances)") {
		t.Errorf("EMR API = %+v", s)
	}
	var matched []string
	for _, s := range r.Steps {
		if s.AppID != "" {
			matched = append(matched, s.ID)
		}
	}
	if strings.Join(matched, ",") != "s-FIXTURESTEP0001" {
		t.Errorf("steps that submitted the app = %v", matched)
	}
	for _, f := range r.Logs.Files {
		if f.Instance == "i-0fee0000000000009" || f.Step == "s-FIXTURESTEP0002" {
			t.Errorf("read a log the application did not use: %s", f.Location)
		}
		if !strings.HasPrefix(f.Location, filepath.Join(bucket, "emr", "j-FIXTURE0049CLUSTER")+"/") { // the stub bucket is a local folder
			t.Errorf("location %q", f.Location)
		}
	}
	for _, name := range []string{"Container logs", "Step logs", "Node logs"} {
		if s := sourceOf(r, name); s.Status != "read" {
			t.Errorf("%s = %+v", name, s)
		}
	}
}

func TestShortHost(t *testing.T) {
	for in, want := range map[string]string{"ip-10-0-2-10.ec2.internal": "ip-10-0-2-10", "IP-10-0-2-10.us-east-1.compute.internal": "ip-10-0-2-10", "10.0.2.10": "ip-10-0-2-10", "host": "host"} {
		if got := shortHost(in); got != want {
			t.Errorf("shortHost(%q) = %q, want %q", in, got, want)
		}
	}
}
