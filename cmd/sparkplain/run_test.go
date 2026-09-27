package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	cttypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/aws/smithy-go"

	"github.com/ryandam9/sparkplain/internal/sparkplain/awsmeta"
	"github.com/ryandam9/sparkplain/internal/sparkplain/source"
)

const fx = "../../testdata/eventlog"

func runCLI(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	// Never pick up a real config file from the machine running the tests.
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("HOME", t.TempDir())
	code := run(args, &out, &errb)
	return code, out.String(), errb.String()
}

func TestRunWritesBothReports(t *testing.T) {
	dir := t.TempDir()
	code, out, errs := runCLI(t, "-app-id", "application_1790380000000_0042", "-eventlog", filepath.Join(fx, "application_1790380000000_0042.zip"), "-out", dir)
	if code != exitOK {
		t.Fatalf("exit %d: %s %s", code, out, errs)
	}
	html, err := os.ReadFile(outPath(dir, "report.html"))
	if err != nil {
		t.Fatal(err)
	}
	js, err := os.ReadFile(outPath(dir, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var r struct {
		SchemaVersion string
		ExitCode      int
		Application   struct{ ID string }
	}
	if err := json.Unmarshal(js, &r); err != nil || r.SchemaVersion == "" || r.Application.ID != "application_1790380000000_0042" {
		t.Fatalf("json %+v %v", r, err)
	}
	secret := regexp.MustCompile(`FAKE-[A-Z0-9-]+|AKIAIOSFODNN7EXAMPLE`)
	for name, b := range map[string][]byte{"html": html, "json": js, "stdout": []byte(out), "stderr": []byte(errs)} {
		if m := secret.Find(b); m != nil {
			t.Errorf("%s leaks %s", name, m)
		}
	}
}

func TestRunExitCodes(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name string
		args []string
		code int
		msg  string
	}{
		{"missing app id", []string{"-eventlog", fx}, exitFatal, "-app-id is required"},
		{"bad app id", []string{"-app-id", "../../etc", "-eventlog", fx}, exitFatal, "does not look like"},
		{"no event log", []string{"-app-id", "application_1_2"}, exitFatal, "pass -eventlog"},
		{"not found", []string{"-app-id", "application_1_2", "-eventlog", fx, "-out", dir}, exitFatal, "no event log for"},
		{"wrong app", []string{"-app-id", "application_1790380000000_0044", "-eventlog", filepath.Join(fx, "application_1790380000000_0042"), "-out", dir}, exitFatal, "belongs to application_1790380000000_0042"},
		{"bad format", []string{"-app-id", "application_1_2", "-eventlog", fx, "-format", "pdf"}, exitFatal, "-format"},
		{"in progress is partial", []string{"-app-id", "application_1790380000000_0045", "-eventlog", fx, "-out", dir}, exitPartial, "partial report"},
		{"version", []string{"-version"}, exitOK, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			code, out, errs := runCLI(t, c.args...)
			if code != c.code || !strings.Contains(errs+out, c.msg) {
				t.Errorf("exit %d (want %d), output %q", code, c.code, errs+out)
			}
		})
	}
}

func TestCorruptEventLogDegrades(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "application_1_2.lz4")
	os.WriteFile(bad, []byte("this is not lz4 at all"), 0o644)
	out := filepath.Join(dir, "out")
	code, _, errs := runCLI(t, "-app-id", "application_1_2", "-eventlog", bad, "-out", out)
	if code != exitPartial {
		t.Fatalf("exit %d: %s", code, errs)
	}
	html, err := os.ReadFile(outPath(out, "report.html"))
	if err != nil || !bytes.Contains(html, []byte("Could not read")) {
		t.Fatalf("degraded report missing: %v", err)
	}
}

func TestConfigFileThresholdsAndFormat(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	os.WriteFile(cfg, []byte("format: json\ntimezone: Australia/Sydney\nthresholds:\n  skew-ratio: 100\n  skew-min-task: 1s\n"), 0o644)
	out := filepath.Join(dir, "out")
	code, _, errs := runCLI(t, "-config", cfg, "-app-id", "application_1790380000000_0042", "-eventlog", filepath.Join(fx, "application_1790380000000_0042"), "-out", out)
	if code != exitOK {
		t.Fatalf("exit %d: %s", code, errs)
	}
	if _, err := os.Stat(outPath(out, "report.html")); err == nil {
		t.Error("format json should not write html")
	}
	js, _ := os.ReadFile(outPath(out, "report.json"))
	if bytes.Contains(js, []byte(`"rule": "stage-skew"`)) {
		t.Error("skew-ratio 100 from the config file should silence the skew rule")
	}
	if !bytes.Contains(js, []byte(`"timeZone": "Australia/Sydney"`)) {
		t.Error("timezone from config not applied")
	}

	os.WriteFile(cfg, []byte("unknown-key: 1\n"), 0o644)
	if code, _, errs := runCLI(t, "-config", cfg, "-app-id", "application_1_2", "-eventlog", fx); code != exitFatal || !strings.Contains(errs, "unknown-key") {
		t.Errorf("unknown config keys should be rejected: %d %s", code, errs)
	}
	if code, _, _ := runCLI(t, "-config", filepath.Join(dir, "missing.yaml"), "-app-id", "application_1_2", "-eventlog", fx); code != exitFatal {
		t.Error("an explicit missing config file is an error")
	}
}

func TestParseSize(t *testing.T) {
	for in, want := range map[string]int64{"10GiB": 10 << 30, "500mb": 500 << 20, "2g": 2 << 30, "1024": 1024, "x": -1, "5 GB": 5 << 30} {
		if got := parseSize(in); got != want {
			t.Errorf("parseSize(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestFormats(t *testing.T) {
	log := filepath.Join(fx, "application_1790380000000_0042")
	for _, c := range []struct {
		format string
		want   []string
	}{
		{"", []string{"report.html", "report.json", "explorer.html"}},
		{"both", []string{"report.html", "report.json"}},
		{"explorer", []string{"explorer.html"}},
		{"html, explorer", []string{"report.html", "explorer.html"}},
	} {
		for i, w := range c.want {
			c.want[i] = "application_1790380000000_0042-" + w // named after the application
		}
		dir := t.TempDir()
		args := []string{"-app-id", "application_1790380000000_0042", "-eventlog", log, "-out", dir}
		if c.format != "" {
			args = append(args, "-format", c.format)
		}
		if code, _, errs := runCLI(t, args...); code != exitOK {
			t.Fatalf("-format %q: exit %d: %s", c.format, code, errs)
		}
		entries, _ := os.ReadDir(dir)
		var got []string
		for _, e := range entries {
			got = append(got, e.Name())
		}
		if len(got) != len(c.want) {
			t.Errorf("-format %q wrote %v, want %v", c.format, got, c.want)
		}
		for _, w := range c.want {
			if _, err := os.Stat(filepath.Join(dir, w)); err != nil {
				t.Errorf("-format %q: %s missing", c.format, w)
			}
		}
		html, _ := os.ReadFile(outPath(dir, "report.html"))
		ex, _ := os.ReadFile(outPath(dir, "explorer.html"))
		hasX, hasR := slices.Contains(c.want, "application_1790380000000_0042-explorer.html"), slices.Contains(c.want, "application_1790380000000_0042-report.html")
		if hasR && bytes.Contains(html, []byte(`href="application_1790380000000_0042-explorer.html"`)) != hasX {
			t.Errorf("-format %q: report links to the explorer = %v, want %v", c.format, !hasX, hasX)
		}
		if hasX && bytes.Contains(ex, []byte(`"reportHref":"application_1790380000000_0042-report.html"`)) != hasR {
			t.Errorf("-format %q: explorer links to the report = %v, want %v", c.format, !hasR, hasR)
		}
	}
	if code, _, errs := runCLI(t, "-app-id", "application_1_2", "-eventlog", log, "-format", "pdf"); code != exitFatal || !strings.Contains(errs, `"pdf"`) {
		t.Errorf("unknown format: exit %d, %s", code, errs)
	}
}

func TestShowFlag(t *testing.T) {
	log := filepath.Join(fx, "application_1790380000000_0042")
	code, out, errs := runCLI(t, "-app-id", "application_1790380000000_0042", "-eventlog", log, "-show", "application_1790380000000_0042:4")
	if code != exitOK || !strings.Contains(out, "SparkListenerEnvironmentUpdate") || strings.Contains(out, "FAKE-") {
		t.Errorf("exit %d, stderr %s, stdout %.120s", code, errs, out)
	}
	if code, _, _ := runCLI(t, "-app-id", "application_1790380000000_0042", "-eventlog", log, "-show", "x:999999"); code != exitFatal {
		t.Errorf("a missing line should exit 2, got %d", code)
	}
}

func TestSourceFlag(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(fx, "application_1790380000000_0051")
	code, _, errs := runCLI(t, "-app-id", "application_1790380000000_0051", "-eventlog", log, "-out", dir, "-source", "../../scripts/fixtures/java")
	if code != exitOK {
		t.Fatalf("exit %d: %s", code, errs)
	}
	page, _ := os.ReadFile(outPath(dir, "explorer.html"))
	if !bytes.Contains(page, []byte("writeTotals(byProvider, args[0]);")) {
		t.Error("the Java source should be embedded")
	}
	if code, _, _ := runCLI(t, "-app-id", "application_1790380000000_0051", "-eventlog", log, "-out", dir, "-source", "/no/such"); code != exitFatal {
		t.Errorf("a missing -source path should exit 2, got %d", code)
	}
}

// stubEMR answers DescribeCluster from a map. Tests never call real AWS.
type stubEMR struct {
	clusters  map[string]*emrtypes.Cluster
	steps     []emrtypes.StepSummary
	instances []emrtypes.Instance
	groups    []emrtypes.InstanceGroup
}

func (s stubEMR) DescribeCluster(_ context.Context, in *emr.DescribeClusterInput, _ ...func(*emr.Options)) (*emr.DescribeClusterOutput, error) {
	c, ok := s.clusters[aws.ToString(in.ClusterId)]
	if !ok {
		return nil, &smithy.GenericAPIError{Code: "InvalidRequestException", Message: "Cluster id is not valid."}
	}
	return &emr.DescribeClusterOutput{Cluster: c}, nil
}
func (stubEMR) ListClusters(context.Context, *emr.ListClustersInput, ...func(*emr.Options)) (*emr.ListClustersOutput, error) {
	return &emr.ListClustersOutput{}, nil
}
func (s stubEMR) ListSteps(context.Context, *emr.ListStepsInput, ...func(*emr.Options)) (*emr.ListStepsOutput, error) {
	return &emr.ListStepsOutput{Steps: s.steps}, nil
}
func (s stubEMR) ListInstances(context.Context, *emr.ListInstancesInput, ...func(*emr.Options)) (*emr.ListInstancesOutput, error) {
	return &emr.ListInstancesOutput{Instances: s.instances}, nil
}
func (s stubEMR) ListInstanceGroups(context.Context, *emr.ListInstanceGroupsInput, ...func(*emr.Options)) (*emr.ListInstanceGroupsOutput, error) {
	return &emr.ListInstanceGroupsOutput{InstanceGroups: s.groups}, nil
}
func (s stubEMR) ListInstanceFleets(context.Context, *emr.ListInstanceFleetsInput, ...func(*emr.Options)) (*emr.ListInstanceFleetsOutput, error) {
	return &emr.ListInstanceFleetsOutput{}, nil
}
func (s stubEMR) DescribeSecurityConfiguration(_ context.Context, in *emr.DescribeSecurityConfigurationInput, _ ...func(*emr.Options)) (*emr.DescribeSecurityConfigurationOutput, error) {
	return nil, &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "not authorized to perform: elasticmapreduce:DescribeSecurityConfiguration"}
}
func (s stubEMR) DescribeStep(_ context.Context, in *emr.DescribeStepInput, _ ...func(*emr.Options)) (*emr.DescribeStepOutput, error) {
	return &emr.DescribeStepOutput{Step: &emrtypes.Step{Id: in.StepId}}, nil
}

// stubCloudWatch answers every query with one point an hour after the
// fixtures' runs, and lists no agent metrics.
type stubCloudWatch struct{}

func (stubCloudWatch) ListMetrics(context.Context, *cloudwatch.ListMetricsInput, ...func(*cloudwatch.Options)) (*cloudwatch.ListMetricsOutput, error) {
	return &cloudwatch.ListMetricsOutput{}, nil
}
func (stubCloudWatch) GetMetricData(_ context.Context, in *cloudwatch.GetMetricDataInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricDataOutput, error) {
	out := &cloudwatch.GetMetricDataOutput{}
	for _, q := range in.MetricDataQueries {
		out.MetricDataResults = append(out.MetricDataResults, cwtypes.MetricDataResult{Id: q.Id, StatusCode: cwtypes.StatusCodeComplete,
			Timestamps: []time.Time{aws.ToTime(in.StartTime).Add(time.Minute)}, Values: []float64{1}})
	}
	return out, nil
}

// stubCloudTrail answers each node's lookup with one call AWS allowed and
// one it refused.
type stubCloudTrail struct{}

func (stubCloudTrail) LookupEvents(_ context.Context, in *cloudtrail.LookupEventsInput, _ ...func(*cloudtrail.Options)) (*cloudtrail.LookupEventsOutput, error) {
	user := aws.ToString(in.LookupAttributes[0].AttributeValue)
	at := aws.ToTime(in.StartTime).Add(time.Minute)
	return &cloudtrail.LookupEventsOutput{Events: []cttypes.Event{
		{EventId: aws.String("ok-" + user), EventName: aws.String("GetCallerIdentity"), EventSource: aws.String("sts.amazonaws.com"), EventTime: aws.Time(at), Username: aws.String(user),
			ReadOnly: aws.String("true"), CloudTrailEvent: aws.String(`{"eventVersion":"1.08"}`)},
		{EventId: aws.String("denied-" + user), EventName: aws.String("AssumeRole"), EventSource: aws.String("sts.amazonaws.com"), EventTime: aws.Time(at), Username: aws.String(user),
			ReadOnly: aws.String("true"), CloudTrailEvent: aws.String(`{"errorCode":"AccessDenied","errorMessage":"User: arn:aws:sts::000000000000:assumed-role/EMR_EC2_DefaultRole/` + user + ` is not authorized to perform: sts:AssumeRole on resource: arn:aws:iam::000000000000:role/nope","userIdentity":{"sessionContext":{"sessionIssuer":{"arn":"arn:aws:iam::000000000000:role/EMR_EC2_DefaultRole"}}}}`),
			Resources: []cttypes.Resource{{ResourceName: aws.String("arn:aws:iam::000000000000:role/nope"), ResourceType: aws.String("AWS::IAM::Role")}}},
	}}, nil
}

// stubEC2 answers DescribeInstanceTypes for m5.xlarge only.
type stubEC2 struct{}

func (stubEC2) DescribeInstanceTypes(_ context.Context, in *ec2.DescribeInstanceTypesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error) {
	out := &ec2.DescribeInstanceTypesOutput{}
	for _, t := range in.InstanceTypes {
		if t == "m5.xlarge" {
			out.InstanceTypes = append(out.InstanceTypes, ec2types.InstanceTypeInfo{InstanceType: t, VCpuInfo: &ec2types.VCpuInfo{DefaultVCpus: aws.Int32(4)},
				MemoryInfo: &ec2types.MemoryInfo{SizeInMiB: aws.Int64(16384)}})
		}
	}
	return out, nil
}

// fakeAWS points the CLI's AWS at local folders (one per bucket) and a
// stubbed EMR, and restores it after the test.
func fakeAWS(t *testing.T, buckets map[string]string, clusters map[string]*emrtypes.Cluster) {
	t.Helper()
	saved := awsDeps
	t.Cleanup(func() { awsDeps = saved })
	awsDeps.config = func(context.Context, string, string) (aws.Config, error) { return aws.Config{Region: "us-east-1"}, nil }
	awsDeps.emr = func(aws.Config) awsmeta.EMRAPI { return stubEMR{clusters: clusters} }
	awsDeps.ec2 = func(aws.Config) awsmeta.EC2API { return stubEC2{} }
	awsDeps.cloudwatch = func(aws.Config) awsmeta.CloudWatchAPI { return stubCloudWatch{} }
	awsDeps.cloudtrail = func(aws.Config) awsmeta.CloudTrailAPI { return stubCloudTrail{} }
	awsDeps.now = func() time.Time { return time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC) }
	interval := awsmeta.LookupInterval
	awsmeta.LookupInterval = 0
	t.Cleanup(func() { awsmeta.LookupInterval = interval })
	awsDeps.s3 = func(_ context.Context, _ aws.Config, bucket string) (source.Store, error) {
		root, ok := buckets[bucket]
		if !ok {
			return nil, &source.Error{Class: source.ClassNotFound, Key: bucket, Err: errors.New("no such bucket")}
		}
		return source.NewLocalStore(root), nil
	}
	// Tests never call real AWS: every client the CLI can make must be
	// replaced above, including ones added later.
	now, before := reflect.ValueOf(awsDeps), reflect.ValueOf(saved)
	for i := 0; i < now.NumField(); i++ {
		if now.Field(i).Pointer() == before.Field(i).Pointer() {
			t.Fatalf("fakeAWS does not stub awsDeps.%s", now.Type().Field(i).Name)
		}
	}
}

func cluster(id, eventLogDir string) *emrtypes.Cluster {
	c := &emrtypes.Cluster{Id: aws.String(id), Name: aws.String("etl"), ReleaseLabel: aws.String("emr-7.3.0"),
		LogUri: aws.String("s3n://logs/emr/"), Status: &emrtypes.ClusterStatus{State: emrtypes.ClusterStateTerminated}}
	if eventLogDir != "" {
		c.Configurations = []emrtypes.Configuration{{Classification: aws.String("spark-defaults"), Properties: map[string]string{"spark.eventLog.dir": eventLogDir}}}
	}
	return c
}

func TestOnlineEventLog(t *testing.T) {
	bucket := t.TempDir()
	os.MkdirAll(filepath.Join(bucket, "spark-events"), 0o755)
	data, _ := os.ReadFile(filepath.Join(fx, "application_1790380000000_0042.zstd"))
	os.WriteFile(filepath.Join(bucket, "spark-events", "application_1790380000000_0042.zstd"), data, 0o644)
	fakeAWS(t, map[string]string{"logs": bucket}, map[string]*emrtypes.Cluster{
		"j-s3": cluster("j-s3", "s3://logs/spark-events/"), "j-hdfs": cluster("j-hdfs", "hdfs:///var/log/spark/apps")})
	app := "application_1790380000000_0042"

	for _, args := range [][]string{
		{"-eventlog", "s3://logs/spark-events/", "-profile", "test"},
		{"-cluster-id", "j-s3", "-profile", "test"},
	} {
		dir := t.TempDir()
		code, out, errs := runCLI(t, append([]string{"-app-id", app, "-out", dir}, args...)...)
		if code != exitOK && code != exitPartial {
			t.Fatalf("%v: exit %d: %s", args, code, errs)
		}
		js, _ := os.ReadFile(outPath(dir, "report.json"))
		if !bytes.Contains(js, []byte(`"id": "application_1790380000000_0042"`)) || !strings.Contains(out, outPath(dir, "report.html")) {
			t.Errorf("%v: no report for the S3 event log: %s", args, errs)
		}
	}
	dir := t.TempDir()
	code, _, errs := runCLI(t, "-app-id", app, "-out", dir, "-cluster-id", "j-hdfs", "-profile", "test")
	js, _ := os.ReadFile(outPath(dir, "report.json"))
	if code != exitPartial || !bytes.Contains(js, []byte("HDFS")) {
		t.Errorf("an HDFS event log dir should give a partial report saying why: exit %d, %s", code, errs)
	}
	if code, _, errs := runCLI(t, "-app-id", app, "-cluster-id", "j-nope", "-profile", "test"); code != exitFatal || !strings.Contains(errs, "not found") {
		t.Errorf("unknown cluster: exit %d, %s", code, errs)
	}
	if code, _, errs := runCLI(t, "-app-id", app, "-cluster-id", "j-s3"); code != exitFatal || !strings.Contains(errs, "-profile") {
		t.Errorf("AWS access without -profile: exit %d, %s", code, errs)
	}
}

// outPath is the output of a kind ("report.html", "report.json",
// "explorer.html") in dir, whatever application it is named after.
func outPath(dir, kind string) string {
	m, _ := filepath.Glob(filepath.Join(dir, "*-"+kind))
	if len(m) == 1 {
		return m[0]
	}
	return filepath.Join(dir, "missing-"+kind)
}

// SP-003: a compressed event log that unpacks past -max-unpacked gives a
// partial report (exit 3) that names the limit, not a complete-looking one.
func TestMaxUnpackedMarksPartial(t *testing.T) {
	dir := t.TempDir()
	code, _, errs := runCLI(t, "-app-id", "application_1790380000000_0042", "-eventlog", filepath.Join(fx, "application_1790380000000_0042.zstd"), "-max-unpacked", "200KiB", "-out", dir)
	if code != exitPartial {
		t.Fatalf("exit %d, want %d: %s", code, exitPartial, errs)
	}
	js, err := os.ReadFile(filepath.Join(dir, "application_1790380000000_0042-report.json"))
	if err != nil || !bytes.Contains(js, []byte("-max-unpacked")) {
		t.Fatalf("report should say which limit cut the log: %v", err)
	}
	if code, _, errs := runCLI(t, "-app-id", "application_1790380000000_0042", "-eventlog", fx, "-max-unpacked", "lots"); code != exitFatal || !strings.Contains(errs, "-max-unpacked") {
		t.Errorf("bad -max-unpacked: exit %d %s", code, errs)
	}
}

// SP-008: a log with no application start event is accepted only when its
// name says it is the application asked for.
func TestUnconfirmedEventLogRefused(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(fx, "application_1790380000000_0044"))
	if err != nil {
		t.Fatal(err)
	}
	var kept []string
	for _, l := range strings.Split(strings.TrimRight(string(data), "\n"), "\n") {
		if !strings.Contains(l, `"Event":"SparkListenerApplicationStart"`) {
			kept = append(kept, l)
		}
	}
	dir := t.TempDir()
	body := []byte(strings.Join(kept, "\n") + "\n")
	named := filepath.Join(dir, "application_1790380000000_0044")
	renamed := filepath.Join(dir, "some-log.txt")
	os.WriteFile(named, body, 0o600)
	os.WriteFile(renamed, body, 0o600)

	code, _, errs := runCLI(t, "-app-id", "application_1790380000000_0044", "-eventlog", renamed, "-out", filepath.Join(dir, "o1"))
	if code != exitFatal || !strings.Contains(errs, "cannot confirm") {
		t.Errorf("unconfirmed log: exit %d %s", code, errs)
	}
	code, _, errs = runCLI(t, "-app-id", "application_1790380000000_0044", "-eventlog", named, "-out", filepath.Join(dir, "o2"))
	if code == exitFatal {
		t.Errorf("a log named after the application should be read: %s", errs)
	}
}

// SP-005: reports and the folders sparkplain creates are private to the
// user who ran it.
func TestOutputIsPrivate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "new", "reports")
	code, _, errs := runCLI(t, "-app-id", "application_1790380000000_0042", "-eventlog", filepath.Join(fx, "application_1790380000000_0042"), "-out", dir)
	if code != exitOK {
		t.Fatalf("exit %d: %s", code, errs)
	}
	for _, d := range []string{dir, filepath.Dir(dir)} {
		if st, err := os.Stat(d); err != nil || st.Mode().Perm() != 0o700 {
			t.Errorf("%s: mode %v, want 0700 (%v)", d, st.Mode().Perm(), err)
		}
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) == 0 {
		t.Fatal("no outputs written")
	}
	for _, e := range entries {
		st, _ := os.Stat(filepath.Join(dir, e.Name()))
		if st.Mode().Perm() != 0o600 {
			t.Errorf("%s: mode %v, want 0600", e.Name(), st.Mode().Perm())
		}
	}
}

func TestWorkersBounded(t *testing.T) {
	for _, w := range []string{"0", "-3", "257", "100000"} {
		if code, _, errs := runCLI(t, "-app-id", "application_1790380000000_0042", "-eventlog", fx, "-workers", w); code != exitFatal || !strings.Contains(errs, "-workers") {
			t.Errorf("-workers %s: exit %d %s", w, code, errs)
		}
	}
	if code, _, errs := runCLI(t, "-app-id", "application_1790380000000_0042", "-eventlog", fx, "-workers", "256", "-out", t.TempDir()); code == exitFatal {
		t.Errorf("-workers 256 should be accepted: %s", errs)
	}
}
