package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"

	"github.com/ryandam9/sparkplain/internal/sparkplain/awsmeta"
	"github.com/ryandam9/sparkplain/internal/sparkplain/source"
)

// hbaseCluster0083 is the HBase test cluster as DescribeCluster and
// ListInstances give it, with its logs in the bucket "logs" under emr/.
func hbaseCluster0083(t *testing.T, hbase bool) (bucket string, stub stubEMR) {
	t.Helper()
	bucket = t.TempDir()
	copyTree(t, filepath.Join(emrlogs, hbaseCluster), filepath.Join(bucket, "emr", hbaseCluster))
	cl := cluster(hbaseCluster, "")
	cl.MasterPublicDnsName = aws.String("ip-10-0-2-11.us-east-1.compute.internal")
	cl.Applications = []types.Application{{Name: aws.String("Spark")}}
	if hbase {
		cl.Applications = append(cl.Applications, types.Application{Name: aws.String("HBase")})
	}
	cl.SecurityConfiguration = aws.String("fixture-security")
	stub = stubEMR{clusters: map[string]*types.Cluster{hbaseCluster: cl}, instances: []types.Instance{
		{Ec2InstanceId: aws.String("i-0fee0000000000001"), PrivateDnsName: aws.String("ip-10-0-2-11.us-east-1.compute.internal"), InstanceType: aws.String("m5.xlarge")},
		{Ec2InstanceId: aws.String("i-0fee0000000000002"), PrivateDnsName: aws.String("ip-10-0-2-10.us-east-1.compute.internal"), InstanceType: aws.String("m5.xlarge")},
	}}
	return bucket, stub
}

// Online, the run starts by checking each source with one small call and
// prints a line for each: what it can read, where, and for what it
// cannot, why and the aws command that repeats the call. -check stops
// there, exiting 3 when something the run would read is not readable.
func TestAccessCheckOnline(t *testing.T) {
	bucket, stub := hbaseCluster0083(t, true)
	fakeAWS(t, map[string]string{"logs": bucket}, stub.clusters)
	awsDeps.emr = func(aws.Config) awsmeta.EMRAPI { return stub }
	awsDeps.cloudtrail = func(aws.Config) awsmeta.CloudTrailAPI { return noCloudTrail{} }
	out := t.TempDir()
	code, stdout, errs := runCLI(t, "-app-id", "application_1790380000000_0092", "-profile", "test", "-cluster-id", hbaseCluster, "-check", "-out", out)
	if code != exitPartial {
		t.Fatalf("exit %d, want %d: %s\n%s", code, exitPartial, errs, stdout)
	}
	for _, want := range []string{
		"  Cluster  j-FIXTURE0083CLUSTER · etl · emr-7.3.0 · terminated\n",
		"  As       assumed-role/fixture/tester · profile test · us-east-1\n",
		"  Logs     s3://logs/emr/j-FIXTURE0083CLUSTER/\n",
		"\n▸ Access check\n  Y EMR API                cluster, steps, instances, groups\n",
		"  N Security configuration fixture-security\n                           Access denied: needs",
		"  Y Container logs         containers/application_1790380000000_0092/ list, read\n",
		"  Y Step logs              steps/  list, read\n",
		"  Y Node logs              node/  list, read\n",
		"  Y HBase server logs      node/*/applications/hbase/ Checked on the primary node, i-0fee0000000000001 · list, read\n",
		"  N Spark event log        The cluster keeps the event log on HDFS (EMR's default, hdfs:///var/log/spark/apps), which sparkplain cannot read.",
		"  Y EC2 instance types     m5.xlarge\n",
		"  N CloudTrail             Access denied: needs cloudtrail:LookupEvents",
		"\n                           try: aws cloudtrail lookup-events --max-results 1 --profile test --region us-east-1\n",
		"  Y Output folder          " + out,
	} {
		if !strings.Contains(flat(stdout), flat(want)) {
			t.Errorf("stdout lacks %q:\n%s", want, stdout)
		}
	}
	if entries, _ := os.ReadDir(out); len(entries) != 0 {
		t.Errorf("-check wrote %d files", len(entries))
	}

	// Without -check the run goes on, and the report keeps the rows.
	code, _, errs = runCLI(t, "-app-id", "application_1790380000000_0092", "-profile", "test", "-cluster-id", hbaseCluster,
		"-eventlog", filepath.Join(fx, "application_1790380000000_0092.zstd"), "-out", out, "-format", "json")
	if code != exitPartial {
		t.Fatalf("run: exit %d: %s", code, errs)
	}
	got := map[string]string{}
	for _, r := range readReport(t, out).AccessCheck {
		got[r.Name] = r.Status
	}
	if got["EMR API"] != "ok" || got["HBase server logs"] != "ok" || got["Spark event log"] != "ok" || got["CloudTrail"] != "denied" {
		t.Errorf("accessCheck = %v", got)
	}
}

// A log folder EMR has not filled yet is readable but empty, which is not
// the same as refused; a bucket that cannot be opened is an error; HBase
// is checked only on a cluster that runs it; what was turned off is not
// checked.
func TestAccessCheckEmptyMissingAndOff(t *testing.T) {
	bucket, stub := hbaseCluster0083(t, false)
	fakeAWS(t, map[string]string{"logs": bucket}, stub.clusters)
	awsDeps.emr = func(aws.Config) awsmeta.EMRAPI { return stub }
	code, stdout, _ := runCLI(t, "-app-id", "application_1790380000000_0999", "-profile", "test", "-cluster-id", hbaseCluster, "-check",
		"-no-cloudwatch", "-no-cloudtrail", "-eventlog", "s3://logs/emr/"+hbaseCluster+"/steps/")
	if code != exitPartial {
		t.Fatalf("exit %d", code)
	}
	for _, want := range []string{
		"  ! Container logs         containers/application_1790380000000_0999/ Readable, but no logs for this application yet",
		"  - HBase server logs      HBase is not installed on this cluster.\n",
		"  Y Spark event log        steps/ From -eventlog · list, read\n",
		"  - CloudWatch             Not asked for (-no-cloudwatch).\n",
		"  - CloudTrail             Not asked for (-no-cloudtrail).\n",
	} {
		if !strings.Contains(flat(stdout), flat(want)) {
			t.Errorf("stdout lacks %q:\n%s", want, stdout)
		}
	}

}

// A log bucket that cannot be opened is an error on every log row.
func TestAccessCheckNoBucket(t *testing.T) {
	_, stub := hbaseCluster0083(t, true)
	fakeAWS(t, map[string]string{}, stub.clusters)
	awsDeps.emr = func(aws.Config) awsmeta.EMRAPI { return stub }
	_, stdout, _ := runCLI(t, "-app-id", "application_1790380000000_0092", "-profile", "test", "-cluster-id", hbaseCluster, "-check", "-no-cloudwatch", "-no-cloudtrail")
	for _, want := range []string{"  N Container logs", "  N Step logs", "  N Node logs", "  N HBase server logs", "no such bucket"} {
		if !strings.Contains(flat(stdout), flat(want)) {
			t.Errorf("stdout lacks %q:\n%s", want, stdout)
		}
	}
}

// Offline, the check looks at the local paths; -check exits 3 when the
// event log is not there, and the report keeps the rows.
func TestAccessCheckOffline(t *testing.T) {
	code, stdout, _ := runCLI(t, "-app-id", "application_1790380000000_0092", "-from", filepath.Join(emrlogs, hbaseCluster), "-eventlog", "/no/such/log", "-check")
	stdout = flat(stdout)
	if code != exitPartial || !strings.Contains(stdout, "As offline: local files only, no AWS calls") ||
		!strings.Contains(stdout, "N Spark event log stat /no/such/log: no such file or directory") ||
		!strings.Contains(stdout, "Y HBase server logs") || !strings.Contains(stdout, "- AWS EMR API, CloudWatch and CloudTrail: not asked for.") {
		t.Errorf("exit %d:\n%s", code, stdout)
	}
	code, _, _ = runCLI(t, "-app-id", "application_1790380000000_0092", "-from", filepath.Join(emrlogs, hbaseCluster), "-eventlog", filepath.Join(fx, "application_1790380000000_0092.zstd"), "-check")
	if code != exitOK {
		t.Errorf("everything readable: exit %d", code)
	}
}

// flat collapses runs of white space, so a check need not know where the
// console wrapped a line.
func flat(s string) string { return strings.Join(strings.Fields(s), " ") }

// noRead lists like its store but refuses every read, as a bucket policy
// that allows s3:ListBucket and not s3:GetObject (or a KMS key the
// profile may not use) does.
type noRead struct{ source.Store }

func (noRead) Peek(context.Context, source.Object) error {
	return &source.Error{Class: source.ClassAccessDenied, Err: errors.New("AccessDenied")}
}

type noGroups struct{ stubEMR }

func (noGroups) ListInstanceGroups(context.Context, *emr.ListInstanceGroupsInput, ...func(*emr.Options)) (*emr.ListInstanceGroupsOutput, error) {
	return nil, denied("elasticmapreduce:ListInstanceGroups")
}

type noDescribeStep struct{ stubEMR }

func (noDescribeStep) DescribeStep(context.Context, *emr.DescribeStepInput, ...func(*emr.Options)) (*emr.DescribeStepOutput, error) {
	return nil, denied("elasticmapreduce:DescribeStep")
}

// listOnly lists a metric but may not read it: cloudwatch:ListMetrics and
// GetMetricData are separate permissions.
type listOnly struct{ stubCloudWatch }

func (listOnly) ListMetrics(context.Context, *cloudwatch.ListMetricsInput, ...func(*cloudwatch.Options)) (*cloudwatch.ListMetricsOutput, error) {
	return &cloudwatch.ListMetricsOutput{Metrics: []cwtypes.Metric{{Namespace: aws.String("AWS/ElasticMapReduce"), MetricName: aws.String("AppsRunning")}}}, nil
}
func (listOnly) GetMetricData(context.Context, *cloudwatch.GetMetricDataInput, ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricDataOutput, error) {
	return nil, denied("cloudwatch:GetMetricData")
}

// The access check tests every access the run needs, not just listing:
// reading an object, reading a metric, each EMR call, the job's script in
// its own bucket, an event log folder a step set, and the local paths.
func TestAccessCheckCoversEveryAccess(t *testing.T) {
	readOnly := unwritableDir(t)
	scriptStep := types.StepSummary{Id: aws.String("s-1"), Name: aws.String("job"), Config: &types.HadoopStepConfig{Jar: aws.String("command-runner.jar"),
		Args: []string{"spark-submit", "--deploy-mode", "cluster", "--conf", "spark.eventLog.dir=s3://code/events/", "s3://code/job.py"}}}
	type checkCase struct {
		name  string
		setup func(bucket string, stub stubEMR)
		args  []string
		want  []string
	}
	cases := []checkCase{
		{"listing allowed, reading refused", func(bucket string, _ stubEMR) {
			awsDeps.s3 = func(context.Context, aws.Config, string) (source.Store, error) {
				return noRead{source.NewLocalStore(bucket)}, nil
			}
		}, nil, []string{"N Container logs containers/application_1790380000000_0092/ Listing works, but reading is refused: needs s3:GetObject (and kms:Decrypt when the bucket uses SSE-KMS).",
			"try: aws s3 cp s3://logs/emr/j-FIXTURE0083CLUSTER/containers/application_1790380000000_0092/"}},
		{"metrics listed, not read", func(string, stubEMR) {
			awsDeps.cloudwatch = func(aws.Config) awsmeta.CloudWatchAPI { return listOnly{} }
		}, nil, []string{"N CloudWatch Access denied: needs cloudwatch:GetMetricData", "try: aws cloudwatch get-metric-statistics --namespace AWS/ElasticMapReduce --metric-name AppsRunning"}},
		{"instance groups refused", func(_ string, stub stubEMR) {
			awsDeps.emr = func(aws.Config) awsmeta.EMRAPI { return noGroups{stub} }
		}, nil, []string{"N EMR API ListInstanceGroups failed (accessDenied): needs elasticmapreduce:ListInstanceGroups.", "try: aws emr list-instance-groups --cluster-id j-FIXTURE0083CLUSTER"}},
		{"step details refused", func(_ string, stub stubEMR) {
			stub.steps = []types.StepSummary{scriptStep}
			awsDeps.emr = func(aws.Config) awsmeta.EMRAPI { return noDescribeStep{stub} }
		}, nil, []string{"N EMR API DescribeStep failed (accessDenied): needs elasticmapreduce:DescribeStep (optional; it gives the step's runtime role)."}},
		{"the job's script and a step's event log folder", func(bucket string, stub stubEMR) {
			stub.steps = []types.StepSummary{scriptStep}
			awsDeps.emr = func(aws.Config) awsmeta.EMRAPI { return stub }
			code := t.TempDir()
			os.MkdirAll(filepath.Join(code, "events"), 0o755)
			os.WriteFile(filepath.Join(code, "job.py"), []byte("print(1)\n"), 0o644)
			os.WriteFile(filepath.Join(code, "events", "application_1790380000000_0092"), []byte("{}\n"), 0o644)
			saved := awsDeps.s3
			awsDeps.s3 = func(ctx context.Context, cfg aws.Config, b string) (source.Store, error) {
				if b == "code" {
					return source.NewLocalStore(code), nil
				}
				return saved(ctx, cfg, b)
			}
		}, nil, []string{"Y Spark event log s3://code/events/ From a step's spark.eventLog.dir · list, read", "Y Job scripts s3://code/job.py The newest step's script · read"}},
		{"the job's script refused", func(bucket string, stub stubEMR) {
			stub.steps = []types.StepSummary{scriptStep}
			awsDeps.emr = func(aws.Config) awsmeta.EMRAPI { return stub }
			saved := awsDeps.s3
			awsDeps.s3 = func(ctx context.Context, cfg aws.Config, b string) (source.Store, error) {
				if b == "code" {
					return noRead{source.NewLocalStore(t.TempDir())}, nil
				}
				return saved(ctx, cfg, b)
			}
		}, nil, []string{"N Job scripts s3://code/job.py Reading is refused: needs s3:GetObject on it", "try: aws s3 cp s3://code/job.py - --profile test"}},
	}
	if readOnly != "" {
		out := filepath.Join(readOnly, "not-writable", "out")
		cases = append(cases, checkCase{"local paths", func(string, stubEMR) {}, []string{"-source", "/no/such/code", "-out", out},
			[]string{"N Source code stat /no/such/code: no such file or directory", "N Output folder " + out + " " + readOnly + " is not writable"}})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bucket, stub := hbaseCluster0083(t, false)
			fakeAWS(t, map[string]string{"logs": bucket}, stub.clusters)
			awsDeps.emr = func(aws.Config) awsmeta.EMRAPI { return stub }
			tc.setup(bucket, stub)
			args := append([]string{"-app-id", "application_1790380000000_0092", "-profile", "test", "-cluster-id", hbaseCluster, "-check"}, tc.args...)
			code, stdout, _ := runCLI(t, args...)
			if code != exitPartial && !strings.Contains(tc.name, "script and") {
				t.Errorf("exit %d", code)
			}
			for _, w := range tc.want {
				if !strings.Contains(flat(stdout), flat(w)) {
					t.Errorf("stdout lacks %q:\n%s", w, stdout)
			}
				}
			}
		})
	}
}

// unwritableDir creates a temporary directory, makes it non-writable for the
// test user, and returns it. Root can bypass these permission bits, so in that
// environment the local-path case is omitted rather than producing a false
// failure.
func unwritableDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if syscall.Access(dir, 2) == nil {
		return ""
	}
	return dir
}
