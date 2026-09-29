package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"

	"github.com/ryandam9/sparkplain/internal/sparkplain/awsmeta"
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
		"Access check   profile test · arn:aws:sts::000000000000:assumed-role/fixture/tester · us-east-1\n",
		"  Y EMR API                j-FIXTURE0083CLUSTER (etl, emr-7.3.0, TERMINATED)\n",
		"  N Security configuration fixture-security\n                           Access denied: needs",
		"  Y Container logs\n    s3://logs/emr/j-FIXTURE0083CLUSTER/containers/application_1790380000000_0092/\n",
		"  Y Step logs              s3://logs/emr/j-FIXTURE0083CLUSTER/steps/\n",
		"  Y Node logs              s3://logs/emr/j-FIXTURE0083CLUSTER/node/\n",
		"  Y HBase server logs\n    s3://logs/emr/j-FIXTURE0083CLUSTER/node/i-0fee0000000000001/applications/hbase/\n",
		"  N Spark event log        The cluster keeps the event log on HDFS (EMR's default, hdfs:///var/log/spark/apps), which sparkplain cannot read.",
		"  Y EC2 instance types     m5.xlarge.\n",
		"  N CloudTrail             Access denied: needs cloudtrail:LookupEvents",
		"try: aws cloudtrail lookup-events --max-results 1 --profile test",
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
		"  N Container logs\n    s3://logs/emr/j-FIXTURE0083CLUSTER/containers/application_1790380000000_0999/\n                           Readable, but no logs for this application yet",
		"  - HBase server logs      HBase is not installed on this cluster.\n",
		"  Y Spark event log        s3://logs/emr/j-FIXTURE0083CLUSTER/steps/\n                           From -eventlog.\n",
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
	if code != exitPartial || !strings.Contains(stdout, "Access check offline: local files only, no AWS calls") ||
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
