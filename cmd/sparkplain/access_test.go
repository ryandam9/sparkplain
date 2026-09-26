package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/aws/smithy-go"

	"github.com/ryandam9/sparkplain/internal/sparkplain/awsmeta"
	"github.com/ryandam9/sparkplain/internal/sparkplain/source"
)

func denied(action string) error {
	return &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "User: arn:aws:sts::000000000000:assumed-role/analyst/me is not authorized to perform: " + action}
}

type noDescribe struct{ stubEMR }

func (noDescribe) DescribeCluster(context.Context, *emr.DescribeClusterInput, ...func(*emr.Options)) (*emr.DescribeClusterOutput, error) {
	return nil, denied("elasticmapreduce:DescribeCluster")
}

type noEC2 struct{}

func (noEC2) DescribeInstanceTypes(context.Context, *ec2.DescribeInstanceTypesInput, ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error) {
	return nil, &smithy.GenericAPIError{Code: "UnauthorizedOperation", Message: "You are not authorized to perform this operation."}
}

type noCloudWatch struct{ stubCloudWatch }

func (noCloudWatch) ListMetrics(context.Context, *cloudwatch.ListMetricsInput, ...func(*cloudwatch.Options)) (*cloudwatch.ListMetricsOutput, error) {
	return nil, denied("cloudwatch:ListMetrics")
}

type noCloudTrail struct{}

func (noCloudTrail) LookupEvents(context.Context, *cloudtrail.LookupEventsInput, ...func(*cloudtrail.Options)) (*cloudtrail.LookupEventsOutput, error) {
	return nil, denied("cloudtrail:LookupEvents")
}

func gapNames(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	for _, g := range readReport(t, dir).AccessGaps {
		out = append(out, g.Source)
	}
	return out
}

// Without DescribeCluster, the run carries on with the event log it was
// given, says what it could not do, and exits 3.
func TestNoAccessToCluster(t *testing.T) {
	fakeAWS(t, nil, map[string]*emrtypes.Cluster{"j-1": cluster("j-1", "")})
	awsDeps.emr = func(aws.Config) awsmeta.EMRAPI { return noDescribe{} }
	dir := t.TempDir()
	code, _, errs := runCLI(t, "-app-id", "application_1790380000000_0042", "-cluster-id", "j-1", "-profile", "test",
		"-eventlog", filepath.Join(fx, "application_1790380000000_0042"), "-out", dir, "-format", "json,html,explorer")
	if code != exitPartial {
		t.Fatalf("exit %d: %s", code, errs)
	}
	if !strings.Contains(errs, "no access to EMR API, so the report does not show the cluster's details") || !strings.Contains(errs, "elasticmapreduce:DescribeCluster") {
		t.Errorf("stderr = %s", errs)
	}
	r := readReport(t, dir)
	if r.Application.Name == "" || len(r.Jobs.Jobs) == 0 {
		t.Error("the event log should still be reported")
	}
	if s := sourceOf(r, "EMR API"); s.Status != "error" || s.Class != "accessDenied" {
		t.Errorf("EMR API = %+v", s)
	}
	if s := sourceOf(r, "Container logs"); s.Status != "not-supplied" || !strings.Contains(s.Detail, "cluster's details") {
		t.Errorf("containers = %+v", s)
	}
	html, _ := os.ReadFile(filepath.Join(dir, "report.html"))
	if !strings.Contains(string(html), "No access to 1 source: parts of this report are missing") {
		t.Error("report.html should open with the access notice")
	}
	page, _ := os.ReadFile(filepath.Join(dir, "explorer.html"))
	if !strings.Contains(string(page), `"accessGaps"`) {
		t.Error("the explorer should carry the access gaps")
	}
}

// No working credentials at all is no access too, not a crash.
func TestNoCredentials(t *testing.T) {
	fakeAWS(t, nil, nil)
	awsDeps.config = func(context.Context, string, string) (aws.Config, error) {
		return aws.Config{}, errors.New("failed to get shared config profile, etl")
	}
	dir := t.TempDir()
	code, _, errs := runCLI(t, "-app-id", "application_1790380000000_0042", "-cluster-id", "j-1", "-profile", "etl",
		"-eventlog", filepath.Join(fx, "application_1790380000000_0042"), "-out", dir, "-format", "json")
	if code != exitPartial || !strings.Contains(errs, "no access to EMR API") {
		t.Fatalf("exit %d: %s", code, errs)
	}
	if got := strings.Join(gapNames(t, dir), ","); got != "EMR API" {
		t.Errorf("gaps = %s", got)
	}
}

// Every AWS source after the cluster refused: the report still comes out,
// listing each gap.
func TestNoAccessAnywhere(t *testing.T) {
	cl := cluster("j-1", "s3://events/spark-events/")
	fakeAWS(t, nil, map[string]*emrtypes.Cluster{"j-1": cl})
	awsDeps.s3 = func(_ context.Context, _ aws.Config, bucket string) (source.Store, error) {
		return nil, &source.Error{Class: source.ClassAccessDenied, Key: bucket, Err: denied("s3:ListBucket")}
	}
	awsDeps.ec2 = func(aws.Config) awsmeta.EC2API { return noEC2{} }
	awsDeps.cloudwatch = func(aws.Config) awsmeta.CloudWatchAPI { return noCloudWatch{} }
	awsDeps.cloudtrail = func(aws.Config) awsmeta.CloudTrailAPI { return noCloudTrail{} }
	saved := awsDeps.emr
	awsDeps.emr = func(c aws.Config) awsmeta.EMRAPI {
		s := saved(c).(stubEMR)
		s.instances = []emrtypes.Instance{{Ec2InstanceId: aws.String("i-1"), PrivateDnsName: aws.String("ip-10-0-0-1.ec2.internal"), InstanceType: aws.String("m5.xlarge")}}
		s.steps = []emrtypes.StepSummary{{Id: aws.String("s-1"), Status: &emrtypes.StepStatus{Timeline: &emrtypes.StepTimeline{}}}}
		return s
	}
	dir := t.TempDir()
	code, _, errs := runCLI(t, "-app-id", "application_1790380000000_0042", "-cluster-id", "j-1", "-profile", "test", "-out", dir, "-format", "json,html")
	if code != exitPartial {
		t.Fatalf("exit %d: %s", code, errs)
	}
	got := strings.Join(gapNames(t, dir), ",")
	for _, want := range []string{"Spark event log", "EC2 API", "Container logs", "Step logs", "Node logs"} {
		if !strings.Contains(got, want) {
			t.Errorf("gaps %s lack %s", got, want)
		}
	}
	if n := strings.Count(errs, "sparkplain: no access to "); n != len(gapNames(t, dir)) {
		t.Errorf("%d stderr lines for %d gaps: %s", n, len(gapNames(t, dir)), errs)
	}
}

// A -from folder sparkplain may not read is a gap, not a crash.
func TestNoAccessToFromFolder(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root reads every folder")
	}
	locked := filepath.Join(t.TempDir(), "logs")
	if err := os.Mkdir(locked, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(locked, 0o755) })
	dir := t.TempDir()
	code, _, errs := runCLI(t, "-app-id", "application_1790380000000_0042", "-from", locked, "-eventlog", filepath.Join(fx, "application_1790380000000_0042"), "-out", dir, "-format", "json")
	if code != exitPartial || !strings.Contains(errs, "no access to Container logs") {
		t.Fatalf("exit %d: %s", code, errs)
	}
	if g := readReport(t, dir).AccessGaps; len(g) != 3 || !strings.HasPrefix(g[0].Needs, "read permission on ") {
		t.Errorf("gaps = %+v", g)
	}
}

// Usage mistakes still stop the run: there is nothing to report on.
func TestUsageMistakesStillStop(t *testing.T) {
	fakeAWS(t, nil, map[string]*emrtypes.Cluster{"j-1": cluster("j-1", "")})
	if code, _, errs := runCLI(t, "-app-id", "application_1790380000000_0042", "-cluster-id", "j-1"); code != exitFatal || !strings.Contains(errs, "-profile") {
		t.Errorf("no -profile: exit %d, %s", code, errs)
	}
	if code, _, errs := runCLI(t, "-app-id", "application_1790380000000_0042", "-cluster-id", "j-nope", "-profile", "test"); code != exitFatal || !strings.Contains(errs, "not found") {
		t.Errorf("unknown cluster: exit %d, %s", code, errs)
	}
}
