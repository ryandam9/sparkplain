package awsmeta

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/aws/smithy-go"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// stubEMR answers from fixed data. Tests never call real AWS.
type stubEMR struct {
	clusters  map[string]*types.Cluster
	pages     [][]types.ClusterSummary
	steps     []types.StepSummary
	instances []types.Instance
	groups    []types.InstanceGroup
	fleets    []types.InstanceFleet
	security  map[string]string
	stepRoles map[string]string
}

func (s *stubEMR) ListInstanceGroups(context.Context, *emr.ListInstanceGroupsInput, ...func(*emr.Options)) (*emr.ListInstanceGroupsOutput, error) {
	return &emr.ListInstanceGroupsOutput{InstanceGroups: s.groups}, nil
}
func (s *stubEMR) ListInstanceFleets(context.Context, *emr.ListInstanceFleetsInput, ...func(*emr.Options)) (*emr.ListInstanceFleetsOutput, error) {
	return &emr.ListInstanceFleetsOutput{InstanceFleets: s.fleets}, nil
}
func (s *stubEMR) DescribeSecurityConfiguration(_ context.Context, in *emr.DescribeSecurityConfigurationInput, _ ...func(*emr.Options)) (*emr.DescribeSecurityConfigurationOutput, error) {
	j, ok := s.security[aws.ToString(in.Name)]
	if !ok {
		return nil, &smithy.GenericAPIError{Code: "InvalidRequestException", Message: "not found"}
	}
	return &emr.DescribeSecurityConfigurationOutput{Name: in.Name, SecurityConfiguration: aws.String(j)}, nil
}
func (s *stubEMR) DescribeStep(_ context.Context, in *emr.DescribeStepInput, _ ...func(*emr.Options)) (*emr.DescribeStepOutput, error) {
	return &emr.DescribeStepOutput{Step: &types.Step{Id: in.StepId, ExecutionRoleArn: aws.String(s.stepRoles[aws.ToString(in.StepId)])}}, nil
}

func (s *stubEMR) DescribeCluster(_ context.Context, in *emr.DescribeClusterInput, _ ...func(*emr.Options)) (*emr.DescribeClusterOutput, error) {
	c, ok := s.clusters[aws.ToString(in.ClusterId)]
	if !ok {
		return nil, &smithy.GenericAPIError{Code: "InvalidRequestException", Message: "Cluster id '" + aws.ToString(in.ClusterId) + "' is not valid."}
	}
	return &emr.DescribeClusterOutput{Cluster: c}, nil
}

func (s *stubEMR) ListClusters(_ context.Context, in *emr.ListClustersInput, _ ...func(*emr.Options)) (*emr.ListClustersOutput, error) {
	i := 0
	if in.Marker != nil {
		i = int(aws.ToString(in.Marker)[0] - '0')
	}
	out := &emr.ListClustersOutput{Clusters: s.pages[i]}
	if i+1 < len(s.pages) {
		out.Marker = aws.String(string(rune('0' + i + 1)))
	}
	return out, nil
}

func (s *stubEMR) ListSteps(context.Context, *emr.ListStepsInput, ...func(*emr.Options)) (*emr.ListStepsOutput, error) {
	return &emr.ListStepsOutput{Steps: s.steps}, nil
}

func (s *stubEMR) ListInstances(context.Context, *emr.ListInstancesInput, ...func(*emr.Options)) (*emr.ListInstancesOutput, error) {
	return &emr.ListInstancesOutput{Instances: s.instances}, nil
}

func summary(id, name string, created time.Time) types.ClusterSummary {
	return types.ClusterSummary{Id: aws.String(id), Name: aws.String(name), Status: &types.ClusterStatus{Timeline: &types.ClusterTimeline{CreationDateTime: aws.Time(created)}}}
}

func TestDescribe(t *testing.T) {
	api := &stubEMR{clusters: map[string]*types.Cluster{"j-1": {
		Id: aws.String("j-1"), Name: aws.String("etl"), ReleaseLabel: aws.String("emr-7.3.0"), LogUri: aws.String("s3n://logs/emr/"),
		ServiceRole: aws.String("EMR_DefaultRole"), Ec2InstanceAttributes: &types.Ec2InstanceAttributes{IamInstanceProfile: aws.String("EMR_EC2_DefaultRole")},
		Applications: []types.Application{{Name: aws.String("Spark"), Version: aws.String("3.5.1")}},
		Status:       &types.ClusterStatus{State: types.ClusterStateTerminated, StateChangeReason: &types.ClusterStateChangeReason{Message: aws.String("Steps completed")}},
		Configurations: []types.Configuration{{Classification: aws.String("spark-defaults"), Properties: map[string]string{
			"spark.eventLog.dir": "s3://logs/spark-events/", "spark.hadoop.fs.s3a.secret.key": "FAKE-S3A-SECRET-0001"}}},
	}}}
	c, err := Describe(context.Background(), api, "j-1")
	if err != nil {
		t.Fatal(err)
	}
	if c.LogURI != "s3n://logs/emr/" || c.InstanceProfile != "EMR_EC2_DefaultRole" || c.State != "TERMINATED" || c.Applications[0] != "Spark 3.5.1" {
		t.Errorf("cluster %+v", c)
	}
	if c.Configurations["spark-defaults/spark.eventLog.dir"] != "s3://logs/spark-events/" || c.Configurations["spark-defaults/spark.hadoop.fs.s3a.secret.key"] != "[redacted]" {
		t.Errorf("configurations %v", c.Configurations)
	}
	if _, err := Describe(context.Background(), api, "j-nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing cluster: %v", err)
	}
}

func TestFindByNamePicksNewest(t *testing.T) {
	now := time.Now()
	api := &stubEMR{pages: [][]types.ClusterSummary{
		{summary("j-old", "etl", now.Add(-48*time.Hour)), summary("j-x", "other", now)},
		{summary("j-new", "etl", now.Add(-time.Hour))},
	}}
	if id, err := FindByName(context.Background(), api, "etl"); err != nil || id != "j-new" {
		t.Errorf("FindByName = %q, %v", id, err)
	}
	if _, err := FindByName(context.Background(), api, "none"); !errors.Is(err, ErrNotFound) {
		t.Errorf("no match: %v", err)
	}
}

func TestStepsRedactArgs(t *testing.T) {
	api := &stubEMR{steps: []types.StepSummary{{Id: aws.String("s-1"), Name: aws.String("load"),
		Config: &types.HadoopStepConfig{Jar: aws.String("command-runner.jar"), Args: []string{"spark-submit", "--conf", "spark.myapp.db.password=FAKE-DB-PASSWORD-0002", "s3://code/etl.py", "--token=FAKE-TOKEN-0003"}},
		Status: &types.StepStatus{State: types.StepStateFailed, FailureDetails: &types.FailureDetails{Reason: aws.String("Unknown Error."), LogFile: aws.String("s3://logs/steps/s-1/")}}}}}
	steps, err := Steps(context.Background(), api, "j-1")
	if err != nil || len(steps) != 1 {
		t.Fatalf("%v %v", steps, err)
	}
	joined := strings.Join(steps[0].Args, " ")
	if strings.Contains(joined, "FAKE-") || !strings.Contains(joined, "s3://code/etl.py") || steps[0].State != "FAILED" {
		t.Errorf("step %+v", steps[0])
	}
}

func TestAppInStepLog(t *testing.T) {
	log := "26/09/26 07:42:28 INFO Client: Requesting a new application from cluster with 1 NodeManagers\n" +
		"26/09/26 07:42:30 INFO Client: Uploading resource s3://b/emr_job.py -> hdfs://ip-10-0-2-11:8020/user/hadoop/.sparkStaging/application_1790408460617_0001/emr_job.py\n" +
		"26/09/26 07:42:40 INFO Client: Submitted application application_1790408460617_0001\n"
	if id := AppInStepLog(strings.NewReader(log)); id != "application_1790408460617_0001" {
		t.Errorf("app %q", id)
	}
	if id := AppInStepLog(strings.NewReader("no app here\n")); id != "" {
		t.Errorf("app %q", id)
	}
}

func TestInstancesMarkPrimary(t *testing.T) {
	inst := func(id, dns string, created time.Time) types.Instance {
		return types.Instance{Ec2InstanceId: aws.String(id), PrivateDnsName: aws.String(dns), PublicDnsName: aws.String(""), InstanceType: aws.String("m5.xlarge"),
			Market: types.MarketTypeSpot, Status: &types.InstanceStatus{State: types.InstanceStateTerminated,
				StateChangeReason: &types.InstanceStateChangeReason{Message: aws.String("Spot instance interrupted")},
				Timeline:          &types.InstanceTimeline{CreationDateTime: aws.Time(created)}}}
	}
	t0 := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	api := &stubEMR{instances: []types.Instance{inst("i-2", "ip-10-0-0-2.ec2.internal", t0.Add(time.Minute)), inst("i-1", "ip-10-0-0-1.ec2.internal", t0)}}
	got, err := Instances(context.Background(), api, model.Cluster{ID: "j-1", PrimaryDNS: "ip-10-0-0-1.ec2.internal"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != "i-1" || !got[0].Primary || got[1].Primary || got[1].Market != "SPOT" || got[1].StateReason != "Spot instance interrupted" {
		t.Errorf("instances = %+v", got)
	}
}

// Steps' ordering contract: oldest first by start time, whatever order the
// API returns them in (ListSteps pages newest first).
func TestStepsOldestFirst(t *testing.T) {
	t0 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	step := func(id string, start time.Time) types.StepSummary {
		return types.StepSummary{Id: aws.String(id), Status: &types.StepStatus{Timeline: &types.StepTimeline{StartDateTime: aws.Time(start)}}}
	}
	api := &stubEMR{steps: []types.StepSummary{step("s-3", t0.Add(2*time.Hour)), step("s-1", t0), step("s-2", t0.Add(time.Hour))}}
	steps, err := Steps(context.Background(), api, "j-1")
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, s := range steps {
		ids = append(ids, s.ID)
	}
	if strings.Join(ids, ",") != "s-1,s-2,s-3" {
		t.Fatalf("order %v, want oldest first", ids)
	}
}
