package awsmeta

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

func TestGroupsMarkInstanceRoles(t *testing.T) {
	api := &stubEMR{groups: []types.InstanceGroup{
		{Id: aws.String("ig-task"), InstanceGroupType: types.InstanceGroupTypeTask, InstanceType: aws.String("m5.xlarge"), Market: types.MarketTypeSpot, RequestedInstanceCount: aws.Int32(1), RunningInstanceCount: aws.Int32(1)},
		{Id: aws.String("ig-master"), InstanceGroupType: types.InstanceGroupTypeMaster, InstanceType: aws.String("m5.xlarge"), Market: types.MarketTypeOnDemand},
	}}
	cl := &model.Cluster{ID: "j-1", Instances: []model.Instance{{ID: "i-1", GroupID: "ig-master"}, {ID: "i-2", GroupID: "ig-task"}, {ID: "i-3", GroupID: "ig-gone"}}}
	if err := Groups(context.Background(), api, cl); err != nil {
		t.Fatal(err)
	}
	if len(cl.Groups) != 2 || cl.Groups[0].Role != "MASTER" || cl.Groups[1].Market != "SPOT" || cl.Groups[1].Running != 1 {
		t.Errorf("groups = %+v", cl.Groups)
	}
	if cl.Instances[0].Role != "MASTER" || cl.Instances[1].Role != "TASK" || cl.Instances[2].Role != "" {
		t.Errorf("instances = %+v", cl.Instances)
	}

	fleets := &stubEMR{fleets: []types.InstanceFleet{{Id: aws.String("if-core"), InstanceFleetType: types.InstanceFleetTypeCore,
		TargetOnDemandCapacity: aws.Int32(1), TargetSpotCapacity: aws.Int32(2), ProvisionedOnDemandCapacity: aws.Int32(1), ProvisionedSpotCapacity: aws.Int32(1),
		InstanceTypeSpecifications: []types.InstanceTypeSpecification{{InstanceType: aws.String("m5.xlarge")}, {InstanceType: aws.String("r5.xlarge")}}}}}
	fc := &model.Cluster{ID: "j-2", Fleets: true, Instances: []model.Instance{{ID: "i-9", GroupID: "if-core"}}}
	if err := Groups(context.Background(), fleets, fc); err != nil {
		t.Fatal(err)
	}
	if g := fc.Groups[0]; !g.Fleet || g.Requested != 3 || g.Running != 2 || strings.Join(g.InstanceTypes, ",") != "m5.xlarge,r5.xlarge" || fc.Instances[0].Role != "CORE" {
		t.Errorf("fleet = %+v, instance %+v", g, fc.Instances[0])
	}
}

func TestSecurityKeepsOnlyThePosture(t *testing.T) {
	doc := map[string]any{
		"EncryptionConfiguration": map[string]any{
			"EnableAtRestEncryption": true, "EnableInTransitEncryption": true,
			"AtRestEncryptionConfiguration": map[string]any{
				"S3EncryptionConfiguration":        map[string]any{"EncryptionMode": "SSE-KMS", "AwsKmsKey": "arn:aws:kms:us-east-1:000000000000:key/FAKE-KEY-0012"},
				"LocalDiskEncryptionConfiguration": map[string]any{"EncryptionKeyProviderType": "AwsKms", "AwsKmsKey": "FAKE-KEY-0013", "EnableEbsEncryption": true},
			},
			"InTransitEncryptionConfiguration": map[string]any{"TLSCertificateConfiguration": map[string]any{"CertificateProviderType": "PEM", "S3Object": "s3://secret/FAKE-CERT-0014.zip"}},
		},
		"AuthenticationConfiguration": map[string]any{"KerberosConfiguration": map[string]any{"Provider": "ClusterDedicatedKdc", "ClusterDedicatedKdcConfiguration": map[string]any{"TicketLifetimeInHours": 24}}},
		"AuthorizationConfiguration":  map[string]any{"IAMConfiguration": map[string]any{"EnableApplicationScopedIAMRole": true}, "LakeFormationConfiguration": map[string]any{"AuthorizedSessionTagValue": "Amazon EMR"}},
	}
	b, _ := json.Marshal(doc)
	api := &stubEMR{security: map[string]string{"prod-sec": string(b)}}
	p, err := Security(context.Background(), api, "prod-sec")
	if err != nil {
		t.Fatal(err)
	}
	want := model.SecurityPosture{Name: "prod-sec", AtRestEncryption: true, S3Encryption: "SSE-KMS", LocalDiskEncryption: true, EBSEncryption: true,
		InTransitEncryption: true, Kerberos: "ClusterDedicatedKdc", LakeFormation: true, RuntimeRoles: true, Source: "EMR DescribeSecurityConfiguration prod-sec"}
	if *p != want {
		t.Errorf("posture = %+v", *p)
	}
	out, _ := json.Marshal(p)
	if strings.Contains(string(out), "FAKE-") {
		t.Errorf("key or certificate kept: %s", out)
	}
	if _, err := Security(context.Background(), api, "missing"); err == nil {
		t.Error("a missing configuration should fail")
	}
}

func TestStepRole(t *testing.T) {
	api := &stubEMR{stepRoles: map[string]string{"s-1": "arn:aws:iam::000000000000:role/etl-runtime"}}
	if r, err := StepRole(context.Background(), api, "j-1", "s-1"); err != nil || r != "arn:aws:iam::000000000000:role/etl-runtime" {
		t.Errorf("role %q, %v", r, err)
	}
	if r, _ := StepRole(context.Background(), api, "j-1", "s-2"); r != "" {
		t.Errorf("a step without a runtime role gave %q", r)
	}
}

type stubEC2 struct{ calls int }

func (s *stubEC2) DescribeInstanceTypes(_ context.Context, in *ec2.DescribeInstanceTypesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error) {
	s.calls++
	out := &ec2.DescribeInstanceTypesOutput{}
	for _, t := range in.InstanceTypes {
		out.InstanceTypes = append(out.InstanceTypes, ec2types.InstanceTypeInfo{InstanceType: t, VCpuInfo: &ec2types.VCpuInfo{DefaultVCpus: aws.Int32(4)}, MemoryInfo: &ec2types.MemoryInfo{SizeInMiB: aws.Int64(16384)}})
	}
	return out, nil
}

func TestInstanceSizes(t *testing.T) {
	cl := &model.Cluster{Instances: []model.Instance{{ID: "i-1", Type: "m5.xlarge"}, {ID: "i-2", Type: "m5.xlarge"}, {ID: "i-3"}}}
	api := &stubEC2{}
	if err := InstanceSizes(context.Background(), api, cl); err != nil {
		t.Fatal(err)
	}
	if api.calls != 1 || cl.Instances[1].VCPU != 4 || cl.Instances[1].MemoryBytes != 16<<30 || cl.Instances[2].VCPU != 0 {
		t.Errorf("calls %d, instances %+v", api.calls, cl.Instances)
	}
}
