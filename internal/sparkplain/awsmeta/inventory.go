package awsmeta

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/emr"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// Groups lists the cluster's instance groups (ListInstanceGroups) or, for
// a fleet cluster, its instance fleets (ListInstanceFleets), and marks
// each instance with its group's role (MASTER, CORE or TASK).
func Groups(ctx context.Context, api EMRAPI, cl *model.Cluster) error {
	var groups []model.InstanceGroup
	if cl.Fleets {
		p := emr.NewListInstanceFleetsPaginator(api, &emr.ListInstanceFleetsInput{ClusterId: aws.String(cl.ID)})
		for p.HasMorePages() {
			page, err := p.NextPage(ctx)
			if err != nil {
				return fmt.Errorf("ListInstanceFleets %s: %w", cl.ID, err)
			}
			for _, f := range page.InstanceFleets {
				g := model.InstanceGroup{ID: aws.ToString(f.Id), Fleet: true, Role: string(f.InstanceFleetType), Name: aws.ToString(f.Name),
					Requested: int(aws.ToInt32(f.TargetOnDemandCapacity) + aws.ToInt32(f.TargetSpotCapacity)),
					Running:   int(aws.ToInt32(f.ProvisionedOnDemandCapacity) + aws.ToInt32(f.ProvisionedSpotCapacity))}
				for _, t := range f.InstanceTypeSpecifications {
					g.InstanceTypes = append(g.InstanceTypes, aws.ToString(t.InstanceType))
				}
				groups = append(groups, g)
			}
		}
	} else {
		p := emr.NewListInstanceGroupsPaginator(api, &emr.ListInstanceGroupsInput{ClusterId: aws.String(cl.ID)})
		for p.HasMorePages() {
			page, err := p.NextPage(ctx)
			if err != nil {
				return fmt.Errorf("ListInstanceGroups %s: %w", cl.ID, err)
			}
			for _, ig := range page.InstanceGroups {
				groups = append(groups, model.InstanceGroup{ID: aws.ToString(ig.Id), Role: string(ig.InstanceGroupType), Name: aws.ToString(ig.Name),
					InstanceTypes: []string{aws.ToString(ig.InstanceType)}, Market: string(ig.Market),
					Requested: int(aws.ToInt32(ig.RequestedInstanceCount)), Running: int(aws.ToInt32(ig.RunningInstanceCount))})
			}
		}
	}
	order := map[string]int{"MASTER": 0, "CORE": 1, "TASK": 2}
	sort.SliceStable(groups, func(i, j int) bool { return order[groups[i].Role] < order[groups[j].Role] })
	cl.Groups = groups
	role := map[string]string{}
	for _, g := range groups {
		role[g.ID] = g.Role
	}
	for i := range cl.Instances {
		if r := role[cl.Instances[i].GroupID]; r != "" {
			cl.Instances[i].Role = r
		}
	}
	return nil
}

// Security reads the cluster's EMR security configuration
// (DescribeSecurityConfiguration): what it encrypts, how it authenticates,
// and whether it turns on Lake Formation or runtime roles. Nothing else in
// the JSON (key ARNs, certificate locations, KDC settings) is kept.
func Security(ctx context.Context, api EMRAPI, name string) (*model.SecurityPosture, error) {
	out, err := api.DescribeSecurityConfiguration(ctx, &emr.DescribeSecurityConfigurationInput{Name: aws.String(name)})
	if err != nil {
		return nil, fmt.Errorf("DescribeSecurityConfiguration %s: %w", name, err)
	}
	var doc struct {
		EncryptionConfiguration struct {
			EnableAtRestEncryption        bool
			EnableInTransitEncryption     bool
			AtRestEncryptionConfiguration struct {
				S3EncryptionConfiguration *struct {
					EncryptionMode string
				}
				LocalDiskEncryptionConfiguration *struct {
					EnableEbsEncryption bool
				}
			}
		}
		AuthenticationConfiguration struct {
			KerberosConfiguration *struct {
				Provider string
			}
		}
		AuthorizationConfiguration struct {
			IAMConfiguration struct {
				EnableApplicationScopedIAMRole bool
			}
			LakeFormationConfiguration *json.RawMessage
		}
	}
	if err := json.Unmarshal([]byte(aws.ToString(out.SecurityConfiguration)), &doc); err != nil {
		return nil, fmt.Errorf("security configuration %s: %w", name, err)
	}
	e := doc.EncryptionConfiguration
	p := &model.SecurityPosture{Name: name, AtRestEncryption: e.EnableAtRestEncryption, InTransitEncryption: e.EnableInTransitEncryption,
		RuntimeRoles:  doc.AuthorizationConfiguration.IAMConfiguration.EnableApplicationScopedIAMRole,
		LakeFormation: doc.AuthorizationConfiguration.LakeFormationConfiguration != nil, Source: "EMR DescribeSecurityConfiguration " + name}
	if e.EnableAtRestEncryption {
		if s3 := e.AtRestEncryptionConfiguration.S3EncryptionConfiguration; s3 != nil {
			p.S3Encryption = s3.EncryptionMode
		}
		if ld := e.AtRestEncryptionConfiguration.LocalDiskEncryptionConfiguration; ld != nil {
			p.LocalDiskEncryption, p.EBSEncryption = true, ld.EnableEbsEncryption
		}
	}
	if k := doc.AuthenticationConfiguration.KerberosConfiguration; k != nil {
		p.Kerberos = k.Provider
	}
	return p, nil
}

// StepRole returns a step's runtime role (DescribeStep), or "" when it ran
// with the cluster's instance profile.
func StepRole(ctx context.Context, api EMRAPI, clusterID, stepID string) (string, error) {
	out, err := api.DescribeStep(ctx, &emr.DescribeStepInput{ClusterId: aws.String(clusterID), StepId: aws.String(stepID)})
	if err != nil {
		return "", fmt.Errorf("DescribeStep %s: %w", stepID, err)
	}
	if out.Step == nil {
		return "", nil
	}
	return aws.ToString(out.Step.ExecutionRoleArn), nil
}

// EC2API is the part of the EC2 client sparkplain uses: one Describe call.
// Tests stub it.
type EC2API interface {
	DescribeInstanceTypes(ctx context.Context, in *ec2.DescribeInstanceTypesInput, opts ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error)
}

// NewEC2 makes a client from a loaded AWS config.
func NewEC2(cfg aws.Config) EC2API { return ec2.NewFromConfig(cfg) }

// InstanceSizes fills each instance's vCPU and memory from EC2
// DescribeInstanceTypes, one call for all the cluster's types.
func InstanceSizes(ctx context.Context, api EC2API, cl *model.Cluster) error {
	seen := map[string]bool{}
	var names []ec2types.InstanceType
	for _, in := range cl.Instances {
		if in.Type != "" && !seen[in.Type] {
			seen[in.Type] = true
			names = append(names, ec2types.InstanceType(in.Type))
		}
	}
	if len(names) == 0 {
		return nil
	}
	sizes := map[string][2]int64{}
	p := ec2.NewDescribeInstanceTypesPaginator(api, &ec2.DescribeInstanceTypesInput{InstanceTypes: names})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("EC2 DescribeInstanceTypes: %w", err)
		}
		for _, t := range page.InstanceTypes {
			var v, m int64
			if t.VCpuInfo != nil {
				v = int64(aws.ToInt32(t.VCpuInfo.DefaultVCpus))
			}
			if t.MemoryInfo != nil {
				m = aws.ToInt64(t.MemoryInfo.SizeInMiB) << 20
			}
			sizes[string(t.InstanceType)] = [2]int64{v, m}
		}
	}
	for i := range cl.Instances {
		if s, ok := sizes[cl.Instances[i].Type]; ok {
			cl.Instances[i].VCPU, cl.Instances[i].MemoryBytes = int(s[0]), s[1]
		}
	}
	return nil
}
