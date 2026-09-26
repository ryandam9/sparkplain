// Package awsmeta reads cluster metadata from AWS APIs (SPEC §3). Every call
// is read-only: Describe and List.
package awsmeta

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/aws/smithy-go"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/redact"
	"github.com/ryandam9/sparkplain/internal/sparkplain/source"
)

// EMRAPI is the part of the EMR client sparkplain uses: reads only. Tests
// stub it.
type EMRAPI interface {
	DescribeCluster(ctx context.Context, in *emr.DescribeClusterInput, opts ...func(*emr.Options)) (*emr.DescribeClusterOutput, error)
	ListClusters(ctx context.Context, in *emr.ListClustersInput, opts ...func(*emr.Options)) (*emr.ListClustersOutput, error)
	ListSteps(ctx context.Context, in *emr.ListStepsInput, opts ...func(*emr.Options)) (*emr.ListStepsOutput, error)
	ListInstances(ctx context.Context, in *emr.ListInstancesInput, opts ...func(*emr.Options)) (*emr.ListInstancesOutput, error)
	ListInstanceGroups(ctx context.Context, in *emr.ListInstanceGroupsInput, opts ...func(*emr.Options)) (*emr.ListInstanceGroupsOutput, error)
	ListInstanceFleets(ctx context.Context, in *emr.ListInstanceFleetsInput, opts ...func(*emr.Options)) (*emr.ListInstanceFleetsOutput, error)
	DescribeSecurityConfiguration(ctx context.Context, in *emr.DescribeSecurityConfigurationInput, opts ...func(*emr.Options)) (*emr.DescribeSecurityConfigurationOutput, error)
	DescribeStep(ctx context.Context, in *emr.DescribeStepInput, opts ...func(*emr.Options)) (*emr.DescribeStepOutput, error)
}

// NewEMR makes a client from a loaded AWS config.
func NewEMR(cfg aws.Config) EMRAPI { return emr.NewFromConfig(cfg) }

// ErrNotFound is returned when no cluster matches.
var ErrNotFound = errors.New("cluster not found")

// Describe reads one cluster. It works for terminated clusters while EMR
// keeps their metadata (about two months).
func Describe(ctx context.Context, api EMRAPI, id string) (model.Cluster, error) {
	out, err := api.DescribeCluster(ctx, &emr.DescribeClusterInput{ClusterId: aws.String(id)})
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) && ae.ErrorCode() == "InvalidRequestException" && strings.Contains(ae.ErrorMessage(), "not valid") {
			return model.Cluster{}, fmt.Errorf("%s: %w", id, ErrNotFound)
		}
		return model.Cluster{}, fmt.Errorf("DescribeCluster %s: %w", id, err)
	}
	c := out.Cluster
	cl := model.Cluster{ID: aws.ToString(c.Id), Name: aws.ToString(c.Name), Release: aws.ToString(c.ReleaseLabel),
		LogURI: aws.ToString(c.LogUri), ServiceRole: aws.ToString(c.ServiceRole), PrimaryDNS: aws.ToString(c.MasterPublicDnsName),
		SecurityConfig: aws.ToString(c.SecurityConfiguration), Source: "EMR DescribeCluster " + id}
	if c.Ec2InstanceAttributes != nil {
		cl.InstanceProfile = aws.ToString(c.Ec2InstanceAttributes.IamInstanceProfile)
	}
	if k := c.KerberosAttributes; k != nil {
		cl.KerberosRealm = aws.ToString(k.Realm) // never the KDC or cross-realm passwords
	}
	cl.Fleets = c.InstanceCollectionType == types.InstanceCollectionTypeInstanceFleet
	if c.Status != nil {
		cl.State = string(c.Status.State)
		if c.Status.StateChangeReason != nil {
			cl.StateCode = string(c.Status.StateChangeReason.Code)
			cl.StateReason = aws.ToString(c.Status.StateChangeReason.Message)
		}
		if t := c.Status.Timeline; t != nil {
			cl.Created, cl.Ended = aws.ToTime(t.CreationDateTime), aws.ToTime(t.EndDateTime)
		}
	}
	for _, a := range c.Applications {
		cl.Applications = append(cl.Applications, aws.ToString(a.Name)+" "+aws.ToString(a.Version))
	}
	flattenConfig(&cl, c.Configurations, "")
	return cl, nil
}

// flattenConfig turns EMR's nested classifications into "classification/key"
// entries, redacted by key.
func flattenConfig(cl *model.Cluster, cfgs []types.Configuration, prefix string) {
	for _, c := range cfgs {
		name := prefix + aws.ToString(c.Classification)
		for k, v := range c.Properties {
			if cl.Configurations == nil {
				cl.Configurations = map[string]string{}
			}
			rv, _ := redact.Value(k, v)
			cl.Configurations[name+"/"+k] = rv
		}
		flattenConfig(cl, c.Configurations, name+"/")
	}
}

// FindByName returns the ID of the most recently created cluster with this
// name, in any state.
func FindByName(ctx context.Context, api EMRAPI, name string) (string, error) {
	var best types.ClusterSummary
	p := emr.NewListClustersPaginator(api, &emr.ListClustersInput{})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return "", fmt.Errorf("ListClusters: %w", err)
		}
		for _, c := range page.Clusters {
			if aws.ToString(c.Name) != name {
				continue
			}
			if best.Id == nil || created(c).After(created(best)) {
				best = c
			}
		}
	}
	if best.Id == nil {
		return "", fmt.Errorf("no cluster named %q: %w", name, ErrNotFound)
	}
	return aws.ToString(best.Id), nil
}

func created(c types.ClusterSummary) time.Time {
	if c.Status != nil && c.Status.Timeline != nil {
		return aws.ToTime(c.Status.Timeline.CreationDateTime)
	}
	return time.Time{}
}

// Steps lists a cluster's steps, oldest first, with arguments redacted.
func Steps(ctx context.Context, api EMRAPI, id string) ([]model.Step, error) {
	var out []model.Step
	p := emr.NewListStepsPaginator(api, &emr.ListStepsInput{ClusterId: aws.String(id)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return out, fmt.Errorf("ListSteps %s: %w", id, err)
		}
		for _, s := range page.Steps {
			st := model.Step{ID: aws.ToString(s.Id), Name: redact.Text(aws.ToString(s.Name)), Source: "EMR ListSteps " + id}
			if s.Config != nil {
				st.Jar = aws.ToString(s.Config.Jar)
				st.Args = redact.Args(s.Config.Args)
			}
			if s.Status != nil {
				st.State = string(s.Status.State)
				if t := s.Status.Timeline; t != nil {
					st.Started, st.Ended = aws.ToTime(t.StartDateTime), aws.ToTime(t.EndDateTime)
				}
				if f := s.Status.FailureDetails; f != nil {
					st.FailureReason, st.FailureMessage = redact.Text(aws.ToString(f.Reason)), redact.Text(aws.ToString(f.Message))
					st.FailureLog = aws.ToString(f.LogFile)
				}
			}
			out = append(out, st)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Started.Before(out[j].Started) })
	return out, nil
}

// Instances lists the cluster's EC2 instances, current and ended
// (ListInstances), marking the primary node by the DNS name DescribeCluster
// gave. Node logs are kept by instance ID, the event log names hosts, and
// this joins the two.
func Instances(ctx context.Context, api EMRAPI, cl model.Cluster) ([]model.Instance, error) {
	var out []model.Instance
	p := emr.NewListInstancesPaginator(api, &emr.ListInstancesInput{ClusterId: aws.String(cl.ID)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return out, fmt.Errorf("ListInstances %s: %w", cl.ID, err)
		}
		for _, i := range page.Instances {
			in := model.Instance{ID: aws.ToString(i.Ec2InstanceId), PrivateDNS: aws.ToString(i.PrivateDnsName),
				PrivateIP: aws.ToString(i.PrivateIpAddress), Type: aws.ToString(i.InstanceType), Market: string(i.Market),
				GroupID: aws.ToString(i.InstanceGroupId)}
			if in.GroupID == "" {
				in.GroupID = aws.ToString(i.InstanceFleetId)
			}
			if st := i.Status; st != nil {
				in.State = string(st.State)
				if st.StateChangeReason != nil {
					in.StateReason = redact.Text(aws.ToString(st.StateChangeReason.Message))
				}
				if t := st.Timeline; t != nil {
					in.Created, in.Ended = aws.ToTime(t.CreationDateTime), aws.ToTime(t.EndDateTime)
				}
			}
			if d := cl.PrimaryDNS; d != "" && (d == in.PrivateDNS || d == aws.ToString(i.PublicDnsName) || d == in.PrivateIP) {
				in.Primary = true
			}
			out = append(out, in)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Created.Before(out[j].Created) })
	return out, nil
}

// ErrorClass names an EMR API error's class for the Sources panel.
func ErrorClass(err error) string {
	var ae smithy.APIError
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return source.ClassTimeout
	case errors.As(err, &ae):
		switch ae.ErrorCode() {
		case "AccessDeniedException", "AccessDenied", "UnauthorizedOperation", "ExpiredTokenException", "UnrecognizedClientException":
			return source.ClassAccessDenied
		case "ThrottlingException", "Throttling", "RequestLimitExceeded":
			return source.ClassThrottled
		}
	}
	return source.ClassOther
}

var appIDRE = regexp.MustCompile(`application_\d{10,}_\d{4,}`)

// AppInStepLog returns the first Spark application ID a step's stderr
// mentions, which is the application it submitted (SPEC §2).
func AppInStepLog(r io.Reader) string {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		if id := appIDRE.FindString(sc.Text()); id != "" {
			return id
		}
	}
	return ""
}
