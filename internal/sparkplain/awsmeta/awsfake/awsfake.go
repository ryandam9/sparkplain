// Package awsfake answers sparkplain's AWS calls from a recording of one
// real cluster (made by scripts/recordaws with read-only calls, then
// scrubbed), so tests run the real query code against real answers without
// calling AWS.
package awsfake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
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
)

// Recording is what one cluster's AWS APIs said, over its lifetime.
type Recording struct {
	RecordedAt    time.Time                   `json:"recordedAt"`
	Cluster       emrtypes.Cluster            `json:"cluster"`
	Steps         []emrtypes.StepSummary      `json:"steps"`
	StepDetails   []emrtypes.Step             `json:"stepDetails"`
	Instances     []emrtypes.Instance         `json:"instances"`
	Groups        []emrtypes.InstanceGroup    `json:"groups"`
	Fleets        []emrtypes.InstanceFleet    `json:"fleets,omitempty"`
	Security      map[string]string           `json:"security,omitempty"` // name → JSON document
	InstanceTypes []ec2types.InstanceTypeInfo `json:"instanceTypes"`
	Metrics       []Metric                    `json:"metrics"`
	AgentMetrics  []cwtypes.Metric            `json:"agentMetrics,omitempty"`
	Events        []cttypes.Event             `json:"events"`
}

// Metric is one metric's points at one period and statistic.
type Metric struct {
	Namespace string              `json:"namespace"`
	Name      string              `json:"name"`
	Stat      string              `json:"stat"`
	Period    int32               `json:"period"`
	Dims      []cwtypes.Dimension `json:"dims"`
	Points    []Point             `json:"points"`
}

// Point is one recorded value.
type Point struct {
	T time.Time `json:"t"`
	V float64   `json:"v"`
}

// Load reads a recording.
func Load(path string) (*Recording, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var r Recording
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &r, nil
}

func notFound(what string) error {
	return &smithy.GenericAPIError{Code: "InvalidRequestException", Message: what + " is not valid."}
}

// EMR answers the EMR calls sparkplain makes.
type EMR struct{ R *Recording }

func (f EMR) DescribeCluster(_ context.Context, in *emr.DescribeClusterInput, _ ...func(*emr.Options)) (*emr.DescribeClusterOutput, error) {
	if aws.ToString(in.ClusterId) != aws.ToString(f.R.Cluster.Id) {
		return nil, notFound("Cluster id '" + aws.ToString(in.ClusterId) + "'")
	}
	c := f.R.Cluster
	return &emr.DescribeClusterOutput{Cluster: &c}, nil
}
func (f EMR) ListClusters(context.Context, *emr.ListClustersInput, ...func(*emr.Options)) (*emr.ListClustersOutput, error) {
	c := f.R.Cluster
	return &emr.ListClustersOutput{Clusters: []emrtypes.ClusterSummary{{Id: c.Id, Name: c.Name, Status: c.Status}}}, nil
}
func (f EMR) ListSteps(context.Context, *emr.ListStepsInput, ...func(*emr.Options)) (*emr.ListStepsOutput, error) {
	return &emr.ListStepsOutput{Steps: f.R.Steps}, nil
}
func (f EMR) ListInstances(context.Context, *emr.ListInstancesInput, ...func(*emr.Options)) (*emr.ListInstancesOutput, error) {
	return &emr.ListInstancesOutput{Instances: f.R.Instances}, nil
}
func (f EMR) ListInstanceGroups(context.Context, *emr.ListInstanceGroupsInput, ...func(*emr.Options)) (*emr.ListInstanceGroupsOutput, error) {
	return &emr.ListInstanceGroupsOutput{InstanceGroups: f.R.Groups}, nil
}
func (f EMR) ListInstanceFleets(context.Context, *emr.ListInstanceFleetsInput, ...func(*emr.Options)) (*emr.ListInstanceFleetsOutput, error) {
	return &emr.ListInstanceFleetsOutput{InstanceFleets: f.R.Fleets}, nil
}
func (f EMR) DescribeSecurityConfiguration(_ context.Context, in *emr.DescribeSecurityConfigurationInput, _ ...func(*emr.Options)) (*emr.DescribeSecurityConfigurationOutput, error) {
	j, ok := f.R.Security[aws.ToString(in.Name)]
	if !ok {
		return nil, notFound("Security configuration '" + aws.ToString(in.Name) + "'")
	}
	return &emr.DescribeSecurityConfigurationOutput{Name: in.Name, SecurityConfiguration: aws.String(j)}, nil
}
func (f EMR) DescribeStep(_ context.Context, in *emr.DescribeStepInput, _ ...func(*emr.Options)) (*emr.DescribeStepOutput, error) {
	for _, s := range f.R.StepDetails {
		if aws.ToString(s.Id) == aws.ToString(in.StepId) {
			s := s
			return &emr.DescribeStepOutput{Step: &s}, nil
		}
	}
	return nil, notFound("Step id '" + aws.ToString(in.StepId) + "'")
}

// EC2 answers DescribeInstanceTypes.
type EC2 struct{ R *Recording }

func (f EC2) DescribeInstanceTypes(_ context.Context, in *ec2.DescribeInstanceTypesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error) {
	out := &ec2.DescribeInstanceTypesOutput{}
	for _, want := range in.InstanceTypes {
		for _, t := range f.R.InstanceTypes {
			if t.InstanceType == want {
				out.InstanceTypes = append(out.InstanceTypes, t)
			}
		}
	}
	return out, nil
}

// CloudWatch answers ListMetrics and GetMetricData from the recorded
// points, for the window and period each query asks.
type CloudWatch struct{ R *Recording }

func (f CloudWatch) ListMetrics(_ context.Context, in *cloudwatch.ListMetricsInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.ListMetricsOutput, error) {
	out := &cloudwatch.ListMetricsOutput{}
	for _, m := range f.R.AgentMetrics {
		if aws.ToString(m.Namespace) != aws.ToString(in.Namespace) {
			continue
		}
		if dimsMatch(m.Dimensions, in.Dimensions) {
			out.Metrics = append(out.Metrics, m)
		}
	}
	return out, nil
}

func dimsMatch(have []cwtypes.Dimension, want []cwtypes.DimensionFilter) bool {
	for _, w := range want {
		ok := false
		for _, h := range have {
			if aws.ToString(h.Name) == aws.ToString(w.Name) && aws.ToString(h.Value) == aws.ToString(w.Value) {
				ok = true
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

func sameDims(a, b []cwtypes.Dimension) bool {
	if len(a) != len(b) {
		return false
	}
	for _, x := range a {
		found := false
		for _, y := range b {
			if aws.ToString(x.Name) == aws.ToString(y.Name) && aws.ToString(x.Value) == aws.ToString(y.Value) {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (f CloudWatch) GetMetricData(_ context.Context, in *cloudwatch.GetMetricDataInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricDataOutput, error) {
	from, to := aws.ToTime(in.StartTime), aws.ToTime(in.EndTime)
	out := &cloudwatch.GetMetricDataOutput{}
	for _, q := range in.MetricDataQueries {
		ms := q.MetricStat
		res := cwtypes.MetricDataResult{Id: q.Id, StatusCode: cwtypes.StatusCodeComplete}
		for _, m := range f.R.Metrics {
			if m.Namespace != aws.ToString(ms.Metric.Namespace) || m.Name != aws.ToString(ms.Metric.MetricName) || m.Stat != aws.ToString(ms.Stat) || !sameDims(m.Dims, ms.Metric.Dimensions) {
				continue
			}
			// Coarser periods than recorded fold the recorded points.
			per := time.Duration(max(aws.ToInt32(ms.Period), m.Period)) * time.Second
			buckets := map[time.Time][]float64{}
			var order []time.Time
			for _, p := range m.Points {
				if p.T.Before(from) || !p.T.Before(to) {
					continue
				}
				b := p.T.Truncate(per)
				if buckets[b] == nil {
					order = append(order, b)
				}
				buckets[b] = append(buckets[b], p.V)
			}
			sort.Slice(order, func(i, j int) bool { return order[i].Before(order[j]) })
			for _, b := range order {
				res.Timestamps = append(res.Timestamps, b)
				res.Values = append(res.Values, fold(m.Stat, buckets[b]))
			}
		}
		out.MetricDataResults = append(out.MetricDataResults, res)
	}
	return out, nil
}

func fold(stat string, vs []float64) float64 {
	v := vs[0]
	var sum float64
	for _, x := range vs {
		sum += x
		switch stat {
		case "Maximum":
			v = max(v, x)
		case "Minimum":
			v = min(v, x)
		}
	}
	switch stat {
	case "Sum":
		return sum
	case "Average":
		return sum / float64(len(vs))
	}
	return v
}

// CloudTrail answers LookupEvents by user name and window, 50 a page.
type CloudTrail struct{ R *Recording }

func (f CloudTrail) LookupEvents(_ context.Context, in *cloudtrail.LookupEventsInput, _ ...func(*cloudtrail.Options)) (*cloudtrail.LookupEventsOutput, error) {
	if len(in.LookupAttributes) != 1 || in.LookupAttributes[0].AttributeKey != cttypes.LookupAttributeKeyUsername {
		return nil, errors.New("awsfake: only lookups by user name are recorded")
	}
	user := aws.ToString(in.LookupAttributes[0].AttributeValue)
	from, to := aws.ToTime(in.StartTime), aws.ToTime(in.EndTime)
	var match []cttypes.Event
	for _, e := range f.R.Events {
		t := aws.ToTime(e.EventTime)
		if aws.ToString(e.Username) == user && !t.Before(from) && !t.After(to) {
			match = append(match, e)
		}
	}
	start := 0
	if in.NextToken != nil {
		start, _ = strconv.Atoi(aws.ToString(in.NextToken))
	}
	size := int(aws.ToInt32(in.MaxResults))
	if size <= 0 {
		size = 50
	}
	end := min(start+size, len(match))
	out := &cloudtrail.LookupEventsOutput{Events: match[start:end]}
	if end < len(match) {
		out.NextToken = aws.String(strconv.Itoa(end))
	}
	return out, nil
}
