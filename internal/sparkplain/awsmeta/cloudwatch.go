package awsmeta

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// CloudWatchAPI is the part of the CloudWatch client sparkplain uses:
// reads only. Tests stub it.
type CloudWatchAPI interface {
	ListMetrics(ctx context.Context, in *cloudwatch.ListMetricsInput, opts ...func(*cloudwatch.Options)) (*cloudwatch.ListMetricsOutput, error)
	GetMetricData(ctx context.Context, in *cloudwatch.GetMetricDataInput, opts ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricDataOutput, error)
}

// NewCloudWatch makes a client from a loaded AWS config.
func NewCloudWatch(cfg aws.Config) CloudWatchAPI { return cloudwatch.NewFromConfig(cfg) }

// MetricSpec is one metric sparkplain reads.
type MetricSpec struct {
	Namespace, Name, Stat, Unit string
}

// MetricSpecs lists the cluster-wide and per-node metrics sparkplain reads
// (scripts/recordaws records the same).
func MetricSpecs() (cluster, host []MetricSpec) {
	return append([]MetricSpec{}, clusterMetrics...), append([]MetricSpec{}, hostMetrics...)
}

// The cluster's metrics, published every minute by EMR 7 (checked on the
// test clusters), and each node's, every 5 minutes with EC2's basic
// monitoring.
var (
	clusterMetrics = []MetricSpec{
		{"AWS/ElasticMapReduce", "ContainerPending", "Maximum", "count"},
		{"AWS/ElasticMapReduce", "ContainerAllocated", "Maximum", "count"},
		{"AWS/ElasticMapReduce", "AppsRunning", "Maximum", "count"},
		{"AWS/ElasticMapReduce", "AppsPending", "Maximum", "count"},
		{"AWS/ElasticMapReduce", "YARNMemoryAvailablePercentage", "Minimum", "percent"},
		{"AWS/ElasticMapReduce", "MemoryTotalMB", "Maximum", "MB"},
		{"AWS/ElasticMapReduce", "MemoryAllocatedMB", "Maximum", "MB"},
		{"AWS/ElasticMapReduce", "MRLostNodes", "Maximum", "count"},
		{"AWS/ElasticMapReduce", "MRUnhealthyNodes", "Maximum", "count"},
		{"AWS/ElasticMapReduce", "S3BytesRead", "Sum", "bytes"},
		{"AWS/ElasticMapReduce", "S3BytesWritten", "Sum", "bytes"},
	}
	hostMetrics = []MetricSpec{
		{"AWS/EC2", "CPUUtilization", "Average", "percent"},
		{"AWS/EC2", "CPUUtilization", "Maximum", "percent"},
		{"AWS/EC2", "NetworkIn", "Sum", "bytes"},
		{"AWS/EC2", "NetworkOut", "Sum", "bytes"},
	}
	// agentMetrics are the CloudWatch agent's metrics sparkplain shows, when
	// the agent publishes them for an instance.
	agentMetrics = map[string]string{"mem_used_percent": "percent", "swap_used_percent": "percent", "disk_used_percent": "percent"}
)

// Retention of the data CloudWatch keeps (1-minute points for 15 days,
// 5-minute points for 63 days).
const (
	minuteDataDays = 15
	fiveMinuteDays = 63
)

// Metrics reads the cluster's and its nodes' metrics from from to to
// (GetMetricData, after ListMetrics finds any CloudWatch agent metrics).
// The query window is clipped to what CloudWatch still keeps, and each
// series says where it came from.
func Metrics(ctx context.Context, api CloudWatchAPI, clusterID string, instances []string, from, to, now time.Time) (*model.MetricsSection, error) {
	sec := &model.MetricsSection{From: from, To: to, Coverage: model.Complete}
	period := 60
	if now.Sub(from) > minuteDataDays*24*time.Hour {
		period = 300
	}
	if now.Sub(from) > fiveMinuteDays*24*time.Hour {
		sec.Coverage = model.NoData
		sec.Missing = append(sec.Missing, fmt.Sprintf("CloudWatch keeps these metrics for %d days; this run is older.", fiveMinuteDays))
		return sec, nil
	}
	type query struct {
		spec     MetricSpec
		dimName  string
		dimValue string
		host     bool
	}
	var qs []query
	for _, m := range clusterMetrics {
		qs = append(qs, query{m, "JobFlowId", clusterID, false})
	}
	for _, id := range instances {
		for _, m := range hostMetrics {
			qs = append(qs, query{m, "InstanceId", id, true})
		}
	}
	// The agent's metrics carry more dimensions than the instance ID, so
	// list them to query each exactly.
	agent := map[int]cwtypes.Metric{}
	for _, id := range instances {
		p := cloudwatch.NewListMetricsPaginator(api, &cloudwatch.ListMetricsInput{Namespace: aws.String("CWAgent"),
			Dimensions: []cwtypes.DimensionFilter{{Name: aws.String("InstanceId"), Value: aws.String(id)}}})
		for p.HasMorePages() {
			page, err := p.NextPage(ctx)
			if err != nil {
				return nil, fmt.Errorf("CloudWatch ListMetrics: %w", err)
			}
			for _, m := range page.Metrics {
				unit, ok := agentMetrics[aws.ToString(m.MetricName)]
				if !ok {
					continue
				}
				agent[len(qs)] = m
				qs = append(qs, query{MetricSpec{"CWAgent", aws.ToString(m.MetricName), "Maximum", unit}, "InstanceId", id, true})
			}
		}
	}
	if len(agent) == 0 && len(instances) > 0 {
		sec.Missing = append(sec.Missing, "Node memory and disk use: they need the CloudWatch agent, which publishes nothing for these nodes.")
	}

	series := make([]model.Series, len(qs))
	var mq []cwtypes.MetricDataQuery
	for i, q := range qs {
		metric := &cwtypes.Metric{Namespace: aws.String(q.spec.Namespace), MetricName: aws.String(q.spec.Name),
			Dimensions: []cwtypes.Dimension{{Name: aws.String(q.dimName), Value: aws.String(q.dimValue)}}}
		if m, ok := agent[i]; ok {
			metric.Dimensions = m.Dimensions
		}
		mq = append(mq, cwtypes.MetricDataQuery{Id: aws.String("q" + strconv.Itoa(i)), ReturnData: aws.Bool(true),
			MetricStat: &cwtypes.MetricStat{Metric: metric, Period: aws.Int32(int32(period)), Stat: aws.String(q.spec.Stat)}})
		series[i] = model.Series{Namespace: q.spec.Namespace, Name: q.spec.Name, Stat: q.spec.Stat, Scope: q.dimValue, Unit: q.spec.Unit, PeriodS: period,
			Points: []model.Point{}, Source: fmt.Sprintf("CloudWatch %s %s (%s, %s=%s)", q.spec.Namespace, q.spec.Name, q.spec.Stat, q.dimName, q.dimValue)}
	}
	// GetMetricData takes up to 500 queries a call.
	for startQ := 0; startQ < len(mq); startQ += 500 {
		batch := mq[startQ:min(startQ+500, len(mq))]
		p := cloudwatch.NewGetMetricDataPaginator(api, &cloudwatch.GetMetricDataInput{MetricDataQueries: batch,
			StartTime: aws.Time(from), EndTime: aws.Time(to), ScanBy: cwtypes.ScanByTimestampAscending})
		for p.HasMorePages() {
			page, err := p.NextPage(ctx)
			if err != nil {
				return nil, fmt.Errorf("CloudWatch GetMetricData: %w", err)
			}
			for _, res := range page.MetricDataResults {
				i, err := strconv.Atoi(aws.ToString(res.Id)[1:])
				if err != nil || i >= len(series) {
					continue
				}
				for k, t := range res.Timestamps {
					if k < len(res.Values) {
						series[i].Points = append(series[i].Points, model.Point{T: t.UTC(), V: res.Values[k]})
					}
				}
				if res.StatusCode != cwtypes.StatusCodeComplete && res.StatusCode != "" {
					series[i].Status = string(res.StatusCode)
				}
			}
		}
	}
	empty := 0
	for i, q := range qs {
		s := series[i]
		sort.Slice(s.Points, func(a, b int) bool { return s.Points[a].T.Before(s.Points[b].T) })
		if len(s.Points) == 0 {
			empty++
		}
		if q.host {
			sec.Hosts = append(sec.Hosts, s)
		} else {
			sec.Cluster = append(sec.Cluster, s)
		}
	}
	if empty == len(qs) {
		sec.Coverage = model.NoData
		sec.Missing = append(sec.Missing, "CloudWatch returned no data for this window.")
	}
	return sec, nil
}
