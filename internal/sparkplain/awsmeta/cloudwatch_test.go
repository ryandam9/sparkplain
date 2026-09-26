package awsmeta

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

// stubCW answers each query with points named after its metric, lists
// agent metrics for one instance, and records what it was asked.
type stubCW struct {
	agentFor string
	calls    int
	queries  int
	window   [2]time.Time
}

func (s *stubCW) ListMetrics(_ context.Context, in *cloudwatch.ListMetricsInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.ListMetricsOutput, error) {
	if aws.ToString(in.Dimensions[0].Value) != s.agentFor {
		return &cloudwatch.ListMetricsOutput{}, nil
	}
	dims := []cwtypes.Dimension{{Name: aws.String("InstanceId"), Value: aws.String(s.agentFor)}, {Name: aws.String("host"), Value: aws.String("ip-10-0-0-2")}}
	return &cloudwatch.ListMetricsOutput{Metrics: []cwtypes.Metric{
		{Namespace: aws.String("CWAgent"), MetricName: aws.String("mem_used_percent"), Dimensions: dims},
		{Namespace: aws.String("CWAgent"), MetricName: aws.String("cpu_usage_iowait"), Dimensions: dims}, // not one sparkplain shows
	}}, nil
}

func (s *stubCW) GetMetricData(_ context.Context, in *cloudwatch.GetMetricDataInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricDataOutput, error) {
	s.calls++
	s.queries += len(in.MetricDataQueries)
	s.window = [2]time.Time{aws.ToTime(in.StartTime), aws.ToTime(in.EndTime)}
	out := &cloudwatch.GetMetricDataOutput{}
	for _, q := range in.MetricDataQueries {
		m := q.MetricStat.Metric
		if aws.ToString(m.Namespace) == "CWAgent" && len(m.Dimensions) != 2 {
			continue // an agent metric must be asked with all its dimensions
		}
		t0 := aws.ToTime(in.StartTime)
		// Out of order on purpose: the reader sorts.
		out.MetricDataResults = append(out.MetricDataResults, cwtypes.MetricDataResult{Id: q.Id, StatusCode: cwtypes.StatusCodePartialData,
			Timestamps: []time.Time{t0.Add(2 * time.Minute), t0.Add(time.Minute)}, Values: []float64{2, 1}})
	}
	return out, nil
}

func TestMetrics(t *testing.T) {
	now := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	from, to := now.Add(-2*time.Hour), now.Add(-time.Hour)
	var ids []string
	for i := 0; i < 130; i++ { // 11 cluster queries + 4 per node: more than one GetMetricData call
		ids = append(ids, "i-"+string(rune('a'+i%26))+strings.Repeat("0", i/26))
	}
	api := &stubCW{agentFor: ids[0]}
	sec, err := Metrics(context.Background(), api, "j-1", ids, from, to, now)
	if err != nil {
		t.Fatal(err)
	}
	if api.calls != 2 || api.queries != 11+4*130+1 || !api.window[0].Equal(from) || !api.window[1].Equal(to) {
		t.Errorf("calls %d, queries %d, window %v", api.calls, api.queries, api.window)
	}
	if len(sec.Cluster) != 11 || len(sec.Hosts) != 4*130+1 {
		t.Fatalf("series: %d cluster, %d hosts", len(sec.Cluster), len(sec.Hosts))
	}
	s := sec.Cluster[0]
	if s.Name != "ContainerPending" || s.PeriodS != 60 || len(s.Points) != 2 || s.Points[0].V != 1 || s.Status != "PartialData" || !strings.Contains(s.Source, "JobFlowId=j-1") {
		t.Errorf("first series = %+v", s)
	}
	agent := sec.Hosts[len(sec.Hosts)-1]
	if agent.Namespace != "CWAgent" || agent.Name != "mem_used_percent" || len(agent.Points) != 2 {
		t.Errorf("agent series = %+v", agent)
	}
	if sec.Coverage != "complete" || len(sec.Missing) != 0 {
		t.Errorf("coverage %v, missing %v", sec.Coverage, sec.Missing)
	}

	// Past 15 days CloudWatch has only 5-minute points; past 63, nothing.
	old, _ := Metrics(context.Background(), &stubCW{}, "j-1", []string{"i-1"}, now.Add(-20*24*time.Hour), now.Add(-20*24*time.Hour+time.Hour), now)
	if old.Cluster[0].PeriodS != 300 || !strings.Contains(strings.Join(old.Missing, " "), "CloudWatch agent") {
		t.Errorf("20 days ago: period %d, missing %v", old.Cluster[0].PeriodS, old.Missing)
	}
	gone, _ := Metrics(context.Background(), &stubCW{}, "j-1", []string{"i-1"}, now.Add(-70*24*time.Hour), now.Add(-70*24*time.Hour+time.Hour), now)
	if gone.Coverage != "no-data" || len(gone.Cluster) != 0 {
		t.Errorf("70 days ago: %+v", gone)
	}
}
