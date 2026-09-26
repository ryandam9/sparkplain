// Command recordaws records what one EMR cluster's AWS APIs say, over the
// cluster's lifetime, for awsfake to answer sparkplain's calls in tests.
// Every call is read-only (Describe, List, Get, Lookup). The output holds
// real IDs, hosts and account numbers: scrub it with
// scripts/fixtures/scrub_emrlogs.py before committing anything.
//
//	go run ./scripts/recordaws -profile default -region ap-southeast-2 -cluster-id j-… -out /tmp/rec.json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
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

	"github.com/ryandam9/sparkplain/internal/sparkplain/awsmeta"
	"github.com/ryandam9/sparkplain/internal/sparkplain/awsmeta/awsfake"
	"github.com/ryandam9/sparkplain/internal/sparkplain/source"
)

func main() {
	profile := flag.String("profile", "", "AWS profile")
	region := flag.String("region", "", "AWS region")
	id := flag.String("cluster-id", "", "EMR cluster ID")
	out := flag.String("out", "", "output JSON file")
	flag.Parse()
	if *id == "" || *out == "" {
		log.Fatal("-cluster-id and -out are required")
	}
	ctx := context.Background()
	cfg, err := source.LoadAWS(ctx, *profile, *region)
	must(err)
	em, e2, cw, ct := emr.NewFromConfig(cfg), ec2.NewFromConfig(cfg), cloudwatch.NewFromConfig(cfg), cloudtrail.NewFromConfig(cfg)
	rec := awsfake.Recording{RecordedAt: time.Now().UTC(), Security: map[string]string{}}

	dc, err := em.DescribeCluster(ctx, &emr.DescribeClusterInput{ClusterId: id})
	must(err)
	rec.Cluster = *dc.Cluster
	for p := emr.NewListStepsPaginator(em, &emr.ListStepsInput{ClusterId: id}); p.HasMorePages(); {
		page, err := p.NextPage(ctx)
		must(err)
		rec.Steps = append(rec.Steps, page.Steps...)
	}
	for _, s := range rec.Steps {
		d, err := em.DescribeStep(ctx, &emr.DescribeStepInput{ClusterId: id, StepId: s.Id})
		must(err)
		rec.StepDetails = append(rec.StepDetails, *d.Step)
	}
	for p := emr.NewListInstancesPaginator(em, &emr.ListInstancesInput{ClusterId: id}); p.HasMorePages(); {
		page, err := p.NextPage(ctx)
		must(err)
		rec.Instances = append(rec.Instances, page.Instances...)
	}
	if rec.Cluster.InstanceCollectionType == emrtypes.InstanceCollectionTypeInstanceFleet {
		for p := emr.NewListInstanceFleetsPaginator(em, &emr.ListInstanceFleetsInput{ClusterId: id}); p.HasMorePages(); {
			page, err := p.NextPage(ctx)
			must(err)
			rec.Fleets = append(rec.Fleets, page.InstanceFleets...)
		}
	} else {
		for p := emr.NewListInstanceGroupsPaginator(em, &emr.ListInstanceGroupsInput{ClusterId: id}); p.HasMorePages(); {
			page, err := p.NextPage(ctx)
			must(err)
			rec.Groups = append(rec.Groups, page.InstanceGroups...)
		}
	}
	if name := aws.ToString(rec.Cluster.SecurityConfiguration); name != "" {
		sc, err := em.DescribeSecurityConfiguration(ctx, &emr.DescribeSecurityConfigurationInput{Name: aws.String(name)})
		must(err)
		rec.Security[name] = aws.ToString(sc.SecurityConfiguration)
	}
	types := map[ec2types.InstanceType]bool{}
	var tl []ec2types.InstanceType
	for _, in := range rec.Instances {
		t := ec2types.InstanceType(aws.ToString(in.InstanceType))
		if !types[t] {
			types[t] = true
			tl = append(tl, t)
		}
	}
	it, err := e2.DescribeInstanceTypes(ctx, &ec2.DescribeInstanceTypesInput{InstanceTypes: tl})
	must(err)
	rec.InstanceTypes = it.InstanceTypes

	from := aws.ToTime(rec.Cluster.Status.Timeline.CreationDateTime).Add(-10 * time.Minute)
	to := aws.ToTime(rec.Cluster.Status.Timeline.EndDateTime)
	if to.IsZero() {
		to = time.Now()
	}
	to = to.Add(10 * time.Minute)
	clusterSpecs, hostSpecs := awsmeta.MetricSpecs()
	type q struct {
		spec awsmeta.MetricSpec
		dims []cwtypes.Dimension
	}
	var qs []q
	for _, s := range clusterSpecs {
		qs = append(qs, q{s, []cwtypes.Dimension{{Name: aws.String("JobFlowId"), Value: id}}})
	}
	for _, in := range rec.Instances {
		for _, s := range hostSpecs {
			qs = append(qs, q{s, []cwtypes.Dimension{{Name: aws.String("InstanceId"), Value: in.Ec2InstanceId}}})
		}
		lm, err := cw.ListMetrics(ctx, &cloudwatch.ListMetricsInput{Namespace: aws.String("CWAgent"),
			Dimensions: []cwtypes.DimensionFilter{{Name: aws.String("InstanceId"), Value: in.Ec2InstanceId}}})
		must(err)
		for _, m := range lm.Metrics {
			rec.AgentMetrics = append(rec.AgentMetrics, m)
			qs = append(qs, q{awsmeta.MetricSpec{Namespace: "CWAgent", Name: aws.ToString(m.MetricName), Stat: "Maximum"}, m.Dimensions})
		}
	}
	for _, x := range qs {
		res, err := cw.GetMetricData(ctx, &cloudwatch.GetMetricDataInput{StartTime: aws.Time(from), EndTime: aws.Time(to), ScanBy: cwtypes.ScanByTimestampAscending,
			MetricDataQueries: []cwtypes.MetricDataQuery{{Id: aws.String("q0"), MetricStat: &cwtypes.MetricStat{Period: aws.Int32(60), Stat: aws.String(x.spec.Stat),
				Metric: &cwtypes.Metric{Namespace: aws.String(x.spec.Namespace), MetricName: aws.String(x.spec.Name), Dimensions: x.dims}}}}})
		must(err)
		m := awsfake.Metric{Namespace: x.spec.Namespace, Name: x.spec.Name, Stat: x.spec.Stat, Period: 60, Dims: x.dims}
		for _, r := range res.MetricDataResults {
			for i, t := range r.Timestamps {
				m.Points = append(m.Points, awsfake.Point{T: t.UTC(), V: r.Values[i]})
			}
		}
		rec.Metrics = append(rec.Metrics, m)
	}
	for _, in := range rec.Instances {
		for p := cloudtrail.NewLookupEventsPaginator(ct, &cloudtrail.LookupEventsInput{StartTime: aws.Time(from), EndTime: aws.Time(to),
			LookupAttributes: []cttypes.LookupAttribute{{AttributeKey: cttypes.LookupAttributeKeyUsername, AttributeValue: in.Ec2InstanceId}}}); p.HasMorePages(); {
			time.Sleep(500 * time.Millisecond) // CloudTrail allows 2 lookups a second
			page, err := p.NextPage(ctx)
			must(err)
			rec.Events = append(rec.Events, page.Events...)
		}
	}
	b, err := json.MarshalIndent(rec, "", " ")
	must(err)
	must(os.WriteFile(*out, b, 0o600))
	log.Printf("recorded %s: %d steps, %d instances, %d metrics, %d events", *id, len(rec.Steps), len(rec.Instances), len(rec.Metrics), len(rec.Events))
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
