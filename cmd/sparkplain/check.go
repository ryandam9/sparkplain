package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/ryandam9/sparkplain/internal/sparkplain/awsmeta"
	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/source"
	"github.com/ryandam9/sparkplain/internal/sparkplain/yarnlog"
)

// STSAPI is the one STS call the access check makes, to say whose
// credentials the run uses.
type STSAPI interface {
	GetCallerIdentity(ctx context.Context, in *sts.GetCallerIdentityInput, opts ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error)
}

// checkTimeout bounds each check: they are single small calls.
var checkTimeout = 15 * time.Second

// checkInput is what the access check needs to know about the run.
type checkInput struct {
	o              options
	eventLogPrefix string // from the config file
}

// accessCheck finds out what the run can read before it reads anything
// (SPEC §6): one small read-only call per source, made in parallel. It
// returns the rows in a fixed order and who the credentials are.
func accessCheck(ctx context.Context, cloud *awsSession, in checkInput) (rows []model.AccessCheck, who string) {
	o := in.o
	if o.clusterID == "" && o.clusterName == "" {
		return offlineCheck(o, in.eventLogPrefix), ""
	}
	cfg, err := cloud.config(ctx)
	if err != nil {
		row := model.AccessCheck{Name: "AWS credentials", Status: "error", Class: "accessDenied", Call: "load profile " + o.profile, Detail: err.Error(),
			Try: "aws sts get-caller-identity --profile " + o.profile}
		return []model.AccessCheck{row, {Name: "Everything else", Status: "skipped", Detail: "Nothing on AWS can be checked or read without credentials."}}, ""
	}
	if id, err := awsDeps.sts(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{}); err == nil {
		who = aws.ToString(id.Arn)
	}
	region := cfg.Region
	try := func(cmd string) string { return cmd + " --profile " + o.profile + " --region " + region }

	// The cluster first: it says where the logs and the event log are.
	emrRow := model.AccessCheck{Name: "EMR API", Call: "DescribeCluster, ListSteps, ListInstances"}
	cl, err := cloud.cluster(ctx, o.clusterID, o.clusterName)
	if err != nil {
		emrRow.Status, emrRow.Class, emrRow.Detail = "denied", awsmeta.ErrorClass(err), err.Error()
		if emrRow.Class != "accessDenied" {
			emrRow.Status = "error"
		}
		emrRow.Try = try("aws emr describe-cluster --cluster-id " + firstNonEmpty(o.clusterID, "<cluster-id>"))
		rows = append(rows, emrRow)
		rows = append(rows, model.AccessCheck{Name: "Logs and the rest", Status: "skipped",
			Detail: "Where the logs are comes from DescribeCluster. Pass -from with a copy of the cluster's logs, or -eventlog, to run without it."})
		return rows, who
	}
	api := awsDeps.emr(cfg)
	var steps, insts error
	var instances []string
	var types []string
	var primary string
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		c, cancel := context.WithTimeout(ctx, checkTimeout)
		defer cancel()
		_, steps = api.ListSteps(c, &emr.ListStepsInput{ClusterId: aws.String(cl.ID)})
	}()
	go func() {
		defer wg.Done()
		c, cancel := context.WithTimeout(ctx, checkTimeout)
		defer cancel()
		out, err := api.ListInstances(c, &emr.ListInstancesInput{ClusterId: aws.String(cl.ID)})
		insts = err
		if err != nil {
			return
		}
		for _, x := range out.Instances {
			id := aws.ToString(x.Ec2InstanceId)
			instances = append(instances, id)
			if t := aws.ToString(x.InstanceType); t != "" && !slices.Contains(types, t) {
				types = append(types, t)
			}
			if aws.ToString(x.PrivateDnsName) == cl.PrimaryDNS || aws.ToString(x.PublicDnsName) == cl.PrimaryDNS {
				primary = id
			}
		}
	}()
	wg.Wait()
	emrRow.Status = "ok"
	emrRow.Detail = strings.Trim(fmt.Sprintf("%s (%s, %s, %s)", cl.ID, cl.Name, cl.Release, cl.State), " ")
	for _, e := range []struct {
		err  error
		call string
	}{{steps, "ListSteps"}, {insts, "ListInstances"}} {
		if e.err != nil {
			emrRow.Status, emrRow.Class = "denied", awsmeta.ErrorClass(e.err)
			emrRow.Detail = fmt.Sprintf("%s failed (%s): %v", e.call, emrRow.Class, e.err)
			emrRow.Try = try(fmt.Sprintf("aws emr %s --cluster-id %s", map[string]string{"ListSteps": "list-steps", "ListInstances": "list-instances"}[e.call], cl.ID))
		}
	}
	rows = append(rows, emrRow)

	var mu sync.Mutex
	add := func(r model.AccessCheck) {
		mu.Lock()
		rows = append(rows, r)
		mu.Unlock()
	}
	var jobs []func(context.Context)

	// The logs under the cluster's log URI.
	bucket, root, ok := yarnlog.LogRoot(cl.LogURI, cl.ID)
	logRows := []struct{ name, prefix string }{
		{"Container logs", root + "containers/" + o.appID + "/"},
		{"Step logs", root + "steps/"},
		{"Node logs", root + "node/"},
	}
	// DescribeCluster gives each application with its version: "HBase 2.4.17-amzn-7".
	hbase := slices.ContainsFunc(cl.Applications, func(a string) bool { name, _, _ := strings.Cut(a, " "); return strings.EqualFold(name, "HBase") })
	if hbase {
		host := primary
		if host == "" && len(instances) > 0 {
			host = instances[0]
		}
		logRows = append(logRows, struct{ name, prefix string }{"HBase server logs", root + "node/" + host + "/applications/hbase/"})
	}
	if !ok {
		for _, lr := range logRows {
			rows = append(rows, model.AccessCheck{Name: lr.name, Status: "skipped", Detail: "The cluster has no log URI, so EMR kept no logs in S3."})
		}
	} else {
		jobs = append(jobs, func(c context.Context) {
			st, err := awsDeps.s3(c, cfg, bucket)
			for _, lr := range logRows {
				r := model.AccessCheck{Name: lr.name, Location: "s3://" + bucket + "/" + lr.prefix, Call: "ListObjectsV2 (one object)",
					Try: try("aws s3 ls s3://" + bucket + "/" + lr.prefix)}
				if err == nil {
					objs, err2 := source.Sample(c, st, lr.prefix, 1)
					r = probed(r, objs, err2, emptyWhy(lr.name))
				} else {
					r = probed(r, nil, err, "")
				}
				add(r)
			}
		})
	}
	if !hbase {
		rows = append(rows, model.AccessCheck{Name: "HBase server logs", Status: "skipped", Detail: "HBase is not installed on this cluster."})
	}

	// Where the event log will come from.
	jobs = append(jobs, func(c context.Context) { add(eventLogCheck(c, cfg, o, in.eventLogPrefix, cl, try)) })

	// CloudWatch, EC2 and CloudTrail.
	if o.noCloudWatch {
		rows = append(rows, model.AccessCheck{Name: "CloudWatch", Status: "skipped", Detail: "Not asked for (-no-cloudwatch)."},
			model.AccessCheck{Name: "EC2 instance types", Status: "skipped", Detail: "Not asked for (-no-cloudwatch)."})
	} else {
		jobs = append(jobs, func(c context.Context) {
			r := model.AccessCheck{Name: "CloudWatch", Call: "ListMetrics (AWS/ElasticMapReduce)", Try: try("aws cloudwatch list-metrics --namespace AWS/ElasticMapReduce --dimensions Name=JobFlowId,Value=" + cl.ID)}
			out, err := awsDeps.cloudwatch(cfg).ListMetrics(c, &cloudwatch.ListMetricsInput{Namespace: aws.String("AWS/ElasticMapReduce"),
				Dimensions: []cwtypes.DimensionFilter{{Name: aws.String("JobFlowId"), Value: aws.String(cl.ID)}}})
			switch {
			case err != nil:
				r = apiFailed(r, err, "cloudwatch:ListMetrics")
			case len(out.Metrics) == 0:
				r.Status, r.Detail = "empty", "Readable, but CloudWatch holds no metrics for this cluster (it keeps them 15 months; a new cluster needs a few minutes)."
			default:
				r.Status, r.Detail = "ok", fmt.Sprintf("%d cluster metrics.", len(out.Metrics))
			}
			add(r)
		}, func(c context.Context) {
			r := model.AccessCheck{Name: "EC2 instance types", Call: "DescribeInstanceTypes"}
			if len(types) == 0 {
				r.Status, r.Detail = "skipped", "No instances listed to look up."
				add(r)
				return
			}
			r.Try = try("aws ec2 describe-instance-types --instance-types " + types[0])
			_, err := awsDeps.ec2(cfg).DescribeInstanceTypes(c, &ec2.DescribeInstanceTypesInput{InstanceTypes: []ec2types.InstanceType{ec2types.InstanceType(types[0])}})
			if err != nil {
				r = apiFailed(r, err, "ec2:DescribeInstanceTypes")
			} else {
				r.Status, r.Detail = "ok", strings.Join(types, ", ")+"."
			}
			add(r)
		})
	}
	if o.noCloudTrail {
		rows = append(rows, model.AccessCheck{Name: "CloudTrail", Status: "skipped", Detail: "Not asked for (-no-cloudtrail)."})
	} else {
		jobs = append(jobs, func(c context.Context) {
			r := model.AccessCheck{Name: "CloudTrail", Call: "LookupEvents (one event)", Try: try("aws cloudtrail lookup-events --max-results 1")}
			if _, err := awsDeps.cloudtrail(cfg).LookupEvents(c, &cloudtrail.LookupEventsInput{MaxResults: aws.Int32(1)}); err != nil {
				r = apiFailed(r, err, "cloudtrail:LookupEvents")
			} else {
				r.Status, r.Detail = "ok", "Readable."
			}
			add(r)
		})
	}
	if cl.SecurityConfig != "" {
		jobs = append(jobs, func(c context.Context) {
			r := model.AccessCheck{Name: "Security configuration", Call: "DescribeSecurityConfiguration", Location: cl.SecurityConfig,
				Try: try("aws emr describe-security-configuration --name " + cl.SecurityConfig)}
			if _, err := api.DescribeSecurityConfiguration(c, &emr.DescribeSecurityConfigurationInput{Name: aws.String(cl.SecurityConfig)}); err != nil {
				r = apiFailed(r, err, "elasticmapreduce:DescribeSecurityConfiguration")
			} else {
				r.Status, r.Detail = "ok", cl.SecurityConfig+"."
			}
			add(r)
		})
	}
	var jw sync.WaitGroup
	for _, j := range jobs {
		jw.Add(1)
		go func() {
			defer jw.Done()
			c, cancel := context.WithTimeout(ctx, checkTimeout)
			defer cancel()
			j(c)
		}()
	}
	jw.Wait()
	return orderChecks(rows), who
}

// checkOrder is the order rows are shown in.
var checkOrder = []string{"AWS", "AWS credentials", "EMR API", "Security configuration", "Container logs", "Step logs", "Node logs", "HBase server logs",
	"Spark event log", "CloudWatch", "EC2 instance types", "CloudTrail", "Logs and the rest", "Everything else"}

func orderChecks(rows []model.AccessCheck) []model.AccessCheck {
	slices.SortStableFunc(rows, func(a, b model.AccessCheck) int {
		return slices.Index(checkOrder, a.Name) - slices.Index(checkOrder, b.Name)
	})
	return rows
}

// emptyWhy says why a readable log prefix may hold nothing yet.
func emptyWhy(name string) string {
	switch name {
	case "Container logs":
		return "Readable, but no logs for this application yet: EMR copies container logs to S3 every few minutes while the cluster runs, and all of them when it ends."
	case "HBase server logs":
		return "Readable, but no HBase logs on the primary node yet: EMR copies them every few minutes."
	}
	return "Readable, but nothing there yet: EMR copies logs to S3 every few minutes."
}

// probed fills a row from a one-object listing.
func probed(r model.AccessCheck, objs []source.Object, err error, empty string) model.AccessCheck {
	switch {
	case err != nil:
		r.Class = source.ClassOf(err)
		r.Status = "error"
		if r.Class == source.ClassAccessDenied {
			r.Status = "denied"
			r.Detail = "Access denied: needs s3:ListBucket and s3:GetObject on the log bucket (and kms:Decrypt when it uses SSE-KMS)."
		} else {
			r.Detail = err.Error()
		}
	case len(objs) == 0:
		r.Status, r.Detail = "empty", empty
	default:
		r.Status, r.Detail, r.Try = "ok", "Readable.", ""
	}
	return r
}

// apiFailed fills a row from a refused or failed API call.
func apiFailed(r model.AccessCheck, err error, permission string) model.AccessCheck {
	r.Class = awsmeta.ErrorClass(err)
	if r.Class == "accessDenied" {
		r.Status, r.Detail = "denied", "Access denied: needs "+permission+" (optional; without it the report marks what it would add as missing)."
	} else {
		r.Status, r.Detail = "error", fmt.Sprintf("%s: %v", r.Class, err)
	}
	return r
}

// eventLogCheck says where the event log will come from and whether it can
// be read there.
func eventLogCheck(ctx context.Context, cfg aws.Config, o options, prefix string, cl model.Cluster, try func(string) string) model.AccessCheck {
	r := model.AccessCheck{Name: "Spark event log"}
	loc, from := o.eventLog, "-eventlog"
	if loc == "" && prefix != "" {
		loc, from = prefix, "eventlog-prefix in the config file"
	}
	if loc == "" {
		dir := cl.Configurations["spark-defaults/spark.eventLog.dir"]
		switch {
		case isS3(dir):
			loc, from = dir, "the cluster's spark.eventLog.dir"
		default:
			where := dir
			if dir == "" {
				dir, where = "hdfs:///var/log/spark/apps", "EMR's default, hdfs:///var/log/spark/apps"
			}
			r.Status, r.Location = "denied", dir
			r.Detail = "The cluster keeps the event log on HDFS (" + where + "), which sparkplain cannot read. Supply it with -eventlog: the Spark History Server's \"Download\", or a copy in S3. The run also tries any S3 spark.eventLog.dir the cluster's steps set."
			return r
		}
	}
	r.Location, r.Call = loc, from
	if !isS3(loc) {
		if _, err := os.Stat(loc); err != nil {
			r.Status, r.Class, r.Detail = "error", "notFound", err.Error()
			if errors.Is(err, os.ErrPermission) {
				r.Status, r.Class = "denied", "accessDenied"
			}
			return r
		}
		r.Status, r.Detail = "ok", "From "+from+"."
		return r
	}
	bucket, key, _ := source.ParseS3(loc)
	r.Call = "ListObjectsV2 (one object), " + from
	r.Try = try("aws s3 ls " + loc)
	st, err := awsDeps.s3(ctx, cfg, bucket)
	if err != nil {
		return probed(r, nil, err, "")
	}
	objs, err := source.Sample(ctx, st, key, 1)
	r = probed(r, objs, err, "Readable, but nothing there: check the path, or that the application's log was written there.")
	if r.Status == "ok" {
		r.Detail = "From " + from + "."
	}
	return r
}

// offlineCheck checks the local paths a run without AWS reads.
func offlineCheck(o options, prefix string) []model.AccessCheck {
	rows := []model.AccessCheck{{Name: "AWS", Status: "skipped", Detail: "EMR API, CloudWatch and CloudTrail: not asked for. Pass -profile and -cluster-id to read from AWS."}}
	loc := firstNonEmpty(o.eventLog, prefix)
	ev := model.AccessCheck{Name: "Spark event log", Location: loc}
	switch {
	case loc == "":
		ev.Status, ev.Detail = "skipped", "Not supplied: pass -eventlog to add jobs, stages and resource use."
	case isS3(loc):
		ev.Status, ev.Detail = "skipped", "On S3: checked when the run reads it."
	default:
		if _, err := os.Stat(loc); err != nil {
			ev.Status, ev.Class, ev.Detail = "error", "notFound", err.Error()
		} else {
			ev.Status, ev.Detail = "ok", "Found."
		}
	}
	rows = append(rows, ev)
	if o.from == "" {
		for _, name := range []string{"Container logs", "Step logs", "Node logs", "HBase server logs"} {
			rows = append(rows, model.AccessCheck{Name: name, Status: "skipped", Detail: "Not asked for: pass -from with a copy of the cluster's logs, or -cluster-id."})
		}
		return orderChecks(rows)
	}
	objs, err := source.NewLocalStore(o.from).List(context.Background(), "")
	for _, lr := range []struct{ name, glob string }{
		{"Container logs", "containers/" + o.appID + "/"},
		{"Step logs", "steps/"},
		{"Node logs", "node/"},
		{"HBase server logs", "node/*/applications/hbase"},
	} {
		r := model.AccessCheck{Name: lr.name, Location: path.Join(o.from, lr.glob)}
		found := false
		for _, x := range objs {
			switch {
			case lr.name == "HBase server logs":
				f := yarnlog.Describe(x.Key)
				found = found || f.Kind == yarnlog.HBaseMaster || f.Kind == yarnlog.HBaseRegion
			case lr.name == "Container logs":
				found = found || strings.Contains(x.Key, "/"+o.appID+"/") || strings.HasPrefix(x.Key, o.appID+"/") || strings.HasPrefix(x.Key, "container_")
			default:
				found = found || strings.Contains("/"+x.Key, "/"+strings.TrimSuffix(lr.glob, "/")+"/")
			}
		}
		switch {
		case err != nil:
			r.Status, r.Class, r.Detail = "error", source.ClassOf(err), err.Error()
		case found:
			r.Status, r.Detail = "ok", "Found."
		default:
			r.Status, r.Detail = "empty", "Not in this copy."
		}
		rows = append(rows, r)
	}
	return orderChecks(rows)
}

// checkExit is the -check exit code: 3 when a source the run would read
// is not readable.
func checkExit(rows []model.AccessCheck) int {
	for _, r := range rows {
		if r.Status != "ok" && r.Status != "skipped" {
			return exitPartial
		}
	}
	return exitOK
}
