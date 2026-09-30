package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
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
	outDir         string // where the outputs will be written
}

// checked is what the access check found: its rows, in a fixed order, and
// what the console heads its output with.
type checked struct {
	rows    []model.AccessCheck
	who     string // the credentials' principal, such as user/ryandam
	cluster string // "j-… · name · emr-7.3.0 · terminated"
	logRoot string // s3://bucket/prefix/<cluster-id>/, or the -from folder
}

// accessCheck finds out what the run can read before it reads anything
// (SPEC §2): one small read-only call per source, made in parallel. A
// log folder is checked by listing one object and reading its first byte,
// since a bucket policy can allow the one and refuse the other, and a
// KMS-encrypted object fails only when read.
func accessCheck(ctx context.Context, cloud *awsSession, in checkInput) checked {
	o := in.o
	local := localChecks(o, in.outDir)
	if o.clusterID == "" && o.clusterName == "" {
		c := offlineCheck(o, in.eventLogPrefix)
		c.rows = orderChecks(append(c.rows, local...))
		return c
	}
	var c checked
	cfg, err := cloud.config(ctx)
	if err != nil {
		c.rows = append(local, model.AccessCheck{Name: "AWS credentials", Status: "error", Class: "accessDenied", Call: "load profile " + o.profile, Detail: err.Error(),
			Try: "aws sts get-caller-identity --profile " + o.profile},
			model.AccessCheck{Name: "Everything else", Status: "skipped", Detail: "Nothing on AWS can be checked or read without credentials."})
		c.rows = orderChecks(c.rows)
		return c
	}
	if id, err := awsDeps.sts(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{}); err == nil {
		arn := aws.ToString(id.Arn)
		c.who = arn[strings.LastIndexByte(arn, ':')+1:]
	}
	region := cfg.Region
	try := func(cmd string) string { return cmd + " --profile " + o.profile + " --region " + region }

	// The cluster first: it says where the logs and the event log are.
	emrRow := model.AccessCheck{Name: "EMR API", Call: "DescribeCluster, ListSteps, ListInstances, ListInstanceGroups or ListInstanceFleets, DescribeStep"}
	cl, err := cloud.cluster(ctx, o.clusterID, o.clusterName)
	if err != nil {
		emrRow.Status, emrRow.Class, emrRow.Detail = "error", awsmeta.ErrorClass(err), err.Error()
		if emrRow.Class == "accessDenied" {
			emrRow.Status = "denied"
		}
		emrRow.Try = try("aws emr describe-cluster --cluster-id " + firstNonEmpty(o.clusterID, "<cluster-id>"))
		c.rows = append(local, emrRow, model.AccessCheck{Name: "Logs and the rest", Status: "skipped",
			Detail: "Where the logs are comes from DescribeCluster. Pass -from with a copy of the cluster's logs, or -eventlog, to run without it."})
		c.rows = orderChecks(c.rows)
		return c
	}
	c.cluster = strings.Join(nonEmpty(cl.ID, cl.Name, cl.Release, strings.ToLower(cl.State)), " · ")
	api := awsDeps.emr(cfg)
	var steps []model.Step
	var stepsErr, instErr, groupsErr, stepErr error
	var instances, types []string
	var primary string
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		x, cancel := context.WithTimeout(ctx, checkTimeout)
		defer cancel()
		steps, stepsErr = awsmeta.Steps(x, api, cl.ID)
		if stepsErr == nil && len(steps) > 0 {
			_, stepErr = api.DescribeStep(x, &emr.DescribeStepInput{ClusterId: aws.String(cl.ID), StepId: aws.String(steps[len(steps)-1].ID)})
		}
	}()
	go func() {
		defer wg.Done()
		x, cancel := context.WithTimeout(ctx, checkTimeout)
		defer cancel()
		out, err := api.ListInstances(x, &emr.ListInstancesInput{ClusterId: aws.String(cl.ID)})
		instErr = err
		if err != nil {
			return
		}
		for _, i := range out.Instances {
			id := aws.ToString(i.Ec2InstanceId)
			instances = append(instances, id)
			if t := aws.ToString(i.InstanceType); t != "" && !slices.Contains(types, t) {
				types = append(types, t)
			}
			if aws.ToString(i.PrivateDnsName) == cl.PrimaryDNS || aws.ToString(i.PublicDnsName) == cl.PrimaryDNS {
				primary = id
			}
		}
	}()
	go func() {
		defer wg.Done()
		x, cancel := context.WithTimeout(ctx, checkTimeout)
		defer cancel()
		if cl.Fleets {
			_, groupsErr = api.ListInstanceFleets(x, &emr.ListInstanceFleetsInput{ClusterId: aws.String(cl.ID)})
		} else {
			_, groupsErr = api.ListInstanceGroups(x, &emr.ListInstanceGroupsInput{ClusterId: aws.String(cl.ID)})
		}
	}()
	wg.Wait()
	emrRow.Status = "ok"
	emrRow.Detail = "cluster, steps, instances, " + map[bool]string{true: "fleets", false: "groups"}[cl.Fleets]
	groupsCall := map[bool]string{true: "ListInstanceFleets", false: "ListInstanceGroups"}[cl.Fleets]
	for _, e := range []struct {
		err       error
		call, cmd string
		optional  bool
	}{
		{stepsErr, "ListSteps", "list-steps", false},
		{instErr, "ListInstances", "list-instances", false},
		{groupsErr, groupsCall, map[bool]string{true: "list-instance-fleets", false: "list-instance-groups"}[cl.Fleets], false},
		{stepErr, "DescribeStep", "describe-step --step-id " + func() string {
			if len(steps) > 0 {
				return steps[len(steps)-1].ID
			}
			return ""
		}(), true},
	} {
		if e.err == nil || emrRow.Status != "ok" {
			continue
		}
		emrRow.Status, emrRow.Class = "denied", awsmeta.ErrorClass(e.err)
		if emrRow.Class != "accessDenied" {
			emrRow.Status = "error"
		}
		need := "elasticmapreduce:" + e.call
		if e.optional {
			need += " (optional; it gives the step's runtime role)"
		}
		emrRow.Detail = fmt.Sprintf("%s failed (%s): needs %s.", e.call, emrRow.Class, need)
		emrRow.Try = try("aws emr " + e.cmd + " --cluster-id " + cl.ID)
	}
	c.rows = append(local, emrRow)

	var mu sync.Mutex
	add := func(r model.AccessCheck) {
		mu.Lock()
		c.rows = append(c.rows, r)
		mu.Unlock()
	}
	var jobs []func(context.Context)

	// The logs under the cluster's log URI.
	bucket, root, ok := yarnlog.LogRoot(cl.LogURI, cl.ID)
	if ok {
		c.logRoot = "s3://" + bucket + "/" + root
	}
	type logRow struct{ name, prefix, where string }
	logRows := []logRow{
		{"Container logs", root + "containers/" + o.appID + "/", ""},
		{"Step logs", root + "steps/", ""},
		{"Node logs", root + "node/", ""},
	}
	if o.hbaseClusterID == "" {
		if clusterHasHBase(&cl) {
			host := primary
			if host == "" && len(instances) > 0 {
				host = instances[0]
			}
			logRows = append(logRows, logRow{"HBase server logs", root + "node/" + host + "/applications/hbase/", host})
		} else {
			c.rows = append(c.rows, model.AccessCheck{Name: "HBase server logs", Status: "skipped",
				Detail: "HBase is not installed on this Spark cluster. If HBase runs on another EMR cluster, pass -hbase-cluster-id <id>."})
		}
	}
	if !ok {
		for _, lr := range logRows {
			c.rows = append(c.rows, model.AccessCheck{Name: lr.name, Status: "skipped", Detail: "The cluster has no log URI, so EMR kept no logs in S3."})
		}
	} else {
		jobs = append(jobs, func(x context.Context) {
			st, err := awsDeps.s3(x, cfg, bucket)
			for _, lr := range logRows {
				r := model.AccessCheck{Name: lr.name, Location: "s3://" + bucket + "/" + lr.prefix, Call: "ListObjectsV2 and GetObject (one byte)",
					Try: try("aws s3 ls s3://" + bucket + "/" + lr.prefix)}
				if err != nil {
					add(probed(r, nil, nil, err, ""))
					continue
				}
				r = readable(x, st, r, lr.prefix, emptyWhy(lr.name))
				if lr.where != "" {
					// The run reads every node's; the primary node's (the
					// Master's) stands for them here.
					r.Location = "s3://" + bucket + "/" + root + "node/*/applications/hbase/"
					if r.Status == "ok" {
						r.Detail = "Checked on the primary node, " + lr.where + ". List, read."
					}
				}
				add(r)
			}
		})
	}

	if o.hbaseClusterID != "" {
		jobs = append(jobs, func(x context.Context) {
			r := model.AccessCheck{Name: "HBase server logs", Location: o.hbaseClusterID,
				Call: "DescribeCluster, ListInstances, ListObjectsV2 and GetObject (one byte)",
				Try:  try("aws emr describe-cluster --cluster-id " + o.hbaseClusterID)}
			hcl, err := cloud.cluster(x, o.hbaseClusterID, "")
			if err != nil {
				r.Status, r.Class, r.Detail = "error", awsmeta.ErrorClass(err), "Could not describe the HBase cluster: "+err.Error()
				if r.Class == "accessDenied" {
					r.Status = "denied"
				}
				add(r)
				return
			}
			if !clusterHasHBase(&hcl) {
				r.Status, r.Detail = "error", "The cluster specified by -hbase-cluster-id does not have HBase installed."
				add(r)
				return
			}
			hinstances, err := awsmeta.Instances(x, api, hcl)
			if err != nil {
				r = apiFailed(r, err, "elasticmapreduce:ListInstances")
				r.Detail = "Could not list the HBase cluster's instances: " + r.Detail
				add(r)
				return
			}
			var host string
			for _, in := range hinstances {
				if in.Primary {
					host = in.ID
					break
				}
			}
			if host == "" && len(hinstances) > 0 {
				host = hinstances[0].ID
			}
			if host == "" {
				r.Status, r.Detail = "error", "The HBase cluster has no instances to inspect."
				add(r)
				return
			}
			hbucket, hroot, ok := yarnlog.LogRoot(hcl.LogURI, hcl.ID)
			if !ok {
				r.Status, r.Detail = "error", "The HBase cluster has no S3 log URI, so its server logs cannot be checked."
				add(r)
				return
			}
			prefix := hroot + "node/" + host + "/applications/hbase/"
			r.Location = "s3://" + hbucket + "/" + hroot + "node/*/applications/hbase/"
			r.Try = try("aws s3 ls s3://" + hbucket + "/" + prefix)
			st, err := awsDeps.s3(x, cfg, hbucket)
			if err != nil {
				add(probed(r, nil, nil, err, ""))
				return
			}
			r = readable(x, st, r, prefix, emptyWhy(r.Name))
			if r.Status == "ok" {
				r.Detail = "Checked on HBase cluster " + hcl.ID + ", primary node " + host + ". List, read."
			}
			add(r)
		})
	}

	// Where the event log will come from, and the job's scripts.
	jobs = append(jobs, func(x context.Context) { add(eventLogCheck(x, cfg, o, in.eventLogPrefix, cl, steps, try)) })
	jobs = append(jobs, func(x context.Context) { add(scriptCheck(x, cfg, steps, try)) })

	// CloudWatch, EC2 and CloudTrail.
	if o.noCloudWatch {
		c.rows = append(c.rows, model.AccessCheck{Name: "CloudWatch", Status: "skipped", Detail: "Not asked for (-no-cloudwatch)."},
			model.AccessCheck{Name: "EC2 instance types", Status: "skipped", Detail: "Not asked for (-no-cloudwatch)."})
	} else {
		jobs = append(jobs, func(x context.Context) { add(cloudWatchCheck(x, cfg, cl.ID, try)) }, func(x context.Context) {
			r := model.AccessCheck{Name: "EC2 instance types", Call: "DescribeInstanceTypes"}
			if len(types) == 0 {
				r.Status, r.Detail = "skipped", "No instances listed to look up."
				add(r)
				return
			}
			r.Try = try("aws ec2 describe-instance-types --instance-types " + types[0])
			_, err := awsDeps.ec2(cfg).DescribeInstanceTypes(x, &ec2.DescribeInstanceTypesInput{InstanceTypes: []ec2types.InstanceType{ec2types.InstanceType(types[0])}})
			if err != nil {
				r = apiFailed(r, err, "ec2:DescribeInstanceTypes")
			} else {
				r.Status, r.Detail = "ok", strings.Join(types, ", ")+"."
			}
			add(r)
		})
	}
	if o.noCloudTrail {
		c.rows = append(c.rows, model.AccessCheck{Name: "CloudTrail", Status: "skipped", Detail: "Not asked for (-no-cloudtrail)."})
	} else {
		jobs = append(jobs, func(x context.Context) {
			r := model.AccessCheck{Name: "CloudTrail", Call: "LookupEvents (one event)", Try: try("aws cloudtrail lookup-events --max-results 1")}
			if _, err := awsDeps.cloudtrail(cfg).LookupEvents(x, &cloudtrail.LookupEventsInput{MaxResults: aws.Int32(1)}); err != nil {
				r = apiFailed(r, err, "cloudtrail:LookupEvents")
			} else {
				r.Status, r.Detail = "ok", "Readable."
			}
			add(r)
		})
	}
	if cl.SecurityConfig != "" {
		jobs = append(jobs, func(x context.Context) {
			r := model.AccessCheck{Name: "Security configuration", Call: "DescribeSecurityConfiguration", Location: cl.SecurityConfig,
				Try: try("aws emr describe-security-configuration --name " + cl.SecurityConfig)}
			if _, err := api.DescribeSecurityConfiguration(x, &emr.DescribeSecurityConfigurationInput{Name: aws.String(cl.SecurityConfig)}); err != nil {
				r = apiFailed(r, err, "elasticmapreduce:DescribeSecurityConfiguration")
			} else {
				r.Status, r.Detail = "ok", "Readable."
			}
			add(r)
		})
	}
	var jw sync.WaitGroup
	for _, j := range jobs {
		jw.Add(1)
		go func() {
			defer jw.Done()
			x, cancel := context.WithTimeout(ctx, checkTimeout)
			defer cancel()
			j(x)
		}()
	}
	jw.Wait()
	c.rows = orderChecks(c.rows)
	return c
}

// checkOrder is the order rows are shown in.
var checkOrder = []string{"AWS", "AWS credentials", "EMR API", "Cluster logs", "Security configuration", "Container logs", "Step logs", "Node logs", "HBase server logs",
	"Spark event log", "Job scripts", "CloudWatch", "EC2 instance types", "CloudTrail", "Logs and the rest", "Everything else", "Source code", "Output folder"}

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

// readable lists one object under prefix and reads its first byte.
func readable(ctx context.Context, st source.Store, r model.AccessCheck, prefix, empty string) model.AccessCheck {
	objs, err := source.Sample(ctx, st, prefix, 1)
	var peekErr error
	if err == nil && len(objs) > 0 {
		peekErr = source.Peek(ctx, st, objs[0])
	}
	return probed(r, objs, peekErr, err, empty)
}

// probed fills a row from a one-object listing and a one-byte read.
func probed(r model.AccessCheck, objs []source.Object, readErr, listErr error, empty string) model.AccessCheck {
	switch {
	case listErr != nil:
		r.Class = source.ClassOf(listErr)
		r.Status = "error"
		if r.Class == source.ClassAccessDenied {
			r.Status = "denied"
			r.Detail = "Listing refused: needs s3:ListBucket on the bucket."
		} else {
			r.Detail = listErr.Error()
		}
	case len(objs) == 0:
		r.Status, r.Detail = "empty", empty
	case readErr != nil:
		r.Class = source.ClassOf(readErr)
		r.Status = "error"
		if r.Class == source.ClassAccessDenied {
			r.Status = "denied"
			r.Detail = "Listing works, but reading is refused: needs s3:GetObject (and kms:Decrypt when the bucket uses SSE-KMS)."
		} else {
			r.Detail = "Listing works, but reading failed: " + readErr.Error()
		}
		if r.Location != "" {
			r.Try = strings.Replace(r.Try, "aws s3 ls "+r.Location, "aws s3 cp "+strings.TrimSuffix(r.Location, "/")+"/"+path.Base(objs[0].Key)+" -", 1)
		}
	default:
		r.Status, r.Detail, r.Try = "ok", "List, read.", ""
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

// cloudWatchCheck lists the cluster's metrics and reads one of them:
// cloudwatch:ListMetrics and GetMetricData are separate permissions.
func cloudWatchCheck(ctx context.Context, cfg aws.Config, clusterID string, try func(string) string) model.AccessCheck {
	r := model.AccessCheck{Name: "CloudWatch", Call: "ListMetrics, GetMetricData (AWS/ElasticMapReduce)",
		Try: try("aws cloudwatch list-metrics --namespace AWS/ElasticMapReduce --dimensions Name=JobFlowId,Value=" + clusterID)}
	api := awsDeps.cloudwatch(cfg)
	out, err := api.ListMetrics(ctx, &cloudwatch.ListMetricsInput{Namespace: aws.String("AWS/ElasticMapReduce"),
		Dimensions: []cwtypes.DimensionFilter{{Name: aws.String("JobFlowId"), Value: aws.String(clusterID)}}})
	switch {
	case err != nil:
		return apiFailed(r, err, "cloudwatch:ListMetrics")
	case len(out.Metrics) == 0:
		r.Status, r.Detail = "empty", "Readable, but CloudWatch holds no metrics for this cluster (it keeps them 15 months; a new cluster needs a few minutes)."
		return r
	}
	m := out.Metrics[0]
	now := awsDeps.now()
	_, err = api.GetMetricData(ctx, &cloudwatch.GetMetricDataInput{StartTime: aws.Time(now.Add(-10 * time.Minute)), EndTime: aws.Time(now),
		MetricDataQueries: []cwtypes.MetricDataQuery{{Id: aws.String("check"), MetricStat: &cwtypes.MetricStat{Metric: &m, Period: aws.Int32(300), Stat: aws.String("Average")}}}})
	if err != nil {
		r = apiFailed(r, err, "cloudwatch:GetMetricData")
		r.Try = try("aws cloudwatch get-metric-statistics --namespace AWS/ElasticMapReduce --metric-name " + aws.ToString(m.MetricName) +
			" --dimensions Name=JobFlowId,Value=" + clusterID + " --statistics Average --period 300 --start-time " + now.Add(-10*time.Minute).UTC().Format(time.RFC3339) + " --end-time " + now.UTC().Format(time.RFC3339))
		return r
	}
	r.Status, r.Detail, r.Try = "ok", fmt.Sprintf("%d cluster metrics. List, read.", len(out.Metrics)), ""
	return r
}

// scriptCheck reads the first byte of the newest step's script on S3,
// which the run shows beside the stages it ran; it is often in another
// bucket than the logs.
func scriptCheck(ctx context.Context, cfg aws.Config, steps []model.Step, try func(string) string) model.AccessCheck {
	r := model.AccessCheck{Name: "Job scripts", Call: "ListObjectsV2 and GetObject (one byte)"}
	var script string
	for i := len(steps) - 1; i >= 0 && script == ""; i-- {
		if s := submitScripts(steps[i].Args); len(s) > 0 {
			script = s[0]
		}
	}
	if script == "" {
		r.Status, r.Detail = "skipped", "No step names a script on S3."
		return r
	}
	bucket, key, _ := source.ParseS3(script)
	r.Location, r.Try, r.Call = script, try("aws s3 cp "+script+" -"), "GetObject (one byte)"
	st, err := awsDeps.s3(ctx, cfg, bucket)
	if err == nil {
		// The run reads the script with GetObject alone, so this does too.
		err = source.Peek(ctx, st, source.Object{Key: key})
	}
	switch cls := source.ClassOf(err); {
	case err == nil:
		r.Status, r.Detail, r.Try = "ok", "The newest step's script. Read.", ""
	case cls == source.ClassAccessDenied:
		r.Status, r.Class, r.Detail = "denied", cls, "Reading is refused: needs s3:GetObject on it (and kms:Decrypt when the bucket uses SSE-KMS). Only the code beside the stages is missing without it."
	default:
		r.Status, r.Class, r.Detail = "error", cls, err.Error()
	}
	return r
}

// eventLogCheck says where the event log will come from and whether it can
// be read there.
func eventLogCheck(ctx context.Context, cfg aws.Config, o options, prefix string, cl model.Cluster, steps []model.Step, try func(string) string) model.AccessCheck {
	r := model.AccessCheck{Name: "Spark event log"}
	loc, from := o.eventLog, "-eventlog"
	if loc == "" && prefix != "" {
		loc, from = prefix, "eventlog-prefix in the config file"
	}
	if loc == "" {
		if dir := cl.Configurations["spark-defaults/spark.eventLog.dir"]; isS3(dir) {
			loc, from = dir, "the cluster's spark.eventLog.dir"
		}
	}
	if loc == "" {
		// Jobs often set it per job in their spark-submit arguments.
		for _, dir := range stepEventLogDirs(steps) {
			bucket, key, _ := source.ParseS3(dir)
			st, err := awsDeps.s3(ctx, cfg, bucket)
			if err != nil {
				continue
			}
			if objs, err := source.Sample(ctx, st, key, 1); err == nil && len(objs) > 0 {
				loc, from = dir, "a step's spark.eventLog.dir"
				break
			}
		}
	}
	if loc == "" {
		dir := cl.Configurations["spark-defaults/spark.eventLog.dir"]
		where := dir
		if dir == "" {
			dir, where = "hdfs:///var/log/spark/apps", "EMR's default, hdfs:///var/log/spark/apps"
		}
		r.Status, r.Location = "denied", dir
		r.Detail = "The cluster keeps the event log on HDFS (" + where + "), which sparkplain cannot read. Supply it with -eventlog: the Spark History Server's \"Download\", or a copy in S3."
		return r
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
	r.Call = "ListObjectsV2 and GetObject (one byte), " + from
	r.Try = try("aws s3 ls " + loc)
	st, err := awsDeps.s3(ctx, cfg, bucket)
	if err != nil {
		return probed(r, nil, nil, err, "")
	}
	r = readable(ctx, st, r, key, "Readable, but nothing there: check the path, or that the application's log was written there.")
	if r.Status == "ok" {
		r.Detail = "From " + from + ". List, read."
	}
	return r
}

// localChecks are the local paths every run needs: the -source code and
// the output folder, which would otherwise fail only once all was read.
func localChecks(o options, outDir string) []model.AccessCheck {
	var rows []model.AccessCheck
	if len(o.sources) > 0 {
		r := model.AccessCheck{Name: "Source code", Location: strings.Join(o.sources, ", "), Status: "ok", Detail: "Found."}
		for _, s := range o.sources {
			if _, err := os.Stat(s); err != nil {
				r.Status, r.Class, r.Detail = "error", "notFound", err.Error()
				break
			}
		}
		rows = append(rows, r)
	}
	if outDir != "" {
		r := model.AccessCheck{Name: "Output folder", Location: outDir, Status: "ok", Detail: "Writable."}
		dir := outDir
		for {
			if _, err := os.Stat(dir); err == nil {
				break
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
			r.Detail = "Will be created."
		}
		// W_OK: the folder (or the one it will be created in) is writable,
		// checked without writing anything.
		if err := syscall.Access(dir, 2); err != nil {
			r.Status, r.Class, r.Detail = "denied", "accessDenied", dir+" is not writable: "+err.Error()+". Pass -out with a folder you can write to."
		}
		rows = append(rows, r)
	}
	return rows
}

// offlineCheck checks the local paths a run without AWS reads.
func offlineCheck(o options, prefix string) checked {
	c := checked{}
	if o.from != "" {
		c.logRoot = strings.TrimSuffix(o.from, "/") + "/"
	}
	c.rows = []model.AccessCheck{{Name: "AWS", Status: "skipped", Detail: "EMR API, CloudWatch and CloudTrail: not asked for. Pass -profile and -cluster-id to read from AWS."}}
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
	c.rows = append(c.rows, ev)
	if o.from == "" {
		c.rows = append(c.rows, model.AccessCheck{Name: "Cluster logs", Status: "skipped",
			Detail: "Container, step, node and HBase logs: not asked for. Pass -from with a copy of the cluster's logs, or -cluster-id."})
		return c
	}
	objs, err := source.NewLocalStore(o.from).List(context.Background(), "")
	for _, lr := range []struct{ name, glob string }{
		{"Container logs", "containers/" + o.appID + "/"},
		{"Step logs", "steps/"},
		{"Node logs", "node/"},
		{"HBase server logs", "node/*/applications/hbase"},
	} {
		r := model.AccessCheck{Name: lr.name, Location: path.Join(o.from, lr.glob) + "/"}
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
		case lr.name == "HBase server logs":
			r.Status, r.Detail = "skipped", "None in this copy: the cluster runs no HBase, or its node/ folder was not copied." // not every cluster has HBase
		default:
			r.Status, r.Detail = "empty", "Not in this copy."
		}
		c.rows = append(c.rows, r)
	}
	return c
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
