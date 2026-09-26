package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/awsmeta"
	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/source"
	"github.com/ryandam9/sparkplain/internal/sparkplain/yarnlog"
)

// clusterLogs is what the run learned from the EMR API and the cluster's
// container, step and node logs.
type clusterLogs struct {
	cluster   *model.Cluster
	steps     []model.Step
	instances []model.Instance
	emr       *model.SourceStatus // the EMR API row; nil offline
	files     []model.LogFile
	sources   []model.SourceStatus
}

// maxStepsSearched bounds how many steps' stderr are read to find the
// one that submitted the application when the event log cannot narrow
// them by time.
const maxStepsSearched = 50

// maxNodesWithoutEventLog bounds the nodes whose logs are read when the
// event log cannot say which ones ran the application.
const maxNodesWithoutEventLog = 50

// emrMetadata lists the cluster's steps and instances (ListSteps,
// ListInstances). It never fails the run: a failed call is reported in
// the EMR API row.
func emrMetadata(ctx context.Context, cloud *awsSession, cl *model.Cluster) clusterLogs {
	out := clusterLogs{cluster: cl}
	emrRow := model.SourceStatus{Name: "EMR API", Status: "read", Location: cl.Source}
	calls := []string{"DescribeCluster"}
	cfg, err := cloud.config(ctx)
	if err != nil {
		emrRow.Status, emrRow.Detail = "error", err.Error()
		out.emr = &emrRow
		return out
	}
	api := awsDeps.emr(cfg)
	var problems []error
	if steps, err := awsmeta.Steps(ctx, api, cl.ID); err != nil {
		problems = append(problems, err)
	} else {
		out.steps = steps
		calls = append(calls, fmt.Sprintf("ListSteps (%d steps)", len(steps)))
	}
	if inst, err := awsmeta.Instances(ctx, api, *cl); err != nil {
		problems = append(problems, err)
	} else {
		out.instances = inst
		calls = append(calls, fmt.Sprintf("ListInstances (%d instances)", len(inst)))
	}
	emrRow.Detail = "Called " + strings.Join(calls, ", ") + "."
	if len(problems) > 0 {
		emrRow.Status, emrRow.Class = "partial", awsmeta.ErrorClass(problems[0])
		emrRow.Detail += " Failed: " + errors.Join(problems...).Error() + "."
	}
	out.emr = &emrRow
	return out
}

// readLogs reads the logs under the cluster's log URI. It never fails the
// run: what it cannot read is reported in the Sources rows.
func (out *clusterLogs) readLogs(ctx context.Context, cloud *awsSession, log *model.EventLog, appID string, lim source.Limits) {
	cl := out.cluster
	cfg, err := cloud.config(ctx)
	if err != nil {
		for _, name := range []string{"Container logs", "Step logs", "Node logs"} {
			out.sources = append(out.sources, model.SourceStatus{Name: name, Status: "error", Class: source.ClassAccessDenied, Detail: err.Error()})
		}
		return
	}
	bucket, root, ok := yarnlog.LogRoot(cl.LogURI, cl.ID)
	if !ok {
		why := "The cluster has no log URI on S3, so EMR kept its logs only on its nodes."
		if cl.LogURI != "" {
			why = fmt.Sprintf("The cluster's log URI %s is not on S3.", cl.LogURI)
		}
		for _, name := range []string{"Container logs", "Step logs", "Node logs"} {
			out.sources = append(out.sources, model.SourceStatus{Name: name, Status: "not-supplied", Detail: why})
		}
		return
	}
	st, err := awsDeps.s3(ctx, cfg, bucket)
	if err != nil {
		for _, name := range []string{"Container logs", "Step logs", "Node logs"} {
			out.sources = append(out.sources, model.SourceStatus{Name: name, Status: "error", Class: source.ClassOf(err),
				Location: "s3://" + bucket + "/" + root, Detail: "Could not open the log bucket: " + err.Error()})
		}
		return
	}
	plan := yarnlog.Plan{Root: root, AppID: appID, Limits: lim}
	plan.Steps, plan.Instances, plan.Since = narrow(out.steps, out.instances, log)
	col := yarnlog.Collect(ctx, st, plan)
	out.files, out.sources = col.Files, col.Sources
	for i := range out.steps {
		for _, s := range col.Steps {
			if out.steps[i].ID == s {
				out.steps[i].AppID = appID
			}
		}
	}
}

// stepEventLogDirs returns the S3 spark.eventLog.dir values the steps'
// spark-submit arguments set, newest step first: jobs often set it per
// job rather than in the cluster's configuration.
func stepEventLogDirs(steps []model.Step) []string {
	var out []string
	seen := map[string]bool{}
	for _, st := range steps {
		for _, a := range st.Args {
			a = strings.TrimPrefix(a, "--conf=")
			v, ok := strings.CutPrefix(a, "spark.eventLog.dir=")
			if !ok || !isS3(v) || seen[v] {
				continue
			}
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}

// narrow picks the steps that may have submitted the application, the
// nodes it ran on, and when it started, from the event log when there is
// one.
func narrow(steps []model.Step, instances []model.Instance, log *model.EventLog) (stepIDs, nodeIDs []string, since time.Time) {
	var start, end time.Time
	hosts := map[string]bool{}
	if log != nil {
		start, end = log.Application.Start, log.Application.End
		for _, e := range log.Executors {
			if e.Host != "" {
				hosts[shortHost(e.Host)] = true
			}
		}
		if h := log.Application.DriverAttributes["NM_HOST"]; h != "" {
			hosts[shortHost(h)] = true
		}
	}
	stepIDs = []string{}
	for _, s := range steps { // newest first, as ListSteps returns them
		if !start.IsZero() {
			// The step started before the application and was still running
			// when it started (a minute's slack for clock skew).
			if s.Started.After(start.Add(time.Minute)) || (!s.Ended.IsZero() && s.Ended.Before(start.Add(-time.Minute))) {
				continue
			}
		}
		stepIDs = append(stepIDs, s.ID)
		if len(stepIDs) == maxStepsSearched {
			break
		}
	}
	nodeIDs = []string{}
	for _, in := range instances {
		ran := hosts[shortHost(in.PrivateDNS)] || hosts[shortHost(in.PrivateIP)]
		overlaps := true // the instance was up while the application ran
		if !start.IsZero() && !in.Ended.IsZero() && in.Ended.Before(start) || !end.IsZero() && in.Created.After(end) {
			overlaps = false
		}
		switch {
		case in.Primary, ran:
			nodeIDs = append(nodeIDs, in.ID)
		case len(hosts) == 0 && overlaps && len(nodeIDs) < maxNodesWithoutEventLog:
			nodeIDs = append(nodeIDs, in.ID) // no event log to say which nodes ran it
		}
	}
	return stepIDs, nodeIDs, start
}

// shortHost is the host name without its domain ("ip-10-0-2-10"), or an
// IP address as is, so ip-10-0-2-10.ec2.internal and
// ip-10-0-2-10.us-east-1.compute.internal match.
func shortHost(h string) string {
	h = strings.ToLower(strings.TrimSpace(h))
	if strings.Count(h, ".") == 3 && strings.Trim(h, "0123456789.") == "" {
		return "ip-" + strings.ReplaceAll(h, ".", "-")
	}
	if i := strings.IndexByte(h, '.'); i > 0 {
		return h[:i]
	}
	return h
}

// offlineLogs reads a local folder given with -from: a copy of the
// cluster's log root (containers/, steps/, node/), a folder holding one
// such copy per cluster (j-…/containers/…), or one application's
// container folders (container_*/stderr.gz).
func offlineLogs(ctx context.Context, dir, appID string, log *model.EventLog, lim source.Limits) (clusterLogs, error) {
	root, appFolder, err := fromLayout(dir, appID)
	if err != nil {
		return clusterLogs{}, err
	}
	var since time.Time
	if log != nil {
		since = log.Application.Start
	}
	col := yarnlog.Collect(ctx, source.NewLocalStore(dir), yarnlog.Plan{Root: root, AppFolder: appFolder, AppID: appID, Since: since, Limits: lim})
	return clusterLogs{files: col.Files, sources: col.Sources}, nil
}

// fromLayout works out what a -from folder holds.
func fromLayout(dir, appID string) (root string, appFolder bool, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", false, fmt.Errorf("-from: %w", err)
	}
	isRoot := func(d string) bool {
		for _, sub := range []string{"containers", "steps", "node"} {
			if fi, err := os.Stat(filepath.Join(d, sub)); err == nil && fi.IsDir() {
				return true
			}
		}
		return false
	}
	if isRoot(dir) {
		return "", false, nil
	}
	var clusters []string
	for _, e := range entries {
		switch {
		case e.IsDir() && strings.HasPrefix(e.Name(), "container_"):
			return "", true, nil
		case e.IsDir() && strings.HasPrefix(e.Name(), "j-") && isRoot(filepath.Join(dir, e.Name())):
			clusters = append(clusters, e.Name())
		}
	}
	sort.Strings(clusters)
	for _, c := range clusters {
		if fi, err := os.Stat(filepath.Join(dir, c, "containers", appID)); err == nil && fi.IsDir() {
			return c + "/", false, nil
		}
	}
	if len(clusters) == 1 {
		return clusters[0] + "/", false, nil
	}
	return "", false, fmt.Errorf("-from %s holds no cluster log folder (containers/, steps/, node/) and no container_* folders; copy the cluster's log folder, for example with aws s3 cp --recursive <LogUri>/<cluster-id>/ %s", dir, dir)
}
