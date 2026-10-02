package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/awsmeta"
	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/report"
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
	ec2       *model.SourceStatus // the EC2 API row; nil offline
	metrics   *model.MetricsSection
	calls     *model.AWSCallsSection
	files     []model.LogFile
	sources   []model.SourceStatus
	// appNodes are the instances that ran the application's driver or
	// executors, as its container logs name them.
	appNodes []string
	// logLoc is the zone the cluster writes its log times in.
	logLoc *time.Location
}

// clusterHasHBase reports whether DescribeCluster lists HBase among the
// applications installed on the EMR cluster.
func clusterHasHBase(cl *model.Cluster) bool {
	return cl != nil && slices.ContainsFunc(cl.Applications, func(a string) bool {
		name, _, _ := strings.Cut(a, " ")
		return strings.EqualFold(name, "HBase")
	})
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
		cl.Instances = inst
		calls = append(calls, fmt.Sprintf("ListInstances (%d instances)", len(inst)))
	}
	if err := awsmeta.Groups(ctx, api, cl); err != nil {
		problems = append(problems, err)
	} else if cl.Fleets {
		calls = append(calls, fmt.Sprintf("ListInstanceFleets (%d fleets)", len(cl.Groups)))
	} else {
		calls = append(calls, fmt.Sprintf("ListInstanceGroups (%d groups)", len(cl.Groups)))
	}
	if cl.SecurityConfig != "" {
		if sec, err := awsmeta.Security(ctx, api, cl.SecurityConfig); err != nil {
			problems = append(problems, err)
		} else {
			cl.Security = sec
			calls = append(calls, "DescribeSecurityConfiguration")
		}
	}
	if len(cl.Instances) > 0 {
		ec2Row := model.SourceStatus{Name: "EC2 API", Status: "read", Detail: "Called DescribeInstanceTypes for each instance type's vCPU and memory."}
		if err := awsmeta.InstanceSizes(ctx, awsDeps.ec2(cfg), cl); err != nil {
			ec2Row.Status, ec2Row.Class = "partial", awsmeta.ErrorClass(err)
			ec2Row.Detail = "Could not read instance sizes, so the Nodes table leaves vCPU and memory out (needs ec2:DescribeInstanceTypes): " + err.Error()
		}
		out.ec2 = &ec2Row
	}
	out.instances = cl.Instances
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
func (out *clusterLogs) readLogs(ctx context.Context, cloud *awsSession, log *model.EventLog, appID string, lim source.Limits, includeHBase bool, off map[string]string) {
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
	plan := yarnlog.Plan{Root: root, AppID: appID, Limits: lim, SkipHBase: !includeHBase, Off: off, Loc: out.logLoc}
	plan.Steps, plan.Instances, plan.Since = narrow(out.steps, out.instances, log)
	if log != nil {
		plan.Until, plan.WindowFrom = log.Application.End, "the event log"
	}
	plan.Others = otherNodes(out.instances, plan.Instances)
	plan.HostIDs, plan.Guessed = map[string]string{}, log == nil || len(log.Executors) == 0
	for _, in := range out.instances {
		for _, h := range []string{in.PrivateDNS, in.PrivateIP} {
			if h != "" {
				plan.HostIDs[shortHost(h)] = in.ID
			}
		}
		if in.Primary || in.Role == "MASTER" {
			plan.Primary = append(plan.Primary, in.ID)
		}
	}
	plan.Lifetimes = map[string][2]time.Time{}
	for _, in := range out.instances {
		plan.Lifetimes[in.ID] = [2]time.Time{in.Created, in.Ended}
	}
	col := yarnlog.Collect(ctx, st, plan)
	out.files, out.sources, out.appNodes = col.Files, col.Sources, col.AppNodes
	api := awsDeps.emr(cfg)
	for i := range out.steps {
		for _, s := range col.Steps {
			if out.steps[i].ID != s {
				continue
			}
			out.steps[i].AppID = appID
			// The step's runtime role, if it had one, is the identity its
			// AWS calls used instead of the instance profile.
			role, err := awsmeta.StepRole(ctx, api, cl.ID, s)
			switch {
			case err != nil && out.emr != nil:
				out.emr.Status, out.emr.Class = "partial", awsmeta.ErrorClass(err)
				out.emr.Detail += " Failed: " + err.Error() + "."
			case err == nil:
				out.steps[i].ExecutionRole = role
				if out.emr != nil {
					out.emr.Detail = strings.TrimSuffix(out.emr.Detail, ".") + ", DescribeStep " + s + "."
				}
			}
		}
	}
}

// readHBaseCluster reads only HBase Master and region-server logs from a
// separate EMR cluster. Spark/YARN metadata remains attached to out.cluster.
func (out *clusterLogs) readHBaseCluster(ctx context.Context, cloud *awsSession, clusterID, clusterName string, log *model.EventLog, lim source.Limits) {
	where := firstNonEmpty(clusterID, clusterName)
	cfg, err := cloud.config(ctx)
	if err != nil {
		out.sources = append(out.sources, model.SourceStatus{Name: "HBase server logs", Status: "error", Class: source.ClassAccessDenied,
			Location: where, Detail: err.Error()})
		return
	}
	cl, err := cloud.hbaseCluster(ctx, clusterID, clusterName)
	if err != nil {
		out.sources = append(out.sources, model.SourceStatus{Name: "HBase server logs", Status: "error", Class: awsmeta.ErrorClass(err),
			Location: where, Detail: "Could not find or describe the HBase cluster: " + err.Error()})
		return
	}
	if !clusterHasHBase(&cl) {
		out.sources = append(out.sources, model.SourceStatus{Name: "HBase server logs", Status: "not-supplied", Location: cl.ID,
			Detail: "The HBase cluster given (" + where + ") does not have HBase installed."})
		return
	}
	bucket, root, ok := yarnlog.LogRoot(cl.LogURI, cl.ID)
	if !ok {
		why := "The HBase cluster has no log URI on S3, so its server logs cannot be read."
		if cl.LogURI != "" {
			why = fmt.Sprintf("The HBase cluster's log URI %s is not on S3.", cl.LogURI)
		}
		out.sources = append(out.sources, model.SourceStatus{Name: "HBase server logs", Status: "not-supplied", Location: cl.ID, Detail: why})
		return
	}
	st, err := awsDeps.s3(ctx, cfg, bucket)
	if err != nil {
		out.sources = append(out.sources, model.SourceStatus{Name: "HBase server logs", Status: "error", Class: source.ClassOf(err),
			Location: "s3://" + bucket + "/" + root + "node/", Detail: "Could not open the HBase cluster's log bucket: " + err.Error()})
		return
	}

	// The application's time: from the event log, else from the Spark
	// cluster's logs read just before (YARN's application summary, or the
	// times its container logs cover), else from its step.
	plan := yarnlog.Plan{Root: root, Limits: lim, Loc: out.logLoc}
	plan.Since, plan.Until, plan.WindowFrom, _ = runWindow(log, out.files, out.steps, out.cluster)
	instances, instErr := awsmeta.Instances(ctx, awsDeps.emr(cfg), cl)
	if instErr == nil {
		plan.Lifetimes = map[string][2]time.Time{}
		for _, in := range instances {
			plan.Instances = append(plan.Instances, in.ID)
			plan.Lifetimes[in.ID] = [2]time.Time{in.Created, in.Ended}
		}
	}
	col := yarnlog.CollectHBase(ctx, st, plan)
	if len(col.Sources) == 0 {
		row := model.SourceStatus{Name: "HBase server logs", Status: "not-supplied",
			Location: "s3://" + bucket + "/" + root + "node/*/applications/hbase/",
			Detail:   hbaseNoneFound(cl, clusterID, col)}
		if instErr != nil {
			row.Detail += " ListInstances also failed: " + instErr.Error()
		}
		out.sources = append(out.sources, row)
		return
	}
	for i := range col.Sources {
		col.Sources[i].Detail = strings.TrimSpace(col.Sources[i].Detail + " Source cluster: " + cl.ID + ".")
	}
	out.files = append(out.files, col.Files...)
	out.sources = append(out.sources, col.Sources...)
}

// hbaseNoneFound says why no HBase server log was read from a separate
// HBase cluster: which cluster that was and how it was chosen, where
// sparkplain looked, and what it found there instead, so a wrong cluster,
// logs not yet copied to S3 and logs named otherwise can be told apart.
func hbaseNoneFound(cl model.Cluster, byID string, col yarnlog.Collection) string {
	which := cl.ID
	switch {
	case byID != "" && cl.Name != "":
		which += fmt.Sprintf(" (%q, given by -hbase-cluster-id)", cl.Name)
	case byID != "":
		which += " (given by -hbase-cluster-id)"
	default:
		which += fmt.Sprintf(" (picked by its name, %q)", cl.Name)
	}
	where := "under its node/ folder"
	if col.HBaseNodes > 0 {
		where = "in node/<instance>/applications/hbase/ on " + model.Plural(col.HBaseNodes, "node", "nodes") + " up during the run"
	}
	msg := "No HBase Master or region server logs were found on the HBase cluster " + which + ". Looked " + where
	if n := len(col.HBaseOther); n > 0 {
		ex := col.HBaseOther[:min(n, 3)]
		return msg + fmt.Sprintf(", and found %s there, such as %s, but none named hbase-<user>-master-<host>.log or hbase-<user>-regionserver-<host>.log (with an hourly .<yyyy-mm-dd-hh>, gzipped).",
			model.Plural(n, "other file", "other files"), strings.Join(ex, ", "))
	}
	return msg + ", and found nothing there: check that this is the cluster HBase ran on, and that EMR copies its logs to S3 (it does about every 5 minutes while the cluster runs, given a log URI)."
}

// stepEventLogDirs returns the S3 spark.eventLog.dir values the steps'
// spark-submit arguments set, newest step first: jobs often set it per
// job rather than in the cluster's configuration. steps is oldest first
// (awsmeta.Steps), so it is walked from the end.
func stepEventLogDirs(steps []model.Step) []string {
	var out []string
	seen := map[string]bool{}
	for i := len(steps) - 1; i >= 0; i-- {
		st := steps[i]
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
	// steps is oldest first (awsmeta.Steps); search from the newest, which
	// is the likeliest to have submitted the application, so a cluster with
	// many steps does not use up maxStepsSearched on old ones.
	for i := len(steps) - 1; i >= 0; i-- {
		s := steps[i]
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

// otherNodes maps the short host name of each instance not already
// chosen to its ID, so the logs can add a node an earlier attempt's driver
// ran on.
func otherNodes(instances []model.Instance, chosen []string) map[string]string {
	skip := map[string]bool{}
	for _, id := range chosen {
		skip[id] = true
	}
	m := map[string]string{}
	for _, in := range instances {
		if skip[in.ID] {
			continue
		}
		for _, h := range []string{in.PrivateDNS, in.PrivateIP} {
			if h != "" {
				m[shortHost(h)] = in.ID
			}
		}
	}
	return m
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
func offlineLogs(ctx context.Context, dir, appID string, log *model.EventLog, lim source.Limits, off map[string]string, loc *time.Location) (clusterLogs, error) {
	root, appFolder, err := fromLayout(dir, appID)
	if err != nil {
		return clusterLogs{}, err
	}
	var since, until time.Time
	if log != nil {
		since, until = log.Application.Start, log.Application.End
	}
	col := yarnlog.Collect(ctx, source.NewLocalStore(dir), yarnlog.Plan{Root: root, AppFolder: appFolder, AppID: appID, Since: since, Until: until, Limits: lim, Off: off, Loc: loc})
	return clusterLogs{files: col.Files, sources: col.Sources, logLoc: loc}, nil
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

// maxScriptBytes caps each script fetched from S3, like -source files.
const maxScriptBytes = 4 << 20

var scriptExts = map[string]bool{".py": true, ".scala": true, ".java": true, ".kt": true, ".sql": true, ".r": true, ".R": true}

// submitScripts returns the application's own source files a spark-submit
// argument list names on S3: the primary resource and --py-files entries.
// Jars are skipped; they are not readable source.
func submitScripts(args []string) []string {
	takesNoValue := map[string]bool{"--verbose": true, "-v": true, "--supervise": true, "--help": true, "-h": true, "--version": true}
	start := 0
	for i, a := range args {
		if a == "spark-submit" || strings.HasSuffix(a, "/spark-submit") {
			start = i + 1
			break
		}
	}
	var out []string
	add := func(p string) {
		if isS3(p) && scriptExts[filepath.Ext(p)] && !slices.Contains(out, p) {
			out = append(out, p)
		}
	}
	for i := start; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "-") {
			add(a) // the primary resource; what follows are its own arguments
			break
		}
		name, val, eq := strings.Cut(a, "=")
		if !eq && !takesNoValue[a] && i+1 < len(args) {
			i++
			val = args[i]
		}
		if name == "--py-files" {
			for _, p := range strings.Split(val, ",") {
				add(strings.TrimSpace(p))
			}
		}
	}
	return out
}

// fetchScripts reads the application's scripts from S3 (GetObject), named
// by the spark-submit arguments of the step that submitted it, for the
// explorer's Code tab. It returns nil when there is nothing to fetch.
func (out *clusterLogs) fetchScripts(ctx context.Context, cloud *awsSession, appID string) ([]report.FetchedSource, *model.SourceStatus) {
	var paths []string
	for _, st := range out.steps {
		if st.AppID == appID {
			paths = append(paths, submitScripts(st.Args)...)
		}
	}
	if len(paths) == 0 {
		return nil, nil
	}
	row := &model.SourceStatus{Name: "Application code", Status: "read", Location: paths[0]}
	cfg, err := cloud.config(ctx)
	if err != nil {
		row.Status, row.Class, row.Detail = "error", source.ClassAccessDenied, err.Error()
		return nil, row
	}
	var got []report.FetchedSource
	var read, failed []string
	for _, p := range paths {
		bucket, key, _ := source.ParseS3(p)
		f := model.SourceFile{Location: p, Status: "read"}
		data, err := func() ([]byte, error) {
			st, err := awsDeps.s3(ctx, cfg, bucket)
			if err != nil {
				return nil, err
			}
			rc, err := st.Open(ctx, source.Object{Key: key})
			if err != nil {
				return nil, err
			}
			defer rc.Close()
			b, err := io.ReadAll(io.LimitReader(rc, maxScriptBytes+1))
			if err == nil && len(b) > maxScriptBytes {
				err = &source.Error{Class: source.ClassTooLarge, Key: p, Err: fmt.Errorf("over %d bytes", maxScriptBytes)}
			}
			return b, err
		}()
		if err != nil {
			f.Status, f.Class, f.Detail = "error", source.ClassOf(err), err.Error()
			row.Class = f.Class
			failed = append(failed, p)
		} else {
			f.Bytes = int64(len(data))
			got = append(got, report.FetchedSource{Path: p, Data: data})
			read = append(read, p)
		}
		row.Files = append(row.Files, f)
	}
	switch {
	case len(failed) == 0:
		row.Detail = fmt.Sprintf("Fetched %s, named in the step's spark-submit arguments. The explorer's Code tab shows it beside the jobs and stages that ran each line, redacted.", strings.Join(read, ", "))
	case len(read) == 0:
		row.Status = "error"
		row.Detail = fmt.Sprintf("Could not fetch %s, named in the step's spark-submit arguments; pass -source with a local copy to see it beside the jobs.", strings.Join(failed, ", "))
	default:
		row.Status = "partial"
		row.Detail = fmt.Sprintf("Fetched %s; could not fetch %s.", strings.Join(read, ", "), strings.Join(failed, ", "))
	}
	return got, row
}

// runWindow is when the application ran: from the event log, else from
// its logs (YARN's application summary, or the times its container logs
// cover; see yarnlog.Window), else from the step that submitted it. how
// says which, for the report.
func runWindow(log *model.EventLog, files []model.LogFile, steps []model.Step, cl *model.Cluster) (from, to time.Time, how string, ok bool) {
	if log != nil && !log.Application.Start.IsZero() {
		from, to = log.Application.Start, log.Application.End
		if to.IsZero() {
			to = from.Add(time.Duration(log.Application.DurationMs) * time.Millisecond)
		}
		return from, to, "the event log", true
	}
	appID := ""
	if log != nil {
		appID = log.Application.ID
	}
	if from, to, how := yarnlog.Window(files, appID); !from.IsZero() && !to.IsZero() {
		return from, to, how, true
	}
	for _, st := range steps {
		if st.AppID != "" && !st.Started.IsZero() {
			// A step cancelled with its cluster has no end: the cluster's
			// end bounds it.
			to = st.Ended
			if to.IsZero() && cl != nil {
				to = cl.Ended
			}
			if to.IsZero() {
				to = awsDeps.now()
			}
			return st.Started, to, "the step that submitted it", true
		}
	}
	return time.Time{}, time.Time{}, "", false
}

// logZoneCheck says when the log times look read in the wrong zone: the
// container logs' first time against a time that is UTC whatever the
// cluster's zone (the event log's start, YARN's application summary, or
// the step's start from the EMR API). Containers start seconds to minutes
// after these, so a gap of a whole time zone (a multiple of 30 minutes,
// give or take 15) means log-timezone is not the cluster's zone.
func logZoneCheck(log *model.EventLog, files []model.LogFile, steps []model.Step, loc *time.Location) string {
	var ref time.Time
	appID := ""
	if log != nil {
		ref, appID = log.Application.Start, log.Application.ID
	}
	if from, _, how := yarnlog.Window(files, appID); ref.IsZero() && how == "YARN's application summary" {
		ref = from
	}
	for _, st := range steps {
		if ref.IsZero() && st.AppID != "" {
			ref = st.Started
		}
	}
	var first time.Time
	for _, f := range files {
		if f.Container != "" && !f.FirstTime.IsZero() && (first.IsZero() || f.FirstTime.Before(first)) {
			first = f.FirstTime
		}
	}
	if ref.IsZero() || first.IsZero() {
		return ""
	}
	diff := first.Sub(ref)
	off := diff.Round(30 * time.Minute)
	if off == 0 || (diff-off).Abs() > 15*time.Minute {
		return ""
	}
	way := "ahead of"
	if off < 0 {
		way = "behind"
	}
	gap := fmt.Sprintf("%g hours", off.Abs().Hours())
	if loc == nil || loc == time.UTC {
		return fmt.Sprintf("Log times look %s %s UTC: the container logs start %s after the application did. Set log-timezone (config file) or -log-timezone to the cluster's time zone, such as Australia/Sydney.", gap, way, gap)
	}
	return fmt.Sprintf("Log times still look %s %s the application's start with log-timezone %s: check that it is the cluster's time zone.", gap, way, loc)
}

// readMetrics reads CloudWatch metrics for the cluster and the nodes that
// were up while the application ran, padded by pad on each side.
func (out *clusterLogs) readMetrics(ctx context.Context, cloud *awsSession, log *model.EventLog, off string, pad time.Duration) {
	row := model.SourceStatus{Name: "CloudWatch", Status: "read"}
	defer func() { out.sources = append(out.sources, row) }()
	app := appInstances(out.cluster.Instances, log, out.files)
	if off != "" && len(app) == 0 {
		row.Status, row.Detail = "not-requested", "Not called: "+off+"."
		return
	}
	from, to, _, ok := runWindow(log, out.files, out.steps, out.cluster)
	if !ok {
		row.Status, row.Detail = "not-supplied", "Not called: nothing says when the application ran (no event log, YARN summary or step)."
		return
	}
	from, to = from.Add(-pad), to.Add(pad)
	// The nodes that ran the application always, and first; with
	// CloudWatch on, the others up during the run too, up to the cap.
	ids, clusterID := slices.Clone(app), out.cluster.ID
	if off != "" {
		clusterID = "" // off: only the application's nodes' own metrics
	} else {
		for _, in := range out.cluster.Instances {
			if len(ids) >= maxNodesWithoutEventLog {
				break
			}
			if (!in.Ended.IsZero() && in.Ended.Before(from)) || (!in.Created.IsZero() && in.Created.After(to)) || slices.Contains(ids, in.ID) {
				continue
			}
			ids = append(ids, in.ID)
		}
	}
	cfg, err := cloud.config(ctx)
	if err == nil {
		out.metrics, err = awsmeta.Metrics(ctx, awsDeps.cloudwatch(cfg), clusterID, ids, from, to, awsDeps.now())
	}
	if err != nil {
		row.Status, row.Class, row.Detail = "error", awsmeta.ErrorClass(err), "Could not read metrics (needs cloudwatch:GetMetricData and cloudwatch:ListMetrics): "+err.Error()
		return
	}
	points := 0
	for _, s := range append(append([]model.Series{}, out.metrics.Cluster...), out.metrics.Hosts...) {
		points += len(s.Points)
	}
	row.Detail = fmt.Sprintf("GetMetricData for the cluster and %s from %s to %s UTC (the run with %s either side): %s series, %s points.",
		model.Plural(len(ids), "node", "nodes"), from.UTC().Format("2006-01-02 15:04"), to.UTC().Format("15:04"), pad,
		model.Num(int64(len(out.metrics.Cluster)+len(out.metrics.Hosts))), model.Num(int64(points)))
	row.Brief = fmt.Sprintf("%s series, %s–%s UTC", model.Num(int64(len(out.metrics.Cluster)+len(out.metrics.Hosts))), from.UTC().Format("15:04"), to.UTC().Format("15:04"))
	if off != "" {
		row.Detail = fmt.Sprintf("CloudWatch is off (%s), but the metrics of the %s that ran this application were read anyway; nothing else. ", off, model.Plural(len(ids), "node", "nodes")) + row.Detail
		row.Brief = "only the application's nodes: " + row.Brief
	}
	if out.metrics.Coverage == model.NoData {
		row.Status = "none"
		row.Detail += " " + strings.Join(out.metrics.Missing, " ")
	}
}

// readCalls looks up in CloudTrail what the nodes the application ran on
// called, padded by pad on each side of the run.
func (out *clusterLogs) readCalls(ctx context.Context, cloud *awsSession, log *model.EventLog, off string, pad time.Duration) {
	row := model.SourceStatus{Name: "CloudTrail", Status: "read"}
	defer func() { out.sources = append(out.sources, row) }()
	if off != "" {
		row.Status, row.Detail = "not-requested", "Not called: "+off+"."
		return
	}
	from, to, _, ok := runWindow(log, out.files, out.steps, out.cluster)
	if !ok {
		row.Status, row.Detail = "not-supplied", "Not called: nothing says when the application ran (no event log, YARN summary or step)."
		return
	}
	from, to = from.Add(-pad), to.Add(pad)
	users := callers(out.cluster.Instances, log, out.files, from, to)
	if len(users) == 0 {
		row.Status, row.Detail = "none", "No node of the cluster is known to have run the application, so there was no one to look up."
		return
	}
	cfg, err := cloud.config(ctx)
	if err == nil {
		out.calls, err = awsmeta.Calls(ctx, awsDeps.cloudtrail(cfg), users, from, to, awsDeps.now())
	}
	if err != nil {
		row.Status, row.Class, row.Detail = "error", awsmeta.ErrorClass(err), "Could not look up AWS calls (needs cloudtrail:LookupEvents): "+err.Error()
		return
	}
	row.Detail = fmt.Sprintf("LookupEvents for %s (%s) from %s to %s UTC: %s events, %s refused.", model.Plural(len(users), "node", "nodes"), strings.Join(users, ", "),
		from.UTC().Format("2006-01-02 15:04"), to.UTC().Format("15:04"), model.Num(int64(out.calls.Events)), model.Num(int64(len(out.calls.Denied))))
	refused := "none refused"
	if n := len(out.calls.Denied); n > 0 {
		refused = fmt.Sprintf("%d refused", n)
	}
	row.Brief = fmt.Sprintf("%s, %s, %s", model.Plural(len(users), "node", "nodes"), model.Plural(out.calls.Events, "call", "calls"), refused)
	switch {
	case out.calls.Coverage == model.NoData:
		row.Status = "none"
		row.Detail += " " + strings.Join(out.calls.Missing[1:], " ")
	case out.calls.Truncated:
		row.Status = "partial"
	}
}

// callers are the instance IDs whose CloudTrail sessions made the
// application's AWS calls: the nodes its driver and executors ran on, or
// without an event log, the driver's node from YARN's records and every
// worker node up during the run. The primary node is left out unless the
// application ran there, since its EMR daemons call AWS all the time.
func callers(instances []model.Instance, log *model.EventLog, files []model.LogFile, from, to time.Time) []string {
	hosts := map[string]bool{}
	if log != nil {
		for _, x := range log.Executors {
			hosts[shortHost(x.Host)] = true
		}
		if log.Driver != nil {
			hosts[shortHost(log.Driver.Host)] = true
		}
	} else {
		for _, h := range yarnlog.AppHosts(files) {
			hosts[h] = true
		}
		for _, f := range files {
			for _, l := range f.Found {
				if h := l.Fields["appMasterHost"]; h != "" {
					hosts[shortHost(h)] = true
				}
				if h := l.Fields["ApplicationMasterhost"]; h != "" {
					hosts[shortHost(h)] = true
				}
			}
		}
	}
	var out []string
	for _, in := range instances {
		up := !(!in.Ended.IsZero() && in.Ended.Before(from)) && !(!in.Created.IsZero() && in.Created.After(to))
		ran := hosts[shortHost(in.PrivateDNS)] || hosts[shortHost(in.PrivateIP)]
		worker := !in.Primary && in.Role != "MASTER"
		if ran || (log == nil && len(hosts) == 0 && up && worker) {
			out = append(out, in.ID)
		}
	}
	return out
}

// noCluster is what an online run can say when it could not describe the
// cluster: the EMR API row with the error, and the sources that need the
// cluster's details marked as unread.
func noCluster(id string, err error) clusterLogs {
	class := awsmeta.ErrorClass(err)
	out := clusterLogs{emr: &model.SourceStatus{Name: "EMR API", Status: "error", Class: class, Location: "EMR DescribeCluster " + id,
		Detail: "Could not describe the cluster: " + err.Error() + ". Without it sparkplain cannot find the cluster's logs, nodes or steps."}}
	for _, name := range []string{"Container logs", "Step logs", "Node logs", "CloudWatch", "CloudTrail"} {
		out.sources = append(out.sources, model.SourceStatus{Name: name, Status: "not-supplied",
			Detail: "Not read: finding it needs the cluster's details from the EMR API, which could not be read."})
	}
	return out
}

// appInstances are the instances that ran the application's driver or
// executors: the hosts the event log names, and those its container logs
// name.
func appInstances(instances []model.Instance, log *model.EventLog, files []model.LogFile) []string {
	hosts := map[string]bool{}
	if log != nil {
		for _, x := range log.Executors {
			hosts[shortHost(x.Host)] = true
		}
		if log.Driver != nil {
			hosts[shortHost(log.Driver.Host)] = true
		}
		if h := log.Application.DriverAttributes["NM_HOST"]; h != "" {
			hosts[shortHost(h)] = true
		}
	}
	for _, h := range yarnlog.AppHosts(files) {
		hosts[h] = true
	}
	delete(hosts, "")
	var out []string
	for _, in := range instances {
		if hosts[shortHost(in.PrivateDNS)] || hosts[shortHost(in.PrivateIP)] {
			out = append(out, in.ID)
		}
	}
	return out
}
