package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emr/types"

	"github.com/ryandam9/sparkplain/internal/sparkplain/awsmeta"
	"github.com/ryandam9/sparkplain/internal/sparkplain/awsmeta/awsfake"
	"github.com/ryandam9/sparkplain/internal/sparkplain/source"
)

const recordings = "../../testdata/aws"

// routeStore serves one bucket from several local folders, by the key's
// first path component (emr-logs/ from testdata/emrlogs, spark-events/
// from testdata/eventlog), as the recorded cluster's bucket held them.
type routeStore struct {
	routes map[string]string // first component → local folder
	bucket string
}

func (r routeStore) split(key string) (source.Store, string, string, bool) {
	first, rest, _ := strings.Cut(key, "/")
	dir, ok := r.routes[first]
	if !ok {
		return nil, "", "", false
	}
	return source.NewLocalStore(dir), first + "/", rest, true
}

func (r routeStore) List(ctx context.Context, prefix string) ([]source.Object, error) {
	st, head, rest, ok := r.split(prefix)
	if !ok {
		return nil, nil
	}
	objs, err := st.List(ctx, rest)
	for i := range objs {
		objs[i].Key = head + objs[i].Key
	}
	return objs, err
}

func (r routeStore) Head(ctx context.Context, key string) (source.Object, bool, error) {
	st, head, rest, ok := r.split(key)
	if !ok {
		return source.Object{}, false, nil
	}
	o, found, err := st.Head(ctx, rest)
	o.Key = head + o.Key
	return o, found, err
}

func (r routeStore) Open(ctx context.Context, o source.Object) (io.ReadCloser, error) {
	st, _, rest, ok := r.split(o.Key)
	if !ok {
		return nil, &source.Error{Class: source.ClassNotFound, Key: o.Key, Err: errors.New("no such key")}
	}
	o.Key = rest
	return st.Open(ctx, o)
}

func (r routeStore) Location(key string) string { return "s3://" + r.bucket + "/" + key }

// replayAWS answers every AWS call the CLI makes from a recording of a
// real cluster, scrubbed, with its logs and event logs in testdata.
func replayAWS(t *testing.T, name string) *awsfake.Recording {
	t.Helper()
	rec, err := awsfake.Load(filepath.Join(recordings, name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	bucket, _, ok := source.ParseS3(aws.ToString(rec.Cluster.LogUri))
	if !ok {
		t.Fatalf("recording's log URI %q is not on S3", aws.ToString(rec.Cluster.LogUri))
	}
	// The phase 3 and 4 jobs' scripts sat in the bucket under p3/ and p4/.
	store := routeStore{bucket: bucket, routes: map[string]string{"emr-logs": emrlogs, "spark-events": fx,
		"p3": "../../testdata/emrscripts/p3", "p4": "../../testdata/emrscripts/p4"}}
	saved := awsDeps
	t.Cleanup(func() { awsDeps = saved })
	awsDeps.config = func(context.Context, string, string) (aws.Config, error) { return aws.Config{Region: "us-east-1"}, nil }
	awsDeps.emr = func(aws.Config) awsmeta.EMRAPI { return awsfake.EMR{R: rec} }
	awsDeps.ec2 = func(aws.Config) awsmeta.EC2API { return awsfake.EC2{R: rec} }
	awsDeps.cloudwatch = func(aws.Config) awsmeta.CloudWatchAPI { return awsfake.CloudWatch{R: rec} }
	awsDeps.cloudtrail = func(aws.Config) awsmeta.CloudTrailAPI { return awsfake.CloudTrail{R: rec} }
	awsDeps.sts = func(aws.Config) STSAPI { return stsAs{arn: "arn:aws:sts::000000000000:assumed-role/fixture/tester"} }
	awsDeps.now = func() time.Time { return rec.RecordedAt }
	awsDeps.s3 = func(_ context.Context, _ aws.Config, b string) (source.Store, error) {
		if b != bucket {
			return nil, &source.Error{Class: source.ClassNotFound, Key: b, Err: errors.New("no such bucket")}
		}
		return store, nil
	}
	interval := awsmeta.LookupInterval
	awsmeta.LookupInterval = 0
	t.Cleanup(func() { awsmeta.LookupInterval = interval })
	requireStubbed(t, saved)
	return rec
}

// replayLive replays a recording as if its cluster were still up: the
// instances that ended with the cluster are RUNNING, with no end time, and
// those that ended earlier (a reclaimed spot node, a scaled-in task node)
// stay TERMINATED, as ListInstances would show a live cluster. The
// recordings were made after each cluster ended, when every instance is
// TERMINATED and sparkplain drops them all (TestTerminatedInstancesNeverShown).
func replayLive(t *testing.T, name string) *awsfake.Recording {
	t.Helper()
	rec := replayAWS(t, name)
	end := aws.ToTime(rec.Cluster.Status.Timeline.EndDateTime)
	for i := range rec.Instances {
		st := rec.Instances[i].Status
		if st == nil || st.Timeline == nil || st.State != emrtypes.InstanceStateTerminated {
			continue
		}
		if ended := aws.ToTime(st.Timeline.EndDateTime); !ended.IsZero() && end.Sub(ended) < time.Minute {
			st.State, st.Timeline.EndDateTime, st.StateChangeReason = emrtypes.InstanceStateRunning, nil, nil
		}
	}
	return rec
}

// findingRules lists a report's finding rules and titles.
func findingRules(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, f := range readReport(t, dir).Findings {
		if _, ok := out[f.Rule]; !ok {
			out[f.Rule] = f.Title
		}
	}
	return out
}

// The first phase 3 test cluster: two applications started together, each
// driver on a different worker node, and neither's 11 GiB executors fitted
// beside it, so both waited until the cluster was stopped.
func TestRecordedDeadlock(t *testing.T) {
	replayLive(t, "j-FIXTURE0056CLUSTER")
	for app, want := range map[string]map[string]string{
		"application_1790380000000_0056": {
			"waited-for-capacity": "Containers waited 12 min 0 s while 34% of YARN memory was free",
			"executor-fit":        "Spark wanted 16 executors, but the cluster had room for 1",
			"shared-cluster":      "2 applications shared the cluster",
		},
		"application_1790380000000_0055": { // the access job, which reached its STS call first
			"access-denied":       "AWS refused access 1 time (sts:AssumeRole on arn:aws:iam::000000000000:role/fixture-no-such-role)",
			"waited-for-capacity": "Containers waited 12 min 0 s while 34% of YARN memory was free",
			"executor-fit":        "Spark wanted 2 executors, but the cluster had room for 1",
		},
	} {
		dir := t.TempDir()
		code, _, errs := runCLI(t, "-app-id", app, "-cluster-id", "j-FIXTURE0056CLUSTER", "-profile", "test", "-out", dir, "-format", "json,html,explorer")
		if code != exitPartial {
			t.Fatalf("%s: exit %d (no event log, so 3): %s", app, code, errs)
		}
		got := findingRules(t, dir)
		for rule, title := range want {
			if got[rule] != title {
				t.Errorf("%s: %s = %q, want %q", app, rule, got[rule], title)
			}
		}
		r := readReport(t, dir)
		for _, name := range []string{"EMR API", "EC2 API", "CloudWatch", "CloudTrail", "Container logs", "Step logs", "Node logs"} {
			if s := sourceOf(r, name); s.Status != "read" {
				t.Errorf("%s: %s = %+v", app, name, s)
			}
		}
		if r.Nodes.Coverage == "needs-event-log" || len(r.Nodes.Hosts) != 3 || r.Metrics == nil || len(r.Metrics.Summary) != 3 {
			t.Errorf("%s: nodes %v (%d hosts), metrics %+v", app, r.Nodes.Coverage, len(r.Nodes.Hosts), r.Metrics)
		}
	}
}

// The second phase 3 test cluster: a busy job beside one refused an STS
// call, a job whose settings SparkContext refused, a heap that ran out, and
// a Python job that allocated 3 GiB too briefly for YARN to notice.
func TestRecordedPhase3(t *testing.T) {
	replayLive(t, "j-FIXTURE0062CLUSTER")
	type want struct {
		code   int
		status string
		has    map[string]string // rule → title prefix
		hasNot []string
	}
	cfg := "First error: IllegalArgumentException: spark.excludeOnFailure.task.maxTaskAttemptsPerNode ( = 2) was >= spark.task.maxFailures ( = 2 )"
	for n, w := range map[string]want{
		"0061": {exitPartial, "failed", map[string]string{ // the access job: refused by STS; CloudTrail never recorded it
			"log-first-failure": "First error: AWSSecurityTokenServiceException: User: arn:aws:sts::000000000000:assumed-role/EMR_EC2_DefaultRole/",
			"access-denied":     "AWS refused access 2 times (sts:AssumeRole on arn:aws:iam::000000000000:role/fixture-no-such-role",
		}, nil},
		// The spot node that was not busy became ready after the run ended.
		"0062": {exitOK, "succeeded", map[string]string{"shared-cluster": "2 applications shared the cluster"}, []string{"log-first-failure", "idle-nodes"}},
		"0063": {exitPartial, "failed", map[string]string{"log-first-failure": cfg}, []string{"waited-for-capacity", "idle-nodes"}},
		"0064": {exitPartial, "failed", map[string]string{"log-first-failure": cfg}, []string{"waited-for-capacity", "idle-nodes"}},
		"0065": {exitPartial, "failed", map[string]string{ // the heap ran out; each executor killed itself, exit 137
			"log-first-failure": "First error: OutOfMemoryError: GC overhead limit exceeded",
			"out-of-memory":     "8 executors ran out of memory (GC overhead limit exceeded)",
		}, []string{"executor-memory-kill"}},
		"0066": {exitOK, "succeeded", map[string]string{}, []string{"executor-memory-kill", "log-first-failure"}}, // 3 GiB for 3 s: YARN never saw it
	} {
		app := "application_1790380000000_" + n
		dir := t.TempDir()
		code, _, errs := runCLI(t, "-app-id", app, "-cluster-id", "j-FIXTURE0062CLUSTER", "-profile", "test", "-out", dir, "-format", "json,html,explorer")
		r := readReport(t, dir)
		if code != w.code {
			var bad []string
			for _, s := range r.Sources {
				if s.Status == "partial" || s.Status == "error" || s.Status == "not-supplied" {
					bad = append(bad, s.Name+": "+s.Status+" "+s.Detail)
				}
			}
			t.Errorf("%s: exit %d, want %d: %s %v", n, code, w.code, errs, bad)
		}
		if r.Application.Status != w.status {
			t.Errorf("%s: status %q, want %q", n, r.Application.Status, w.status)
		}
		got := findingRules(t, dir)
		for rule, title := range w.has {
			if !strings.HasPrefix(got[rule], title) {
				t.Errorf("%s: %s = %q, want %q…", n, rule, got[rule], title)
			}
		}
		for _, rule := range w.hasNot {
			if _, ok := got[rule]; ok {
				t.Errorf("%s: unexpected %s: %q", n, rule, got[rule])
			}
		}
		// The two spot nodes that ended before the cluster did are dropped,
		// and 0066 ran its executors on one of them (ip-10-0-2-13), so no
		// node of it is known to look up in CloudTrail.
		if len(r.Cluster.Instances) != 3 || r.Cluster.Instances[0].VCPU != 4 || (r.AWSCalls == nil) != (n == "0066") || r.Metrics == nil {
			t.Errorf("%s: cluster %+v, calls %v, metrics %v", n, r.Cluster, r.AWSCalls != nil, r.Metrics != nil)
		}
	}
}

// The phase 4 live check's cluster: a spot task node was reclaimed 80 s
// into the first application, taking attempt 1's driver with it, and
// attempt 2 finished on the core node; then NOAA and a second copy of the
// findings job ran at once. Recorded after the cluster ended, so every
// log, the daemons' included, had reached S3.
func TestRecordedPhase4(t *testing.T) {
	replayLive(t, "j-FIXTURE0071CLUSTER")
	type want struct {
		has    map[string]string // rule → title prefix
		hasNot []string
		expl   map[string][]string // rule → phrases its explanation holds
	}
	for n, w := range map[string]want{
		// The reclaimed spot node is TERMINATED, so it is dropped: no
		// spot-interrupted finding and no word of it from EMR; YARN's own
		// log line about the node still explains the retry.
		"0071": {map[string]string{
			"app-retried":  "YARN restarted the application after 1 failed attempt",
			"memory-spill": "3 stages spilled 2.2 GiB to disk",
			"driver-gaps":  "No Spark job ran for 2 min 20 s (54%) of the run",
			"task-retries": "1 task attempt failed, and its retry succeeded",
		}, []string{"idle-nodes", "log-first-failure", "stage-skew", "spot-interrupted"}, map[string][]string{
			"app-retried": {"Its driver ran on ip-10-0-2-12.us-east-1.compute.internal.", "YARN reported that node DECOMMISSIONING",
				"INTERNAL_ERROR_BROADCAST", "and attempt 2 finished"},
		}},
		"0072": {map[string]string{
			"waited-for-capacity": "Containers waited 2 min 53 s for room on the cluster",
			"executor-fit":        "Spark wanted 39 executors, but the cluster had room for 2",
			"shared-cluster":      "2 applications shared the cluster",
		}, []string{"idle-nodes", "app-retried", "spot-interrupted"}, map[string][]string{
			"shared-cluster": {"1 worker node ran nothing for this application. It is possible that the other application used it."},
		}},
		"0073": {map[string]string{
			"waited-for-capacity": "Containers waited 3 min 24 s for room on the cluster",
			"memory-spill":        "3 stages spilled 2.3 GiB to disk",
			"shared-cluster":      "2 applications shared the cluster",
		}, []string{"idle-nodes", "app-retried", "spot-interrupted"}, map[string][]string{
			"shared-cluster": {"It is possible that the other application used it."},
		}},
	} {
		dir := t.TempDir()
		code, out, errs := runCLI(t, "-app-id", "application_1790380000000_"+n, "-cluster-id", "j-FIXTURE0071CLUSTER", "-profile", "test", "-out", dir, "-format", "json,html,explorer")
		if code != exitOK {
			t.Errorf("%s: exit %d: %s", n, code, errs)
		}
		r := readReport(t, dir)
		if r.Application.Status != "succeeded" {
			t.Errorf("%s: status %q", n, r.Application.Status)
		}
		got := map[string]string{}
		expl := map[string]string{}
		for _, f := range r.Findings {
			got[f.Rule], expl[f.Rule] = f.Title, f.Explanation
		}
		for rule, title := range w.has {
			if !strings.HasPrefix(got[rule], title) {
				t.Errorf("%s: %s = %q, want %q…", n, rule, got[rule], title)
			}
		}
		for _, rule := range w.hasNot {
			if _, ok := got[rule]; ok {
				t.Errorf("%s: unexpected %s: %q", n, rule, got[rule])
			}
		}
		for rule, phrases := range w.expl {
			for _, p := range phrases {
				if !strings.Contains(expl[rule], p) {
					t.Errorf("%s: %s explanation lacks %q: %s", n, rule, p, expl[rule])
				}
			}
		}
		// With the cluster ended, every worker's YARN capacity and CPU is
		// known, the reclaimed spot node's (one CloudWatch point) included.
		for _, h := range r.Nodes.Hosts {
			if h.Instance == nil || h.Instance.Role == "MASTER" {
				continue
			}
			if h.YARNMemoryBytes == 0 || h.HostCPU == nil {
				t.Errorf("%s: node %s: YARN memory %d, CPU %v", n, h.Name, h.YARNMemoryBytes, h.HostCPU)
			}
		}
		if n == "0071" && (len(r.Summary.Sentences) == 0 || !strings.Contains(r.Summary.Sentences[0], "finished on attempt 2, after YARN restarted it.") || !strings.Contains(out, "What happened")) {
			t.Errorf("0071: summary does not say which attempt finished: %q", r.Summary.Sentences)
		}
	}
}

// No terminated instance shows anywhere: not in report.json, the report or
// the explorer, not even a node that ran the application. Recorded after
// the cluster ended, every instance of j-FIXTURE0071CLUSTER is TERMINATED.
func TestTerminatedInstancesNeverShown(t *testing.T) {
	rec := replayAWS(t, "j-FIXTURE0071CLUSTER")
	dir := t.TempDir()
	runCLI(t, "-app-id", "application_1790380000000_0071", "-cluster-id", "j-FIXTURE0071CLUSTER", "-profile", "test", "-out", dir, "-format", "json,html,explorer")
	if r := readReport(t, dir); r.Cluster == nil || len(r.Cluster.Instances) != 0 {
		t.Fatalf("cluster instances %+v, want none", r.Cluster)
	}
	files, err := filepath.Glob(filepath.Join(dir, "*"))
	if err != nil || len(files) != 3 {
		t.Fatalf("outputs %v %v", files, err)
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, in := range rec.Instances {
			if id := aws.ToString(in.Ec2InstanceId); strings.Contains(string(b), id) {
				t.Errorf("%s names terminated instance %s", filepath.Base(f), id)
			}
		}
	}
}
