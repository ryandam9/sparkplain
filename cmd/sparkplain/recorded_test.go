package main

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

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
	store := routeStore{bucket: bucket, routes: map[string]string{"emr-logs": emrlogs, "spark-events": fx}}
	saved := awsDeps
	t.Cleanup(func() { awsDeps = saved })
	awsDeps.config = func(context.Context, string, string) (aws.Config, error) { return aws.Config{Region: "us-east-1"}, nil }
	awsDeps.emr = func(aws.Config) awsmeta.EMRAPI { return awsfake.EMR{R: rec} }
	awsDeps.ec2 = func(aws.Config) awsmeta.EC2API { return awsfake.EC2{R: rec} }
	awsDeps.cloudwatch = func(aws.Config) awsmeta.CloudWatchAPI { return awsfake.CloudWatch{R: rec} }
	awsDeps.cloudtrail = func(aws.Config) awsmeta.CloudTrailAPI { return awsfake.CloudTrail{R: rec} }
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
	replayAWS(t, "j-FIXTURE0056CLUSTER")
	for app, want := range map[string]map[string]string{
		"application_1790380000000_0056": {
			"waited-for-capacity": "Containers waited 12 min 0 s while 34% of YARN memory was free",
			"executor-fit":        "Spark wanted 16 executors; the cluster had room for 1",
			"shared-cluster":      "2 applications shared the cluster",
		},
		"application_1790380000000_0055": { // the access job, which reached its STS call first
			"access-denied":       "AWS refused access 1 time: sts:AssumeRole on arn:aws:iam::000000000000:role/fixture-no-such-role",
			"waited-for-capacity": "Containers waited 12 min 0 s while 34% of YARN memory was free",
			"executor-fit":        "Spark wanted 2 executors; the cluster had room for 1",
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
