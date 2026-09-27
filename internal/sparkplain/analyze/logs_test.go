package analyze

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/eventlog"
	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/source"
	"github.com/ryandam9/sparkplain/internal/sparkplain/yarnlog"
)

const emrlogs = "../../../testdata/emrlogs"

// logFile classifies synthetic log text as the file at key.
func logFile(t *testing.T, key, text string) model.LogFile {
	t.Helper()
	f := yarnlog.Describe(key)
	res, err := yarnlog.Classify(strings.NewReader(text), key, f, yarnlog.Options{AppID: "application_1_1"})
	if err != nil {
		t.Fatal(err)
	}
	return model.LogFile{Location: key, Kind: string(f.Kind), Container: f.Container, Step: f.Step, Instance: f.Instance, Lines: res.Read, Found: res.Lines}
}

func runWithLogs(l *model.EventLog, cl *model.Cluster, files ...model.LogFile) *model.Report {
	ev := model.SourceStatus{Name: "Spark event log", Status: "read"}
	if l == nil {
		ev.Status = "not-supplied"
	}
	return Run(Input{AppID: "application_1_1", Tool: "t", TimeZone: "UTC", EventLog: l, EventSource: ev, Cluster: cl, Logs: files, LogsRead: true,
		LogSources: []model.SourceStatus{{Name: "Container logs", Status: "read"}}})
}

// The failed EMR run: no event log, because SparkContext never started.
func TestLogsExplainFailedRunWithoutEventLog(t *testing.T) {
	col := yarnlog.Collect(context.Background(), source.NewLocalStore(emrlogs), yarnlog.Plan{Root: "j-FIXTURE0052CLUSTER/", AppID: "application_1790380000000_0052"})
	r := Run(Input{AppID: "application_1790380000000_0052", Tool: "t", TimeZone: "UTC", Logs: col.Files, LogsRead: true, LogSources: col.Sources,
		EventSource: model.SourceStatus{Name: "Spark event log", Status: "not-supplied"}})
	a := r.Application
	if a.ID != "application_1790380000000_0052" || a.Name != "emr_job2.py" || a.User != "hadoop" || a.Queue != "default" || a.Status != model.StatusFailed ||
		!strings.Contains(a.StatusReason, "exited with code 13 (SparkContext never started") || a.DurationMs <= 0 {
		t.Errorf("application = %+v", a)
	}
	got := rules(r)
	f, ok := got["log-first-failure"]
	if !ok || r.Findings[0].Rule != "log-first-failure" {
		t.Fatalf("first failure missing or not first: %v", keys(got))
	}
	all := f.Title + f.Explanation + f.Fix
	for _, e := range f.Evidence {
		all += "\n" + e.Text
	}
	for _, want := range []string{
		"First error: FileNotFoundException: No such file or directory 's3://sparkplain-fixtures/spark-events'",
		"in the driver's stderr at 10:15:43 UTC", // attempt 1, before attempt 2
		"YARN started the application 2 times",
		"the Python code: emr_job2.py line 7",
		"2 attempts failed, exit code 13",
		"Step failed with exitCode 1",
		"Spark could not open its event log folder", // the step set spark.eventLog.dir to that path
	} {
		if !strings.Contains(all, want) {
			t.Errorf("first failure lacks %q:\n%s", want, all)
		}
	}
	if f.Evidence[0].Ref != "executor:driver" || !strings.HasSuffix(f.Evidence[0].Source.File, "container_1790380000000_0052_01_000001/stderr.gz") || f.Evidence[0].Source.Line != 57 {
		t.Errorf("evidence = %+v", f.Evidence[0])
	}
	if s := strings.Join(r.Summary.Sentences, " "); !strings.Contains(s, "emr_job2.py (application_1790380000000_0052) failed as hadoop") ||
		!strings.Contains(s, "The first error in the logs was") || !strings.Contains(s, "There is no event log") {
		t.Errorf("summary = %q", s)
	}
	if r.Identity.Coverage != model.Partial || r.Identity.Facts[0].Value != "hadoop" {
		t.Errorf("identity = %+v", r.Identity)
	}
	if r.ExitCode != 3 {
		t.Errorf("exit %d: the event log is missing", r.ExitCode)
	}
	for _, cov := range r.Coverage {
		if cov.ID == "summary" && cov.Coverage != model.Partial {
			t.Errorf("summary coverage = %+v", cov)
		}
	}
}

// Containers join to executors through the CONTAINER_ID YARN gave Spark.
func TestLogsJoinExecutors(t *testing.T) {
	in, err := resolveFixture("application_1790380000000_0050")
	if err != nil {
		t.Fatal(err)
	}
	col := yarnlog.Collect(context.Background(), source.NewLocalStore(filepath.Join(emrlogs, "j-FIXTURE0050CLUSTER")), yarnlog.Plan{AppID: "application_1790380000000_0050"})
	r := Run(Input{Tool: "t", TimeZone: "UTC", EventLog: in, EventSource: model.SourceStatus{Name: "Spark event log", Status: "read"}, Logs: col.Files, LogsRead: true, LogSources: col.Sources})
	joined := map[string]string{}
	for _, f := range r.Logs.Files {
		if f.Container != "" {
			joined[f.Container] = f.Executor + "@" + f.Host
		}
	}
	for c, want := range map[string]string{
		"container_1790380000000_0050_01_000001": "driver@ip-10-0-2-10.us-east-1.compute.internal",
		"container_1790380000000_0050_01_000002": "1@ip-10-0-2-12.us-east-1.compute.internal",
		"container_1790380000000_0050_01_000005": "4@ip-10-0-2-10.us-east-1.compute.internal",
	} {
		if joined[c] != want {
			t.Errorf("%s = %q, want %q", c, joined[c], want)
		}
	}
	for _, f := range r.Findings {
		if f.Severity != model.Info {
			t.Errorf("a clean run gained a finding from its logs: %+v", f)
		}
	}
	if r.Logs.Coverage != model.Complete {
		t.Errorf("logs coverage = %v", r.Logs.Coverage)
	}
}

func resolveFixture(app string) (*model.EventLog, error) {
	in, err := eventlog.Resolve(filepath.Join(fixtures, app), app, eventlog.Limits{})
	if err != nil {
		return nil, err
	}
	defer in.Close()
	return eventlog.Parse(context.Background(), in, eventlog.Options{})
}

const (
	driverErr = "containers/application_1_1/container_1_1_01_000001/stderr"
	exec2Err  = "containers/application_1_1/container_1_1_01_000002/stderr"
	nmLog     = "node/i-0fee0000000000001/applications/hadoop-yarn/hadoop-yarn-nodemanager-ip-10-0-0-5.log"
	rmLog     = "node/i-0fee0000000000002/applications/hadoop-yarn/hadoop-yarn-resourcemanager-ip-10-0-0-1.log"
)

// The event log's memory kill gains YARN's own words and the executor's
// last error; lost executors gain the error in their own logs.
func TestLogsSharpenExecutorFindings(t *testing.T) {
	killed := &model.Executor{ID: "2", Host: "ip-10-0-0-5", Removed: time.Unix(1_790_000_100, 0), RemovedReason: "Container killed by YARN for exceeding physical memory limits",
		RemovalKind: model.RemovalMemoryKill, Attributes: map[string]string{"CONTAINER_ID": "container_1_1_01_000002"}, RemovedSource: model.Source{File: "ev", Line: 9}}
	lost := &model.Executor{ID: "3", Host: "ip-10-0-0-6", Removed: time.Unix(1_790_000_200, 0), RemovedReason: "Executor heartbeat timed out",
		RemovalKind: model.RemovalLost, Attributes: map[string]string{"CONTAINER_ID": "container_1_1_01_000003"}, RemovedSource: model.Source{File: "ev", Line: 12}}
	r := runWithLogs(synthetic(nil, killed, lost), nil,
		logFile(t, nmLog, `2024-01-01 10:00:00,000 WARN org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl (Container Monitor): Container [pid=1,containerID=container_1_1_01_000002] is running 1B beyond the 'PHYSICAL' memory limit. Current usage: 5.6 GB of 5.5 GB physical memory used; 7 GB of 27 GB virtual memory used. Killing container.
`),
		logFile(t, "containers/application_1_1/container_1_1_01_000003/stderr", `24/01/01 10:00:00 ERROR Executor: Exception in task 1.0 in stage 3.0 (TID 11)
java.io.IOException: No space left on device
	at com.example.Job.write(Job.java:3)
`))
	got := rules(r)
	mk := got["executor-memory-kill"]
	if !strings.Contains(mk.Explanation, "Current usage: 5.6 GB of 5.5 GB physical memory used") || len(mk.Evidence) != 2 || mk.Evidence[1].Source.File != nmLog {
		t.Errorf("memory kill = %+v", mk)
	}
	lf := got["executor-lost"]
	if len(lf.Evidence) != 2 || !strings.Contains(lf.Evidence[1].Text, "executor 3's own log: Exception in task 1.0 in stage 3.0 (TID 11) — IOException: No space left on device") ||
		lf.Evidence[1].Ref != "executor:3" || !strings.Contains(lf.Fix, "last error in each lost executor's own log") {
		t.Errorf("lost = %+v", lf)
	}
	if n := strings.Count(strings.Join(keys(got), " "), "executor-memory-kill"); n != 1 {
		t.Errorf("memory kill findings = %d, want one", n)
	}
}

func TestLogOnlyFindings(t *testing.T) {
	cl := &model.Cluster{ID: "j-1", InstanceProfile: "EMR_EC2_Fixture", ServiceRole: "EMR_Fixture", Source: "EMR DescribeCluster j-1"}
	r := runWithLogs(nil, cl,
		logFile(t, driverErr, `24/01/01 10:00:00 INFO metastore: Trying to connect to metastore with URI thrift://meta.example.internal:9083
24/01/01 10:00:01 ERROR Utils: Uncaught exception in thread main
java.lang.OutOfMemoryError: Java heap space
24/01/01 10:00:02 WARN HiveClientImpl: failed
com.amazonaws.services.glue.model.AccessDeniedException: User: arn:aws:sts::000000000000:assumed-role/EMR_EC2_Fixture/i-1 is not authorized to perform: glue:GetTable on resource: arn:aws:glue:us-east-1:000000000000:table/db/t (Service: AWSGlue; Status Code: 400)
24/01/01 10:00:03 WARN Client: Exception encountered while connecting to the server
javax.security.sasl.SaslException: GSS initiate failed [Caused by GSSException: No valid credentials provided]
24/01/01 10:00:04 ERROR AsyncProcess: region lookup failed
org.apache.hadoop.hbase.client.RetriesExhaustedException: Failed after attempts=16
24/01/01 10:00:05 ERROR Hive: Unable to instantiate org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient
`),
		logFile(t, rmLog, `2024-01-01 09:59:00,000 INFO org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl (RM Event dispatcher): Updating application attempt appattempt_1_1_000001 with final state: FAILED, and exit status: 15
2024-01-01 10:10:00,000 INFO org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary (RM Event dispatcher): appId=application_1_1,name=etl.py,user=etl,queue=batch,state=FINISHED,trackingUrl=x,appMasterHost=h,submitTime=1704102000000,startTime=1704102000000,launchTime=1704102001000,finishTime=1704102600000,finalStatus=SUCCEEDED,memorySeconds=1,vcoreSeconds=1,applicationType=SPARK,diagnostics=
`),
		logFile(t, "steps/s-FIXTURESTEP0001/controller", `INFO startExec 'hadoop jar /var/lib/aws/emr/step-runner/hadoop-jars/command-runner.jar spark-submit --deploy-mode cluster s3://code/etl.py'
2024-01-01T10:11:00.000Z WARN Step failed with exitCode 1 and took 660 seconds
`))
	got := rules(r)
	want := map[string]string{
		"out-of-memory":     "The driver ran out of memory (Java heap space)",
		"access-denied":     "AWS refused access 1 time: glue:GetTable on arn:aws:glue:us-east-1:000000000000:table/db/t",
		"kerberos-failure":  "Kerberos authentication failed (1 error in the logs)",
		"metastore-failure": "The table catalog could not be reached (1 error in the logs)",
		"hbase-failure":     "HBase could not be reached (1 error in the logs)",
		"step-failed":       "The step failed although Spark finished",
		"app-retried":       "YARN restarted the application after 1 failed attempt",
	}
	for rule, title := range want {
		if got[rule].Title != title {
			t.Errorf("%s: title %q, want %q", rule, got[rule].Title, title)
		}
	}
	if !strings.Contains(got["access-denied"].Fix, "Grant the instance profile EMR_EC2_Fixture") {
		t.Errorf("access fix = %q", got["access-denied"].Fix)
	}
	if !strings.Contains(got["app-retried"].Explanation, "exited with code 15 (the application's main class threw an exception)") {
		t.Errorf("retried = %q", got["app-retried"].Explanation)
	}
	if _, ok := got["log-first-failure"]; !ok {
		t.Error("the failed step should get a first failure")
	}
	facts := map[string]string{}
	for _, f := range r.Identity.Facts {
		facts[f.Label] = f.Value
	}
	if facts["Ran as"] != "etl" || facts["YARN queue"] != "batch" || facts["EC2 instance profile"] != "EMR_EC2_Fixture" || facts["EMR security configuration"] != "none" ||
		facts["Metastore connection"] != "failed (1 error)" {
		t.Errorf("identity = %v", facts)
	}
	if _, ok := got["bootstrap-failed"]; ok {
		t.Error("bootstrap-failed needs the cluster's own state reason")
	}
}

// EMR logs "bootstrap action 1 failed" on healthy clusters; only the
// cluster's state reason makes it a finding.
func TestBootstrapNeedsClusterState(t *testing.T) {
	boot := logFile(t, "node/i-0fee0000000000001/bootstrap-actions/master.log", `2024-01-01 10:00:00,000 ERROR InstanceConfiguratorDoConfigureThread: i-0fee0000000000001: failed to start. bootstrap action 1 failed with non-zero exit code.
`)
	if _, ok := rules(runWithLogs(nil, &model.Cluster{ID: "j-1", StateCode: "ALL_STEPS_COMPLETED"}, boot))["bootstrap-failed"]; ok {
		t.Error("a healthy cluster got bootstrap-failed")
	}
	f := rules(runWithLogs(nil, &model.Cluster{ID: "j-1", StateCode: "BOOTSTRAP_FAILURE", StateReason: "On the master instance (i-1), bootstrap action 1 returned a non-zero return code"}, boot))["bootstrap-failed"]
	if f.Severity != model.Critical || len(f.Evidence) != 2 {
		t.Errorf("bootstrap = %+v", f)
	}
}

// Without logs, nothing about them appears, and the lost-executor advice
// says how to get them.
func TestNoLogsRequested(t *testing.T) {
	lost := &model.Executor{ID: "1", Host: "h", Removed: time.Unix(1_790_000_100, 0), RemovedReason: "Executor heartbeat timed out", RemovalKind: model.RemovalLost}
	r := Run(Input{Tool: "t", EventLog: synthetic(nil, lost), EventSource: model.SourceStatus{Name: "Spark event log", Status: "read"}})
	if r.Logs != nil || r.ExitCode != 0 {
		t.Errorf("logs %+v, exit %d", r.Logs, r.ExitCode)
	}
	if f := rules(r)["executor-lost"]; !strings.Contains(f.Fix, "-cluster-id") {
		t.Errorf("fix = %q", f.Fix)
	}
	for _, s := range r.Sources {
		if s.Name == "Container logs" && s.Status != "not-requested" {
			t.Errorf("container logs = %+v", s)
		}
	}
}

// Exit 137 after HotSpot's out-of-memory banner is the JVM killing itself,
// not YARN: no memory-kill finding, and the first error is the heap.
func TestSelfKilledIsNotAMemoryKill(t *testing.T) {
	out := logFile(t, "containers/application_1_1/container_1_1_01_000002/stdout", `# java.lang.OutOfMemoryError: GC overhead limit exceeded
#   Executing /bin/sh -c "kill -9 14396
`)
	drv := logFile(t, driverErr, `26/09/26 16:10:29 INFO YarnAllocator: Completed container container_1_1_01_000002 on host: ip-10-0-2-10 (state: COMPLETE, exit status: 137)
26/09/26 16:10:29 WARN YarnAllocator: Container from a bad node: container_1_1_01_000002 on host: ip-10-0-2-10. Exit status: 137. Diagnostics: [2026-09-26 16:10:29.891]Container killed on request. Exit code is 137
26/09/26 16:10:29 ERROR YarnClusterScheduler: Lost executor 1 on ip-10-0-2-10: Container from a bad node: container_1_1_01_000002 on host: ip-10-0-2-10. Exit status: 137. Diagnostics: Container killed on request. Exit code is 137
26/09/26 16:10:30 INFO ApplicationMaster: Final app status: FAILED, exitCode: 1, (reason: User application exited with status 1)
`)
	r := runWithLogs(nil, nil, drv, out)
	got := rules(r)
	if _, ok := got["executor-memory-kill"]; ok {
		t.Error("a JVM that killed itself was reported as a YARN memory kill")
	}
	if got["out-of-memory"].Title != "1 executor ran out of memory (GC overhead limit exceeded)" {
		t.Errorf("oom = %q", got["out-of-memory"].Title)
	}
	if got["log-first-failure"].Title != "First error: OutOfMemoryError: GC overhead limit exceeded" {
		t.Errorf("first = %q", got["log-first-failure"].Title)
	}
}

// The phase 4 test cluster's first attempt: its driver ran on a spot node
// that YARN gave notice for and EMR took back, the attempt failed on a
// broadcast its executors could no longer fetch, and attempt 2 finished.
// The ResourceManager's log had not reached S3, so only the driver's own
// log and the event log say an attempt was retried.
func TestRetriedAttemptOnLostSpotNode(t *testing.T) {
	l := synthetic(nil, &model.Executor{ID: "1", Host: "ip-10-0-0-2.ec2.internal", Cores: 2})
	l.Application.AttemptID, l.Application.DeployMode = "2", "cluster"
	l.Application.DriverAttributes = map[string]string{"CONTAINER_ID": "container_1_1_02_000001", "NM_HOST": "ip-10-0-0-2.ec2.internal"}
	start := l.Application.Start
	cl := &model.Cluster{ID: "j-1", Instances: []model.Instance{
		{ID: "i-2", PrivateDNS: "ip-10-0-0-2.ec2.internal", Role: "CORE", Market: "ON_DEMAND", Created: start.Add(-time.Hour)},
		{ID: "i-4", PrivateDNS: "ip-10-0-0-4.ec2.internal", Role: "TASK", Market: "SPOT", Created: start.Add(-time.Hour), Ended: start.Add(time.Minute),
			StateReason: "Spot Instance was terminated due to not enough capacity in the Spot Instance pool."},
	}}
	r := runWithLogs(l, cl,
		logFile(t, driverErr, `26/09/22 14:12:00 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, ip-10-0-0-4.ec2.internal, 34591, None)
26/09/22 14:12:09 INFO YarnAllocator: Yarn node state updated for host ip-10-0-0-4.ec2.internal to DECOMMISSIONING
26/09/22 14:12:13 WARN TaskSetManager: Lost task 1.0 in stage 0.0 (TID 1) (ip-10-0-0-2.ec2.internal executor 2): java.io.IOException: org.apache.spark.SparkException: [INTERNAL_ERROR_BROADCAST] Failed to get broadcast_0_piece0 of broadcast_0
26/09/22 14:12:14 ERROR TaskSetManager: Task 5 in stage 0.0 failed 4 times; aborting job
26/09/22 14:12:14 INFO ApplicationMaster: Final app status: FAILED, exitCode: 1, (reason: User application exited with status 1)
`),
		logFile(t, "containers/application_1_1/container_1_1_02_000001/stderr", `26/09/22 14:30:00 INFO ApplicationMaster: Final app status: SUCCEEDED, exitCode: 0
`))
	f, ok := rules(r)["app-retried"]
	if !ok {
		t.Fatalf("no app-retried: %v", keys(rules(r)))
	}
	for _, want := range []string{"The first attempt's application master exited with code 1", "Its driver ran on ip-10-0-0-4.ec2.internal.",
		"YARN reported that node DECOMMISSIONING at 14:12:09 UTC", "EMR reports instance i-4 ended at 14:14:20 UTC: Spot Instance was terminated",
		"Its first error, in the driver's stderr in attempt 1: Lost task 1.0 in stage 0.0 (TID 1)", "INTERNAL_ERROR_BROADCAST", "and attempt 2 finished"} {
		if !strings.Contains(f.Explanation, want) {
			t.Errorf("explanation lacks %q: %s", want, f.Explanation)
		}
	}
	if !strings.HasPrefix(f.Fix, "The driver ran on a spot node that was taken back.") {
		t.Errorf("fix = %q", f.Fix)
	}
	for _, e := range f.Evidence {
		if e.Ref != "" {
			t.Errorf("attempt 1's lines must not link to the event log's executors: %+v", e)
		}
	}
	if s := r.Summary.Sentences[0]; !strings.Contains(s, "and finished on attempt 2, after YARN restarted it.") {
		t.Errorf("summary = %q", s)
	}
	for _, lf := range r.Logs.Files {
		if lf.Container == "container_1_1_01_000001" && (lf.Executor != "driver" || lf.EarlierAttempt != 1 || lf.Host != "ip-10-0-0-4.ec2.internal") {
			t.Errorf("attempt 1's driver log = %+v", lf)
		}
	}
	// The spot node ran none of attempt 2's executors, but it took attempt
	// 1's driver, and EMR said it was a spot reclaim.
	spot := rules(r)["spot-interrupted"]
	if spot.Severity != model.Warning || !strings.Contains(spot.Explanation, "It took the driver of attempt 1 with it. Attempt 1 failed without its driver") ||
		!strings.Contains(spot.Explanation, "EMR says why: Spot Instance was terminated due to not enough capacity in the Spot Instance pool.") ||
		strings.Contains(spot.Explanation, "inferred") || len(spot.Evidence) != 3 || !strings.Contains(spot.Evidence[1].Text, "YARN reported the node DECOMMISSIONING at 14:12:09 UTC") {
		t.Errorf("spot = %+v", spot)
	}
	if _, ok := rules(r)["idle-nodes"]; ok {
		t.Error("a node that went away mid-run is not idle")
	}
	// Without the logs, a spot node that ran nothing of this application
	// is only a note.
	quiet := rules(runWithLogs(l, cl))["spot-interrupted"]
	if quiet.Severity != model.Info || !strings.Contains(quiet.Explanation, "lost no work on it") {
		t.Errorf("quiet spot = %+v", quiet)
	}
}

// An event log from attempt 2 with no earlier attempt's logs still says
// the application was restarted.
func TestRetriedAttemptFromEventLogOnly(t *testing.T) {
	l := synthetic(nil, &model.Executor{ID: "1", Host: "h", Cores: 2})
	l.Application.AttemptID = "2"
	f, ok := rules(runWithLogs(l, nil))["app-retried"]
	if !ok || f.Title != "YARN restarted the application: the event log is from attempt 2" || !strings.Contains(f.Explanation, "why they failed is not known") {
		t.Errorf("app-retried = %+v", f)
	}
	l.Application.AttemptID = "1"
	if _, ok := rules(runWithLogs(l, nil))["app-retried"]; ok {
		t.Error("attempt 1 was not retried")
	}
}

// The 5-node test cluster's Ookla job failed in its own code before any
// Spark job ran, on both attempts. Spark still closed the event log
// normally, so only YARN's records say the application failed: it must
// not read as "finished on attempt 2".
func TestFailedAttemptsDespiteCleanEventLog(t *testing.T) {
	l := synthetic(nil, &model.Executor{ID: "1", Host: "ip-10-0-0-2.ec2.internal", Cores: 2})
	l.Application.AttemptID, l.Application.DeployMode = "2", "cluster"
	l.Application.DriverAttributes = map[string]string{"CONTAINER_ID": "container_1_1_02_000001"}
	exit := `26/09/27 09:48:0%d INFO ApplicationMaster: Final app status: FAILED, exitCode: 1, (reason: User application exited with status 1)
`
	r := runWithLogs(l, nil,
		logFile(t, "containers/application_1_1/container_1_1_01_000001/stdout", `Traceback (most recent call last):
  File "/mnt/yarn/p5_speedtest.py", line 18, in <module>
py4j.protocol.Py4JJavaError: An error occurred while calling o228.parquet.
: java.lang.AssertionError: assertion failed: Conflicting directory structures detected. Suspicious paths:
`),
		logFile(t, driverErr, fmt.Sprintf(exit, 1)),
		logFile(t, "containers/application_1_1/container_1_1_02_000001/stderr", fmt.Sprintf(exit, 9)),
		logFile(t, "steps/s-FIXTURESTEP0001/controller", `2026-09-27T09:48:10.000Z WARN Step failed with exitCode 1 and took 54 seconds
`))
	a := r.Application
	if a.Status != model.StatusFailed || !strings.Contains(a.StatusReason, "the application master exited with code 1") {
		t.Errorf("application = %s: %s", a.Status, a.StatusReason)
	}
	got := rules(r)
	for _, rule := range []string{"app-retried", "step-failed"} {
		if f, ok := got[rule]; ok {
			t.Errorf("unexpected %s: %s", rule, f.Title)
		}
	}
	if f := got["log-first-failure"]; !strings.Contains(f.Title, "Conflicting directory structures detected") || !strings.Contains(f.Explanation, "YARN started the application 2 times") {
		t.Errorf("first failure = %+v", f)
	}
	if s := r.Summary.Sentences[0]; !strings.Contains(s, "and failed.") || strings.Contains(s, "finished") {
		t.Errorf("summary = %q", s)
	}

	// YARN's summary alone is enough, as for an attempt whose driver log
	// was lost.
	l.Application.Status, l.Application.StatusReason = model.StatusSucceeded, ""
	l.Application.AttemptID = "1"
	r = runWithLogs(l, nil, logFile(t, rmLog, `2026-09-27 09:48:10,000 INFO org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary (RM Event dispatcher): appId=application_1_1,name=job.py,user=hadoop,queue=default,state=FINISHED,trackingUrl=x,appMasterHost=h,submitTime=1790000000000,startTime=1790000000000,launchTime=1790000001000,finishTime=1790000010000,finalStatus=FAILED,memorySeconds=1,vcoreSeconds=1,applicationType=SPARK,diagnostics=
`))
	if r.Application.Status != model.StatusFailed || r.Application.StatusReason != "Spark's event log ends normally, but YARN recorded the application as failed." {
		t.Errorf("from YARN's summary: %s: %s", r.Application.Status, r.Application.StatusReason)
	}
}
