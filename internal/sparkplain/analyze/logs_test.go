package analyze

import (
	"context"
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
