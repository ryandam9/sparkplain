package analyze

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// Each HBase problem is its own finding, with the table, server or
// ZooKeeper address the client named (HISTORY.md, phase 5 step 2).
func TestHBaseFindings(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, text, rule, title string
		inExpl, inFix           string
	}{
		{"missing table, its retries folded in", `26/09/29 05:18:52 ERROR AsyncProcess: Failed to get region location
org.apache.hadoop.hbase.TableNotFoundException: sp_missing
26/09/29 05:18:53 ERROR BufferedMutatorImpl: flush failed
org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException: Failed 250 actions: sp_missing: 250 times, servers with issues: null
`, "hbase-table-missing", "HBase table sp_missing does not exist (2 errors in the logs)", "namespace", "create '<table>'"},
		{"ZooKeeper on the wrong port", `26/09/29 05:23:54 WARN ReadOnlyZKClient: 0x38268de7 to zk.example.internal:2182 failed for get of /hbase/hbaseid, code = CONNECTIONLOSS, retries = 2, give up
`, "hbase-zookeeper", "The HBase client could not reach ZooKeeper at zk.example.internal:2182 (1 error in the logs)", "This client used port 2182", "hbase.zookeeper.property.clientPort"},
		{"scanner leases", `26/09/29 05:58:44 INFO ZooKeeper: closed
org.apache.hadoop.hbase.UnknownScannerException: org.apache.hadoop.hbase.UnknownScannerException: Unknown scanner '-1'. This can happen due to any of the following reasons: b) Scanner lease expired because of long wait between consecutive client checkins
`, "hbase-scanner-expired", "HBase scanner leases expired (1 time in the logs)", "60 s by default", "hbase.mapreduce.scan.cachedrows"},
		{"memstore full", `26/09/29 06:08:04 INFO AsyncRequestFutureImpl: id=10, table=sp_hot, attempt=6/16, failureCount=2048ops, last exception=org.apache.hadoop.hbase.RegionTooBusyException: org.apache.hadoop.hbase.RegionTooBusyException: Over memstore limit=2.0 M, regionName=bd53, server=rs1.example.internal,16020,1790655886216
26/09/29 06:08:05 INFO AsyncRequestFutureImpl: id=10, table=sp_hot, attempt=3/16, failureCount=12ops, last exception=org.apache.hadoop.hbase.RegionTooBusyException: org.apache.hadoop.hbase.RegionTooBusyException: Over memstore limit=2.0 M, regionName=bd53, server=rs1.example.internal,16020,1790655886216
`, "hbase-busy", "HBase refused writes to sp_hot for a short time (2 times in the logs)", "The region server on rs1.example.internal refused writes because the memstore of the region was more than its limit (2.0 M). The memstore is the memory that holds writes until HBase flushes them to disk. The client waited and tried again, up to attempt 6 of 16", "Pre-split"},
		{"call queue full", `24/01/01 10:00:00 WARN ScannerCallable: scan failed
org.apache.hadoop.hbase.CallQueueTooBigException: Call queue is full on rs1.example.internal,16020,1700000000000, too many items queued ?
`, "hbase-busy", "HBase refused writes for a short time (1 time in the logs)", "its queue of requests was full", "fewer tasks"},
		{"regions moving", `24/01/01 10:00:00 WARN RpcRetryingCallerImpl: Call exception
org.apache.hadoop.hbase.NotServingRegionException: sp_orders,2,1700000000000.c5f9. is not online on rs1.example.internal,16020,1700000000000
`, "hbase-region-moved", "HBase regions were moving or opening during the run (1 error in the logs)", "HBase moved, split or opened the region again", "balancer"},
		{"region server timed out", `24/01/01 10:00:00 ERROR Executor: Exception in task 0.0 in stage 1.0 (TID 3)
org.apache.hadoop.hbase.ipc.CallTimeoutException: Call to address=rs1.example.internal/10.0.0.6:16020 failed on local exception: org.apache.hadoop.hbase.ipc.CallTimeoutException: Call[id=5,methodName=Scan], waitTime=60001ms, rpcTimeout=60000ms
24/01/01 10:00:01 ERROR AsyncProcess: gave up
org.apache.hadoop.hbase.ipc.CallTimeoutException: Call to address=rs1.example.internal/10.0.0.6:16020 failed
`, "hbase-server", "HBase region servers did not answer (1 error in the logs)", "timed out", "hbase.rpc.timeout"},
		{"retries alone", `24/01/01 10:00:04 ERROR AsyncProcess: region lookup failed
org.apache.hadoop.hbase.client.RetriesExhaustedException: Failed after attempts=16
`, "hbase-retries", "HBase calls gave up after all their retries (1 error in the logs)", "hbase.client.retries.number", "first HBase error"},
		{"an HBase error the report does not know", `24/01/01 10:00:04 ERROR HTable: odd
org.apache.hadoop.hbase.DoNotRetryIOException: something new
`, "hbase-error", "HBase calls failed (1 error in the logs)", "does not know", "first error"},
		{"HBase refuses the user", `24/01/01 10:00:00 ERROR Executor: write failed
org.apache.hadoop.hbase.security.AccessDeniedException: org.apache.hadoop.hbase.security.AccessDeniedException: Insufficient permissions for user 'etl' (table=sp_orders, action=WRITE)
`, "hbase-access-denied", "HBase refused access on sp_orders (1 time)", "not in AWS", "grant '<user>'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := runWithLogs(nil, nil, logFile(t, exec2Err, tc.text))
			got := rules(r)
			f, ok := got[tc.rule]
			if !ok {
				t.Fatalf("no %s finding: %v", tc.rule, got)
			}
			if f.Title != tc.title || !strings.Contains(f.Explanation, tc.inExpl) || !strings.Contains(f.Fix, tc.inFix) {
				t.Errorf("finding = %q\n%s\nFix: %s", f.Title, f.Explanation, f.Fix)
			}
			if len(f.Evidence) == 0 || f.Evidence[0].Source.File != exec2Err {
				t.Errorf("evidence = %+v", f.Evidence)
			}
			for rule := range got {
				if strings.HasPrefix(rule, "hbase-") && rule != tc.rule {
					t.Errorf("also %s: %s", rule, got[rule].Title)
				}
			}
			if _, ok := got["access-denied"]; ok {
				t.Error("an HBase refusal is not an AWS one")
			}
		})
	}
}

// A class missing at run time names the jar that needed it and where EMR
// keeps the one that provides it; a failed run with no critical line still
// gets its first error.
func TestClasspathFinding(t *testing.T) {
	t.Parallel()
	r := runWithLogs(nil, nil, logFile(t, driverErr, `26/09/29 05:04:53 WARN TaskSetManager: Lost task 0.0 in stage 0.0 (TID 0) (ip-10-0-2-12.example.internal executor 1): java.lang.NoClassDefFoundError: com/google/protobuf/RpcChannel
	at java.lang.ClassLoader.defineClass1(Native Method) ~[?:?]
	at org.apache.hadoop.hbase.client.BufferedMutatorImpl.<init>(BufferedMutatorImpl.java:103) ~[hbase-client-2.4.17-amzn-7.jar:2.4.17-amzn-7]
26/09/29 05:04:54 WARN TaskSetManager: Lost task 1.0 in stage 0.0 (TID 1) (ip-10-0-2-12.example.internal executor 1): java.lang.NoClassDefFoundError: org/slf4j/impl/StaticLoggerBinder
	at org.apache.hadoop.hbase.spark.Logging.initializeLogging(Logging.scala:109) ~[hbase-spark-1.0.1.jar:1.0.1]
26/09/29 05:04:56 INFO ApplicationMaster: Final app status: FAILED, exitCode: 15, (reason: User class threw exception: org.apache.spark.SparkException: Job aborted.)
`))
	got := rules(r)
	f := got["classpath-clash"]
	if f.Title != "2 classes were missing at run time (com.google.protobuf.RpcChannel, org.slf4j.impl.StaticLoggerBinder)" ||
		!strings.Contains(f.Explanation, "hbase-client-2.4.17-amzn-7.jar needed com.google.protobuf.RpcChannel, but no jar on the classpath had it (NoClassDefFoundError, 1 line in the logs)") ||
		!strings.Contains(f.Explanation, "hbase-spark-1.0.1.jar needed org.slf4j.impl.StaticLoggerBinder") ||
		!strings.Contains(f.Fix, "/usr/lib/hadoop/lib/protobuf-java-2.5.0.jar") || !strings.Contains(f.Fix, "SLF4J 1.7 binding") {
		t.Errorf("classpath = %q\n%s\nFix: %s", f.Title, f.Explanation, f.Fix)
	}
	if ff := got["log-first-failure"]; !strings.Contains(ff.Title, "RpcChannel") || !strings.Contains(ff.Fix, "missing-class finding") {
		t.Errorf("first failure = %q, fix %q", ff.Title, ff.Fix)
	}

	// Only a warning: a failed run still names its earliest error.
	r = runWithLogs(nil, nil, logFile(t, driverErr, `24/01/01 10:00:00 WARN TaskSetManager: Lost task 0.0 in stage 0.0 (TID 0) (h executor 1): java.lang.IllegalStateException: boom
24/01/01 10:00:05 INFO ApplicationMaster: Final app status: FAILED, exitCode: 15, (reason: User class threw exception: org.apache.spark.SparkException: Job aborted.)
`))
	if ff := rules(r)["log-first-failure"]; !strings.Contains(ff.Title, "IllegalStateException: boom") {
		t.Errorf("first failure from a warning = %q", ff.Title)
	}
}

// The HBase section joins the logs of every process: tables and how they
// were used, regions read per region server, ZooKeeper connections per
// process, and library versions from the jars YARN localized when there is
// no event log. A run that never touched HBase has no section.
func TestHBaseSection(t *testing.T) {
	t.Parallel()
	zk := "26/09/29 05:14:00 INFO ZooKeeper: Initiating client connection, connectString=zk.example.internal:2181 sessionTimeout=90000 watcher=x@%d\n"
	exec := fmt.Sprintf(zk, 1) + fmt.Sprintf(zk, 2) + fmt.Sprintf(zk, 3) + `26/09/29 05:14:00 INFO NewHadoopRDD: Input split: Split(tablename=sp_orders, startrow=4, endrow=6, regionLocation=rs2.example.internal, regionname=45)
26/09/29 05:14:05 INFO NewHadoopRDD: Input split: Split(tablename=sp_orders, startrow=6, endrow=8, regionLocation=rs2.example.internal, regionname=52)
26/09/29 05:14:08 INFO NewHadoopRDD: Input split: Split(tablename=sp_orders, startrow=8, endrow=, regionLocation=rs1.example.internal, regionname=a4)
26/09/29 05:14:10 INFO TableOutputFormat: Created table instance for sp_totals
`
	drv := fmt.Sprintf(zk, 4) + `26/09/29 05:13:50 INFO ApplicationMaster: resources:
    hbase-client-2.4.17-amzn-7.jar -> resource { scheme: "hdfs" }
    hbase-spark-1.0.1.jar -> resource { scheme: "hdfs" }
`
	r := runWithLogs(nil, nil, logFile(t, driverErr, drv), logFile(t, exec2Err, exec))
	h := r.HBase
	if h == nil {
		t.Fatal("no HBase section")
	}
	if h.Quorum != "zk.example.internal:2181" || h.Sessions != 4 || h.MostSessions != 3 || h.MostSessionsBy != "container container_1_1_01_000002" {
		t.Errorf("zookeeper = %q %d, most %d by %q", h.Quorum, h.Sessions, h.MostSessions, h.MostSessionsBy)
	}
	var libs []string
	for _, l := range h.Libraries {
		libs = append(libs, l.Name+" "+l.Version)
	}
	if strings.Join(libs, ", ") != "HBase client 2.4.17-amzn-7, HBase Spark connector 1.0.1" {
		t.Errorf("libraries = %v", libs)
	}
	var tables []string
	for _, x := range h.Tables {
		tables = append(tables, fmt.Sprintf("%s r=%v w=%v %v %v", x.Name, x.Read, x.Written, x.APIs, x.Regions))
	}
	want := "sp_orders r=true w=false [TableInputFormat] [{rs2.example.internal 2} {rs1.example.internal 1}]; sp_totals r=false w=true [TableOutputFormat] []"
	if strings.Join(tables, "; ") != want {
		t.Errorf("tables = %s\nwant %s", strings.Join(tables, "; "), want)
	}
	if len(h.Missing) != 1 || !strings.Contains(h.Missing[0], "need the event log") {
		t.Errorf("missing = %v", h.Missing)
	}

	if r := runWithLogs(nil, nil, logFile(t, driverErr, "24/01/01 10:00:00 INFO SparkContext: Running Spark version 3.5.1\n")); r.HBase != nil {
		t.Errorf("a run without HBase has %+v", r.HBase)
	}
}

// A TableInputFormat scan that took most of the run: the stage is found
// from its RDD and the executors' split lines, the scanner lease that
// expired while it ran is tied to it, and the scan's regions give the
// hotspot and locality findings. The executor opened a connection per task.
func TestHBaseSlowFindings(t *testing.T) {
	t.Parallel()
	start := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	stage := &model.Stage{ID: 0, Name: "count at Scan.java:10", NumTasks: 4, JobIDs: []int{0}, Submitted: start.Add(30 * time.Second), Completed: start.Add(330 * time.Second),
		TaskType: "ResultTask", RDDs: []model.StageRDD{{ID: 0, Name: "NewHadoopRDD", Callsite: "newAPIHadoopRDD at Scan.java:8"}},
		Totals:       model.TaskTotals{Tasks: 4, Succeeded: 4, RunTimeMs: 600_000, CPUTimeNs: 30_000 * 1e6},
		TaskDuration: model.Dist{Count: 4, P50: 60_000}, Slowest: &model.TaskRef{TaskID: 3, ExecutorID: "1", Host: "rs1.example.internal", DurationMs: 300_000},
		Source: model.Source{File: "eventlog", Line: 9}}
	l := &model.EventLog{
		Application: model.Application{ID: "application_1_1", Start: start, End: start.Add(6 * time.Minute), DurationMs: 360_000, Status: model.StatusSucceeded},
		Jobs:        []*model.Job{{ID: 0, Description: "scan events", StageIDs: []int{0}}},
		Stages:      []*model.Stage{stage},
	}
	var exec strings.Builder
	for i := range 60 {
		fmt.Fprintf(&exec, "24/01/01 10:00:31 INFO ZooKeeper: Initiating client connection, connectString=zk.example.internal:2181 sessionTimeout=90000 watcher=x@%d\n", i)
	}
	for _, rs := range []string{"rs2", "rs2", "rs2", "rs1"} {
		fmt.Fprintf(&exec, "24/01/01 10:00:32 INFO NewHadoopRDD: Input split: Split(tablename=events, startrow=, endrow=, regionLocation=%s.example.internal, regionname=ab)\n", rs)
	}
	exec.WriteString(`24/01/01 10:03:00 INFO ZooKeeper: closed
org.apache.hadoop.hbase.UnknownScannerException: org.apache.hadoop.hbase.UnknownScannerException: Unknown scanner '-1'. b) Scanner lease expired because of long wait between consecutive client checkins
`)
	f := logFile(t, exec2Err, exec.String())
	f.Host = "rs1.example.internal"
	r := runWithLogs(l, nil, f)
	h := r.HBase
	if h == nil || len(h.Stages) != 1 || h.Stages[0].API != "TableInputFormat" || h.Stages[0].Reads[0] != "events" || h.Stages[0].Retries["scanner"] != 1 {
		t.Fatalf("hbase = %+v", h)
	}
	if h.TimeMs != 300_000 || h.LocalRegions != 1 || h.RemoteRegions != 3 {
		t.Errorf("time %d, local %d, remote %d", h.TimeMs, h.LocalRegions, h.RemoteRegions)
	}
	got := rules(r)
	for rule, want := range map[string][]string{
		"hbase-time": {"Stages reading or writing HBase took 5 min 0 s of the 6 min 0 s run (83%)", "Its tasks used the CPU for 5.0% of their run time. As a result, they waited for most of the time",
			"1 expired scanner lease", "Its slowest task took 5 min 0 s on rs1.example.internal, compared with a median of 1 min 0 s"},
		"hbase-scanner-expired": {"They occurred while stage 0 (scan events) ran."},
		"hbase-zk-connections":  {"Container container_1_1_01_000002 opened 60 ZooKeeper connections to reach HBase", "has no cache"},
		"hbase-hotspot":         {"Region server rs2.example.internal held 3 of the 4 regions of events that the run read", "75%"},
		"hbase-remote-regions":  {"3 of 4 HBase regions were read from another node", "spark.locality.wait"},
	} {
		f, ok := got[rule]
		if !ok {
			t.Errorf("no %s", rule)
			continue
		}
		text := f.Title + " " + f.Explanation + " " + f.Fix
		for _, w := range want {
			if !strings.Contains(text, w) {
				t.Errorf("%s: %q lacks %q", rule, text, w)
			}
		}
	}
	if got["hbase-time"].Severity != model.Warning {
		t.Errorf("hbase-time severity %s", got["hbase-time"].Severity)
	}
	// The diagram pins these to the executor and the node they are about.
	if ev := got["hbase-remote-regions"].Evidence; len(ev) != 1 || ev[0].Text != "container container_1_1_01_000002 on rs1.example.internal read 3 of its 4 regions from another node" {
		t.Errorf("remote-regions evidence = %+v", ev)
	}
	if ev := got["hbase-hotspot"].Evidence; ev[len(ev)-1].Ref != model.NodeRef("rs2.example.internal") {
		t.Errorf("hotspot evidence = %+v", ev)
	}

	// A short run, a shared connection and an even spread: none of them.
	l.Application.DurationMs, stage.Completed = 50_000, start.Add(40*time.Second)
	r = runWithLogs(l, nil, logFile(t, exec2Err, `24/01/01 10:00:31 INFO ZooKeeper: Initiating client connection, connectString=zk.example.internal:2181 sessionTimeout=90000 watcher=x@1
24/01/01 10:00:32 INFO NewHadoopRDD: Input split: Split(tablename=events, startrow=, endrow=, regionLocation=rs1.example.internal, regionname=ab)
24/01/01 10:00:32 INFO NewHadoopRDD: Input split: Split(tablename=events, startrow=, endrow=, regionLocation=rs2.example.internal, regionname=ab)
`))
	for _, rule := range []string{"hbase-time", "hbase-zk-connections", "hbase-hotspot", "hbase-remote-regions"} {
		if f, ok := rules(r)[rule]; ok {
			t.Errorf("unexpected %s: %s", rule, f.Title)
		}
	}
}

// HBase's own logs are the cluster's, so only events that name the run's
// tables, a scan from its executors or a whole region server are its own;
// they give findings the client's logs cannot, and the server's side of
// the client's busy and scanner findings.
func TestHBaseServerFindings(t *testing.T) {
	t.Parallel()
	start := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	stage := &model.Stage{ID: 0, Name: "scan", NumTasks: 2, JobIDs: []int{0}, Submitted: start.Add(10 * time.Second), Completed: start.Add(290 * time.Second),
		TaskType: "ResultTask", RDDs: []model.StageRDD{{ID: 0, Name: "NewHadoopRDD", Callsite: "newAPIHadoopRDD at Scan.java:8"}},
		Totals: model.TaskTotals{Tasks: 2, Succeeded: 2, RunTimeMs: 200_000, CPUTimeNs: 150_000 * 1e6}, Source: model.Source{File: "eventlog", Line: 9}}
	l := &model.EventLog{
		Application: model.Application{ID: "application_1_1", Start: start, End: start.Add(5 * time.Minute), DurationMs: 300_000, Status: model.StatusSucceeded},
		Jobs:        []*model.Job{{ID: 0, Description: "scan events", StageIDs: []int{0}}},
		Stages:      []*model.Stage{stage},
	}
	exec := logFile(t, exec2Err, `24/01/01 10:00:20 INFO NewHadoopRDD: Input split: Split(tablename=events, startrow=, endrow=, regionLocation=ip-10-0-0-6.example.internal, regionname=ab)
24/01/01 10:01:00 INFO ZooKeeper: closed
org.apache.hadoop.hbase.UnknownScannerException: org.apache.hadoop.hbase.UnknownScannerException: Unknown scanner '-1'. b) Scanner lease expired because of long wait between consecutive client checkins
24/01/01 10:02:00 INFO AsyncRequestFutureImpl: id=1, table=events, attempt=3/16, failureCount=5ops, last exception=org.apache.hadoop.hbase.RegionTooBusyException: org.apache.hadoop.hbase.RegionTooBusyException: Over memstore limit=2.0 M, regionName=ab, server=ip-10-0-0-6.example.internal,16020,1
`)
	exec.Host = "ip-10-0-0-5.example.internal"
	master := logFile(t, "node/i-0fee0000000000001/applications/hbase/hbase-hbase-master-ip-10-0-0-1.example.internal.log.gz", `2024-01-01 10:01:10,000 INFO  [PEWorker-4] procedure2.ProcedureExecutor: Finished pid=30, state=SUCCESS; TransitRegionStateProcedure table=events, region=554f, REOPEN/MOVE in 793 msec
2024-01-01 10:01:11,000 INFO  [PEWorker-4] procedure2.ProcedureExecutor: Finished pid=31, state=SUCCESS; TransitRegionStateProcedure table=events, region=17c3, REOPEN/MOVE in 12 msec
2024-01-01 10:01:12,000 INFO  [PEWorker-12] procedure2.ProcedureExecutor: Finished pid=79, state=SUCCESS; SplitTableRegionProcedure table=other, parent=4562, daughterA=badc, daughterB=5fcf in 920 msec
2024-01-01 10:03:00,000 INFO  [RegionServerTracker-0] assignment.AssignmentManager: Scheduled ServerCrashProcedure pid=335 for ip-10-0-0-7.example.internal,16020,1 (carryingMeta=false) ip-10-0-0-7.example.internal,16020,1/CRASHED/regionCount=3/lock=x
`)
	rs := logFile(t, "node/i-0fee0000000000002/applications/hbase/hbase-hbase-regionserver-ip-10-0-0-6.example.internal.log.gz", `2024-01-01 10:01:05,000 INFO  [leaseChecker] regionserver.RSRpcServices: Scanner lease 1 expired clientIPAndPort=10.0.0.5:47002, userName=hadoop, regionInfo=events,,1.ab.
2024-01-01 10:01:06,000 INFO  [leaseChecker] regionserver.RSRpcServices: Scanner lease 2 expired clientIPAndPort=10.0.0.9:47002, userName=hadoop, regionInfo=events,,1.ab.
2024-01-01 10:02:00,000 WARN  [handler=27] regionserver.HRegion: Region is too busy due to exceeding memstore size limit.
2024-01-01 10:02:01,000 WARN  [handler=28] regionserver.HRegion: Region is too busy due to exceeding memstore size limit.
2024-01-01 10:02:02,000 INFO  [MemStoreFlusher.0] regionserver.HRegion: Flushing 1595e783b53d99cd5eef43b6debb2682 1/1 column families, dataSize=1.16 MB heapSize=1.47 MB
2024-01-01 10:02:30,000 WARN  [JvmPauseMonitor] util.JvmPauseMonitor: Detected pause in JVM or host machine (eg GC): pause of approximately 12345ms
2024-01-01 10:02:40,000 WARN  [handler=3] ipc.RpcServer: (responseTooSlow): {"method":"Scan","param":"region= events,,1.ab., scanner_id= 1","processingtimems":14002,"client":"10.0.0.5:47002"}
`)
	r := runWithLogs(l, nil, exec, master, rs)
	got := rules(r)
	for rule, want := range map[string][]string{
		"hbase-server-lost":     {"Region server ip-10-0-0-7.example.internal stopped while the run used HBase", "moved its 3 regions to other region servers", "This occurred while stage 0 (scan events) ran."},
		"hbase-regions-changed": {"HBase moved 2 regions of events while the run used it", "closes for a short time"},
		"hbase-server-pause":    {"Region server ip-10-0-0-6.example.internal paused for 12 s", "garbage collection"},
		"hbase-slow-calls":      {"Region servers logged 1 call to the tables of the run as too slow or too large", "hbase.ipc.warn.response.time"},
		"hbase-scanner-expired": {"The logs of the region servers show 1 expired lease on events from the executors of this run."},
		"hbase-busy":            {"The log of the region server shows 2 refusals while the run wrote. It also shows 1 memstore flush. The server flushed as fast as it could."},
	} {
		f, ok := got[rule]
		if !ok {
			t.Errorf("no %s", rule)
			continue
		}
		text := f.Title + " " + f.Explanation
		for _, w := range want {
			if !strings.Contains(text, w) {
				t.Errorf("%s: %q lacks %q", rule, text, w)
			}
		}
	}
	for rule, host := range map[string]string{"hbase-server-lost": "ip-10-0-0-7.example.internal", "hbase-server-pause": "ip-10-0-0-6.example.internal", "hbase-busy": "ip-10-0-0-6.example.internal"} {
		ev := got[rule].Evidence
		if !slices.ContainsFunc(ev, func(e model.Evidence) bool { return e.Ref == model.NodeRef(host) }) {
			t.Errorf("%s evidence should refer to node %s: %+v", rule, host, ev)
		}
	}
	mine := map[string]bool{}
	for _, e := range r.HBase.ServerEvents {
		mine[e.Event+" "+e.Table+" "+e.Detail] = e.Mine
	}
	for k, want := range map[string]bool{"split other ": false, "moved events ": true, "scanner events client 10.0.0.5": true, "scanner events client 10.0.0.9": false, "flush  ": false} {
		if v, ok := mine[k]; !ok || v != want {
			t.Errorf("%q mine = %v (present %v), want %v; all %v", k, v, ok, want, mine)
		}
	}
	if s := r.HBase.Stages[0].ServerEvents; s["moved"] != 2 || s["server-lost"] != 1 || s["pause"] != 1 {
		t.Errorf("stage server events = %v", s)
	}
}
