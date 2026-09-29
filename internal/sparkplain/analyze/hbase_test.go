package analyze

import (
	"strings"
	"testing"
)

// Each HBase problem is its own finding, with the table, server or
// ZooKeeper address the client named (SPEC §8, phase 5 step 2).
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
`, "hbase-zookeeper", "HBase's ZooKeeper could not be reached at zk.example.internal:2182 (1 error in the logs)", "this client dialled port 2182", "hbase.zookeeper.property.clientPort"},
		{"scanner leases", `26/09/29 05:58:44 INFO ZooKeeper: closed
org.apache.hadoop.hbase.UnknownScannerException: org.apache.hadoop.hbase.UnknownScannerException: Unknown scanner '-1'. This can happen due to any of the following reasons: b) Scanner lease expired because of long wait between consecutive client checkins
`, "hbase-scanner-expired", "HBase scanner leases expired (1 time in the logs)", "60 s unless changed", "hbase.mapreduce.scan.cachedrows"},
		{"memstore full", `26/09/29 06:08:04 INFO AsyncRequestFutureImpl: id=10, table=sp_hot, attempt=6/16, failureCount=2048ops, last exception=org.apache.hadoop.hbase.RegionTooBusyException: org.apache.hadoop.hbase.RegionTooBusyException: Over memstore limit=2.0 M, regionName=bd53, server=rs1.example.internal,16020,1790655886216
26/09/29 06:08:05 INFO AsyncRequestFutureImpl: id=10, table=sp_hot, attempt=3/16, failureCount=12ops, last exception=org.apache.hadoop.hbase.RegionTooBusyException: org.apache.hadoop.hbase.RegionTooBusyException: Over memstore limit=2.0 M, regionName=bd53, server=rs1.example.internal,16020,1790655886216
`, "hbase-busy", "HBase pushed back on writes to sp_hot (2 times in the logs)", "The region server on rs1.example.internal refused writes because the region's memstore, the memory that holds writes until they are flushed to disk, was over its limit (2.0 M). The client waited and retried, up to attempt 6 of 16", "pre-split"},
		{"call queue full", `24/01/01 10:00:00 WARN ScannerCallable: scan failed
org.apache.hadoop.hbase.CallQueueTooBigException: Call queue is full on rs1.example.internal,16020,1700000000000, too many items queued ?
`, "hbase-busy", "HBase pushed back on writes (1 time in the logs)", "its queue of waiting requests was full", "Fewer tasks"},
		{"regions moving", `24/01/01 10:00:00 WARN RpcRetryingCallerImpl: Call exception
org.apache.hadoop.hbase.NotServingRegionException: sp_orders,2,1700000000000.c5f9. is not online on rs1.example.internal,16020,1700000000000
`, "hbase-region-moved", "HBase regions were moving or opening during the run (1 error in the logs)", "moving, splitting or reopening", "balancer"},
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
`, "hbase-error", "HBase calls failed (1 error in the logs)", "does not recognise", "first error"},
		{"HBase refuses the user", `24/01/01 10:00:00 ERROR Executor: write failed
org.apache.hadoop.hbase.security.AccessDeniedException: org.apache.hadoop.hbase.security.AccessDeniedException: Insufficient permissions for user 'etl' (table=sp_orders, action=WRITE)
`, "hbase-access-denied", "HBase refused access on sp_orders (1 time)", "not an AWS one", "grant '<user>'"},
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
	if f.Title != "2 classes were missing at run time: com.google.protobuf.RpcChannel, org.slf4j.impl.StaticLoggerBinder" ||
		!strings.Contains(f.Explanation, "hbase-client-2.4.17-amzn-7.jar needed com.google.protobuf.RpcChannel, but no jar on the classpath provided it (NoClassDefFoundError; 1 line in the logs)") ||
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
