package yarnlog

import (
	"testing"
)

// HBase client errors are told apart by what fixes them (SPEC §8, phase 5
// step 2). The first five are lines from the phase 5 test cluster (HBase
// 2.4.17, scrubbed); the rest use HBase 2.4's own exception messages, which
// the test cluster's client never logged.
func TestHBaseProblems(t *testing.T) {
	t.Parallel()
	const drv = "containers/application_1700000000000_0001/container_1700000000000_0001_01_000001/stderr"
	const exe = "containers/application_1700000000000_0001/container_1700000000000_0001_01_000002/stderr"
	for _, tc := range []struct {
		name, file, text, want string
		fields                 map[string]string
	}{
		{"missing table", exe, `26/09/29 05:18:52 ERROR AsyncProcess: Failed to get region location
org.apache.hadoop.hbase.TableNotFoundException: sp_missing
	at org.apache.hadoop.hbase.client.ConnectionImplementation.locateRegionInMeta(ConnectionImplementation.java:989) ~[hbase-client-2.4.17-amzn-7.jar:2.4.17-amzn-7]
`, "hbase/critical", map[string]string{"hbase": "table-missing", "table": "sp_missing"}},
		{"retries on a missing table", exe, `26/09/29 05:18:53 ERROR Executor: Exception in task 1.0 in stage 0.0 (TID 1)
org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException: Failed 250 actions: sp_missing: 250 times, servers with issues: null
	at org.apache.hadoop.hbase.client.BufferedMutatorImpl.makeException(BufferedMutatorImpl.java:328)
`, "task-error/critical", map[string]string{"hbase": "retries", "table": "sp_missing", "cause": "HBase"}},
		{"ZooKeeper port nothing listens on", drv, `26/09/29 05:23:54 WARN ReadOnlyZKClient: 0x38268de7 to ip-10-0-2-11.us-east-1.compute.internal:2182 failed for get of /hbase/hbaseid, code = CONNECTIONLOSS, retries = 2, give up
`, "hbase/critical", map[string]string{"hbase": "zookeeper", "zookeeper": "ip-10-0-2-11.us-east-1.compute.internal:2182"}},
		{"scanner lease expired, retried", exe, `26/09/29 05:58:44 INFO ZooKeeper: Session: 0x100000283420126 closed
org.apache.hadoop.hbase.UnknownScannerException: org.apache.hadoop.hbase.UnknownScannerException: Unknown scanner '-2213009885270900724'. This can happen due to any of the following reasons: a) Scanner id given is wrong, b) Scanner lease expired because of long wait between consecutive client checkins, c) Server may be closing down, d) RegionServer restart during upgrade.
	at org.apache.hadoop.hbase.regionserver.RSRpcServices.getRegionScanner(RSRpcServices.java:3104)
`, "hbase/warning", map[string]string{"hbase": "scanner"}},
		{"region over its memstore limit, retried at INFO", exe, `26/09/29 06:08:04 INFO AsyncRequestFutureImpl: id=10, table=sp_hot, attempt=6/16, failureCount=2048ops, last exception=org.apache.hadoop.hbase.RegionTooBusyException: org.apache.hadoop.hbase.RegionTooBusyException: Over memstore limit=2.0 M, regionName=bd53c140afadb8316d2fce84ef3f701e, server=ip-10-0-2-12.us-east-1.compute.internal,16020,1790655886216
	at org.apache.hadoop.hbase.regionserver.HRegion.checkResources(HRegion.java:5104)
26/09/29 06:08:06 INFO AsyncRequestFutureImpl: id=10, table=sp_hot, attempt=7/16, succeeded on ip-10-0-2-12.us-east-1.compute.internal,16020,1790655886216, tracking started Tue Sep 29 06:08:02 UTC 2026
`, "hbase/warning", map[string]string{"hbase": "busy", "table": "sp_hot", "server": "ip-10-0-2-12.us-east-1.compute.internal", "limit": "2.0 M", "attempt": "6", "attempts": "16"}},
		{"call queue full", exe, `24/01/01 10:00:00 WARN ScannerCallable: scan failed
org.apache.hadoop.hbase.CallQueueTooBigException: Call queue is full on ip-10-0-0-6.example.internal,16020,1700000000000, too many items queued ?
`, "hbase/warning", map[string]string{"hbase": "busy"}},
		{"region moved", exe, `24/01/01 10:00:00 WARN RpcRetryingCallerImpl: Call exception
org.apache.hadoop.hbase.exceptions.RegionMovedException: Region moved to: hostname=ip-10-0-0-7.example.internal port=16020 startCode=1700000000000. As of locationSeqNum=12.
`, "hbase/warning", map[string]string{"hbase": "moved"}},
		{"region not served", exe, `24/01/01 10:00:00 WARN RpcRetryingCallerImpl: Call exception
org.apache.hadoop.hbase.NotServingRegionException: sp_orders,2,1700000000000.c5f9987a58264d61b8312c955f8a2914. is not online on ip-10-0-0-6.example.internal,16020,1700000000000
`, "hbase/warning", map[string]string{"hbase": "moved"}},
		{"region server timed out", exe, `24/01/01 10:00:00 ERROR Executor: Exception in task 0.0 in stage 1.0 (TID 3)
org.apache.hadoop.hbase.ipc.CallTimeoutException: Call to address=ip-10-0-0-6.example.internal/10.0.0.6:16020 failed on local exception: org.apache.hadoop.hbase.ipc.CallTimeoutException: Call[id=5,methodName=Scan], waitTime=60001ms, rpcTimeout=60000ms
`, "task-error/critical", map[string]string{"hbase": "server", "cause": "HBase"}},
		{"HBase refuses the user, not AWS", drv, `24/01/01 10:00:00 ERROR ApplicationMaster: User class threw exception:
org.apache.hadoop.hbase.security.AccessDeniedException: org.apache.hadoop.hbase.security.AccessDeniedException: Insufficient permissions for user 'etl' (table=sp_orders, action=WRITE)
`, "access-denied/critical", map[string]string{"service": "HBase", "table": "sp_orders"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := classifyText(t, tc.file, tc.text, Options{AppID: "application_1700000000000_0001"})
			if got := kinds(res); got != tc.want {
				t.Fatalf("kinds = %q, want %q\n%+v", got, tc.want, res.Lines)
			}
			l := res.Lines[len(res.Lines)-1]
			for k, v := range tc.fields {
				if l.Fields[k] != v {
					t.Errorf("%s = %q, want %q (fields %v)", k, l.Fields[k], v, l.Fields)
				}
			}
		})
	}
}

// A class missing at run time names the class and, from the first stack
// frame with a jar, the code that needed it. Lines from the phase 5 test
// cluster, whose hbase-spark connector needed SLF4J 1 and whose HBase
// client needed protobuf 2.5.
func TestClasspath(t *testing.T) {
	t.Parallel()
	const drv = "containers/application_1700000000000_0001/container_1700000000000_0001_01_000001/stderr"
	for _, tc := range []struct {
		name, text, want, class, err, neededBy string
	}{
		{"SLF4J 1 binding", `26/09/29 04:58:04 ERROR ApplicationMaster: User class threw exception:
java.lang.NoClassDefFoundError: org/slf4j/impl/StaticLoggerBinder
	at org.apache.hadoop.hbase.spark.Logging.initializeLogging(Logging.scala:109) ~[hbase-spark-1.0.1.jar:1.0.1]
Caused by: java.lang.ClassNotFoundException: org.slf4j.impl.StaticLoggerBinder
	at java.net.URLClassLoader.findClass(URLClassLoader.java:445) ~[?:?]
`, "classpath/critical", "org.slf4j.impl.StaticLoggerBinder", "NoClassDefFoundError", "hbase-spark-1.0.1.jar"},
		{"protobuf 2.5, lost task", `26/09/29 05:04:53 WARN TaskSetManager: Lost task 0.0 in stage 0.0 (TID 0) (ip-10-0-2-12.us-east-1.compute.internal executor 1): java.lang.NoClassDefFoundError: com/google/protobuf/RpcChannel
	at java.lang.ClassLoader.defineClass1(Native Method) ~[?:?]
	at org.apache.hadoop.hbase.client.BufferedMutatorImpl.<init>(BufferedMutatorImpl.java:103) ~[hbase-client-2.4.17-amzn-7.jar:2.4.17-amzn-7]
`, "task-error/critical", "com.google.protobuf.RpcChannel", "NoClassDefFoundError", "hbase-client-2.4.17-amzn-7.jar"},
		{"another version", `24/01/01 10:00:00 ERROR Executor: Exception in task 0.0 in stage 0.0 (TID 0)
java.lang.NoSuchMethodError: 'void com.google.common.base.Preconditions.checkArgument(boolean, java.lang.String, java.lang.Object)'
	at org.example.Job.run(Job.java:1) ~[app.jar:?]
`, "task-error/critical", "'void", "NoSuchMethodError", "app.jar"},
		{"a library probing logs a warning", `24/01/01 10:00:00 WARN Utils: optional class not found: java.lang.ClassNotFoundException: org.example.Optional
`, "classpath/warning", "org.example.Optional", "ClassNotFoundException", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := classifyText(t, drv, tc.text, Options{AppID: "application_1700000000000_0001"})
			if got := kinds(res); got != tc.want {
				t.Fatalf("kinds = %q, want %q\n%+v", got, tc.want, res.Lines)
			}
			f := res.Lines[0].Fields
			if f["class"] != tc.class || f["classError"] != tc.err || f["neededBy"] != tc.neededBy {
				t.Errorf("fields = %v", f)
			}
		})
	}
}
