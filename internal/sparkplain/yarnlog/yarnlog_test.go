package yarnlog

import (
	"compress/gzip"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

const emrlogs = "../../../testdata/emrlogs"

func TestDescribe(t *testing.T) {
	t.Parallel()
	for key, want := range map[string]File{
		"containers/application_1_0001/container_1790380000000_0001_01_000001/stderr.gz":                    {Kind: ContainerStderr, App: "application_1_0001", Container: "container_1790380000000_0001_01_000001"},
		"logs/j-X/containers/application_1_0001/container_e02_1790380000000_0001_02_000003/stdout":          {Kind: ContainerStdout, App: "application_1_0001", Container: "container_e02_1790380000000_0001_02_000003"},
		"containers/application_1_0001/container_1790380000000_0001_01_000001/prelaunch.err.gz":             {Kind: ContainerStderr, App: "application_1_0001", Container: "container_1790380000000_0001_01_000001"},
		"containers/application_1_0001/container_1790380000000_0001_01_000001/directory.info.gz":            {App: "application_1_0001", Container: "container_1790380000000_0001_01_000001"},
		"steps/s-FIXTURESTEP0001/controller.gz":                                                             {Kind: StepController, Step: "s-FIXTURESTEP0001"},
		"steps/s-FIXTURESTEP0001/stderr.gz":                                                                 {Kind: StepStderr, Step: "s-FIXTURESTEP0001"},
		"steps/s-FIXTURESTEP0001/stdout.gz":                                                                 {Step: "s-FIXTURESTEP0001"},
		"node/i-0fee0000000000001/applications/hadoop-yarn/hadoop-yarn-nodemanager-ip-10-0-2-10.log.gz":     {Kind: NodeManager, Instance: "i-0fee0000000000001"},
		"node/i-0fee0000000000001/applications/hadoop-yarn/hadoop-yarn-resourcemanager-ip-10-0-2-10.log.gz": {Kind: ResourceManager, Instance: "i-0fee0000000000001"},
		"node/i-0fee0000000000001/applications/hadoop-yarn/hadoop-yarn-nodemanager-ip-10-0-2-10.out.gz":     {Instance: "i-0fee0000000000001"},
		"node/i-0fee0000000000001/bootstrap-actions/master.log.gz":                                          {Kind: Bootstrap, Instance: "i-0fee0000000000001"},
		"node/i-0fee0000000000001/bootstrap-actions/1/stderr.gz":                                            {Kind: BootstrapOutput, Instance: "i-0fee0000000000001"},
		"node/i-0fee0000000000001/daemons/instance-state/console.log-2026-09-26-10-10.gz":                   {Instance: "i-0fee0000000000001"},
		"spark-events/application_1_0001":                                                                   {},
	} {
		if got := Describe(key); got != want {
			t.Errorf("Describe(%q) = %+v, want %+v", key, got, want)
		}
	}
	if !Describe("containers/application_1790380000000_0001/container_1790380000000_0001_02_000001/stderr").Driver() || Describe("containers/application_1790380000000_0001/container_1790380000000_0001_02_000002/stderr").Driver() {
		t.Error("Driver() should hold for the first container of an attempt only")
	}
}

// classifyTree classifies every file of a scrubbed cluster log tree, keyed
// by its path under the tree.
func classifyTree(t *testing.T, cluster, appID string) map[string]Result {
	t.Helper()
	root := filepath.Join(emrlogs, cluster)
	out := map[string]Result{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		f := Describe(rel)
		if f.Kind == Unread {
			return nil
		}
		fh, err := os.Open(p)
		if err != nil {
			return err
		}
		defer fh.Close()
		zr, err := gzip.NewReader(fh)
		if err != nil {
			return err
		}
		res, err := Classify(zr, rel, f, Options{AppID: appID})
		if err != nil {
			t.Errorf("%s: %v", rel, err)
		}
		checkInvariants(t, res)
		out[rel] = res
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) == 0 {
		t.Fatalf("no logs under %s", root)
	}
	return out
}

// checkInvariants holds for every result: provenance on every line, a
// known severity, and no planted secret anywhere.
func checkInvariants(t *testing.T, res Result) {
	t.Helper()
	for _, l := range res.Lines {
		if l.Source.File != res.Name || l.Source.Line < 1 || l.Source.Line > res.Read || (l.Source.EndLine != 0 && l.Source.EndLine < l.Source.Line) {
			t.Errorf("%s: bad source %+v (read %d) for %q", res.Name, l.Source, res.Read, l.Text)
		}
		if l.Count < 1 || (l.Count > 1 && l.LastLine < l.Source.Line) {
			t.Errorf("%s: bad count %d / last line %d for %q", res.Name, l.Count, l.LastLine, l.Text)
		}
		switch l.Severity {
		case model.Info, model.Warning, model.Critical:
		default:
			t.Errorf("%s: severity %q", res.Name, l.Severity)
		}
		all := l.Text + strings.Join(l.Detail, "\n")
		for _, v := range l.Fields {
			all += "\n" + v
		}
		for _, secret := range []string{"FAKE-EMR-PASSWORD", "FAKE-PLANTED", "AKIAFAKEFAKEFAKEFAKE"} {
			if strings.Contains(all, secret) {
				t.Errorf("%s: planted secret %s in %q", res.Name, secret, all)
			}
		}
	}
}

// find returns the lines of kind in the file whose path ends with suffix.
func find(t *testing.T, got map[string]Result, suffix string, kind model.LogKind) []model.LogLine {
	t.Helper()
	var out []model.LogLine
	n := 0
	for name, res := range got {
		if !strings.HasSuffix(name, suffix) {
			continue
		}
		n++
		for _, l := range res.Lines {
			if l.Kind == kind {
				out = append(out, l)
			}
		}
	}
	if n != 1 {
		t.Fatalf("%d files end with %s", n, suffix)
	}
	return out
}

// findKind returns the lines of kind in every file of a file kind.
func findKind(got map[string]Result, fk FileKind, kind model.LogKind) []model.LogLine {
	var out []model.LogLine
	for _, res := range got {
		if res.File.Kind != fk {
			continue
		}
		for _, l := range res.Lines {
			if l.Kind == kind {
				out = append(out, l)
			}
		}
	}
	return out
}

func one(t *testing.T, ls []model.LogLine, what string) model.LogLine {
	t.Helper()
	if len(ls) != 1 {
		t.Fatalf("%s: got %d lines, want 1: %+v", what, len(ls), ls)
	}
	return ls[0]
}

// The failed EMR run (0052): the script's SparkSession could not start
// because spark.eventLog.dir named an S3 prefix with no objects.
func TestFailedRun(t *testing.T) {
	t.Parallel()
	got := classifyTree(t, "j-FIXTURE0052CLUSTER", "application_1790380000000_0052")
	const driver = "container_1790380000000_0052_02_000001/"

	tb := one(t, find(t, got, driver+"stdout.gz", model.LogTraceback), "driver traceback")
	if tb.Severity != model.Critical || !strings.HasSuffix(tb.Fields["pyFile"], "/emr_job2.py") || tb.Fields["pyLine"] != "7" ||
		tb.Fields["pyException"] != "py4j.protocol.Py4JJavaError" || tb.Fields["root"] != "java.io.FileNotFoundException" ||
		!strings.Contains(tb.Fields["rootMessage"], "s3://sparkplain-fixtures/spark-events") {
		t.Errorf("traceback = %+v", tb)
	}
	if !strings.Contains(strings.Join(tb.Detail, "\n"), `spark = SparkSession.builder.appName("sparkplain_emr_test2").getOrCreate()`) {
		t.Errorf("traceback detail misses the failing line of code: %q", tb.Detail)
	}
	for _, d := range tb.Detail {
		if strings.Contains(d, "pyspark.zip") {
			t.Errorf("traceback detail keeps PySpark's own frames: %q", d)
		}
	}

	ex := find(t, got, driver+"stderr.gz", model.LogException)
	if len(ex) != 2 || ex[0].Text != "Error initializing SparkContext." || ex[0].Severity != model.Critical ||
		ex[0].Fields["root"] != "java.io.FileNotFoundException" || ex[0].Source.EndLine <= ex[0].Source.Line {
		t.Errorf("driver exceptions = %+v", ex)
	}
	exit := one(t, find(t, got, driver+"stderr.gz", model.LogAppExit), "driver final status")
	if exit.Fields["status"] != "FAILED" || exit.Fields["exitCode"] != "13" || !strings.Contains(exit.Fields["meaning"], "SparkContext never started") || exit.Severity != model.Critical {
		t.Errorf("final status = %+v", exit)
	}

	var nmExits []string
	for name, res := range got {
		if res.File.Kind == NodeManager {
			for _, l := range res.Lines {
				if l.Kind == model.LogContainerEnd {
					nmExits = append(nmExits, l.Fields["container"]+"="+l.Fields["exitCode"])
				}
			}
			if len(res.Lines) == 0 {
				t.Errorf("%s: nothing classified", name)
			}
		}
	}
	sort.Strings(nmExits) // one NodeManager ran each attempt's driver
	if strings.Join(nmExits, " ") != "container_1790380000000_0052_01_000001=13 container_1790380000000_0052_02_000001=13" {
		t.Errorf("NodeManager exits = %v", nmExits)
	}

	rm := findKind(got, ResourceManager, model.LogAppExit) // the ResourceManager's two failed attempts
	if len(rm) != 2 || rm[0].Fields["attempt"] != "appattempt_1790380000000_0052_000001" || rm[1].Fields["status"] != "FAILED" {
		t.Errorf("attempts = %+v", rm)
	}
	sum := one(t, findKind(got, ResourceManager, model.LogAppSummary), "app summary")
	if sum.Fields["finalStatus"] != "FAILED" || sum.Severity != model.Critical || strings.Contains(sum.Fields["diagnostics"], "INFO") || sum.Fields["user"] != "hadoop" {
		t.Errorf("summary = %+v", sum.Fields)
	}
	for name, res := range got {
		if res.File.Kind == ResourceManager {
			for _, l := range res.Lines {
				if l.Kind == model.LogException {
					t.Errorf("%s: the driver stack the ResourceManager quotes was classified: %q", name, l.Text)
				}
			}
		}
	}

	rep := one(t, find(t, got, "steps/s-FIXTURESTEP0001/stderr.gz", model.LogAppReport), "app report")
	detail := strings.Join(rep.Detail, "\n")
	if rep.Severity != model.Critical || rep.Fields["exitCode"] != "13" || rep.Fields["state"] != "FAILED" || rep.Fields["queue"] != "default" ||
		!strings.Contains(detail, "Exit code: 13") || strings.Contains(detail, "INFO ") || strings.Contains(detail, "ApplicationMaster:") {
		t.Errorf("app report = %+v\ndetail:\n%s", rep, detail)
	}
	stepEx := one(t, find(t, got, "steps/s-FIXTURESTEP0001/stderr.gz", model.LogException), "step exceptions (quoted ones skipped)")
	if !strings.HasPrefix(stepEx.Text, `Exception in thread "main" org.apache.spark.SparkException: Application application_1790380000000_0052 finished with failed status`) {
		t.Errorf("step exception = %q", stepEx.Text)
	}
	if n := len(find(t, got, "steps/s-FIXTURESTEP0001/stderr.gz", model.LogAppExit)); n != 0 {
		t.Errorf("quoted driver lines were classified in the step log: %d app exits", n)
	}
	st := one(t, find(t, got, "steps/s-FIXTURESTEP0001/controller.gz", model.LogStepStatus), "step status")
	if st.Fields["status"] != "failed" || st.Fields["exitCode"] != "1" || st.Severity != model.Critical {
		t.Errorf("step status = %+v", st)
	}
	res := one(t, find(t, got, "steps/s-FIXTURESTEP0001/stderr.gz", model.LogResource), "uploaded script")
	if res.Fields["path"] != "s3://sparkplain-fixtures/emr_job2.py" {
		t.Errorf("script = %+v", res.Fields)
	}
	boot := one(t, find(t, got, "bootstrap-actions/master.log.gz", model.LogBootstrap), "bootstrap")
	if boot.Severity != model.Info || boot.Fields["action"] != "1" {
		t.Errorf("bootstrap = %+v", boot)
	}
}

// The first EMR run (0049): a deliberate cast failure in stage 22, and a
// step that tried to run s3-dist-cp, which the release lacks.
func TestTaskFailureRun(t *testing.T) {
	t.Parallel()
	got := classifyTree(t, "j-FIXTURE0049CLUSTER", "application_1790380000000_0049")
	te := one(t, find(t, got, "container_1790380000000_0049_01_000002/stderr.gz", model.LogTaskError), "executor task errors folded")
	if te.Count != 7 || te.Fields["stage"] != "22.0" || te.Fields["root"] != "org.apache.spark.SparkNumberFormatException" ||
		!strings.Contains(te.Detail[0], "CAST_INVALID_INPUT") || te.LastLine <= te.Source.Line {
		t.Errorf("task error = %+v", te)
	}
	sig := one(t, find(t, got, "container_1790380000000_0049_01_000002/stderr.gz", model.LogSignal), "executor signal")
	if sig.Severity != model.Info || sig.Fields["afterShutdown"] != "true" {
		t.Errorf("SIGTERM after the driver's shutdown should be info: %+v", sig)
	}
	nm := one(t, find(t, got, "nodemanager-ip-10-0-2-10.us-east-1.compute.internal.log.gz", model.LogContainerEnd), "executor exit")
	if nm.Fields["exitCode"] != "143" || nm.Severity != model.Info {
		t.Errorf("exit 143 = %+v", nm)
	}

	sub := one(t, find(t, got, "steps/s-FIXTURESTEP0001/controller.gz", model.LogSubmit), "spark-submit command")
	if !strings.Contains(sub.Text, "spark.myapp.db.password=[redacted]") || !strings.Contains(sub.Text, "s3://sparkplain-fixtures/emr_job.py") {
		t.Errorf("command = %q", sub.Text)
	}
	cp := one(t, find(t, got, "steps/s-FIXTURESTEP0002/stderr.gz", model.LogException), "s3-dist-cp failure")
	if cp.Severity != model.Critical || cp.Fields["exception"] != "java.lang.RuntimeException" || cp.Fields["root"] != "java.io.IOException" ||
		cp.Fields["rootMessage"] != "error=2, No such file or directory" {
		t.Errorf("s3-dist-cp = %+v", cp)
	}
	ids := find(t, got, "steps/s-FIXTURESTEP0001/stderr.gz", model.LogIdentity)
	if len(ids) != 1 || ids[0].Fields["user"] != "hadoop" || ids[0].Fields["queue"] != "default" {
		t.Errorf("identity = %+v", ids)
	}
}

// The speculation run (0050): a speculative copy won, and Spark killed the
// original; executors were stopped with SIGTERM at the end.
func TestSpeculationRun(t *testing.T) {
	t.Parallel()
	got := classifyTree(t, "j-FIXTURE0050CLUSTER", "application_1790380000000_0050")
	te := one(t, find(t, got, "container_1790380000000_0050_01_000001/stderr.gz", model.LogTaskError), "killed twin")
	if te.Severity != model.Info || te.Fields["reason"] != "TaskKilled (Stage finished)" {
		t.Errorf("killed twin = %+v", te)
	}
	for name, res := range got {
		for _, l := range res.Lines {
			if l.Severity == model.Critical {
				t.Errorf("%s: a successful run has a critical line: %+v", name, l)
			}
		}
	}
}

func classifyText(t *testing.T, name, text string, opt Options) Result {
	t.Helper()
	res, err := Classify(strings.NewReader(text), name, Describe(name), opt)
	if err != nil {
		t.Fatal(err)
	}
	checkInvariants(t, res)
	return res
}

func kinds(res Result) string {
	var out []string
	for _, l := range res.Lines {
		out = append(out, string(l.Kind)+"/"+string(l.Severity))
	}
	return strings.Join(out, " ")
}

// Rules the test clusters never triggered, with lines in the formats of
// Spark 3.5.1's and Hadoop 3.3's classes (checked against their strings).
func TestSyntheticRules(t *testing.T) {
	t.Parallel()
	const drv = "containers/application_1700000000000_0001/container_1700000000000_0001_01_000001/stderr"
	const exe = "containers/application_1700000000000_0001/container_1700000000000_0001_01_000002/stderr"
	for _, tc := range []struct {
		name, file, text, want string
		check                  func(t *testing.T, res Result)
	}{
		{"memory kill reported by the driver", drv, `24/01/01 10:00:00 ERROR YarnScheduler: Lost executor 2 on ip-10-0-0-5.example.internal: Container killed by YARN for exceeding physical memory limits. 5.5 GB of 5.5 GB physical memory used. Consider boosting spark.executor.memoryOverhead.
24/01/01 10:00:00 WARN YarnSchedulerBackend$YarnSchedulerEndpoint: Requesting driver to remove executor 2 for reason Container marked as failed: container_1700000000000_0001_01_000003 on host: ip-10-0-0-5.example.internal. Exit status: 137. Diagnostics: [2024-01-01 10:00:00.000]Container killed on request. Exit code is 137
24/01/01 10:00:00 WARN YarnAllocator: Container marked as failed: container_1700000000000_0001_01_000003 on host: ip-10-0-0-5.example.internal. Exit status: 137. Diagnostics: [2024-01-01 10:00:00.000]Container killed on request. Exit code is 137
24/01/01 10:00:01 WARN TaskSetManager: Lost task 3.0 in stage 4.0 (TID 40) (ip-10-0-0-5.example.internal executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Container killed by YARN for exceeding physical memory limits. 5.5 GB of 5.5 GB physical memory used. Consider boosting spark.executor.memoryOverhead.
`, "lost-executor/critical lost-executor/critical container-exit/critical lost-executor/critical", func(t *testing.T, res Result) {
			l := res.Lines[3]
			if l.Fields["executor"] != "2" || l.Fields["causedByApp"] != "true" || l.Fields["stage"] != "4.0" {
				t.Errorf("lost task = %+v", l.Fields)
			}
			if c := res.Lines[2]; c.Fields["exitCode"] != "137" || c.Fields["container"] != "container_1700000000000_0001_01_000003" || !strings.Contains(c.Fields["meaning"], "SIGKILL") {
				t.Errorf("container = %+v", c.Fields)
			}
		}},
		{"NodeManager memory kill, Hadoop 3 wording", "node/i-0fee0000000000001/applications/hadoop-yarn/hadoop-yarn-nodemanager-ip-10-0-0-5.log",
			`2024-01-01 10:00:00,000 WARN org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl (Container Monitor): Container [pid=4242,containerID=container_1700000000000_0001_01_000003] is running 123456789B beyond the 'PHYSICAL' memory limit. Current usage: 5.6 GB of 5.5 GB physical memory used; 7.2 GB of 27.5 GB virtual memory used. Killing container.
2024-01-01 10:00:00,100 WARN org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor (ContainersLauncher #3): Exit code from container container_1700000000000_0001_01_000003 is : 137
2024-01-01 10:00:00,200 WARN org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor (ContainersLauncher #3): Exit code from container container_1700000000000_0002_01_000003 is : 137
2024-01-01 10:00:00,300 ERROR org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdaterImpl (main): NodeLabels sent from NM while registration were rejected by RM.
`, "memory-kill/critical container-exit/critical", func(t *testing.T, res Result) {
				if l := res.Lines[0]; l.Fields["limit"] != "physical" || l.Fields["container"] != "container_1700000000000_0001_01_000003" || !strings.HasPrefix(l.Fields["usage"], "Current usage: 5.6 GB") {
					t.Errorf("memory kill = %+v", l.Fields)
				}
			}},
		{"executor out of memory in a task", exe, `24/01/01 10:00:00 INFO Executor: Running task 1.0 in stage 3.0 (TID 11)
24/01/01 10:00:05 ERROR Executor: Exception in task 1.0 in stage 3.0 (TID 11)
java.lang.OutOfMemoryError: Java heap space
	at java.base/java.util.Arrays.copyOf(Arrays.java:3537)
	at com.example.etl.Loader.buffer(Loader.java:88)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
24/01/01 10:00:05 ERROR SparkUncaughtExceptionHandler: Uncaught exception in thread Thread[Executor task launch worker for task 1.0 in stage 3.0 (TID 11),5,main]
java.lang.OutOfMemoryError: Java heap space
	at java.base/java.util.Arrays.copyOf(Arrays.java:3537)
`, "task-error/critical out-of-memory/critical", func(t *testing.T, res Result) {
			l := res.Lines[0]
			if l.Fields["cause"] != "OutOfMemoryError" || l.Fields["oom"] != "Java heap space" || l.Fields["tid"] != "11" {
				t.Errorf("task OOM = %+v", l.Fields)
			}
			if d := strings.Join(l.Detail, "\n"); !strings.Contains(d, "com.example.etl.Loader.buffer(Loader.java:88)") || strings.Contains(d, "java.util.Arrays") || strings.Contains(d, "org.apache.spark") {
				t.Errorf("detail should keep only the application's frames: %q", l.Detail)
			}
		}},
		{"JVM flags are not an out-of-memory error", drv, `24/01/01 10:00:00 WARN Client: launching with '-XX:OnOutOfMemoryError=kill -9 %p' -XX:+ExitOnOutOfMemoryError
24/01/01 10:00:00 INFO Client: added ./__spark_libs__/hive-metastore-2.3.9-amzn-2.jar
24/01/01 10:00:00 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir.
`, "", nil},
		{"S3 access denied", drv, `24/01/01 10:00:00 ERROR FileFormatWriter: Aborting job 7.
com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied (Service: Amazon S3; Status Code: 403; Error Code: AccessDenied; Request ID: FAKE1; S3 Extended Request ID: FAKE2), S3 Extended Request ID: FAKE2
	at com.amazonaws.http.AmazonHttpClient.handle(AmazonHttpClient.java:1)
Caused by: java.nio.file.AccessDeniedException: s3://example-bucket/out/part-0000.parquet: PutObject
`, "access-denied/critical", func(t *testing.T, res Result) {
			if p := res.Lines[0].Fields["path"]; p != "s3://example-bucket/out/part-0000.parquet" {
				t.Errorf("path = %q", p)
			}
		}},
		{"Glue refuses an IAM role", drv, `24/01/01 10:00:00 WARN HiveClientImpl: Failed to get database
com.amazonaws.services.glue.model.AccessDeniedException: User: arn:aws:sts::000000000000:assumed-role/EMR_EC2_Fixture/i-0fee0000000000001 is not authorized to perform: glue:GetDatabase on resource: arn:aws:glue:us-east-1:000000000000:catalog (Service: AWSGlue; Status Code: 400; Error Code: AccessDeniedException)
`, "access-denied/critical", func(t *testing.T, res Result) {
			if l := res.Lines[0]; l.Fields["action"] != "glue:GetDatabase" || l.Fields["resource"] != "arn:aws:glue:us-east-1:000000000000:catalog" {
				t.Errorf("glue = %+v", l.Fields)
			}
		}},
		{"Kerberos", drv, `24/01/01 10:00:00 INFO UserGroupInformation: Login successful for user etl@EXAMPLE.COM using keytab file etl.keytab. Keytab auto renewal enabled : false
24/01/01 10:00:01 WARN Client: Exception encountered while connecting to the server
javax.security.sasl.SaslException: GSS initiate failed [Caused by GSSException: No valid credentials provided (Mechanism level: Failed to find any Kerberos tgt)]
`, "identity/info kerberos/critical", func(t *testing.T, res Result) {
			if l := res.Lines[0]; l.Fields["principal"] != "etl@EXAMPLE.COM" || l.Fields["keytab"] != "etl.keytab." {
				t.Errorf("login = %+v", l.Fields)
			}
		}},
		{"Hive metastore", drv, `24/01/01 10:00:00 INFO metastore: Trying to connect to metastore with URI thrift://ip-10-0-0-9.example.internal:9083
24/01/01 10:00:00 INFO metastore: Opened a connection to metastore, current connections: 1
24/01/01 10:00:05 WARN metastore: Failed to connect to the MetaStore Server...
24/01/01 10:00:09 ERROR Hive: Unable to instantiate org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient
`, "metastore/info metastore/info metastore/critical metastore/critical", func(t *testing.T, res Result) {
			if u := res.Lines[0].Fields["uri"]; u != "thrift://ip-10-0-0-9.example.internal:9083" {
				t.Errorf("uri = %q", u)
			}
		}},
		{"HBase", drv, `24/01/01 10:00:00 INFO ZooKeeper: Initiating client connection, connectString=ip-10-0-0-9.example.internal:2181 sessionTimeout=90000 watcher=org.apache.hadoop.hbase.zookeeper.ReadOnlyZKClient$$Lambda$1
24/01/01 10:01:00 ERROR AsyncProcess: Failed to get region location
org.apache.hadoop.hbase.client.RetriesExhaustedException: Failed after attempts=16, exceptions:
`, "hbase/info hbase/critical", func(t *testing.T, res Result) {
			if q := res.Lines[0].Fields["quorum"]; q != "ip-10-0-0-9.example.internal:2181" {
				t.Errorf("quorum = %q", q)
			}
		}},
		{"SIGTERM without a shutdown request, heartbeats, self-exit", exe, `24/01/01 10:00:00 ERROR CoarseGrainedExecutorBackend: RECEIVED SIGNAL TERM
24/01/01 10:00:01 ERROR CoarseGrainedExecutorBackend: Executor self-exiting due to : Driver ip-10-0-0-1.example.internal:40000 disassociated! Shutting down.
`, "signal/warning lost-executor/warning", nil},
		{"heartbeat timeout", drv, `24/01/01 10:00:00 WARN HeartbeatReceiver: Removing executor 4 with no recent heartbeats: 160000 ms exceeds timeout 120000 ms
`, "lost-executor/warning", func(t *testing.T, res Result) {
			if r := res.Lines[0].Fields["reason"]; r != "no heartbeat for 160000 ms (timeout 120000 ms)" {
				t.Errorf("reason = %q", r)
			}
		}},
		{"secrets in messages and commands", drv, `24/01/01 10:00:00 ERROR JdbcUtils: connect failed for jdbc:postgresql://etl:FAKE-PLANTED-1@db.example.internal/x password=FAKE-PLANTED-2
java.sql.SQLException: login failed, token=FAKE-PLANTED-3 key AKIAFAKEFAKEFAKEFAKE
`, "exception/warning", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := classifyText(t, tc.file, tc.text, Options{AppID: "application_1700000000000_0001"})
			if got := kinds(res); got != tc.want {
				t.Fatalf("kinds = %q, want %q\n%+v", got, tc.want, res.Lines)
			}
			if tc.check != nil {
				tc.check(t, res)
			}
		})
	}
}

func TestControllerRedactsCommand(t *testing.T) {
	t.Parallel()
	res := classifyText(t, "steps/s-FIXTURESTEP0009/controller", `2024-01-01T10:00:00.000Z INFO Ensure step 9 jar file command-runner.jar
INFO startExec 'hadoop jar /var/lib/aws/emr/step-runner/hadoop-jars/command-runner.jar spark-submit --conf spark.hadoop.fs.s3a.secret.key=FAKE-PLANTED-4 --conf spark.executor.memory=4g -Dapi.token=FAKE-PLANTED-5 s3://example-bucket/job.py'
INFO Environment:
  AWS_SECRET_ACCESS_KEY=FAKE-PLANTED-6
2024-01-01T10:05:00.000Z INFO Step succeeded with exitCode 0 and took 300 seconds
`, Options{})
	if got := kinds(res); got != "submit/info step-status/info" {
		t.Fatalf("kinds = %q", got)
	}
	if c := res.Lines[0].Text; !strings.Contains(c, "spark.executor.memory=4g") || !strings.Contains(c, "s3://example-bucket/job.py") || strings.Contains(c, "FAKE") {
		t.Errorf("command = %q", c)
	}
}

func TestFoldingAndCaps(t *testing.T) {
	t.Parallel()
	var b strings.Builder
	for i := 0; i < 30; i++ {
		b.WriteString("24/01/01 10:00:00 ERROR Executor: Exception in task " + strconv.Itoa(i) + ".0 in stage 1.0 (TID " + strconv.Itoa(100+i) + ")\r\n")
		b.WriteString("java.lang.IllegalStateException: row " + strconv.Itoa(i) + " is bad\r\n\tat com.example.Job.run(Job.java:12)\r\n")
	}
	for i := 0; i < 10; i++ {
		b.WriteString("24/01/01 10:00:00 ERROR Thing: distinct failure " + strings.Repeat("x", i) + "\n")
	}
	b.WriteString("24/01/01 10:00:00 ERROR Thing: " + strings.Repeat("y", 100<<10) + "\n")
	b.WriteString("24/01/01 10:00:00 ERROR Thing: last")
	res := classifyText(t, "containers/application_1700000000000_0001/container_1700000000000_0001_01_000002/stderr", b.String(), Options{MaxEntries: 5})
	if len(res.Lines) != 5 || res.Lines[0].Count != 30 || res.Lines[0].LastLine != 88 || res.Lines[0].Fields["root"] != "java.lang.IllegalStateException" {
		t.Fatalf("folded = %+v", res.Lines[0])
	}
	if res.Dropped != 8 || res.Truncated != 1 || res.Read != 102 {
		t.Errorf("dropped %d, truncated %d, read %d", res.Dropped, res.Truncated, res.Read)
	}
	if strings.ContainsRune(res.Lines[0].Detail[0], '\r') {
		t.Error("carriage return kept")
	}
}

func TestContainerExitMeaning(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		code   int
		driver bool
		want   string
	}{
		{13, true, "SparkContext never started"}, {13, false, "exited with status 13"}, {137, false, "SIGKILL"},
		{143, true, "SIGTERM"}, {-104, false, "physical memory"}, {52, false, "Java heap"}, {130, false, "signal 2"},
	} {
		if got := ContainerExitMeaning(tc.code, tc.driver); !strings.Contains(got, tc.want) {
			t.Errorf("ContainerExitMeaning(%d, %v) = %q, want %q", tc.code, tc.driver, got, tc.want)
		}
	}
}

func FuzzClassify(f *testing.F) {
	f.Add("24/01/01 10:00:00 ERROR Executor: Exception in task 1.0 in stage 3.0 (TID 11)\njava.lang.OutOfMemoryError: x\n\tat a.b(C.java:1)\n")
	f.Add("Traceback (most recent call last):\n  File \"/x/job.py\", line 7, in <module>\n    boom()\nValueError: no\n: java.io.IOException: e\n")
	f.Add("26/09/26 10:15:32 INFO Client: \n\t diagnostics: x\n[2026-09-26 10:15:54.501]y\nLast 4096 bytes of stderr :\n\t final status: FAILED\n")
	f.Add("26/09/26 10:15:55 ERROR Client: Application diagnostics message: x\n. Failing the application.\n")
	for _, kind := range []string{"stderr", "stdout"} {
		f.Add("x\n" + kind)
	}
	names := []string{
		"containers/application_1700000000000_0001/container_1700000000000_0001_01_000001/stdout",
		"containers/application_1700000000000_0001/container_1700000000000_0001_01_000002/stderr",
		"steps/s-FIXTURESTEP0001/stderr", "steps/s-FIXTURESTEP0001/controller",
		"node/i-0fee0000000000001/applications/hadoop-yarn/hadoop-yarn-nodemanager-x.log",
		"node/i-0fee0000000000001/applications/hadoop-yarn/hadoop-yarn-resourcemanager-x.log",
	}
	f.Fuzz(func(t *testing.T, text string) {
		for _, name := range names {
			res, err := Classify(strings.NewReader(text), name, Describe(name), Options{AppID: "application_1700000000000_0001"})
			if err != nil {
				t.Fatal(err)
			}
			for _, l := range res.Lines {
				if l.Source.Line < 1 || l.Source.Line > res.Read || l.Count < 1 || len(l.Detail) > maxDetail {
					t.Fatalf("%s: bad line %+v (read %d)", name, l, res.Read)
				}
			}
		}
	})
}

// Capacity lines are node-wide, so they are kept though they name no
// application; container placements are kept for this application only.
func TestCapacityAndRequests(t *testing.T) {
	t.Parallel()
	nm := classifyText(t, "node/i-0fee0000000000001/applications/hadoop-yarn/hadoop-yarn-nodemanager-ip-10-0-0-2.log",
		`2024-01-01 10:00:00,000 INFO org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdaterImpl (main): Registered with ResourceManager as ip-10-0-0-2.ec2.internal:8041 with total resource of <memory:12288, vCores:4>
`, Options{AppID: "application_1700000000000_0001"})
	if got := kinds(nm); got != "node-capacity/info" || nm.Lines[0].Fields["memoryMB"] != "12288" || nm.Lines[0].Fields["host"] != "ip-10-0-0-2.ec2.internal" {
		t.Errorf("nodemanager = %s %+v", got, nm.Lines)
	}
	rm := classifyText(t, "node/i-0fee0000000000002/applications/hadoop-yarn/hadoop-yarn-resourcemanager-ip-10-0-0-1.log",
		`2024-01-01 10:00:00,000 INFO org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode (SchedulerEventDispatcher:Event Processor): Assigned container container_1700000000000_0001_01_000002 of capacity <memory:11264, max memory:12288, vCores:1, max vCores:4> on host ip-10-0-0-2.ec2.internal:8041, which has 1 containers, <memory:11264, vCores:1> used and <memory:1024, vCores:3> available after allocation
2024-01-01 10:00:01,000 INFO org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode (SchedulerEventDispatcher:Event Processor): Assigned container container_1700000000000_0009_01_000002 of capacity <memory:1024, max memory:12288, vCores:1, max vCores:4> on host ip-10-0-0-2.ec2.internal:8041, which has 2 containers, <memory:12288, vCores:2> used and <memory:0, vCores:2> available after allocation
`, Options{AppID: "application_1700000000000_0001"})
	if got := kinds(rm); got != "container-assigned/info" {
		t.Fatalf("resourcemanager = %s", got)
	}
	if f := rm.Lines[0].Fields; f["memoryMB"] != "11264" || f["availableMB"] != "1024" || f["host"] != "ip-10-0-0-2.ec2.internal" || f["vcores"] != "1" {
		t.Errorf("assigned = %+v", f)
	}
	drv := classifyText(t, "containers/application_1700000000000_0001/container_1700000000000_0001_01_000001/stderr", `24/01/01 10:01:10 INFO YarnAllocator: Will request 50 executor container(s) for  ResourceProfile Id: 0, each with 4 core(s) and 11264 MB memory. with custom resources: <memory:11264>
24/01/01 10:01:11 INFO YarnAllocator: Launching executor with 9485m of heap (plus 1779m overhead/off heap) and 4 cores
24/01/01 10:01:12 INFO YarnAllocator: Launching executor with 9485m of heap (plus 1779m overhead/off heap) and 4 cores
24/01/01 10:01:15 INFO YarnAllocator: Canceling requests for 49 executor container(s) to have a new desired total 1 executors.
24/01/01 10:01:16 INFO YarnAllocator: Will request 2 executor container(s) for  ResourceProfile Id: 0, each with 4 core(s) and 11264 MB memory.
24/01/01 10:01:20 INFO YarnAllocator: Driver requested a total number of 42 executor(s) for resource profile id: 0.
24/01/01 10:01:25 INFO YarnAllocator: Driver requested a total number of 7 executor(s) for resource profile id: 0.
`, Options{})
	var whats []string
	for _, l := range drv.Lines {
		whats = append(whats, l.Fields["what"]+"@"+strconv.FormatInt(l.Source.Line, 10))
	}
	if strings.Join(whats, " ") != "launch@2 executors@1 most-desired@6" || drv.Lines[0].Count != 2 {
		t.Errorf("driver requests = %v, %+v", whats, drv.Lines)
	}
	step := classifyText(t, "steps/s-FIXTURESTEP0001/stderr", `26/09/26 11:08:19 INFO Client: Verifying our application has not requested more than the maximum memory capability of the cluster (12288 MB per container)
26/09/26 11:08:19 INFO Client: Will allocate AM container, with 2432 MB memory including 384 MB overhead
`, Options{})
	if got := kinds(step); got != "yarn-request/info yarn-request/info" || step.Lines[1].Fields["memoryMB"] != "2432" || step.Lines[0].Fields["what"] != "max-container" {
		t.Errorf("step = %s %+v", got, step.Lines)
	}
}

// HotSpot's banner when an executor's heap runs out: Spark starts
// executors with -XX:OnOutOfMemoryError="kill -9 %p", so the JVM kills
// itself and its container exits 137 with no YARN memory kill. Lines as
// the phase 3 test cluster wrote them, in the executor's stdout.
func TestHotSpotOutOfMemory(t *testing.T) {
	t.Parallel()
	res := classifyText(t, "containers/application_1700000000000_0001/container_1700000000000_0001_01_000002/stdout", `#
# java.lang.OutOfMemoryError: GC overhead limit exceeded
# -XX:OnOutOfMemoryError="kill -9 %p
kill -9 %p
kill -9 %p"
#   Executing /bin/sh -c "kill -9 14396
kill -9 14396
kill -9 14396"...
`, Options{})
	if got := kinds(res); got != "out-of-memory/critical" {
		t.Fatalf("kinds = %s", got)
	}
	f := res.Lines[0].Fields
	if f["oom"] != "GC overhead limit exceeded" || f["selfKilled"] != "true" || f["root"] != "java.lang.OutOfMemoryError" {
		t.Errorf("fields = %+v", f)
	}
	// In stderr after timed lines, the banner takes the last time seen.
	res = classifyText(t, "containers/application_1700000000000_0001/container_1700000000000_0001_01_000002/stderr", `26/09/26 16:10:20 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
# java.lang.OutOfMemoryError: Java heap space
`, Options{})
	if res.Lines[0].Time.IsZero() || res.Lines[0].Time.Format("15:04:05") != "16:10:20" {
		t.Errorf("time = %v", res.Lines[0].Time)
	}
}

// Where an attempt's driver ran, and YARN's notice that a node is leaving,
// as the phase 4 test cluster's first attempt logged them when its spot
// node was reclaimed.
func TestDriverHostAndNodeState(t *testing.T) {
	t.Parallel()
	res := classifyText(t, "containers/application_1700000000000_0001/container_1700000000000_0001_01_000001/stderr", `26/09/27 08:02:00 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, ip-10-0-0-4.ec2.internal, 34591, None)
26/09/27 08:02:00 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, ip-10-0-0-4.ec2.internal, 34591, None)
26/09/27 08:02:01 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(1, ip-10-0-0-2.ec2.internal, 40001, None)
26/09/27 08:02:09 INFO YarnAllocator: Yarn node state updated for host ip-10-0-0-4.ec2.internal to DECOMMISSIONING
26/09/27 08:02:30 INFO YarnAllocator: Yarn node state updated for host ip-10-0-0-5.ec2.internal to RUNNING
`, Options{})
	if got := kinds(res); got != "driver-host/info node-state/warning" {
		t.Fatalf("kinds = %s", got)
	}
	if h := res.Lines[0].Fields["host"]; h != "ip-10-0-0-4.ec2.internal" || res.Lines[0].Source.Line != 2 {
		t.Errorf("driver host = %q at line %d", h, res.Lines[0].Source.Line)
	}
	if f := res.Lines[1].Fields; f["host"] != "ip-10-0-0-4.ec2.internal" || f["state"] != "DECOMMISSIONING" {
		t.Errorf("node state = %+v", f)
	}
}
