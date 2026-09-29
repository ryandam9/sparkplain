package yarnlog

import (
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// header is the parsed prefix of a log4j line.
type header struct {
	time   time.Time
	level  string // INFO, WARN, ERROR, FATAL, …
	logger string // the class, short (Spark) or full (Hadoop)
	msg    string
}

var (
	// Spark on EMR: "26/09/26 10:15:53 ERROR SparkContext: Error initializing SparkContext."
	sparkHeadRE = regexp.MustCompile(`^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) (TRACE|DEBUG|INFO|WARN|ERROR|FATAL) ([^\s:]+): ?(.*)$`)
	// Hadoop daemons and EMR's instance controller:
	// "2026-09-26 10:15:43,675 WARN org.apache…DefaultContainerExecutor (ContainersLauncher #0): Exit code …"
	// HBase's daemons: "2026-09-29 06:00:00,369 INFO  [PEWorker-6] procedure.MasterProcedureScheduler: …".
	hbaseHeadRE  = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d{3} (TRACE|DEBUG|INFO|WARN|ERROR|FATAL)\s+\[[^\]]*\] (\S+?): ?(.*)$`)
	hadoopHeadRE = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d{3} (TRACE|DEBUG|INFO|WARN|ERROR|FATAL) (\S+?):?(?: \([^)]*\))?: ?(.*)$`)
	// The step controller: "2026-09-26T10:15:56.651Z WARN Step failed …", or "INFO startExec …" with no time.
	controllerHeadRE = regexp.MustCompile(`^(?:(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.\d{3}Z )?(TRACE|DEBUG|INFO|WARN|ERROR|FATAL) (.*)$`)
)

// parseHeader reads a log4j line prefix. Times carry no zone in these logs;
// they are read as UTC, the default on EMR nodes.
func parseHeader(kind FileKind, line string) (header, bool) {
	if len(line) < 6 {
		return header{}, false
	}
	if m := sparkHeadRE.FindStringSubmatch(line); m != nil {
		t, _ := time.Parse("06/01/02 15:04:05", m[1])
		return header{time: t, level: m[2], logger: m[3], msg: m[4]}, true
	}
	if m := hbaseHeadRE.FindStringSubmatch(line); m != nil {
		t, _ := time.Parse("2006-01-02 15:04:05", m[1])
		return header{time: t, level: m[2], logger: m[3], msg: m[4]}, true
	}
	if m := hadoopHeadRE.FindStringSubmatch(line); m != nil {
		t, _ := time.Parse("2006-01-02 15:04:05", m[1])
		return header{time: t, level: m[2], logger: m[3], msg: m[4]}, true
	}
	if kind == StepController {
		if m := controllerHeadRE.FindStringSubmatch(line); m != nil {
			var t time.Time
			if m[1] != "" {
				t, _ = time.Parse("2006-01-02T15:04:05", m[1])
			}
			return header{time: t, level: m[2], msg: m[3]}, true
		}
	}
	return header{}, false
}

// shortLogger is the class name without its package.
func shortLogger(l string) string {
	if i := strings.LastIndexByte(l, '.'); i >= 0 {
		return l[i+1:]
	}
	return l
}

var (
	// A Java exception headline: "java.io.IOException: msg", "Caused by: …",
	// py4j's ": java.io.FileNotFoundException: …", or an uncaught
	// 'Exception in thread "main" org.apache…SparkException: …'.
	javaExcRE = regexp.MustCompile(`^(?:Caused by: |Suppressed: |\t*Suppressed: |: |Exception in thread "[^"]*" )?((?:[A-Za-z_$][\w$]*\.)+[A-Z][\w$]*(?:Exception|Error|Throwable|Failure)[\w$]*)(?::\s*(.*))?$`)
	causeRE   = regexp.MustCompile(`^(?:Caused by: |: )`)
	atFrameRE = regexp.MustCompile(`^\s+at [\w.$<>/-]+\(`)
	moreRE    = regexp.MustCompile(`^\s*\.\.\. \d+ (?:more|common frames omitted)`)

	// Python: "  File "/…/job.py", line 7, in <module>" and the final
	// "py4j.protocol.Py4JJavaError: An error occurred …".
	pyFrameRE = regexp.MustCompile(`^\s*File "([^"]+)", line (\d+), in (.*)$`)
	pyExcRE   = regexp.MustCompile(`^([A-Za-z_][\w.]*(?:Error|Exception|Exit|Interrupt|Warning|Failure)|[A-Za-z_][\w]*\.[A-Za-z_][\w.]*): (.*)$`)
)

// Line rules, each checked against real EMR 7.3.0 logs or against the
// strings in Spark 3.5.1's and Hadoop 3.3's own classes.
var (
	// Driver (TaskSchedulerImpl, YarnAllocator, YarnSchedulerBackend, HeartbeatReceiver).
	lostExecutorRE  = regexp.MustCompile(`^Lost executor (\S+) on (\S+?): (.*)$`)
	removeExecRE    = regexp.MustCompile(`^Requesting driver to remove executor (\S+) for reason (.*)$`)
	heartbeatRE     = regexp.MustCompile(`^Removing executor (\S+) with no recent heartbeats: (\d+) ms exceeds timeout (\d+) ms`)
	markedFailedRE  = regexp.MustCompile(`^Container (marked as failed|from a bad node): (\S+) on host: (\S+?)\. Exit status: (-?\d+)\. Diagnostics: ?(.*)$`)
	completedRE     = regexp.MustCompile(`^Completed container (\S+)(?: on host: (\S+))? \(state: (\w+), exit status: (-?\d+)\)`)
	lostTaskRE      = regexp.MustCompile(`^Lost task (\S+) in stage (\S+) \(TID (\d+)\) \(([^)]*)\): (.*)$`)
	execLostFailRE  = regexp.MustCompile(`ExecutorLostFailure \(executor (\S+) exited (caused by one of the running tasks|unrelated to the running tasks)\) Reason: (.*)$`)
	finalStatusRE   = regexp.MustCompile(`^Final app status: (\w+), exitCode: (-?\d+)(?:, \(reason: (.*)\))?`)
	userAppExitRE   = regexp.MustCompile(`^User application exited with status (-?\d+)`)
	uncaughtRE      = regexp.MustCompile(`^Uncaught exception in thread (.*)$`)
	sparkCtxFailRE  = regexp.MustCompile(`^Error initializing SparkContext`)
	appDiagMsgRE    = regexp.MustCompile(`^Application diagnostics message: `)
	failingAppRE    = regexp.MustCompile(`^\. Failing the application\.?$`)
	yarnMemKilledRE = regexp.MustCompile(`(?i)Container killed by YARN for exceeding (physical |virtual )?memory limits`)

	// Executor (Executor, CoarseGrainedExecutorBackend, SignalUtils).
	taskExcRE  = regexp.MustCompile(`^Exception in task (\S+) in stage (\S+) \(TID (\d+)\)`)
	signalRE   = regexp.MustCompile(`^RECEIVED SIGNAL (\w+)`)
	selfExitRE = regexp.MustCompile(`^Executor self-exiting due to : (.*)$`)
	// The driver's own block manager names its host; YarnAllocator passes
	// on YARN's node updates, such as a spot node given notice
	// (DECOMMISSIONING), checked on the phase 4 test cluster.
	driverHostRE   = regexp.MustCompile(`^Registered BlockManager BlockManagerId\(driver, ([^,\s]+), \d+`)
	nodeStateRE    = regexp.MustCompile(`^Yarn node state updated for host (\S+) to (\w+)`)
	shutdownCmdRE  = regexp.MustCompile(`^Driver commanded a shutdown`)
	submittedAppRE = regexp.MustCompile(`^Submitted application (application_\d+_\d+)`)

	// spark-submit's YARN client in the step's stderr.
	appReportRE    = regexp.MustCompile(`^Application report for (application_\d+_\d+) \(state: (\w+)\)`)
	reportKeyRE    = regexp.MustCompile(`^\t (client token|diagnostics|ApplicationMaster host|ApplicationMaster RPC port|queue|start time|final status|tracking URL|user): ?(.*)$`)
	uploadRE       = regexp.MustCompile(`^Uploading resource (\S+) -> `)
	diagExitCodeRE = regexp.MustCompile(`exitCode: (-?\d+)`)

	// The step controller.
	startExecRE  = regexp.MustCompile(`^startExec '(.*)'$`)
	stepStatusRE = regexp.MustCompile(`^Step (succeeded|failed|cancelled) with exitCode (-?\d+) and took (\d+) seconds`)

	// NodeManager and ResourceManager.
	nmExitRE     = regexp.MustCompile(`^Exit code from container (\S+) is : (-?\d+)`)
	nmMemLimitRE = regexp.MustCompile(`(?i)Container \[pid=(\d+),containerID=(\S+?)\] is running (?:(\S+) )?beyond (?:the )?'?(physical|virtual)'? memory limits?\.? ?(.*)$`)
	rmAttemptRE  = regexp.MustCompile(`^Updating application attempt (appattempt_\d+_\d+_\d+) with final state: (\w+), and exit status: (-?\d+)`)
	rmSummaryRE  = regexp.MustCompile(`^appId=(application_\d+_\d+),`)
	summaryKVRE  = regexp.MustCompile(`(?:^|,)(\w+)=((?:[^,\\]|\\.)*)`)

	// Capacity and requests (ResourceManager, NodeManager, driver, step).
	rmCapacityRE  = regexp.MustCompile(`^NodeManager from node ([^\s(]+)\(.*registered with capability: <memory:(\d+), vCores:(\d+)`)
	nmCapacityRE  = regexp.MustCompile(`^Registered with ResourceManager as ([^\s:]+):\d+ with total resource of <memory:(\d+), vCores:(\d+)`)
	assignedRE    = regexp.MustCompile(`^Assigned container (\S+) of capacity <memory:(\d+), max memory:(\d+), vCores:(\d+), max vCores:(\d+)> on host ([^\s:]+)(?::\d+)?, which has (\d+) containers, <memory:(\d+), vCores:\d+> used and <memory:(\d+), vCores:\d+> available`)
	willRequestRE = regexp.MustCompile(`^Will request (\d+) executor container\(s\) for\s+ResourceProfile Id: (\d+), each with (\d+) core\(s\) and (\d+) MB memory`)
	cancelRE      = regexp.MustCompile(`^Canceling requests for (\d+) executor container\(s\) to have a new desired total (\d+) executors`)
	desiredRE     = regexp.MustCompile(`^Driver requested a total number of (\d+) executor\(s\)`)
	launchHeapRE  = regexp.MustCompile(`^Launching executor with (\d+)m of heap \(plus (\d+)m overhead/off heap\) and (\d+) cores`)
	amRequestRE   = regexp.MustCompile(`^Will allocate AM container, with (\d+) MB memory including (\d+) MB overhead`)
	maxAllocRE    = regexp.MustCompile(`maximum memory capability of the cluster \((\d+) MB per container\)`)

	// EMR's instance controller in bootstrap-actions/master.log.
	bootstrapFailRE = regexp.MustCompile(`^(i-\w+): failed to start\. bootstrap action (\d+) failed with non-zero exit code`)
	bootstrapDoneRE = regexp.MustCompile(`^(i-\w+): all bootstrap actions complete`)

	// HotSpot's banner when a JVM started with -XX:OnOutOfMemoryError runs
	// out of memory (Spark starts every executor with "kill -9 %p"), then
	// the command it runs. Checked on the phase 3 test cluster.
	hotspotOOMRE  = regexp.MustCompile(`^#\s+java\.lang\.OutOfMemoryError: ?(.*)$`)
	hotspotKillRE = regexp.MustCompile(`^#\s+Executing /bin/sh -c "kill -9`)

	// Anywhere. Checked in this order; the first match names the line.
	oomRE       = regexp.MustCompile(`java\.lang\.OutOfMemoryError(?::\s*(.*))?|^MemoryError\b`)
	accessRE    = regexp.MustCompile(`(?i)access denied|\bAccessDenied|status code: 403|403 Forbidden|is not authorized to perform|insufficient lake formation permission|\bExpiredToken\b|\bInvalidAccessKeyId\b|\bSignatureDoesNotMatch\b|UnrecognizedClientException`)
	authActRE   = regexp.MustCompile(`is not authorized to perform: (\S+)(?: on resource: (\S+))?`)
	s3PathRE    = regexp.MustCompile(`\bs3[an]?://[^\s'"]+`)
	kerberosRE  = regexp.MustCompile(`GSSException|KrbException|javax\.security\.auth\.login\.LoginException|Client cannot authenticate via:\s*\[[^\]]*KERBEROS|SASL authentication failed|(?i)kerberos (?:login|authentication) fail`)
	krbLoginRE  = regexp.MustCompile(`Login successful for user (\S+) using keytab file (\S+)`)
	metaFailRE  = regexp.MustCompile(`Failed to connect to the MetaStore Server|Could not connect to meta ?store|MetaException|Unable to instantiate \S*MetaStoreClient|AWSGlue\w*Exception|services\.glue\.model\.\w+Exception`)
	metaConnRE  = regexp.MustCompile(`Trying to connect to metastore with URI (\S+)|Connected to metastore|Opened a connection to metastore`)
	hbaseFailRE = regexp.MustCompile(`org\.apache\.hadoop\.hbase\.\S*(?:Exception|Error)|RetriesExhaustedException|KeeperException|failed for get of \S+, code = |Can't get connection to ZooKeeper|Unable to (?:set watcher|connect) (?:on znode|to ZooKeeper)`)
	hbaseConnRE = regexp.MustCompile(`Initiating client connection, connectString=(\S+)|hbase\.zookeeper\.quorum`)

	// What an HBase client error shows, most specific first, because each
	// has its own fix. Checked on the phase 5 test cluster (HBase 2.4.17):
	// a missing table, a wrong ZooKeeper port, expired scanner leases and
	// a region over its memstore limit. Region moves and a stopped region
	// server left no client line there; their exceptions are HBase's own
	// class names.
	hbaseTableRE   = regexp.MustCompile(`TableNotFoundException: ([\w:.-]+)`)
	hbaseZKRE      = regexp.MustCompile(`KeeperException\$ConnectionLossException|KeeperException\$SessionExpiredException|failed for get of \S+, code = CONNECTIONLOSS|Can't get connection to ZooKeeper|Unable to (?:set watcher|connect) (?:on znode|to ZooKeeper)`)
	hbaseScannerRE = regexp.MustCompile(`UnknownScannerException|ScannerTimeoutException|regionserver\.LeaseException|OutOfOrderScannerNextException`)
	hbaseBusyRE    = regexp.MustCompile(`RegionTooBusyException|CallQueueTooBigException`)
	hbaseMovedRE   = regexp.MustCompile(`NotServingRegionException|RegionMovedException|RegionOpeningException`)
	hbaseServerRE  = regexp.MustCompile(`CallTimeoutException|ConnectionClosingException|ConnectionClosedException|ServerNotRunningYetException|RegionServerStoppedException|RegionServerAbortedException`)
	hbaseRetryRE   = regexp.MustCompile(`RetriesExhausted(?:WithDetails)?Exception`)
	hbaseAccessRE  = regexp.MustCompile(`hbase\.security\.AccessDeniedException`)
	// Where the error happened: the table, the region server (host,port,
	// start code) and the ZooKeeper address the client tried.
	hbaseTableAtRE  = regexp.MustCompile(`\btable=([\w:.-]+)|Failed \d+ actions?: ([\w:.-]+):`)
	hbaseServerAtRE = regexp.MustCompile(`\bserver=([\w.-]+),\d+,\d+`)
	hbaseZKAtRE     = regexp.MustCompile(`to (\S+:\d+) failed for get of`)
	hbaseLimitRE    = regexp.MustCompile(`Over memstore limit=([\d.]+ ?[KMGT]?)`)
	// HBase's client logs its retries of a batch at INFO, with the error
	// that made it retry: "id=10, table=sp_hot, attempt=6/16,
	// failureCount=2048ops, last exception=…RegionTooBusyException: …".
	hbaseAsyncRE = regexp.MustCompile(`^id=\d+, table=([\w:.-]+), attempt=(\d+)/(\d+), failureCount=(\d+)ops, last exception=`)
	// Tables used through HBase's MapReduce API (checked on the phase 5
	// test cluster): the driver sizing a table's regions before a
	// TableInputFormat scan, each executor task's region split and the
	// region server holding it, and TableOutputFormat opening a table.
	hbaseSizingRE = regexp.MustCompile(`^Calculating region sizes for table "([\w:.-]+)"`)
	hbaseSplitRE  = regexp.MustCompile(`^Input split: Split\(tablename=([\w:.-]+), .*\bregionLocation=([\w.-]+), regionname=`)
	hbaseOutputRE = regexp.MustCompile(`^Created table instance for ([\w:.-]+)`)
	// The ZooKeeper client's connection line, up to its quorum: the
	// watcher after it differs on every connection.
	zkConnectRE = regexp.MustCompile(`^Initiating client connection, connectString=\S+`)

	// HBase's own logs (phase 5 step 7). Checked on the test cluster (HBase
	// 2.4.17): the Master finishing a region move or split and scheduling
	// a lost region server's recovery; a region server refusing writes,
	// dropping an idle scanner, flushing and compacting, and stopping. JVM
	// pauses, slow calls and store-file pile-ups did not happen there; those
	// rules use HBase 2.4's own messages.
	hbMovedRE   = regexp.MustCompile(`^Finished pid=\d+, state=SUCCESS; TransitRegionStateProcedure table=([\w:.-]+), region=\w+, REOPEN/MOVE in `)
	hbSplitRE   = regexp.MustCompile(`^Finished pid=\d+, state=SUCCESS; SplitTableRegionProcedure table=([\w:.-]+), `)
	hbCrashRE   = regexp.MustCompile(`^Scheduled ServerCrashProcedure pid=\d+ for ([\w.-]+),\d+,\d+ \(carryingMeta=(true|false)\).*?regionCount=(\d+)`)
	hbBusyRE    = regexp.MustCompile(`^Region is too busy due to exceeding memstore size limit`)
	hbLeaseRE   = regexp.MustCompile(`^Scanner lease -?\d+ expired clientIPAndPort=([\d.]+):\d+, userName=([^,]+), regionInfo=([\w:.-]+),`)
	hbStoppedRE = regexp.MustCompile(`^STOPPED: (.*)$`)
	hbAbortRE   = regexp.MustCompile(`^\*+ ABORTING region server ([\w.-]+),\d+,\d+: (.*?)\s*\**$`)
	hbPauseRE   = regexp.MustCompile(`^Detected pause in JVM or host machine \(eg GC\): pause of approximately (\d+)ms`)
	hbSlowRE    = regexp.MustCompile(`^\(response(TooSlow|TooLarge)\): `)
	hbSlowMsRE  = regexp.MustCompile(`"processingtimems":(\d+)`)
	hbSlowOpRE  = regexp.MustCompile(`"method":"(\w+)"`)
	hbSlowTblRE = regexp.MustCompile(`region= ([\w:.-]+),`)
	hbStoreRE   = regexp.MustCompile(`^([\w:.-]+),\S* has too many store files`)
	hbFlushRE   = regexp.MustCompile(`^Flushing [0-9a-f]{32} \d+/\d+ column families, dataSize=`)
	hbCompactRE = regexp.MustCompile(`^Completed compaction region=([\w:.-]+),`)

	// A file YARN localized for the application, as the application
	// master lists them: "    hbase-client-2.4.17-amzn-7.jar -> resource { scheme: …".
	localizedRE = regexp.MustCompile(`^\s+([\w.+-]+) -> resource \{`)

	// A class missing, or of another version, at run time. The first stack
	// frame that names its jar (log4j adds "~[jar:version]") is the code
	// that needed it.
	classpathRE = regexp.MustCompile(`java\.lang\.(NoClassDefFoundError|ClassNotFoundException|NoSuchMethodError|NoSuchFieldError|AbstractMethodError|IncompatibleClassChangeError)(?::\s*(?:Could not initialize class\s+)?(\S+))?`)
	frameJarRE  = regexp.MustCompile(`^\s+at .*~?\[([\w.+-]+\.jar):[^\]]*\]`)

	// Application, attempt and container IDs, to keep node logs to one app.
	idRE = regexp.MustCompile(`(?:application|appattempt|container(?:_e\d+)?)_(\d{10,}_\d{4,})`)
)

// ContainerExitMeaning explains a container's exit status in plain words:
// YARN's own negative codes (ContainerExitStatus), Spark's executor codes
// (SparkExitCode, ExecutorExitCode), Spark's application master codes, and
// signals (128 + n).
func ContainerExitMeaning(code int, driver bool) string {
	if driver {
		switch code {
		case 10:
			return "the driver hit an uncaught exception"
		case 11:
			return "too many executors failed (spark.executor.maxNumFailures)"
		case 12:
			return "the application master could not keep reporting to YARN"
		case 13:
			return "SparkContext never started: the application failed before or while creating it"
		case 15:
			return "the application's main class threw an exception"
		case 16:
			return "the application ended before SparkContext was created"
		case 17:
			return "the application master lost its connection to the driver"
		}
	}
	switch code {
	case 0:
		return "finished normally"
	case 1:
		return "the process failed with a general error; its stderr says why"
	case 50:
		return "the executor hit an uncaught exception"
	case 51:
		return "the executor hit an uncaught exception, and another while handling it"
	case 52:
		return "the executor ran out of Java heap memory (OutOfMemoryError)"
	case 53:
		return "the executor could not create its local disk directories"
	case 56:
		return "the executor could not reach the driver with heartbeats"
	case 127:
		return "a command was not found"
	case 134:
		return "the JVM aborted (signal 6), often a native crash; look for an hs_err file"
	case 137:
		return "killed with signal 9 (SIGKILL): on YARN usually a memory-limit kill or the node's out-of-memory killer"
	case 139:
		return "the process crashed with a segmentation fault (signal 11)"
	case 143:
		return "stopped with signal 15 (SIGTERM): normal when Spark releases an executor or the application ends"
	case -100:
		return "YARN released the container (aborted), for example when its node was lost"
	case -101:
		return "the node's disks failed"
	case -102:
		return "YARN preempted the container for another application"
	case -103:
		return "YARN killed the container for exceeding its virtual memory limit"
	case -104:
		return "YARN killed the container for exceeding its physical memory limit"
	case -105:
		return "the application master asked YARN to kill it"
	case -106:
		return "the ResourceManager killed it"
	case -107:
		return "killed after the application finished"
	case -108:
		return "killed by the container scheduler, for example to make room"
	case -109:
		return "killed for writing too much log output"
	}
	if code > 128 && code < 160 {
		return "stopped by signal " + strconv.Itoa(code-128)
	}
	return "exited with status " + strconv.Itoa(code) + "; its stderr says why"
}

// exitSeverity ranks a container exit status on its own; joining with the
// event log (SPEC §8 phase 2 step 5) can soften it, for example for 143 at
// the end of a successful application.
func exitSeverity(code int) model.Severity {
	switch code {
	case 0, 143, -105, -107:
		return model.Info
	case 137, 52, -103, -104:
		return model.Critical
	}
	return model.Warning
}
