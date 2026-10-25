package yarnlog

import (
	"path"
	"regexp"
	"strings"
	"time"
)

// FileKind says which classifier rules apply to a log file.
type FileKind string

const (
	ContainerStderr  FileKind = "container-stderr"   // a driver's or executor's stderr (also prelaunch.err)
	ContainerStdout  FileKind = "container-stdout"   // a driver's or executor's stdout: Python tracebacks, prints
	StepController   FileKind = "step-controller"    // EMR's step runner: the command and the step's exit code
	StepStderr       FileKind = "step-stderr"        // spark-submit's own output: submission and YARN's final report
	StepStdout       FileKind = "step-stdout"        // the step's stdout: in client mode, what the driver printed
	NodeManager      FileKind = "nodemanager"        // a node's YARN NodeManager: container exits and memory kills
	ResourceManager  FileKind = "resourcemanager"    // the primary node's YARN ResourceManager: attempts and the app summary
	Bootstrap        FileKind = "bootstrap"          // a node's bootstrap-actions/master.log
	BootstrapOutput  FileKind = "bootstrap-output"   // one bootstrap action's stderr or stdout
	HBaseMaster      FileKind = "hbase-master"       // the HBase Master's log: region moves, splits, lost region servers
	HBaseRegion      FileKind = "hbase-regionserver" // a region server's log: busy regions, scanner leases, slow calls, stops
	StandaloneWorker FileKind = "standalone-worker"  // a Spark Standalone worker's daemon log: executors launched and how they ended
	StandaloneMaster FileKind = "standalone-master"  // a Spark Standalone master's daemon log: applications and executors placed
	Unread           FileKind = ""                   // a file sparkplain does not classify
)

// File is what a log's path says about it, under the cluster's log root
// (<LogUri>/<cluster-id>/, SPEC §2).
type File struct {
	Kind      FileKind
	App       string // application_…, for container logs
	Container string // container_…, for container logs
	Step      string // s-…, for step logs
	Instance  string // i-…, for node logs
	// Executor is a Spark Standalone executor's ID, from its log's folder
	// (work/<app-id>/<executor-id>/stderr); its logs have no container.
	Executor string
	// Daemon is a Spark Standalone daemon log's process ("worker-1"), so a
	// worker restarted into rolled logs (.out.1, …) is one worker.
	Daemon string
	// Host and Hour are an HBase log's host and, for a log rolled each
	// hour, the hour (UTC) it holds; zero for the current log.
	Host string
	Hour time.Time
}

// hbaseLogRE reads an HBase daemon log's name, as EMR keeps it:
// hbase-hbase-regionserver-<host>.log, and rolled each hour,
// hbase-hbase-regionserver-<host>.log.2026-09-29-06.
var hbaseLogRE = regexp.MustCompile(`^hbase-[\w-]+?-(master|regionserver)-(.+?)\.log(?:\.(\d{4}-\d{2}-\d{2}-\d{2}))?$`)

// Driver reports whether a container log is an application master's, which
// in cluster mode is the driver: the first container of each attempt.
func (f File) Driver() bool {
	return f.Container != "" && strings.HasSuffix(f.Container, "_000001")
}

// DescribeStandalone reads the key of a Spark Standalone executor's log:
// <app-id>/<executor-id>/stderr (or stdout) under a worker's work folder,
// with anything in front, such as work/ or <host>/work/ for copies from
// several machines. The executor ID is the folder's name. The folder above
// the application's, when it is neither work nor missing, is taken as the
// machine's name. Anything else is Unread.
func DescribeStandalone(key, appID string) File {
	parts := strings.Split(key, "/")
	n := len(parts)
	if n < 3 || parts[n-3] != appID || !allDigits(parts[n-2]) {
		return File{}
	}
	f := File{App: appID, Executor: parts[n-2]}
	switch parts[n-1] {
	case "stderr":
		f.Kind = ContainerStderr
	case "stdout":
		f.Kind = ContainerStdout
	default:
		return File{}
	}
	i := n - 4
	if i >= 0 && parts[i] == "work" {
		i--
	}
	if i >= 0 {
		f.Host = parts[i]
	}
	return f
}

// DescribeStandaloneDriver reads the key of a Spark Standalone driver's
// log in cluster deploy mode: driver-<id>/stderr (or stdout) under a
// worker's work folder. Which application the driver ran is not in the
// path; the caller ties it (StandaloneDriverID, or the log's own line).
func DescribeStandaloneDriver(key string) File {
	parts := strings.Split(key, "/")
	n := len(parts)
	if n < 2 || !strings.HasPrefix(parts[n-2], "driver-") {
		return File{}
	}
	f := File{Executor: "driver"}
	switch parts[n-1] {
	case "stderr":
		f.Kind = ContainerStderr
	case "stdout":
		f.Kind = ContainerStdout
	default:
		return File{}
	}
	i := n - 3
	if i >= 0 && parts[i] == "work" {
		i--
	}
	if i >= 0 {
		f.Host = parts[i]
	}
	return f
}

// standaloneDaemonRE reads a Standalone daemon log's name, as
// spark-daemon.sh writes it: spark-<user>-org.apache.spark.deploy.worker.
// Worker-<n>-<host>.out, rolled at each start to .out.1 to .out.5.
var standaloneDaemonRE = regexp.MustCompile(`^spark-.*-org\.apache\.spark\.deploy\.(worker\.Worker|master\.Master)-(\d+)-(.+)\.out(?:\.\d+)?$`)

// DescribeStandaloneDaemon reads the key of a Spark Standalone worker's or
// master's daemon log, anywhere in a copy (usually $SPARK_HOME/logs/); the
// host is the one in its name.
func DescribeStandaloneDaemon(key string) File {
	m := standaloneDaemonRE.FindStringSubmatch(path.Base(key))
	if m == nil {
		return File{}
	}
	f := File{Host: m[3], Kind: StandaloneMaster, Daemon: "master-" + m[2]}
	if m[1] == "worker.Worker" {
		f.Kind, f.Daemon = StandaloneWorker, "worker-"+m[2]
	}
	return f
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

var containerRE = regexp.MustCompile(`^container_(?:e\d+_)?\d+_\d+_\d+_\d+$`)

// Describe reads a key such as containers/<app>/<container>/stderr.gz,
// steps/<step>/controller.gz or node/<instance>/applications/hadoop-yarn/…
// relative to the cluster's log root; a longer key with the log root in
// front works too.
func Describe(key string) File {
	parts := strings.Split(key, "/")
	base := strings.TrimSuffix(strings.TrimSuffix(path.Base(key), ".gz"), ".bz2")
	for i, p := range parts {
		switch {
		case p == "containers" && i+3 < len(parts) && strings.HasPrefix(parts[i+1], "application_") && containerRE.MatchString(parts[i+2]):
			f := File{App: parts[i+1], Container: parts[i+2]}
			switch base {
			case "stderr", "prelaunch.err":
				f.Kind = ContainerStderr
			case "stdout":
				f.Kind = ContainerStdout
			}
			return f
		case p == "steps" && i+2 < len(parts) && strings.HasPrefix(parts[i+1], "s-"):
			f := File{Step: parts[i+1]}
			switch base {
			case "controller":
				f.Kind = StepController
			case "stderr":
				f.Kind = StepStderr
			case "stdout":
				f.Kind = StepStdout
			}
			return f
		case p == "node" && i+2 < len(parts) && strings.HasPrefix(parts[i+1], "i-"):
			f := File{Instance: parts[i+1]}
			rest := parts[i+2:]
			switch {
			case rest[0] == "bootstrap-actions" && base == "master.log":
				f.Kind = Bootstrap
			case rest[0] == "bootstrap-actions" && (base == "stderr" || base == "stdout"):
				f.Kind = BootstrapOutput
			case rest[0] == "applications" && strings.Contains(base, "nodemanager") && strings.Contains(base, ".log"):
				f.Kind = NodeManager
			case rest[0] == "applications" && strings.Contains(base, "resourcemanager") && strings.Contains(base, ".log"):
				f.Kind = ResourceManager
			case rest[0] == "applications" && len(rest) > 1 && rest[1] == "hbase":
				if m := hbaseLogRE.FindStringSubmatch(base); m != nil {
					f.Kind, f.Host = HBaseRegion, m[2]
					if m[1] == "master" {
						f.Kind = HBaseMaster
					}
					if m[3] != "" {
						f.Hour, _ = time.Parse("2006-01-02-15", m[3])
					}
				}
			}
			return f
		}
	}
	return File{}
}
