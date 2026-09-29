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
	ContainerStderr FileKind = "container-stderr"   // a driver's or executor's stderr (also prelaunch.err)
	ContainerStdout FileKind = "container-stdout"   // a driver's or executor's stdout: Python tracebacks, prints
	StepController  FileKind = "step-controller"    // EMR's step runner: the command and the step's exit code
	StepStderr      FileKind = "step-stderr"        // spark-submit's own output: submission and YARN's final report
	NodeManager     FileKind = "nodemanager"        // a node's YARN NodeManager: container exits and memory kills
	ResourceManager FileKind = "resourcemanager"    // the primary node's YARN ResourceManager: attempts and the app summary
	Bootstrap       FileKind = "bootstrap"          // a node's bootstrap-actions/master.log
	BootstrapOutput FileKind = "bootstrap-output"   // one bootstrap action's stderr or stdout
	HBaseMaster     FileKind = "hbase-master"       // the HBase Master's log: region moves, splits, lost region servers
	HBaseRegion     FileKind = "hbase-regionserver" // a region server's log: busy regions, scanner leases, slow calls, stops
	Unread          FileKind = ""                   // a file sparkplain does not classify
)

// File is what a log's path says about it, under the cluster's log root
// (<LogUri>/<cluster-id>/, SPEC §2).
type File struct {
	Kind      FileKind
	App       string // application_…, for container logs
	Container string // container_…, for container logs
	Step      string // s-…, for step logs
	Instance  string // i-…, for node logs
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
