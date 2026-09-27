package model

import "time"

// LogKind names what a classified container, step or node log line shows.
type LogKind string

const (
	LogException    LogKind = "exception"      // a Java exception, with its cause chain
	LogTraceback    LogKind = "traceback"      // a Python traceback
	LogOutOfMemory  LogKind = "out-of-memory"  // a JVM or Python out-of-memory error
	LogMemoryKill   LogKind = "memory-kill"    // YARN killed a container over its memory limit
	LogContainerEnd LogKind = "container-exit" // a container ended with a non-zero status
	LogAppExit      LogKind = "app-exit"       // the application master's final status, per attempt
	LogLostExecutor LogKind = "lost-executor"  // the driver lost an executor, and why
	LogTaskError    LogKind = "task-error"     // a task failed (executor or driver side)
	LogSignal       LogKind = "signal"         // a process received a signal
	LogAccess       LogKind = "access-denied"  // S3, IAM, Glue, Lake Formation or KMS refused access
	LogKerberos     LogKind = "kerberos"       // a Kerberos or SASL failure
	LogMetastore    LogKind = "metastore"      // Hive metastore or Glue catalog connections and failures
	LogHBase        LogKind = "hbase"          // HBase and ZooKeeper connections and failures
	LogIdentity     LogKind = "identity"       // who the application ran as: user, queue, principal
	LogSubmit       LogKind = "submit"         // the command a step ran (redacted)
	LogSubmitted    LogKind = "submitted"      // the application a step submitted
	LogResource     LogKind = "resource"       // a file spark-submit uploaded, such as the script
	LogStepStatus   LogKind = "step-status"    // how a step ended
	LogAppReport    LogKind = "app-report"     // YARN's final report on the application
	LogAppSummary   LogKind = "app-summary"    // the ResourceManager's summary: user, queue, resources used
	LogBootstrap    LogKind = "bootstrap"      // a bootstrap action's outcome
	LogError        LogKind = "error"          // any other ERROR or FATAL line
	// What YARN had and what Spark asked for (phase 3, step 2).
	LogNodeCapacity      LogKind = "node-capacity"      // a node's YARN memory and vCores
	LogContainerAssigned LogKind = "container-assigned" // YARN placed one of the application's containers
	LogYarnRequest       LogKind = "yarn-request"       // Spark asked YARN for (or cancelled) containers of a size
	// Where each attempt ran, and nodes YARN said were leaving.
	LogDriverHost LogKind = "driver-host" // the host an attempt's driver ran on
	LogNodeState  LogKind = "node-state"  // YARN told the driver a node is decommissioning or lost
)

// LogLine is one classified line of a container, step or node log, or one
// multi-line block (an exception and its stack, a traceback, a YARN report)
// starting there. Repeats in the same file are folded into Count.
type LogLine struct {
	Kind     LogKind  `json:"kind"`
	Severity Severity `json:"severity"`
	Source   Source   `json:"source"`
	// Time is when the line was logged, read as UTC (EMR's default), or
	// zero when the line carries none.
	Time time.Time `json:"time,omitzero"`
	// Text is the line itself, redacted.
	Text string `json:"text"`
	// Detail holds what explains it, redacted: an exception's headline and
	// causes with the application's own frames, or a traceback's frames.
	Detail []string `json:"detail,omitempty"`
	// Fields are values read from the line, such as exitCode, container,
	// executor, stage, user or queue.
	Fields map[string]string `json:"fields,omitempty"`
	Count  int               `json:"count"`
	// LastLine is the line of the last repeat, when Count > 1.
	LastLine int64 `json:"lastLine,omitempty"`
}
