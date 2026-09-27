// Package yarnlog classifies the lines of an EMR cluster's container, step
// and node logs (SPEC §3): exceptions and tracebacks with their causes,
// out-of-memory errors and memory kills, container exit codes, lost
// executors, access, Kerberos, metastore and HBase errors, the command a
// step ran and how it ended, and who the application ran as. Every line it
// keeps is redacted and records its file and line.
//
// Files stream through in one pass; only the classified lines are kept,
// with repeats folded.
package yarnlog

import (
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/redact"
)

// Options tune one file's classification.
type Options struct {
	// AppID keeps NodeManager and ResourceManager lines to this
	// application's own containers and attempts; these daemons log every
	// application on the node. Empty keeps all.
	AppID string
	// MaxEntries caps the distinct lines kept per file; default 500.
	MaxEntries int
}

// Result is what one file held.
type Result struct {
	Name      string          // the file's name, as given
	File      File            // what its path says
	Lines     []model.LogLine // in file order
	Read      int64           // lines read
	Truncated int             // lines longer than 64 KiB, cut
	Dropped   int             // distinct lines past MaxEntries, not kept
}

// Caps on what one kept line carries.
const (
	maxText        = 2000
	maxDetailLine  = 500
	maxDetail      = 40
	maxBlockLines  = 400  // lines of one exception or traceback read for its detail
	maxQuotedLines = 2000 // lines of YARN's quoted diagnostics skipped before giving up
	maxUserFrames  = 3
)

// Classify reads one log file and returns the lines it recognises. name is
// recorded as each line's source file; f says which rules apply (see
// Describe). An error means the file could not be read to its end; what was
// read before it is still returned.
func Classify(r io.Reader, name string, f File, opt Options) (Result, error) {
	if opt.MaxEntries <= 0 {
		opt.MaxEntries = 500
	}
	c := &classifier{res: Result{Name: name, File: f}, opt: opt, index: map[string]int{}, lastOOM: -1}
	if m := idRE.FindStringSubmatch(opt.AppID); m != nil {
		c.appKey = m[1]
	}
	lr := newLineReader(r)
	var err error
	for {
		var b []byte
		b, err = lr.next()
		if err != nil {
			break
		}
		c.n++
		c.feed(string(b))
	}
	c.closeReport()
	c.flush()
	if c.maxRequestLine != nil {
		c.add(c.maxRequestLine)
	}
	if c.maxDesiredLine != nil {
		c.add(c.maxDesiredLine)
	}
	c.res.Read, c.res.Truncated = c.n, lr.truncated
	if err == io.EOF {
		err = nil
	}
	return c.res, err
}

type classifier struct {
	res    Result
	opt    Options
	appKey string // "<ts>_<n>" of opt.AppID
	index  map[string]int
	n      int64

	blk *block  // an open exception, traceback or lead line with its stack
	rep *report // an open YARN application report (step stderr)

	quoting  int  // lines left to skip in YARN's quoted diagnostics; 0 when not quoting
	shutdown bool // the driver told this executor to stop, so SIGTERM is expected

	lastApp, lastState string // from the last "Application report for …"

	// lastTime is the last logged time seen, given to lines that carry none
	// (stacks, HotSpot's banner), so they sort near where they happened.
	lastTime time.Time
	lastOOM  int // index of the last HotSpot out-of-memory line, or -1

	// The most executors the driver asked for at once, kept as one line.
	maxDesired     int
	maxDesiredLine *model.LogLine
	maxRequest     int
	maxRequestLine *model.LogLine
}

// block is a multi-line entry being read: an exception with its stack, a
// traceback, or a classified line followed by the stack it logged.
type block struct {
	lead  *model.LogLine // the classified line that opened it, or nil
	start int64
	end   int64
	lines []string
}

// report is spark-submit's multi-line "Application report" in a step's
// stderr, whose diagnostics can quote the driver's stderr at length.
type report struct {
	start, end int64
	time       time.Time
	keys       map[string]string
	cur        string
	diag       []string
}

func (c *classifier) feed(line string) {
	kind := c.res.File.Kind
	if c.quoting > 0 {
		// YARN's diagnostics quote the driver's own stderr, which was (or
		// will be) classified from the driver's log: skip it here.
		switch {
		case failingAppRE.MatchString(line):
			c.quoting = 0
			return
		case strings.HasPrefix(line, "Exception in thread "):
			c.quoting = 0
		default:
			c.quoting--
			return
		}
	}
	if c.rep != nil {
		if m := reportKeyRE.FindStringSubmatch(line); m != nil {
			c.rep.cur, c.rep.end = m[1], c.n
			c.rep.keys[m[1]] = m[2]
			return
		}
		if c.rep.cur == "diagnostics" && c.n-c.rep.start < maxQuotedLines {
			if len(c.rep.diag) < maxBlockLines {
				c.rep.diag = append(c.rep.diag, line)
			}
			c.rep.end = c.n
			return
		}
		c.closeReport()
	}
	if h, ok := parseHeader(kind, line); ok {
		c.flush()
		if !h.time.IsZero() {
			c.lastTime = h.time
		}
		c.header(h, line)
		return
	}
	if m := hotspotOOMRE.FindStringSubmatch(line); m != nil {
		c.flush()
		l := c.entry(model.LogOutOfMemory, model.Critical, c.lastTime, "java.lang.OutOfMemoryError: "+m[1])
		l.Fields["oom"], l.Fields["root"], l.Fields["rootMessage"] = redact.Text(m[1]), "java.lang.OutOfMemoryError", redact.Text(m[1])
		c.add(l)
		c.lastOOM = len(c.res.Lines) - 1
		return
	}
	if hotspotKillRE.MatchString(line) {
		if c.lastOOM >= 0 && c.lastOOM < len(c.res.Lines) {
			c.res.Lines[c.lastOOM].Fields["selfKilled"] = "true" // the JVM ran kill -9 on itself: exit 137 without YARN
		}
		return
	}
	if c.blk != nil {
		if strings.TrimSpace(line) == "" {
			c.flush()
			return
		}
		if len(c.blk.lines) < maxBlockLines {
			c.blk.lines = append(c.blk.lines, line)
		}
		c.blk.end = c.n
		return
	}
	switch kind {
	case NodeManager, ResourceManager, Bootstrap, StepController:
		return // daemons quote other processes' stacks; those are read from their own logs
	}
	if javaExcRE.MatchString(line) || strings.HasPrefix(line, "Traceback (most recent call last):") {
		c.blk = &block{start: c.n, end: c.n, lines: []string{line}}
	}
}

// entry starts a kept line at the current position.
func (c *classifier) entry(kind model.LogKind, sev model.Severity, t time.Time, text string) *model.LogLine {
	return &model.LogLine{Kind: kind, Severity: sev, Time: t, Text: clip(redact.Text(text), maxText),
		Source: model.Source{File: c.res.Name, Line: c.n}, Fields: map[string]string{}, Count: 1}
}

// lead keeps l open so a stack logged after it joins its detail.
func (c *classifier) lead(l *model.LogLine) {
	c.blk = &block{lead: l, start: c.n, end: c.n}
}

func (c *classifier) header(h header, line string) {
	msg := h.msg
	switch c.res.File.Kind {
	case StepController:
		c.controller(h)
		return
	case ResourceManager:
		if !c.capacity(h) && c.mine(line) {
			c.resourceManager(h)
		}
		return
	case NodeManager:
		if !c.capacity(h) && c.mine(line) {
			c.nodeManager(h)
		}
		return
	case Bootstrap:
		if m := bootstrapFailRE.FindStringSubmatch(msg); m != nil {
			// EMR writes this for the primary node even on clusters with no
			// bootstrap actions (all four test clusters, whose
			// DescribeCluster lists none), so it is only info until the
			// cluster's state reason confirms it.
			l := c.entry(model.LogBootstrap, model.Info, h.time, msg)
			l.Fields["instance"], l.Fields["action"] = m[1], m[2]
			c.add(l)
		}
		return
	}
	lg := shortLogger(h.logger)
	problem := h.level == "WARN" || h.level == "ERROR" || h.level == "FATAL"

	// spark-submit's YARN client, in the step's stderr.
	if c.res.File.Kind == StepStderr {
		switch {
		case lg == "Client" && strings.TrimSpace(msg) == "":
			c.rep = &report{start: c.n, end: c.n, time: h.time, keys: map[string]string{}}
			return
		case appDiagMsgRE.MatchString(msg):
			c.quoting = maxQuotedLines // repeats the report's diagnostics
			return
		}
		if m := appReportRE.FindStringSubmatch(msg); m != nil {
			c.lastApp, c.lastState = m[1], m[2]
			return
		}
		if m := submittedAppRE.FindStringSubmatch(msg); m != nil {
			l := c.entry(model.LogSubmitted, model.Info, h.time, msg)
			l.Fields["app"] = m[1]
			c.add(l)
			return
		}
		if m := uploadRE.FindStringSubmatch(msg); m != nil {
			if strings.HasPrefix(m[1], "s3") {
				l := c.entry(model.LogResource, model.Info, h.time, msg)
				l.Fields["path"] = redact.Text(m[1])
				c.add(l)
			}
			return
		}
	}

	var l *model.LogLine
	switch {
	case lostExecutorRE.MatchString(msg):
		m := lostExecutorRE.FindStringSubmatch(msg)
		l = c.entry(model.LogLostExecutor, lostSeverity(m[3]), h.time, msg)
		l.Fields["executor"], l.Fields["host"], l.Fields["reason"] = m[1], m[2], redact.Text(m[3])
	case removeExecRE.MatchString(msg):
		m := removeExecRE.FindStringSubmatch(msg)
		l = c.entry(model.LogLostExecutor, lostSeverity(m[2]), h.time, msg)
		l.Fields["executor"], l.Fields["reason"] = m[1], redact.Text(m[2])
	case heartbeatRE.MatchString(msg):
		m := heartbeatRE.FindStringSubmatch(msg)
		l = c.entry(model.LogLostExecutor, model.Warning, h.time, msg)
		l.Fields["executor"], l.Fields["reason"] = m[1], "no heartbeat for "+m[2]+" ms (timeout "+m[3]+" ms)"
	case markedFailedRE.MatchString(msg):
		m := markedFailedRE.FindStringSubmatch(msg)
		code, _ := strconv.Atoi(m[4])
		sev := exitSeverity(code)
		if yarnMemKilledRE.MatchString(m[5]) {
			sev = model.Critical
		}
		l = c.entry(model.LogContainerEnd, sev, h.time, msg)
		l.Fields["container"], l.Fields["host"], l.Fields["exitCode"] = m[2], m[3], m[4]
		l.Fields["meaning"] = ContainerExitMeaning(code, strings.HasSuffix(m[2], "_000001"))
		if m[1] == "from a bad node" {
			l.Fields["badNode"] = "true"
		}
	case completedRE.MatchString(msg):
		m := completedRE.FindStringSubmatch(msg)
		code, _ := strconv.Atoi(m[4])
		if code == 0 {
			return
		}
		l = c.entry(model.LogContainerEnd, exitSeverity(code), h.time, msg)
		l.Fields["container"], l.Fields["exitCode"] = m[1], m[4]
		if m[2] != "" {
			l.Fields["host"] = m[2]
		}
		l.Fields["meaning"] = ContainerExitMeaning(code, strings.HasSuffix(m[1], "_000001"))
	case lostTaskRE.MatchString(msg):
		m := lostTaskRE.FindStringSubmatch(msg)
		if e := execLostFailRE.FindStringSubmatch(m[5]); e != nil {
			l = c.entry(model.LogLostExecutor, lostSeverity(e[3]), h.time, msg)
			l.Fields["executor"], l.Fields["reason"] = e[1], redact.Text(e[3])
			l.Fields["causedByApp"] = strconv.FormatBool(e[2] == "caused by one of the running tasks")
		} else {
			sev := model.Warning
			if strings.HasPrefix(m[5], "TaskKilled") {
				sev = model.Info // killed by Spark: a speculative twin finished first, or the stage ended
			}
			l = c.entry(model.LogTaskError, sev, h.time, msg)
			l.Fields["reason"] = clip(redact.Text(m[5]), maxDetailLine)
		}
		l.Fields["task"], l.Fields["stage"], l.Fields["tid"], l.Fields["where"] = m[1], m[2], m[3], m[4]
	case taskExcRE.MatchString(msg):
		m := taskExcRE.FindStringSubmatch(msg)
		l = c.entry(model.LogTaskError, model.Warning, h.time, msg)
		l.Fields["task"], l.Fields["stage"], l.Fields["tid"] = m[1], m[2], m[3]
	case signalRE.MatchString(msg):
		m := signalRE.FindStringSubmatch(msg)
		sev := model.Warning
		if m[1] == "TERM" && c.shutdown {
			sev = model.Info // the driver asked it to stop first
		}
		l = c.entry(model.LogSignal, sev, h.time, msg)
		l.Fields["signal"] = m[1]
		if c.shutdown {
			l.Fields["afterShutdown"] = "true"
		}
	case willRequestRE.MatchString(msg):
		// Dynamic allocation asks again and again; the largest request says
		// the executor size and how many Spark wanted at once.
		m := willRequestRE.FindStringSubmatch(msg)
		if n, _ := strconv.Atoi(m[1]); n > c.maxRequest || c.maxRequestLine == nil {
			c.maxRequest = n
			l = c.entry(model.LogYarnRequest, model.Info, h.time, msg)
			l.Fields["what"], l.Fields["count"], l.Fields["profile"], l.Fields["cores"], l.Fields["memoryMB"] = "executors", m[1], m[2], m[3], m[4]
			c.maxRequestLine = l
		}
		return
	case cancelRE.MatchString(msg):
		return // the most desired total below says what the churn added up to
	case desiredRE.MatchString(msg):
		m := desiredRE.FindStringSubmatch(msg)
		if n, _ := strconv.Atoi(m[1]); n > c.maxDesired || c.maxDesiredLine == nil {
			c.maxDesired = n
			c.maxDesiredLine = c.entry(model.LogYarnRequest, model.Info, h.time, msg)
			c.maxDesiredLine.Fields["what"], c.maxDesiredLine.Fields["total"] = "most-desired", m[1]
		}
		return
	case launchHeapRE.MatchString(msg):
		m := launchHeapRE.FindStringSubmatch(msg)
		l = c.entry(model.LogYarnRequest, model.Info, h.time, msg)
		l.Fields["what"], l.Fields["heapMB"], l.Fields["overheadMB"], l.Fields["cores"] = "launch", m[1], m[2], m[3]
		c.add(l)
		return
	case amRequestRE.MatchString(msg):
		m := amRequestRE.FindStringSubmatch(msg)
		l = c.entry(model.LogYarnRequest, model.Info, h.time, msg)
		l.Fields["what"], l.Fields["memoryMB"], l.Fields["overheadMB"] = "am", m[1], m[2]
		c.add(l)
		return
	case maxAllocRE.MatchString(msg):
		m := maxAllocRE.FindStringSubmatch(msg)
		l = c.entry(model.LogYarnRequest, model.Info, h.time, msg)
		l.Fields["what"], l.Fields["memoryMB"] = "max-container", m[1]
		c.add(l)
		return
	case shutdownCmdRE.MatchString(msg):
		c.shutdown = true
		return
	case driverHostRE.MatchString(msg):
		l = c.entry(model.LogDriverHost, model.Info, h.time, msg)
		l.Fields["host"] = driverHostRE.FindStringSubmatch(msg)[1]
	case nodeStateRE.MatchString(msg):
		m := nodeStateRE.FindStringSubmatch(msg)
		if m[2] == "RUNNING" || m[2] == "NEW" {
			return
		}
		l = c.entry(model.LogNodeState, model.Warning, h.time, msg)
		l.Fields["host"], l.Fields["state"] = m[1], m[2]
	case selfExitRE.MatchString(msg):
		m := selfExitRE.FindStringSubmatch(msg)
		l = c.entry(model.LogLostExecutor, model.Warning, h.time, msg)
		l.Fields["reason"] = redact.Text(m[1])
	case finalStatusRE.MatchString(msg):
		m := finalStatusRE.FindStringSubmatch(msg)
		code, _ := strconv.Atoi(m[2])
		sev := model.Info
		if m[1] == "FAILED" || m[1] == "KILLED" {
			sev = model.Critical
		}
		l = c.entry(model.LogAppExit, sev, h.time, msg)
		l.Fields["status"], l.Fields["exitCode"] = m[1], m[2]
		l.Fields["meaning"] = ContainerExitMeaning(code, true)
		if m[3] != "" {
			l.Fields["reason"] = redact.Text(m[3])
		}
	case userAppExitRE.MatchString(msg):
		return // the final status line that follows says the same, with the exit code
	case sparkCtxFailRE.MatchString(msg):
		l = c.entry(model.LogException, model.Critical, h.time, msg)
	case uncaughtRE.MatchString(msg):
		l = c.entry(model.LogException, model.Critical, h.time, msg)
	case krbLoginRE.MatchString(msg):
		m := krbLoginRE.FindStringSubmatch(msg)
		l = c.entry(model.LogIdentity, model.Info, h.time, msg)
		l.Fields["principal"], l.Fields["keytab"], l.Fields["via"] = redact.Text(m[1]), redact.Text(m[2]), "Kerberos login"
	case metaConnRE.MatchString(msg):
		m := metaConnRE.FindStringSubmatch(msg)
		l = c.entry(model.LogMetastore, model.Info, h.time, msg)
		if m[1] != "" {
			l.Fields["uri"] = redact.Text(m[1])
		}
	case !problem && hbaseConnRE.MatchString(msg):
		m := hbaseConnRE.FindStringSubmatch(msg)
		l = c.entry(model.LogHBase, model.Info, h.time, msg)
		if m[1] != "" {
			l.Fields["quorum"] = redact.Text(m[1])
		}
	case problem:
		l = c.entry(model.LogError, model.Info, h.time, msg)
		if tagged(l, msg) || h.level != "WARN" {
			break
		}
		return // WARN lines are kept only when a rule names them
	default:
		return
	}
	c.lead(l)
}

func (c *classifier) controller(h header) {
	if m := startExecRE.FindStringSubmatch(h.msg); m != nil {
		cmd := redact.Command(m[1])
		l := c.entry(model.LogSubmit, model.Info, h.time, cmd)
		l.Text = clip(cmd, maxText)
		l.Fields["command"] = l.Text
		c.add(l)
		return
	}
	if m := stepStatusRE.FindStringSubmatch(h.msg); m != nil {
		sev := model.Info
		if m[1] != "succeeded" {
			sev = model.Critical
		}
		l := c.entry(model.LogStepStatus, sev, h.time, h.msg)
		l.Fields["status"], l.Fields["exitCode"], l.Fields["seconds"] = m[1], m[2], m[3]
		c.add(l)
	}
}

// capacity keeps a node's YARN capacity, which the ResourceManager and
// NodeManager log once per node for every application to share.
func (c *classifier) capacity(h header) bool {
	m := rmCapacityRE.FindStringSubmatch(h.msg)
	if m == nil {
		m = nmCapacityRE.FindStringSubmatch(h.msg)
	}
	if m == nil {
		return false
	}
	l := c.entry(model.LogNodeCapacity, model.Info, h.time, h.msg)
	l.Fields["host"], l.Fields["memoryMB"], l.Fields["vcores"] = m[1], m[2], m[3]
	c.add(l)
	return true
}

func (c *classifier) resourceManager(h header) {
	if m := assignedRE.FindStringSubmatch(h.msg); m != nil {
		l := c.entry(model.LogContainerAssigned, model.Info, h.time, h.msg)
		l.Fields["container"], l.Fields["memoryMB"], l.Fields["nodeMaxMB"], l.Fields["vcores"] = m[1], m[2], m[3], m[4]
		l.Fields["host"], l.Fields["containersOnNode"], l.Fields["usedMB"], l.Fields["availableMB"] = m[6], m[7], m[8], m[9]
		c.add(l)
		return
	}
	if m := rmAttemptRE.FindStringSubmatch(h.msg); m != nil {
		code, _ := strconv.Atoi(m[3])
		sev := model.Info
		switch {
		case m[2] == "FAILED" || m[2] == "KILLED":
			sev = model.Critical
		case code == 0 || code == -1000: // -1000 is YARN's "no exit status yet"
			return
		}
		l := c.entry(model.LogAppExit, sev, h.time, h.msg)
		l.Fields["attempt"], l.Fields["status"], l.Fields["exitCode"] = m[1], m[2], m[3]
		l.Fields["meaning"] = ContainerExitMeaning(code, true)
		c.add(l)
		return
	}
	if strings.HasSuffix(h.logger, "ApplicationSummary") && rmSummaryRE.MatchString(h.msg) {
		kv := map[string]string{}
		for _, m := range summaryKVRE.FindAllStringSubmatch(h.msg, -1) {
			kv[m[1]] = strings.ReplaceAll(m[2], `\,`, ",")
		}
		sev := model.Info
		if kv["finalStatus"] == "FAILED" || kv["finalStatus"] == "KILLED" {
			sev = model.Critical
		}
		l := c.entry(model.LogAppSummary, sev, h.time, h.msg)
		for _, k := range []string{"appId", "name", "user", "queue", "state", "finalStatus", "appMasterHost", "submitTime", "startTime", "launchTime",
			"finishTime", "memorySeconds", "vcoreSeconds", "preemptedMemorySeconds", "preemptedVcoreSeconds", "preemptedAMContainers",
			"preemptedNonAMContainers", "applicationType", "diagnostics", "totalAllocatedContainers"} {
			v := kv[k]
			if k == "diagnostics" {
				v, _, _ = strings.Cut(v, `\n`) // the rest quotes the driver's stderr
			}
			if v != "" {
				l.Fields[k] = clip(redact.Text(v), maxDetailLine)
			}
		}
		c.add(l)
		if kv["user"] != "" || kv["queue"] != "" {
			id := c.entry(model.LogIdentity, model.Info, h.time, "user "+kv["user"]+", queue "+kv["queue"])
			id.Fields["user"], id.Fields["queue"], id.Fields["via"] = kv["user"], kv["queue"], "YARN ResourceManager"
			c.add(id)
		}
	}
}

func (c *classifier) nodeManager(h header) {
	if m := nmExitRE.FindStringSubmatch(h.msg); m != nil {
		code, _ := strconv.Atoi(m[2])
		if code == 0 {
			return
		}
		l := c.entry(model.LogContainerEnd, exitSeverity(code), h.time, h.msg)
		l.Fields["container"], l.Fields["exitCode"] = m[1], m[2]
		l.Fields["meaning"] = ContainerExitMeaning(code, strings.HasSuffix(m[1], "_000001"))
		c.add(l)
		return
	}
	if m := nmMemLimitRE.FindStringSubmatch(h.msg); m != nil {
		l := c.entry(model.LogMemoryKill, model.Critical, h.time, h.msg)
		l.Fields["container"], l.Fields["limit"] = m[2], strings.ToLower(m[4])
		if m[3] != "" {
			l.Fields["over"] = m[3]
		}
		if m[5] != "" {
			l.Fields["usage"] = redact.Text(m[5])
		}
		c.add(l)
	}
}

// mine reports whether a daemon's line is about this application.
func (c *classifier) mine(line string) bool {
	if c.appKey == "" {
		return true
	}
	for _, m := range idRE.FindAllStringSubmatch(line, -1) {
		if m[1] == c.appKey {
			return true
		}
	}
	return false
}

func (c *classifier) closeReport() {
	r := c.rep
	if r == nil {
		return
	}
	c.rep = nil
	final := r.keys["final status"]
	if final == "" || final == "UNDEFINED" {
		return // a progress report while the application runs
	}
	sev := model.Info
	if final == "FAILED" || final == "KILLED" {
		sev = model.Critical
	}
	diag := strings.TrimSpace(r.keys["diagnostics"])
	text := "final status " + final
	if c.lastState != "" {
		text = "state " + c.lastState + ", " + text
	}
	if diag != "" && diag != "N/A" {
		text += ": " + diag
	}
	l := c.entry(model.LogAppReport, sev, r.time, text)
	l.Source = model.Source{File: c.res.Name, Line: r.start, EndLine: r.end}
	l.Fields["finalStatus"] = final
	if c.lastApp != "" {
		l.Fields["app"], l.Fields["state"] = c.lastApp, c.lastState
	}
	for _, k := range []string{"queue", "user", "start time", "ApplicationMaster host"} {
		if v := strings.TrimSpace(r.keys[k]); v != "" && v != "N/A" {
			l.Fields[strings.ReplaceAll(k, " ", "")] = redact.Text(v)
		}
	}
	if m := diagExitCodeRE.FindStringSubmatch(diag); m != nil {
		code, _ := strconv.Atoi(m[1])
		l.Fields["exitCode"], l.Fields["meaning"] = m[1], ContainerExitMeaning(code, true)
	}
	seen, quoted := map[string]bool{}, false
	for _, d := range r.diag {
		d = strings.TrimSpace(d)
		switch {
		case strings.HasPrefix(d, "Last 4096 bytes of"):
			quoted = true // the tail of the container's own logs follows
			continue
		case diagStampRE.MatchString(d):
			quoted = false
		}
		if quoted || len(l.Detail) == maxDetail || d == "" || seen[d] || atFrameRE.MatchString(d) || !diagKeepRE.MatchString(d) {
			continue
		}
		seen[d] = true
		l.Detail = append(l.Detail, clip(redact.Text(d), maxDetailLine))
	}
	c.add(l)
	if u, q := strings.TrimSpace(r.keys["user"]), strings.TrimSpace(r.keys["queue"]); u != "" || q != "" {
		id := c.entry(model.LogIdentity, model.Info, r.time, "user "+u+", queue "+q)
		id.Source = l.Source
		id.Fields["user"], id.Fields["queue"], id.Fields["via"] = u, q, "YARN application report"
		c.add(id)
	}
}

// diagKeepRE picks the lines of YARN's diagnostics worth showing.
var diagKeepRE = regexp.MustCompile(`(?i)exit ?code|exit status|container id|killed|memory|exception|error`)

// diagStampRE starts each of YARN's own diagnostic messages.
var diagStampRE = regexp.MustCompile(`^\[\d{4}-\d{2}-\d{2} [\d:.]+\]`)

// flush ends the open block and keeps what it shows.
func (c *classifier) flush() {
	b := c.blk
	if b == nil {
		return
	}
	c.blk = nil
	l := b.lead
	if l == nil {
		if len(b.lines) == 0 {
			return
		}
		first := b.lines[0]
		switch {
		case strings.HasPrefix(first, "Traceback"):
			sev := model.Warning
			if c.res.File.Kind == ContainerStdout && c.res.File.Driver() {
				sev = model.Critical
			}
			l = &model.LogLine{Kind: model.LogTraceback, Severity: sev}
		case strings.HasPrefix(first, "Exception in thread "):
			l = &model.LogLine{Kind: model.LogException, Severity: model.Critical}
		default:
			l = &model.LogLine{Kind: model.LogException, Severity: model.Warning}
		}
		l.Source = model.Source{File: c.res.Name, Line: b.start}
		l.Fields, l.Count, l.Time = map[string]string{}, 1, c.lastTime
		l.Text = clip(redact.Text(first), maxText)
		b.lines = b.lines[1:]
	}
	if b.end > l.Source.Line {
		l.Source.EndLine = b.end
	}
	summarise(l, b.lines)
	if l.Kind == model.LogError && l.Fields["exception"] != "" {
		l.Kind, l.Severity = model.LogException, model.Warning
	}
	all := l.Text + "\n" + strings.Join(l.Detail, "\n")
	tagged(l, all)
	c.add(l)
}

// tagged names a line by what its text shows (out of memory, access,
// Kerberos, metastore or HBase failures), raising its severity. A task
// error or lost executor keeps its kind and gains a cause instead.
func tagged(l *model.LogLine, text string) bool {
	var kind model.LogKind
	var cause string
	switch {
	case oomRE.MatchString(text):
		kind, cause = model.LogOutOfMemory, "OutOfMemoryError"
		if m := oomRE.FindStringSubmatch(text); m != nil && m[1] != "" {
			l.Fields["oom"] = clip(redact.Text(m[1]), maxDetailLine)
		}
	case accessRE.MatchString(text):
		kind, cause = model.LogAccess, "access denied"
		if m := authActRE.FindStringSubmatch(text); m != nil {
			l.Fields["action"] = m[1]
			if m[2] != "" {
				l.Fields["resource"] = redact.Text(m[2])
			}
		}
		if p := s3PathRE.FindString(text); p != "" {
			l.Fields["path"] = redact.Text(strings.TrimRight(p, ".,;:)"))
		}
	case kerberosRE.MatchString(text):
		kind, cause = model.LogKerberos, "Kerberos"
	case metaFailRE.MatchString(text):
		kind, cause = model.LogMetastore, "metastore"
	case hbaseFailRE.MatchString(text):
		kind, cause = model.LogHBase, "HBase"
	default:
		return false
	}
	switch l.Kind {
	case model.LogTaskError, model.LogLostExecutor, model.LogContainerEnd, model.LogAppExit:
		l.Fields["cause"] = cause
	default:
		l.Kind = kind
	}
	l.Severity = model.Critical
	return true
}

// summarise fills a line's detail from the lines of its block: exception
// headlines and causes with the application's own frames, and a
// traceback's frames outside PySpark with its exception.
func summarise(l *model.LogLine, lines []string) {
	var root, rootMsg string
	if m := javaExcRE.FindStringSubmatch(l.Text); m != nil {
		l.Fields["exception"], root, rootMsg = m[1], m[1], m[2]
	}
	frames, userFrames := 0, 0
	var lastPy string
	for i, s := range lines {
		if len(l.Detail) >= maxDetail {
			break
		}
		switch {
		case atFrameRE.MatchString(s):
			frames++
			if userFrames < maxUserFrames && !systemFrame(s) {
				userFrames++
				l.Detail = append(l.Detail, clip(redact.Text(strings.TrimSpace(s)), maxDetailLine))
			}
		case moreRE.MatchString(s):
		case pyFrameRE.MatchString(s):
			m := pyFrameRE.FindStringSubmatch(s)
			if pythonLib(m[1]) {
				continue
			}
			lastPy = m[1] + ":" + m[2]
			l.Fields["pyFile"], l.Fields["pyLine"] = redact.Text(m[1]), m[2]
			l.Detail = append(l.Detail, clip(redact.Text(strings.TrimSpace(s)), maxDetailLine))
			if i+1 < len(lines) && strings.HasPrefix(lines[i+1], "    ") {
				l.Detail = append(l.Detail, clip(redact.Code(strings.TrimSpace(lines[i+1])), maxDetailLine))
			}
		case strings.HasPrefix(s, "    ") && i > 0 && pyFrameRE.MatchString(lines[i-1]):
			// a traceback's code line, kept with its frame above
		default:
			if m := javaExcRE.FindStringSubmatch(s); m != nil {
				if l.Fields["exception"] == "" {
					l.Fields["exception"] = m[1]
				}
				if lastPy != "" && l.Fields["pyException"] == "" && !causeRE.MatchString(s) {
					l.Fields["pyException"] = m[1] // a traceback's last line, such as py4j's Py4JJavaError
				}
				root, rootMsg = m[1], m[2]
				userFrames = 0
			} else if m := pyExcRE.FindStringSubmatch(s); m != nil && lastPy != "" && l.Fields["pyException"] == "" {
				l.Fields["pyException"] = m[1]
			}
			l.Detail = append(l.Detail, clip(redact.Text(strings.TrimRight(s, " \t")), maxDetailLine))
		}
	}
	if root != "" {
		l.Fields["root"] = root
		if rootMsg = strings.TrimSpace(rootMsg); rootMsg != "" {
			l.Fields["rootMessage"] = clip(redact.Text(rootMsg), maxDetailLine)
		}
	}
	if frames > 0 {
		l.Fields["frames"] = strconv.Itoa(frames)
	}
}

// systemFrame reports whether a stack frame is Spark's, the JVM's or
// another library's rather than the application's.
func systemFrame(s string) bool {
	s = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(s), "at "))
	if i := strings.IndexByte(s, '/'); i >= 0 && i < strings.IndexByte(s, '(') {
		s = s[i+1:] // "java.base/java.lang.Thread.run(…)"
	}
	for _, p := range systemPrefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

var systemPrefixes = []string{"org.apache.spark.", "scala.", "java.", "javax.", "jdk.", "sun.", "com.sun.", "py4j.",
	"org.apache.hadoop.", "org.apache.hive.", "org.apache.parquet.", "com.amazon.", "com.amazonaws.", "software.amazon.",
	"io.netty.", "org.eclipse.jetty.", "org.sparkproject.", "com.google.", "com.fasterxml."}

// pythonLib reports whether a traceback frame is in PySpark, py4j or the
// Python standard library.
func pythonLib(file string) bool {
	return strings.Contains(file, "/pyspark.zip/") || strings.Contains(file, "/py4j-") || strings.Contains(file, "/pyspark/") ||
		strings.Contains(file, "/lib/python3") || strings.Contains(file, "/lib64/python3")
}

func lostSeverity(reason string) model.Severity {
	if yarnMemKilledRE.MatchString(reason) || strings.Contains(reason, "Exit status: 137") {
		return model.Critical
	}
	return model.Warning
}

// foldKinds are folded with their numbers ignored: the same exception in
// many tasks is one entry with a count. Other kinds keep one entry per
// executor or container.
var foldKinds = map[model.LogKind]bool{model.LogTaskError: true, model.LogException: true, model.LogError: true,
	model.LogTraceback: true, model.LogAccess: true, model.LogOutOfMemory: true, model.LogKerberos: true,
	model.LogMetastore: true, model.LogHBase: true}

var digitsRE = regexp.MustCompile(`\d+`)

// add keeps l, or folds it into an earlier entry for the same thing.
func (c *classifier) add(l *model.LogLine) {
	key := string(l.Kind) + "\x00" + l.Text
	if len(l.Detail) > 0 {
		key += "\x00" + l.Detail[0]
	}
	if foldKinds[l.Kind] {
		key = digitsRE.ReplaceAllString(key, "#")
	}
	if i, ok := c.index[key]; ok {
		e := &c.res.Lines[i]
		e.Count++
		e.LastLine = l.Source.Line
		return
	}
	if len(c.res.Lines) >= c.opt.MaxEntries {
		c.res.Dropped++
		return
	}
	if len(l.Fields) == 0 {
		l.Fields = nil
	}
	c.index[key] = len(c.res.Lines)
	c.res.Lines = append(c.res.Lines, *l)
}

// clip cuts s to at most n bytes, on a rune boundary.
func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !isRuneStart(s[n]) {
		n--
	}
	return s[:n] + "…"
}

func isRuneStart(b byte) bool { return b&0xC0 != 0x80 }
