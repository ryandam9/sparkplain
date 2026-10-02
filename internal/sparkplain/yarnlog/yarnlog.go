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
	"encoding/json"
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
	// From and To keep an HBase daemon's lines to the application's time:
	// these logs hold every application's. Zero keeps all.
	From, To time.Time
}

// Result is what one file held.
type Result struct {
	Name      string          // the file's name, as given
	File      File            // what its path says
	Lines     []model.LogLine // in file order
	Read      int64           // lines read
	Truncated int             // lines longer than 64 KiB, cut
	Dropped   int             // distinct lines past MaxEntries, not kept
	Splits    []model.HBaseSplit
	// FirstTime and LastTime are the first and last times the file's lines
	// start with, recognised or not: a container's log brackets when it ran.
	FirstTime, LastTime time.Time
	Scans               []model.HBaseScan // sparkplain-scan lines, decoded
	// RegionEvents are what an HBase server logged about one region during
	// the run (flushes, compactions, closes and opens, moves, splits,
	// refused writes, slow calls), up to maxRegionEvents.
	RegionEvents []model.HBaseRegionEvent
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
		c.stamp(b)
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

	// stampKey is the last time prefix parsed by stamp, and stampTime its
	// time: lines in the same second are not parsed again.
	stampKey  string
	stampTime time.Time

	// TableInputFormat splits whose size line has not come yet: one group
	// while tasks overlap, since their lines interleave (see splitSize).
	splitGroup []int
	splitSizes []splitSize
	splitOpen  int
	// splitOf finds a split by the task that logged it, when the layout
	// prints the thread: its size line then needs no guessing.
	splitOf map[int64]int
	// started holds when each task thread logged "Running task", until
	// its split or its end: a task logs its split just after it starts.
	started map[int64]time.Time

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
	if i := strings.Index(line, scanMarker); i >= 0 && (kind == ContainerStdout || kind == ContainerStderr || kind == StepStdout || kind == StepStderr) {
		c.flush()
		if h, ok := parseHeader(kind, line); ok && !h.time.IsZero() {
			c.lastTime = h.time
		}
		c.scanLine(line[i+len(scanMarker):])
		return
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
	if m := localizedRE.FindStringSubmatch(line); m != nil && (kind == ContainerStderr || kind == ContainerStdout) {
		c.flush()
		l := c.entry(model.LogLocalized, model.Info, c.lastTime, m[1])
		l.Fields["name"] = redact.Text(m[1])
		c.add(l)
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
	case HBaseRegion:
		// A refused write names its region only in the exception under the
		// warning.
		if m := rgBusyRE.FindStringSubmatch(line); m != nil && c.inWindow(c.lastTime) {
			c.addRegionEvent(model.HBaseRegionEvent{Time: c.lastTime, Event: "busy", Region: m[2], Detail: "refused writes: its memstore was over " + m[1]})
		}
		return
	case NodeManager, ResourceManager, Bootstrap, StepController, HBaseMaster:
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
	case HBaseMaster, HBaseRegion:
		c.hbaseServer(h)
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
	if t := threadTask(h.thread); t != nil {
		c.taskLine(t.TaskID, h)
	}

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
	case executorHostRE.MatchString(msg):
		m := executorHostRE.FindStringSubmatch(msg)
		l = c.entry(model.LogExecutorHost, model.Info, h.time, msg)
		l.Fields["executor"], l.Fields["host"] = m[1], m[2]
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
	case hbaseAsyncRE.MatchString(msg):
		// HBase's client retrying a batch, at INFO: it recovered or will
		// give up at the last attempt. The error follows in the text.
		m := hbaseAsyncRE.FindStringSubmatch(msg)
		l = c.entry(model.LogHBase, model.Warning, h.time, msg)
		l.Fields["table"], l.Fields["attempt"], l.Fields["attempts"] = m[1], m[2], m[3]
	case hbaseSizingRE.MatchString(msg):
		m := hbaseSizingRE.FindStringSubmatch(msg)
		l = c.entry(model.LogHBaseUse, model.Info, h.time, "TableInputFormat is reading "+m[1])
		l.Fields["table"], l.Fields["access"], l.Fields["api"] = m[1], "read", "TableInputFormat"
		c.add(l)
		return
	case hbaseSplitRE.MatchString(msg):
		// One line per task; kept as one entry per table and region
		// server, whose count is the regions read there, and each as a
		// split of its own.
		m := hbaseSplitRE.FindStringSubmatch(msg)
		l = c.entry(model.LogHBaseUse, model.Info, h.time, "TableInputFormat read a region of "+m[1]+" held on "+m[2])
		l.Fields["table"], l.Fields["access"], l.Fields["api"], l.Fields["server"] = m[1], "read", "TableInputFormat", m[2]
		c.add(l)
		c.split(m[1], m[2], msg, h)
		return
	case hbaseSplitLenRE.MatchString(msg):
		c.splitSize(hbaseSplitLenRE.FindStringSubmatch(msg), h)
		return
	case hbaseOutputRE.MatchString(msg):
		m := hbaseOutputRE.FindStringSubmatch(msg)
		l = c.entry(model.LogHBaseUse, model.Info, h.time, "TableOutputFormat is writing "+m[1])
		l.Fields["table"], l.Fields["access"], l.Fields["api"] = m[1], "write", "TableOutputFormat"
		c.add(l)
		return
	case !problem && hbaseConnRE.MatchString(msg):
		m := hbaseConnRE.FindStringSubmatch(msg)
		text := msg
		if z := zkConnectRE.FindString(msg); z != "" {
			text = z // one entry per quorum, counting the connections
		}
		l = c.entry(model.LogHBase, model.Info, h.time, text)
		if m[1] != "" {
			l.Fields["quorum"] = redact.Text(m[1])
		}
	case problem:
		l = c.entry(model.LogError, model.Info, h.time, msg)
		if tagged(l, msg) {
			if h.level == "WARN" && l.Kind == model.LogClasspath {
				l.Severity = model.Warning // libraries probe for optional classes and log the miss
			}
			break
		}
		if h.level != "WARN" {
			break
		}
		return // WARN lines are kept only when a rule names them
	default:
		return
	}
	c.lead(l)
}

// hbaseServer keeps what an HBase Master or region server logged while the
// application ran. Each kind of event is one entry per table (or client),
// whose count is how often it happened; the line's own text would not fold,
// since it names a region or procedure each time.
func (c *classifier) hbaseServer(h header) {
	o := c.opt
	if h.time.IsZero() || !o.From.IsZero() && h.time.Before(o.From.Truncate(time.Second)) || !o.To.IsZero() && h.time.After(o.To) {
		return
	}
	msg := h.msg
	host := c.res.File.Host
	c.regionEvent(h)
	keep := func(sev model.Severity, event, text string, fields ...string) {
		l := c.entry(model.LogHBaseServer, sev, h.time, text)
		l.Fields["event"], l.Fields["host"] = event, host
		for i := 0; i+1 < len(fields); i += 2 {
			if fields[i+1] != "" {
				l.Fields[fields[i]] = redact.Text(fields[i+1])
			}
		}
		c.add(l)
	}
	var m []string
	switch {
	case hbMovedRE.MatchString(msg):
		m = hbMovedRE.FindStringSubmatch(msg)
		keep(model.Warning, "moved", "HBase moved a region of "+m[1], "table", m[1])
	case hbSplitRE.MatchString(msg):
		m = hbSplitRE.FindStringSubmatch(msg)
		keep(model.Warning, "split", "HBase split a region of "+m[1], "table", m[1])
	case hbCrashRE.MatchString(msg):
		m = hbCrashRE.FindStringSubmatch(msg)
		keep(model.Critical, "server-lost", msg, "server", m[1], "meta", m[2], "regions", m[3])
	case hbBusyRE.MatchString(msg):
		keep(model.Warning, "busy", "Region is too busy due to exceeding memstore size limit")
	case hbLeaseRE.MatchString(msg):
		m = hbLeaseRE.FindStringSubmatch(msg)
		keep(model.Warning, "scanner", "Scanner lease expired for a client at "+m[1]+" reading "+m[3], "client", m[1], "user", m[2], "table", m[3])
	case hbStoppedRE.MatchString(msg):
		keep(model.Critical, "server-stopped", msg, "server", host, "reason", hbStoppedRE.FindStringSubmatch(msg)[1])
	case hbAbortRE.MatchString(msg):
		m = hbAbortRE.FindStringSubmatch(msg)
		keep(model.Critical, "server-stopped", msg, "server", m[1], "reason", m[2], "aborted", "true")
	case hbPauseRE.MatchString(msg):
		keep(model.Warning, "pause", msg, "ms", hbPauseRE.FindStringSubmatch(msg)[1])
	case hbSlowRE.MatchString(msg):
		kind := "slow-call"
		if hbSlowRE.FindStringSubmatch(msg)[1] == "TooLarge" {
			kind = "large-response"
		}
		var ms, op, tbl string
		if m := hbSlowMsRE.FindStringSubmatch(msg); m != nil {
			ms = m[1]
		}
		if m := hbSlowOpRE.FindStringSubmatch(msg); m != nil {
			op = m[1]
		}
		if m := hbSlowTblRE.FindStringSubmatch(msg); m != nil {
			tbl = m[1]
		}
		keep(model.Warning, kind, "HBase logged a "+strings.ReplaceAll(kind, "-", " ")+" ("+op+") on "+tbl, "ms", ms, "method", op, "table", tbl)
	case hbStoreRE.MatchString(msg):
		m = hbStoreRE.FindStringSubmatch(msg)
		keep(model.Warning, "store-files", "A region of "+m[1]+" had too many store files to flush", "table", m[1])
	case hbFlushRE.MatchString(msg):
		keep(model.Info, "flush", "HBase flushed a region's memstore to disk")
	case hbCompactRE.MatchString(msg):
		m = hbCompactRE.FindStringSubmatch(msg)
		keep(model.Info, "compaction", "HBase compacted a region of "+m[1], "table", m[1])
	}
}

// maxRegionEvents caps the region events kept per server log.
const maxRegionEvents = 20000

// inWindow reports whether t falls in the run's time, as hbaseServer
// checks its lines.
func (c *classifier) inWindow(t time.Time) bool {
	o := c.opt
	return !t.IsZero() && (o.From.IsZero() || !t.Before(o.From.Truncate(time.Second))) && (o.To.IsZero() || !t.After(o.To))
}

// regionEvent keeps what an HBase server logged about one region: the
// sparkplain run ties it to the tasks reading that region at the time.
func (c *classifier) regionEvent(h header) {
	msg := h.msg
	switch {
	case strings.HasSuffix(h.logger, "HRegion") && rgFlushRE.MatchString(msg):
		m := rgFlushRE.FindStringSubmatch(msg)
		ms, _ := strconv.ParseInt(m[3], 10, 64)
		c.addRegionEvent(model.HBaseRegionEvent{Time: h.time, Event: "flush", Region: m[2], DurationMs: ms, Detail: "wrote " + m[1] + " of memstore to disk"})
	case strings.HasSuffix(h.logger, "HStore") && rgCompactRE.MatchString(msg):
		m := rgCompactRE.FindStringSubmatch(msg)
		sec, _ := strconv.ParseInt(m[4], 10, 64)
		c.addRegionEvent(model.HBaseRegionEvent{Time: h.time, Event: "compaction", Region: m[2], DurationMs: sec * 1000,
			Detail: "rewrote " + m[1] + " store files into one of " + m[3]})
	case strings.HasSuffix(h.logger, "UnassignRegionHandler") && rgCloseRE.MatchString(msg):
		c.addRegionEvent(model.HBaseRegionEvent{Time: h.time, Event: "closed", Region: rgCloseRE.FindStringSubmatch(msg)[1], Detail: "closed the region: it stopped serving it"})
	case strings.HasSuffix(h.logger, "AssignRegionHandler") && rgOpenedRE.MatchString(msg):
		m := rgOpenedRE.FindStringSubmatch(msg)
		c.addRegionEvent(model.HBaseRegionEvent{Time: h.time, Event: "opened", Region: m[2], Table: m[1], Detail: "opened the region: it serves it from now"})
	case rgMoveRE.MatchString(msg):
		m := rgMoveRE.FindStringSubmatch(msg)
		why := "a client asked"
		if m[4] != "" {
			why = "the balancer asked"
		}
		c.addRegionEvent(model.HBaseRegionEvent{Time: h.time, Event: "move", Region: m[1], Detail: "the Master moved it from " + m[2] + " to " + m[3] + " (" + why + ")"})
	case rgSplitRE.MatchString(msg):
		m := rgSplitRE.FindStringSubmatch(msg)
		sec, _ := strconv.ParseFloat(m[5], 64)
		c.addRegionEvent(model.HBaseRegionEvent{Time: h.time, Event: "split", Region: m[2], Table: m[1], DurationMs: int64(sec * 1000),
			Detail: "the Master split it into " + m[3] + " and " + m[4]})
	case hbSlowRE.MatchString(msg) && rgSlowRE.MatchString(msg):
		var ms int64
		if m := hbSlowMsRE.FindStringSubmatch(msg); m != nil {
			ms, _ = strconv.ParseInt(m[1], 10, 64)
		}
		op := "a call"
		if m := hbSlowOpRE.FindStringSubmatch(msg); m != nil {
			op = "a " + m[1] + " call"
		}
		c.addRegionEvent(model.HBaseRegionEvent{Time: h.time, Event: "slow-call", Region: rgSlowRE.FindStringSubmatch(msg)[1], DurationMs: ms,
			Detail: op + " on it was slow (responseTooSlow)"})
	}
}

// addRegionEvent keeps a region event at the current line.
func (c *classifier) addRegionEvent(e model.HBaseRegionEvent) {
	if len(c.res.RegionEvents) >= maxRegionEvents {
		return
	}
	e.Host, e.Source = c.res.File.Host, model.Source{File: c.res.Name, Line: c.n}
	c.res.RegionEvents = append(c.res.RegionEvents, e)
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
		l.Fields["host"], l.Fields["containersOnNode"], l.Fields["usedMB"], l.Fields["availableMB"] = m[6], m[7], m[8], m[10]
		l.Fields["usedVcores"], l.Fields["availableVcores"] = m[9], m[11]
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
	if tagged(l, all) && l.Fields["class"] != "" {
		l.Fields["neededBy"] = neededBy(l.Text, b.lines)
	}
	c.add(l)
}

// neededBy names the jar of the first stack frame, after the missing-class
// error, that says which jar it is in: the code that needed the class.
func neededBy(lead string, lines []string) string {
	seen := classpathRE.MatchString(lead)
	for _, s := range lines {
		if !seen {
			seen = classpathRE.MatchString(s)
			continue
		}
		if m := frameJarRE.FindStringSubmatch(s); m != nil {
			return m[1]
		}
	}
	return ""
}

// tagged names a line by what its text shows (out of memory, a missing
// class, access, Kerberos, metastore or HBase failures), raising its
// severity. A task error or lost executor keeps its kind and gains a cause
// instead.
func tagged(l *model.LogLine, text string) bool {
	var kind model.LogKind
	var cause string
	sev := model.Critical
	switch {
	case oomRE.MatchString(text):
		kind, cause = model.LogOutOfMemory, "OutOfMemoryError"
		if m := oomRE.FindStringSubmatch(text); m != nil && m[1] != "" {
			l.Fields["oom"] = clip(redact.Text(m[1]), maxDetailLine)
		}
	case classpathRE.MatchString(text):
		kind, cause = model.LogClasspath, "missing class"
		if l.Kind == model.LogClasspath && l.Severity == model.Warning {
			sev = model.Warning // a WARN line's probe, seen again with its stack
		}
		m := classpathRE.FindStringSubmatch(text)
		l.Fields["classError"] = m[1]
		if m[2] != "" {
			l.Fields["class"] = redact.Text(strings.ReplaceAll(strings.TrimRight(m[2], ".,;:'\""), "/", "."))
		}
	case accessRE.MatchString(text):
		kind, cause = model.LogAccess, "access denied"
		if hbaseAccessRE.MatchString(text) {
			l.Fields["service"] = "HBase"
			if m := hbaseTableAtRE.FindStringSubmatch(text); m != nil {
				l.Fields["table"] = redact.Text(m[1] + m[2])
			}
		}
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
		sev = hbaseDetail(l, text)
	default:
		return false
	}
	switch l.Kind {
	case model.LogTaskError, model.LogLostExecutor, model.LogContainerEnd, model.LogAppExit:
		l.Fields["cause"] = cause
	default:
		l.Kind = kind
	}
	l.Severity = sev
	return true
}

// hbaseDetail names the HBase problem text shows (Fields "hbase") and
// where: the table, the region server and the ZooKeeper address tried. The
// client retries expired scanners, busy regions and moved regions itself,
// so those are warnings; the run's own failure says when they were fatal.
func hbaseDetail(l *model.LogLine, text string) model.Severity {
	problem, sev := "other", model.Critical
	switch {
	case hbaseTableRE.MatchString(text):
		problem = "table-missing"
		l.Fields["table"] = redact.Text(hbaseTableRE.FindStringSubmatch(text)[1])
	case hbaseZKRE.MatchString(text):
		problem = "zookeeper"
	case hbaseScannerRE.MatchString(text):
		problem, sev = "scanner", model.Warning
	case hbaseBusyRE.MatchString(text):
		problem, sev = "busy", model.Warning
		if m := hbaseLimitRE.FindStringSubmatch(text); m != nil {
			l.Fields["limit"] = m[1]
		}
	case hbaseMovedRE.MatchString(text):
		problem, sev = "moved", model.Warning
	case hbaseServerRE.MatchString(text):
		problem = "server"
	case hbaseRetryRE.MatchString(text):
		problem = "retries"
	}
	l.Fields["hbase"] = problem
	if m := hbaseTableAtRE.FindStringSubmatch(text); m != nil && l.Fields["table"] == "" {
		l.Fields["table"] = redact.Text(m[1] + m[2])
	}
	if m := hbaseServerAtRE.FindStringSubmatch(text); m != nil {
		l.Fields["server"] = redact.Text(m[1])
	}
	if m := hbaseZKAtRE.FindStringSubmatch(text); m != nil {
		l.Fields["zookeeper"] = redact.Text(m[1])
	}
	return sev
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
	model.LogMetastore: true, model.LogHBase: true, model.LogClasspath: true}

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

// maxSplits caps the TableInputFormat splits kept from one file: one per
// task the executor ran.
const maxSplits = 20000

type splitSize struct {
	bytes int64
	src   model.Source
}

// split keeps one TableInputFormat split.
func (c *classifier) split(table, server, msg string, h header) {
	if len(c.res.Splits) == maxSplits {
		return
	}
	sp := model.HBaseSplit{Table: table, Server: server, Time: h.time, Source: model.Source{File: c.res.Name, Line: c.n}}
	if m := hbaseSplitRowsRE.FindStringSubmatch(msg); m != nil {
		sp.StartRow, sp.EndRow, sp.Region = redact.Clean(m[1]), redact.Clean(m[2]), m[3]
	}
	sp.Task = threadTask(h.thread)
	if sp.Task != nil {
		sp.Task.Start = h.time
		if t, ok := c.started[sp.Task.TaskID]; ok {
			sp.Task.Start = t
			delete(c.started, sp.Task.TaskID)
		}
	}
	c.res.Splits = append(c.res.Splits, sp)
	if sp.Task != nil {
		if c.splitOf == nil {
			c.splitOf = map[int64]int{}
		}
		c.splitOf[sp.Task.TaskID] = len(c.res.Splits) - 1
		return
	}
	c.splitGroup = append(c.splitGroup, len(c.res.Splits)-1)
	c.splitOpen++
}

// maxStarted caps the tasks waiting for their split line: a task that
// reads no HBase logs none, and its Running line is dropped at its end.
const maxStarted = 10_000

// taskLine times a task from the lines its thread logs: Spark's executor
// logs "Running task …" as the task starts and "Finished task …" (or
// "Exception in task …") as it ends, both on the task's own thread. Only
// tasks that logged a split are kept.
func (c *classifier) taskLine(tid int64, h header) {
	switch {
	case strings.HasPrefix(h.msg, "Running task "):
		if c.started == nil {
			c.started = map[int64]time.Time{}
		}
		if len(c.started) < maxStarted {
			c.started[tid] = h.time
		}
	case strings.HasPrefix(h.msg, "Finished task "), taskExcRE.MatchString(h.msg):
		delete(c.started, tid)
		k, ok := c.splitOf[tid]
		if !ok {
			return
		}
		t := c.res.Splits[k].Task
		if t.End.IsZero() {
			t.End, t.EndSource = h.time, model.Source{File: c.res.Name, Line: c.n}
			t.Failed = !strings.HasPrefix(h.msg, "Finished task ")
		}
	}
}

// threadTask reads the task a Spark executor thread runs from its name.
func threadTask(thread string) *model.SplitTask {
	m := taskThreadRE.FindStringSubmatch(thread)
	if m == nil {
		return nil
	}
	n := make([]int64, 5)
	for i := range n {
		n[i], _ = strconv.ParseInt(m[i+1], 10, 64)
	}
	return &model.SplitTask{Partition: int(n[0]), Attempt: int(n[1]), Stage: int(n[2]), StageAttempt: int(n[3]), TaskID: n[4]}
}

// splitSize gives a split its size line. Each task logs its split, then
// its size, but tasks running at once interleave their lines and the log
// format names no thread. So the lines are taken a group at a time, from
// a split with none open to the moment none is open again: a group of one
// split gets its size; a larger group's splits get theirs only when every
// size in it is the same, and otherwise none, never a guess.
func (c *classifier) splitSize(m []string, h header) {
	v, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return
	}
	mult := map[string]float64{"": 1, "K": 1 << 10, "M": 1 << 20, "G": 1 << 30, "T": 1 << 40, "P": 1 << 50, "E": 1 << 60}[m[2]]
	size := splitSize{int64(v * mult), model.Source{File: c.res.Name, Line: c.n}}
	// The thread names the task: its split is known exactly.
	if t := threadTask(h.thread); t != nil {
		if k, ok := c.splitOf[t.TaskID]; ok && c.res.Splits[k].SizeBytes == 0 {
			c.res.Splits[k].SizeBytes, c.res.Splits[k].SizeSource = size.bytes, size.src
		}
		return
	}
	if c.splitOpen == 0 {
		return
	}
	c.splitSizes = append(c.splitSizes, size)
	if c.splitOpen--; c.splitOpen > 0 {
		return
	}
	same := len(c.splitSizes) == len(c.splitGroup)
	for _, z := range c.splitSizes {
		same = same && z.bytes == c.splitSizes[0].bytes
	}
	if same {
		for i, k := range c.splitGroup {
			c.res.Splits[k].SizeBytes, c.res.Splits[k].SizeSource = c.splitSizes[i].bytes, c.splitSizes[i].src
		}
	}
	c.splitGroup, c.splitSizes = c.splitGroup[:0], c.splitSizes[:0]
}

// scanMarker starts a line a job prints to show sparkplain its
// TableInputFormat scan, where it may (not in production, where printing
// it is often not allowed): sparkplain-scan {"table": …, "scan": …}, the
// scan being what the job passes as hbase.mapreduce.scan.
const scanMarker = "sparkplain-scan "

// scanLine decodes a sparkplain-scan line. Only the decoded, redacted scan
// is kept, never the string, which holds the filters' values as they are.
func (c *classifier) scanLine(js string) {
	var m map[string]any
	if len(js) > 1<<20 || json.Unmarshal([]byte(strings.TrimSpace(js)), &m) != nil {
		l := c.entry(model.LogHBaseScan, model.Warning, c.lastTime, "A sparkplain-scan line is not a JSON object with \"table\" and \"scan\"")
		c.add(l)
		return
	}
	str := func(keys ...string) string {
		for _, k := range keys {
			if v, ok := m[k].(string); ok && v != "" {
				return v
			}
		}
		return ""
	}
	table, raw := redact.Clean(str("table", "hbase.mapreduce.inputtable")), str("scan", "hbase.mapreduce.scan")
	sc, err := DecodeScan(raw)
	if raw == "" || err != nil {
		why := "it has no scan"
		if err != nil {
			why = err.Error()
		}
		l := c.entry(model.LogHBaseScan, model.Warning, c.lastTime, "A sparkplain-scan line could not be decoded: "+why)
		l.Fields["table"] = table
		c.add(l)
		return
	}
	if sc.Table == "" {
		sc.Table = table
	}
	sc.Source, sc.Time = model.Source{File: c.res.Name, Line: c.n}, c.lastTime
	if len(c.res.Scans) < 1000 {
		c.res.Scans = append(c.res.Scans, *sc)
	}
	l := c.entry(model.LogHBaseScan, model.Info, c.lastTime, "The job printed its scan of "+sc.Table+": rows "+sc.Rows())
	l.Fields["table"] = sc.Table
	c.add(l)
}

// stamp keeps the file's first and last times, from any line that starts
// with one in a layout EMR's logs use (2026-10-02 05:12:03,123, its ISO
// form, or Spark's 26/10/02 05:12:03), whatever follows: a log4j pattern
// sparkplain does not otherwise read still dates the file.
func (c *classifier) stamp(b []byte) {
	if len(b) < 17 || b[0] < '0' || b[0] > '9' {
		return
	}
	var n int
	var layout string
	switch {
	case len(b) >= 19 && b[4] == '-' && b[7] == '-' && b[13] == ':' && b[16] == ':' && (b[10] == ' ' || b[10] == 'T'):
		n, layout = 19, "2006-01-02 15:04:05"
		if b[10] == 'T' {
			layout = "2006-01-02T15:04:05"
		}
	case b[2] == '/' && b[5] == '/' && b[8] == ' ' && b[11] == ':' && b[14] == ':':
		n, layout = 17, "06/01/02 15:04:05"
	default:
		return
	}
	if string(b[:n]) != c.stampKey {
		t, err := time.Parse(layout, string(b[:n]))
		if err != nil {
			return
		}
		c.stampKey, c.stampTime = string(b[:n]), t
	}
	if c.res.FirstTime.IsZero() || c.stampTime.Before(c.res.FirstTime) {
		c.res.FirstTime = c.stampTime
	}
	if c.stampTime.After(c.res.LastTime) {
		c.res.LastTime = c.stampTime
	}
}
