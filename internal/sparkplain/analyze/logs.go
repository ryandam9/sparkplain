package analyze

import (
	"fmt"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// logView is what analyzeLogs worked out from the container, step and
// node logs, for the analyzers after it.
type logView struct {
	hits  []hit // every classified line, in file order
	first *hit  // the first failure, when the application or its step failed
}

// hit is one classified line and the file it is in.
type hit struct {
	f *model.LogFile
	l *model.LogLine
}

// who names the process or daemon a line came from.
func (h hit) who() string {
	switch f := h.f; f.Kind {
	case "container-stderr", "container-stdout":
		stream := strings.TrimPrefix(f.Kind, "container-")
		switch f.Executor {
		case "driver":
			if f.EarlierAttempt > 0 {
				return fmt.Sprintf("the driver's %s in attempt %d", stream, f.EarlierAttempt)
			}
			return "the driver's " + stream
		case "":
			return "container " + f.Container + " " + stream
		case "am":
			return "the application master's " + stream
		}
		return "executor " + f.Executor + "'s " + stream
	case "step-stderr", "step-controller":
		return "step " + f.Step + " " + strings.TrimPrefix(f.Kind, "step-")
	case "nodemanager":
		return "the NodeManager on " + nodeName(f)
	case "resourcemanager":
		return "the ResourceManager"
	case "bootstrap", "bootstrap-output":
		return "the bootstrap log on " + nodeName(f)
	}
	return path.Base(h.f.Location)
}

func nodeName(f *model.LogFile) string {
	if f.Host != "" {
		return f.Host
	}
	return f.Instance
}

// evidence cites the line, naming the executor it is about.
func (h hit) evidence(text string) model.Evidence {
	ev := model.Evidence{Source: h.l.Source, Text: text}
	if h.f.EarlierAttempt > 0 {
		return ev // the event log's executors are the last attempt's
	}
	if x := h.f.Executor; x != "" && x != "am" {
		ev.Ref = model.ExecutorRef(x)
	} else if x := h.l.Fields["executor"]; x != "" {
		ev.Ref = model.ExecutorRef(x)
	}
	return ev
}

// says is what a line shows, in short: an exception's root cause, or the
// line itself, with how often it repeated.
func (h hit) says() string {
	l := h.l
	s := l.Text
	if root := l.Fields["root"]; root != "" && l.Kind != model.LogAppExit {
		s = shortClass(root)
		if m := l.Fields["rootMessage"]; m != "" {
			s += ": " + m
		}
		if !strings.Contains(l.Text, root) && l.Kind != model.LogTraceback && !strings.HasPrefix(l.Text, "Traceback") {
			s = strings.TrimSuffix(l.Text, ".") + " — " + s
		}
	}
	if l.Count > 1 {
		s += fmt.Sprintf(" (%d times)", l.Count)
	}
	return clip(s, 400)
}

func shortClass(c string) string {
	if i := strings.LastIndexByte(c, '.'); i >= 0 {
		return c[i+1:]
	}
	return c
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && s[n]&0xC0 == 0x80 {
		n--
	}
	return s[:n] + "…"
}

// analyzeLogs joins the logs to the event log's executors, fills in the
// application from YARN's records when there is no event log, and adds the
// findings only the logs can show.
func analyzeLogs(c *ctx, r *model.Report) {
	if r.Logs == nil {
		return
	}
	v := &logView{}
	c.logs = v
	joinContainers(c, r)
	for i := range r.Logs.Files {
		f := &r.Logs.Files[i]
		for j := range f.Found {
			v.hits = append(v.hits, hit{f, &f.Found[j]})
		}
	}
	applicationFromLogs(c, r)
	statusFromLogs(c, r)
	firstFailure(c, r)
	memoryKillFindings(c, r)
	lostExecutorCauses(c, r)
	accessFindings(c, r)
	connectionFindings(c)
	hbaseFindings(c)
	classpathFinding(c)
	stepAndAttemptFindings(c, r)
	bootstrapFinding(c, r)
	fitFindings(c, r)
	r.Logs.Coverage = model.Complete
	for _, s := range r.Sources {
		switch s.Name {
		case "Container logs", "Step logs", "Node logs":
			if s.Status == "partial" || s.Status == "error" || s.Status == "not-supplied" {
				r.Logs.Coverage = model.Partial
				r.Logs.Missing = append(r.Logs.Missing, s.Name+": "+s.Detail)
			}
		}
	}
}

// joinContainers names the executor each container ran, from the
// CONTAINER_ID attribute YARN gives Spark.
func joinContainers(c *ctx, r *model.Report) {
	exec := map[string]string{}
	host := map[string]string{}
	if c.has() {
		for _, x := range c.log.Executors {
			if id := x.Attributes["CONTAINER_ID"]; id != "" {
				exec[id], host[id] = x.ID, x.Host
			}
		}
		if id := c.log.Application.DriverAttributes["CONTAINER_ID"]; id != "" {
			exec[id], host[id] = "driver", c.log.Application.DriverAttributes["NM_HOST"]
		}
	}
	// The attempt the event log describes; containers of other attempts
	// are marked, since its executors are not theirs.
	final := 0
	if c.has() {
		final = containerAttempt(c.log.Application.DriverAttributes["CONTAINER_ID"])
		if final == 0 {
			final, _ = strconv.Atoi(c.log.Application.AttemptID)
		}
	}
	// The first container of each attempt the event log does not name is
	// the application master, which is the driver in cluster mode.
	cluster := c.has() && c.log.Application.DeployMode == "cluster"
	for _, f := range r.Logs.Files {
		for _, l := range f.Found {
			if l.Kind == model.LogSubmit && (strings.Contains(l.Text, "--deploy-mode cluster") || strings.Contains(l.Text, "spark.submit.deployMode=cluster")) {
				cluster = true
			}
		}
	}
	instHost := map[string]string{}
	if r.Cluster != nil {
		for _, in := range r.Cluster.Instances {
			instHost[in.ID] = in.PrivateDNS
		}
	}
	for i := range r.Logs.Files {
		f := &r.Logs.Files[i]
		switch {
		case f.Container != "" && exec[f.Container] != "":
			f.Executor, f.Host = exec[f.Container], host[f.Container]
		case f.Container != "" && strings.HasSuffix(f.Container, "_000001"):
			f.Executor = "am"
			if cluster {
				f.Executor = "driver"
			}
			for _, l := range f.Found {
				if l.Kind == model.LogDriverHost {
					f.Host = l.Fields["host"]
				}
			}
		case f.Instance != "" && instHost[f.Instance] != "":
			f.Host = instHost[f.Instance]
		}
		if a := containerAttempt(f.Container); a > 0 && final > 0 && a != final {
			f.EarlierAttempt = a
		}
		// With no event log (or a container it does not name), an
		// executor's own log says which executor it was; not for an
		// earlier attempt, whose executor IDs the final one reuses.
		if f.Executor == "" && f.Container != "" && f.EarlierAttempt == 0 {
			for _, l := range f.Found {
				if l.Kind == model.LogExecutorHost {
					f.Executor, f.Host = l.Fields["executor"], l.Fields["host"]
					break
				}
			}
		}
	}
}

// containerAttempt is the YARN attempt number in a container ID
// (container_[e<epoch>_]<ts>_<app>_<attempt>_<n>), or 0.
func containerAttempt(id string) int {
	if !containerRE.MatchString(id) {
		return 0
	}
	parts := strings.Split(id, "_")
	n, _ := strconv.Atoi(parts[len(parts)-2])
	return n
}

// applicationFromLogs fills in the application's name, user, queue,
// status and times from YARN's records when there is no event log.
func applicationFromLogs(c *ctx, r *model.Report) {
	if c.rebuilt {
		// The driver's log does not say who ran it, or in which queue.
		a := &r.Application
		for _, h := range c.logs.hits {
			if k := h.l.Kind; (k == model.LogAppSummary || k == model.LogAppReport) && a.User == "" {
				a.User = h.l.Fields["user"]
				if a.Queue == "" {
					a.Queue = h.l.Fields["queue"]
				}
			}
		}
		return
	}
	if c.has() {
		return
	}
	a := &r.Application
	var failedAttempts []hit
	for _, h := range c.logs.hits {
		l := h.l
		switch l.Kind {
		case model.LogAppSummary:
			f := l.Fields
			a.Name, a.User, a.Queue, a.Source = f["name"], f["user"], f["queue"], l.Source
			a.Status = statusOf(f["finalStatus"])
			a.Start, a.End = msTime(f["startTime"]), msTime(f["finishTime"])
			if !a.Start.IsZero() && !a.End.IsZero() {
				a.DurationMs = a.End.Sub(a.Start).Milliseconds()
			}
		case model.LogAppReport:
			if a.Status == "" {
				a.Status = statusOf(l.Fields["finalStatus"])
				a.User, a.Queue, a.Source = l.Fields["user"], l.Fields["queue"], l.Source
			}
		case model.LogAppExit:
			if l.Fields["status"] == "FAILED" || l.Fields["status"] == "KILLED" {
				failedAttempts = append(failedAttempts, h)
			}
			if a.Status == "" && l.Fields["attempt"] == "" {
				a.Status = statusOf(l.Fields["status"])
			}
		}
	}
	if a.Status == "" {
		a.Status = model.StatusUnknown
	}
	if a.Status == model.StatusFailed && len(failedAttempts) > 0 {
		last := failedAttempts[len(failedAttempts)-1].l
		a.StatusReason = fmt.Sprintf("Its application master exited with code %s (%s).", last.Fields["exitCode"], last.Fields["meaning"])
	}
}

func statusOf(yarn string) string {
	switch yarn {
	case "SUCCEEDED":
		return model.StatusSucceeded
	case "FAILED", "KILLED":
		return model.StatusFailed
	}
	return ""
}

func msTime(ms string) time.Time {
	n, err := strconv.ParseInt(ms, 10, 64)
	if err != nil || n <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(n).UTC()
}

var containerRE = regexp.MustCompile(`container_(?:e\d+_)?\d+_\d+_\d+_\d+`)

// causeKinds are the lines that can be a failure's cause; exits, reports
// and summaries only say that something failed.
var causeKinds = map[model.LogKind]int{
	model.LogOutOfMemory: 0, model.LogMemoryKill: 0, model.LogAccess: 0, model.LogKerberos: 0, model.LogMetastore: 0, model.LogHBase: 0, model.LogClasspath: 0,
	model.LogException: 1, model.LogTraceback: 1, model.LogTaskError: 1, model.LogLostExecutor: 2, model.LogContainerEnd: 2, model.LogBootstrap: 3,
}

// firstFailure finds the earliest error across all the logs when the
// application or its step failed, and says what it was.
func firstFailure(c *ctx, r *model.Report) {
	failed := r.Application.Status == model.StatusFailed
	var stepFailed *hit
	for i, h := range c.logs.hits {
		if h.l.Kind == model.LogStepStatus && h.l.Fields["status"] != "succeeded" {
			stepFailed = &c.logs.hits[i]
		}
	}
	if !failed && stepFailed == nil {
		return
	}
	var cands []hit
	for _, h := range c.logs.hits {
		// spark-submit's "Application … finished with failed status" only
		// reports that the application failed.
		if h.f.Kind == "step-stderr" && strings.Contains(h.l.Text, "finished with failed status") {
			continue
		}
		if _, ok := causeKinds[h.l.Kind]; ok && h.l.Severity == model.Critical {
			cands = append(cands, h)
		}
	}
	if len(cands) == 0 {
		// Nothing critical, such as an exception the driver logged as a
		// warning before the run ended: the earliest warning is the lead.
		for _, h := range c.logs.hits {
			if k := h.l.Kind; (k == model.LogException || k == model.LogTraceback || k == model.LogTaskError) && h.l.Severity == model.Warning {
				cands = append(cands, h)
			}
		}
	}
	if len(cands) == 0 {
		return
	}
	sort.SliceStable(cands, func(i, j int) bool {
		a, b := cands[i].l, cands[j].l
		switch {
		case a.Time.IsZero() != b.Time.IsZero():
			return !a.Time.IsZero() // lines without a time (stdout) last
		case !a.Time.Equal(b.Time):
			return a.Time.Before(b.Time)
		}
		return causeKinds[a.Kind] < causeKinds[b.Kind]
	})
	first := cands[0]
	// A container's exit is the effect; its own log says the cause. Stdout
	// carries no times, so a cause written there (HotSpot's out-of-memory
	// banner, a traceback) sorts after the exit it caused.
	if k := first.l.Kind; k == model.LogContainerEnd || k == model.LogLostExecutor {
		id := first.l.Fields["container"]
		if id == "" {
			id = containerRE.FindString(first.l.Text)
		}
		for _, h := range cands {
			if id != "" && h.f.Container == id && causeKinds[h.l.Kind] <= 1 {
				first = h
				break
			}
		}
	}
	c.logs.first = &first
	l := first.l
	ev := []model.Evidence{first.evidence(first.who() + ": " + first.says())}
	for _, d := range l.Detail {
		if len(ev) == 4 {
			break
		}
		if strings.HasPrefix(d, "at ") && !strings.Contains(d, "(") {
			continue
		}
		if strings.HasPrefix(d, "at ") || strings.HasPrefix(d, "File ") {
			ev = append(ev, first.evidence("where: "+d))
		}
	}
	// The user's code, from a traceback in the same container.
	for _, h := range c.logs.hits {
		if h.l.Kind == model.LogTraceback && h.f.Container != "" && h.f.Container == first.f.Container && h.l != l && h.l.Fields["pyFile"] != "" {
			ev = append(ev, h.evidence(fmt.Sprintf("the Python code: %s line %s", path.Base(h.l.Fields["pyFile"]), h.l.Fields["pyLine"])))
			break
		}
	}
	ends, _, _ := attemptExits(c)
	attempts := len(ends)
	var lastExit *hit
	for n, e := range ends {
		if lastExit == nil || n >= attemptOf(lastExit) {
			lastExit = e.line()
		}
	}
	if lastExit != nil {
		ev = append(ev, lastExit.evidence(fmt.Sprintf("YARN: %s, exit code %s (%s)", model.Plural(attempts, "attempt failed", "attempts failed"), lastExit.l.Fields["exitCode"], lastExit.l.Fields["meaning"])))
	}
	if stepFailed != nil {
		ev = append(ev, stepFailed.evidence(fmt.Sprintf("%s: %s", stepFailed.who(), stepFailed.l.Text)))
	}
	when := ""
	if !l.Time.IsZero() {
		when = " at " + c.clock(l.Time)
	}
	what := "the application failed"
	if !failed {
		what = "its step failed"
	}
	expl := fmt.Sprintf("This is the first error in the logs. It is in %s%s, before %s. The errors after it are usually its results.", first.who(), when, what)
	if attempts > 1 {
		expl += fmt.Sprintf(" YARN started the application %d times, and each attempt failed in the same way.", attempts)
	}
	title := first.says()
	if root := l.Fields["root"]; root != "" {
		title = shortClass(root) + ": " + l.Fields["rootMessage"]
	}
	c.add(model.Finding{Rule: "log-first-failure", Severity: model.Critical, Section: "summary",
		Title: "First error: " + clip(strings.TrimSuffix(title, ": "), 160), Explanation: expl, Evidence: ev, Fix: firstFailureFix(c, r, first)})
}

func firstFailureFix(c *ctx, r *model.Report, h hit) string {
	l := h.l
	root := l.Fields["root"] + " " + l.Fields["pyException"]
	msg := l.Fields["rootMessage"]
	switch {
	case l.Kind == model.LogOutOfMemory || l.Kind == model.LogMemoryKill:
		return "Refer to the memory finding below for the change to make."
	case l.Kind == model.LogAccess:
		return "Refer to the access finding below for the permission to give."
	case l.Kind == model.LogClasspath || l.Fields["cause"] == "missing class":
		return "Refer to the missing-class finding below for the jar to add."
	case l.Kind == model.LogHBase || l.Fields["cause"] == "HBase":
		return "Refer to the HBase finding below for the failure and its correction."
	case strings.Contains(root, "FileNotFoundException") && eventLogDirIn(c, r, msg):
		return "Spark could not open its event log folder, so SparkContext did not start. On S3, the folder must have an object before Spark starts. Do one of these:\n- Make an object in the folder, for example an empty spark-events/ marker.\n- Set spark.eventLog.dir to a prefix that exists."
	case strings.Contains(root, "FileNotFoundException"):
		return "Make sure that the path exists. Make sure that the job reads the correct path.\nA frequent cause is an input from an earlier step that failed."
	case strings.Contains(root, "ClassNotFoundException") || strings.Contains(root, "NoClassDefFoundError"):
		return "A class was missing at run time. Do one of these:\n- Add its jar with --jars or --packages.\n- Make sure that the versions of the jars on the cluster are correct."
	case strings.Contains(root, "ModuleNotFoundError") || strings.Contains(root, "ImportError"):
		return "A Python module was missing on a node. Do one of these:\n- Send it with --py-files.\n- Install it on all nodes with a bootstrap action."
	case strings.Contains(root, "AnalysisException"):
		return "Spark SQL did not accept the query. A table, column or function name is incorrect or missing. Correct the name in the query."
	}
	return "Correct this error first. The evidence shows where it occurred.\nThe lines after it in the same log show its results."
}

// eventLogDirIn reports whether msg names the application's event log
// folder.
func eventLogDirIn(c *ctx, r *model.Report, msg string) bool {
	var dirs []string
	if r.Cluster != nil {
		dirs = append(dirs, r.Cluster.Configurations["spark-defaults/spark.eventLog.dir"])
	}
	for _, h := range c.logs.hits {
		if h.l.Kind == model.LogSubmit {
			if _, v, ok := strings.Cut(h.l.Text, "spark.eventLog.dir="); ok {
				v, _, _ = strings.Cut(v, " ")
				dirs = append(dirs, v)
			}
		}
	}
	for _, d := range dirs {
		if d = strings.TrimSuffix(d, "/"); d != "" && strings.Contains(msg, d) {
			return true
		}
	}
	return false
}

// memoryKillFindings adds the logs' memory kills to the event log's
// executor-memory-kill finding (or makes one), and reports out-of-memory
// errors.
func memoryKillFindings(c *ctx, r *model.Report) {
	// A JVM that ran out of heap kills itself (-XX:OnOutOfMemoryError="kill
	// -9 %p", which Spark sets), so its container also exits 137: that is
	// an out-of-memory error, not YARN enforcing a memory limit.
	selfKilled := map[string]bool{}
	for _, h := range c.logs.hits {
		if h.l.Kind == model.LogOutOfMemory && h.f.Container != "" {
			selfKilled[h.f.Container] = true
		}
	}
	mentions := func(text string) bool {
		for id := range selfKilled {
			if strings.Contains(text, id) {
				return true
			}
		}
		return false
	}
	var kills []hit
	containers := map[string]bool{}
	var usage string
	for _, h := range c.logs.hits {
		l := h.l
		if l.Kind != model.LogMemoryKill && (selfKilled[l.Fields["container"]] || mentions(l.Text)) {
			continue
		}
		switch {
		case l.Kind == model.LogMemoryKill:
			kills = append(kills, h)
			containers[l.Fields["container"]] = true
			if usage == "" {
				usage = l.Fields["usage"]
			}
		case l.Kind == model.LogContainerEnd && (l.Fields["exitCode"] == "137" || l.Fields["exitCode"] == "-104" || l.Fields["exitCode"] == "-103"):
			kills = append(kills, h)
			containers[l.Fields["container"]] = true
		case l.Kind == model.LogLostExecutor && l.Severity == model.Critical:
			kills = append(kills, h)
		}
	}
	if len(kills) > 0 {
		var ev []model.Evidence
		for i, h := range kills {
			if i == 5 {
				ev = append(ev, model.Evidence{Text: fmt.Sprintf("… and %d more lines in the logs", len(kills)-5)})
				break
			}
			ev = append(ev, h.evidence(h.who()+": "+h.says()))
		}
		if f := c.finding("executor-memory-kill"); f != nil {
			f.Evidence = append(f.Evidence, ev...)
			if usage != "" {
				f.Explanation += "\nThe message from YARN: “" + usage + "”."
			}
		} else {
			n := len(containers)
			if n == 0 {
				n = len(kills)
			}
			expl := "YARN stops a container that uses more memory than YARN gave it. That memory is the Java heap and the overhead. The overhead also holds the Python workers and the off-heap buffers. Exit code 137 means that SIGKILL stopped the container."
			if usage != "" {
				expl += "\nThe message from YARN: “" + usage + "”."
			}
			c.add(model.Finding{Rule: "executor-memory-kill", Severity: model.Critical, Section: "executors",
				Title:       fmt.Sprintf("YARN killed %s because they used too much memory", model.Plural(n, "container", "containers")),
				Explanation: expl, Evidence: ev,
				Fix: "Do one of these:\n- Increase spark.executor.memoryOverhead. PySpark jobs often use 20–40% of the heap for it.\n- Use fewer cores for each executor, so that fewer tasks share the memory."})
		}
	}

	byWho := map[string][]hit{}
	var order []string
	for _, h := range c.logs.hits {
		if h.l.Kind != model.LogOutOfMemory && h.l.Fields["cause"] != "OutOfMemoryError" {
			continue
		}
		who := h.f.Executor
		if who == "" {
			who = h.f.Container
		}
		if byWho[who] == nil {
			order = append(order, who)
		}
		byWho[who] = append(byWho[who], h)
	}
	if len(order) == 0 {
		return
	}
	var ev []model.Evidence
	kind := ""
	driver := false
	for _, who := range order {
		h := byWho[who][0]
		if len(ev) < 6 {
			ev = append(ev, h.evidence(h.who()+": "+h.says()))
		}
		if kind == "" {
			kind = h.l.Fields["oom"]
		}
		driver = driver || who == "driver"
	}
	title := fmt.Sprintf("%s ran out of memory", model.Plural(len(order), "executor", "executors"))
	fix := "Give each task more memory. Do one of these:\n- Increase spark.executor.memory.\n- Use fewer cores for each executor.\n- Divide the work into more partitions (spark.sql.shuffle.partitions), so that each task holds less data."
	switch {
	case driver && len(order) == 1:
		title = "The driver ran out of memory"
		fix = "Do one of these:\n- Do not bring large results to the driver (collect, toPandas, large broadcasts).\n- Increase spark.driver.memory."
	case order[0] == "am" && len(order) == 1:
		title = "The application master ran out of memory"
		fix = "In cluster mode, the application master is the driver. Do one of these:\n- Do not bring large results to it (collect, toPandas, large broadcasts).\n- Increase spark.driver.memory.\nIn client mode, increase spark.yarn.am.memory."
	}
	if kind != "" {
		title += " (" + kind + ")"
	}
	if strings.Contains(kind, "Metaspace") {
		fix = "Metaspace holds the classes that the JVM loaded. Do one of these:\n- Increase -XX:MaxMetaspaceSize in spark.executor.extraJavaOptions.\n- Load fewer jars."
	}
	c.add(model.Finding{Rule: "out-of-memory", Severity: model.Critical, Section: "memory", Title: title,
		Explanation: "The Java process asked for more memory than it had, and it stopped with an OutOfMemoryError. The task failed, and often the executor failed too. The container logs show this, also when the event log shows only a lost executor or a failed task.",
		Evidence:    ev, Fix: fix})
}

// lostExecutorCauses adds, for each executor the event log says was lost,
// the last error in its own container log: often the only place the real
// cause is written.
func lostExecutorCauses(c *ctx, r *model.Report) {
	last := map[string]hit{}
	for _, h := range c.logs.hits {
		x := h.f.Executor
		if x == "" || x == "driver" || x == "am" || h.l.Severity == model.Info {
			continue
		}
		switch h.l.Kind {
		case model.LogException, model.LogOutOfMemory, model.LogTraceback, model.LogSignal, model.LogLostExecutor, model.LogTaskError, model.LogAccess:
			last[x] = h
		}
	}
	for _, rule := range []string{"executor-lost", "executor-memory-kill", "executor-decommissioned"} {
		f := c.finding(rule)
		if f == nil {
			continue
		}
		for _, e := range f.Evidence {
			x := strings.TrimPrefix(e.Ref, "executor:")
			h, ok := last[x]
			if !ok || e.Ref == "" {
				continue
			}
			f.Evidence = append(f.Evidence, h.evidence(fmt.Sprintf("executor %s's own log: %s", x, h.says())))
			delete(last, x)
			if rule == "executor-memory-kill" && h.l.Kind == model.LogOutOfMemory && !strings.Contains(f.Explanation, "the JVM stopped itself") {
				f.Explanation += "\nThe logs of the executors show that the Java heap became full and the JVM stopped itself. Spark starts executors with -XX:OnOutOfMemoryError=\"kill -9 %p\", which also gives exit code 137. As a result, the cause was the heap, not the YARN limit on the container."
				f.Fix = "Give each task more heap. Do one of these:\n- Increase spark.executor.memory.\n- Use fewer cores for each executor.\n- Use more partitions, so that each task holds less data.\nAn increase of spark.executor.memoryOverhead does not help here."
			}
		}
		if rule == "executor-lost" {
			f.Fix = "Read the evidence. It includes the last error in the log of each lost executor, if the executor wrote one.\nA lost executor with no error of its own usually lost its node (a spot interruption or a node failure). Or a long garbage collection stopped its heartbeats."
		}
	}
	if c.has() {
		return
	}
	// Without the event log, the driver's own lines are the record of lost
	// executors.
	var lost []hit
	for _, h := range c.logs.hits {
		if h.l.Kind == model.LogLostExecutor && h.l.Severity == model.Warning && h.f.Executor == "driver" {
			lost = append(lost, h)
		}
	}
	if len(lost) == 0 {
		return
	}
	var ev []model.Evidence
	seen := map[string]bool{}
	for _, h := range lost {
		x := h.l.Fields["executor"]
		if seen[x] || len(ev) >= 6 {
			continue
		}
		seen[x] = true
		ev = append(ev, h.evidence(h.who()+": "+h.says()))
	}
	c.add(model.Finding{Rule: "executor-lost", Severity: model.Warning, Section: "executors",
		Title:       fmt.Sprintf("Spark lost %s during the run", model.Plural(len(seen), "executor", "executors")),
		Explanation: "The driver lost contact with these executors. The process stopped, it stopped its heartbeats, or its node stopped. Spark calculated their tasks and their shuffle data again.",
		Evidence:    ev,
		Fix:         "Find the cause in the log of each executor, near that time.\nIf you give the event log, this finding also shows the tasks that the loss affected."})
}

// accessFindings reports every refusal by S3, IAM, Glue, Lake Formation or
// KMS, grouped by what was refused.
func accessFindings(c *ctx, r *model.Report) {
	type group struct {
		what  string
		hits  []hit
		count int
	}
	groups := map[string]*group{}
	var order []string
	for _, h := range c.logs.hits {
		if h.l.Kind != model.LogAccess || h.l.Fields["service"] == "HBase" {
			continue // HBase's own refusals are reported apart
		}
		what := h.l.Fields["action"]
		if p := h.l.Fields["path"]; p != "" {
			what = strings.TrimSpace(what + " " + p)
		} else if res := h.l.Fields["resource"]; res != "" {
			what += " on " + res
		}
		if what == "" {
			what = "an AWS call"
		}
		g := groups[what]
		if g == nil {
			g = &group{what: what}
			groups[what] = g
			order = append(order, what)
		}
		g.hits = append(g.hits, h)
		g.count += h.l.Count
	}
	if len(order) == 0 {
		return
	}
	var ev []model.Evidence
	total := 0
	for _, w := range order {
		g := groups[w]
		total += g.count
		if len(ev) < 6 {
			ev = append(ev, g.hits[0].evidence(g.hits[0].who()+": "+g.hits[0].says()))
		}
	}
	role := "the cluster's EC2 instance profile"
	if r.Cluster != nil && r.Cluster.InstanceProfile != "" {
		role = "the instance profile " + r.Cluster.InstanceProfile
	}
	c.add(model.Finding{Rule: "access-denied", Severity: model.Critical, Section: "access",
		Title:       fmt.Sprintf("AWS refused access %s (%s)", model.Plural(total, "time", "times"), clip(strings.Join(order, "; "), 200)),
		Explanation: "AWS refused a request to S3, Glue, Lake Formation or KMS, so the task or query that sent it failed. On EMR, jobs use " + role + ". Other credentials or a runtime role can replace it.",
		Evidence:    ev,
		Fix:         "Give " + role + " the refused action on that resource, or add the Lake Formation permission. Then run the job again.\nAlso examine the bucket policy and the KMS key policy, if there is one."})
}

// connectionFindings reports Kerberos and metastore failures; HBase's are
// told apart in hbaseFindings.
func connectionFindings(c *ctx) {
	for _, k := range []struct {
		kind              model.LogKind
		rule, title, expl string
		fix               string
	}{
		{model.LogKerberos, "kerberos-failure", "Kerberos authentication failed", "A process could not get or use a Kerberos ticket, so the Hadoop service refused the process.",
			"Make sure of these:\n- The principal and the keytab are correct and not expired (kinit -kt).\n- The clocks of the nodes agree.\n- All nodes can reach the KDC."},
		{model.LogMetastore, "metastore-failure", "Spark could not reach the table catalog", "Spark could not connect to the Hive metastore or the Glue Data Catalog. As a result, the queries that name tables failed.",
			"Make sure of these:\n- The metastore runs, and all nodes can reach it (security groups, hive.metastore.uris).\n- For Glue, the role has the glue: permissions."},
	} {
		var ev []model.Evidence
		n := 0
		for _, h := range c.logs.hits {
			if h.l.Kind == k.kind && h.l.Severity == model.Critical {
				n += h.l.Count
				if len(ev) < 5 {
					ev = append(ev, h.evidence(h.who()+": "+h.says()))
				}
			}
		}
		if n > 0 {
			c.add(model.Finding{Rule: k.rule, Severity: model.Critical, Section: "access", Title: fmt.Sprintf("%s (%s in the logs)", k.title, model.Plural(n, "error", "errors")),
				Explanation: k.expl, Evidence: ev, Fix: k.fix})
		}
	}
}

// stepAndAttemptFindings reports a step that failed although Spark
// finished, and an application YARN had to restart.
func stepAndAttemptFindings(c *ctx, r *model.Report) {
	if r.Application.Status == model.StatusSucceeded {
		for _, h := range c.logs.hits {
			if h.l.Kind == model.LogStepStatus && h.l.Fields["status"] != "succeeded" {
				c.add(model.Finding{Rule: "step-failed", Severity: model.Critical, Section: "summary",
					Title:       "The step failed although Spark finished",
					Explanation: fmt.Sprintf("Spark reported success, but step %s ended with exit code %s. As a result, EMR set the step to failed. EMR can then do the action that the step sets on failure, for example cancel the steps after it or stop the cluster.", h.f.Step, h.l.Fields["exitCode"]),
					Evidence:    []model.Evidence{h.evidence(h.who() + ": " + h.l.Text)},
					Fix:         "Read the stderr of the step after the point where the application finished.\nThe cause is code that runs after the Spark session ends, or the exit code of the script."})
			}
		}
	}
	retriedFinding(c, r)
}

// attemptEnd is how one YARN attempt ended, as the ResourceManager and the
// attempt's own driver ("Final app status") logged it.
type attemptEnd struct{ rm, am *hit }

// line is the attempt's ending as the driver logged it, else YARN.
func (e attemptEnd) line() *hit {
	if e.am != nil {
		return e.am
	}
	return e.rm
}

func attemptOf(h *hit) int {
	if a := h.l.Fields["attempt"]; a != "" {
		return attemptNumber(a)
	}
	return containerAttempt(h.f.Container)
}

// attemptExits reads the attempts that failed or were killed, whether any
// attempt's driver or YARN's summary says the application succeeded, and
// the summary's final status ("" without one).
func attemptExits(c *ctx) (failed map[int]attemptEnd, succeeded bool, final string) {
	failed = map[int]attemptEnd{}
	for i := range c.logs.hits {
		h := &c.logs.hits[i]
		switch h.l.Kind {
		case model.LogAppExit:
			switch h.l.Fields["status"] {
			case "FAILED", "KILLED":
				n, e := attemptOf(h), failed[attemptOf(h)]
				if h.l.Fields["attempt"] != "" {
					e.rm = h
				} else if h.f.Container != "" {
					e.am = h
				}
				failed[n] = e
			case "SUCCEEDED":
				succeeded = true
			}
		case model.LogAppSummary:
			final = h.l.Fields["finalStatus"]
			if final == "SUCCEEDED" {
				succeeded = true
			}
		}
	}
	return failed, succeeded, final
}

// statusFromLogs corrects an application the event log says finished when
// YARN says it failed. Spark closes its event log normally on the way out
// even when the application's own code failed outside any Spark job (on
// the 5-node test cluster, a Parquet read refused before any job ran), so
// the last attempt's own exit, or YARN's summary, has the last word.
func statusFromLogs(c *ctx, r *model.Report) {
	a := &r.Application
	if !c.has() || a.Status != model.StatusSucceeded {
		return
	}
	ends, _, final := attemptExits(c)
	last, _ := strconv.Atoi(a.AttemptID)
	if last == 0 {
		last = containerAttempt(a.DriverAttributes["CONTAINER_ID"])
	}
	e, ok := ends[last]
	if !ok && final != "FAILED" && final != "KILLED" {
		return
	}
	a.Status = model.StatusFailed
	state := "failed"
	if final == "KILLED" {
		state = "killed"
	}
	a.StatusReason = "Spark's event log ends normally, but YARN recorded the application as " + state + "."
	if h := e.line(); ok && h != nil {
		a.StatusReason = fmt.Sprintf("Spark's event log ends normally, but the application master exited with code %s (%s), so YARN recorded it as failed.", h.l.Fields["exitCode"], h.l.Fields["meaning"])
	}
}

// retriedFinding reports an application YARN had to restart: a failed
// attempt in the ResourceManager's log or in an attempt's own driver log,
// or an event log from a later attempt, when a later attempt finished.
func retriedFinding(c *ctx, r *model.Report) {
	ends, logSucceeded, _ := attemptExits(c)
	failed := map[int]hit{} // attempt → its failed exit, the driver's own line preferred
	for n, e := range ends {
		if e.am != nil {
			failed[n] = *e.am
		} else {
			failed[n] = *e.rm
		}
	}
	succeeded := r.Application.Status == model.StatusSucceeded || (logSucceeded && r.Application.Status != model.StatusFailed)
	last, _ := strconv.Atoi(r.Application.AttemptID)
	if !succeeded || (len(failed) == 0 && last < 2) {
		return
	}
	fix := "Find the error of the first attempt in its driver log (container …_01_000001).\nMake sure that the writes of the application are idempotent. The new attempt does the work again, and it can write the same data two times."
	if len(failed) == 0 {
		c.add(model.Finding{Rule: "app-retried", Severity: model.Warning, Section: "summary",
			Title:       fmt.Sprintf("YARN started the application again, and the event log is from attempt %d", last),
			Explanation: "An earlier attempt failed, and YARN started the application again (spark.yarn.maxAppAttempts). sparkplain did not find the logs of the earlier attempts, so the cause of their failure is not known. The event log shows only the last attempt.",
			Evidence:    []model.Evidence{{Source: r.Application.Source, Text: fmt.Sprintf("Spark event log: application attempt %d", last)}},
			Fix:         fix})
		return
	}
	nums := make([]int, 0, len(failed))
	for n := range failed {
		nums = append(nums, n)
	}
	sort.Ints(nums)
	n0 := nums[0]
	h := failed[n0]
	ev := []model.Evidence{h.evidence(h.who() + ": " + h.l.Text)}
	which := "The first attempt"
	if n0 > 1 {
		which = fmt.Sprintf("Attempt %d", n0)
	}
	expl := fmt.Sprintf("The application master of %s exited with code %s (%s).", strings.ToLower(which[:1])+which[1:], h.l.Fields["exitCode"], h.l.Fields["meaning"])
	// Where its driver ran, and whether that node went away.
	var host string
	for _, x := range c.logs.hits {
		if x.l.Kind == model.LogDriverHost && containerAttempt(x.f.Container) == n0 {
			host = x.l.Fields["host"]
			ev = append(ev, x.evidence(fmt.Sprintf("%s: the driver ran on %s", x.who(), host)))
			break
		}
	}
	lostNode, spot := false, false
	if host != "" {
		expl += " Its driver ran on " + host + "."
		var notes []string
		for _, x := range c.logs.hits {
			if x.l.Kind == model.LogNodeState && hostKey(x.l.Fields["host"]) == hostKey(host) && containerAttempt(x.f.Container) == n0 {
				notes = append(notes, fmt.Sprintf("YARN reported that node %s at %s", x.l.Fields["state"], c.clock(x.l.Time)))
				ev = append(ev, x.evidence(x.who()+": "+x.l.Text))
				lostNode = true
				break
			}
		}
		if r.Cluster != nil {
			for _, in := range r.Cluster.Instances {
				if (hostKey(in.PrivateDNS) == hostKey(host) || hostKey(in.PrivateIP) == hostKey(host)) && !in.Ended.IsZero() && (c.end.IsZero() || !in.Ended.After(c.end.Add(time.Hour))) {
					notes = append(notes, fmt.Sprintf("EMR reports instance %s ended at %s: %s", in.ID, c.clock(in.Ended), strings.TrimSuffix(orNone(in.StateReason), ".")))
					ev = append(ev, model.Evidence{Text: fmt.Sprintf("EMR ListInstances: %s (%s, %s) ended at %s: %s", in.ID, host, strings.ToLower(strings.ReplaceAll(in.Market, "_", "-")), in.Ended.UTC().Format("15:04:05 UTC"), orNone(in.StateReason))})
					lostNode, spot = true, in.Market == "SPOT"
				}
			}
		}
		if len(notes) > 0 {
			expl += " " + strings.Join(notes, ", and ") + "."
		}
	}
	if cause := attemptCause(c, n0); cause != nil {
		expl += fmt.Sprintf("\nThe first error of that attempt, in %s: %s.", cause.who(), strings.TrimSuffix(cause.says(), "."))
		ev = append(ev, cause.evidence(cause.who()+": "+cause.says()))
	}
	later := "a later attempt"
	if last > n0 {
		later = fmt.Sprintf("attempt %d", last)
	}
	expl += fmt.Sprintf("\nYARN started the application again (spark.yarn.maxAppAttempts), and %s finished. The event log shows only the last attempt.", later)
	switch {
	case spot:
		fix = "The driver ran on a spot node, and AWS took the node back. Do not run the driver on spot capacity. Do one of these:\n- Run the driver in client mode on the primary node.\n- Use YARN node labels, so that application masters go only to on-demand nodes.\n" + fix
	case lostNode:
		fix = "The node of the driver stopped while the driver ran. Run the driver on nodes that stay for all of the run, such as core nodes.\n" + fix
	}
	c.add(model.Finding{Rule: "app-retried", Severity: model.Warning, Section: "summary",
		Title:       fmt.Sprintf("YARN restarted the application after %s", model.Plural(len(failed), "failed attempt", "failed attempts")),
		Explanation: expl, Evidence: ev, Fix: fix})
}

// attemptNumber reads the attempt from appattempt_<ts>_<app>_<n>.
func attemptNumber(id string) int {
	i := strings.LastIndexByte(id, '_')
	n, _ := strconv.Atoi(id[i+1:])
	return n
}

// attemptCause is the earliest error in an attempt's own containers:
// exceptions and failed tasks, which Spark logs as warnings when it
// retries them, before the lines that only say it gave up.
func attemptCause(c *ctx, n int) *hit {
	var best *hit
	for i, h := range c.logs.hits {
		rank, ok := causeKinds[h.l.Kind]
		if !ok || rank > 1 || h.l.Severity == model.Info || containerAttempt(h.f.Container) != n {
			continue
		}
		if best == nil || earlier(h.l, best.l) {
			best = &c.logs.hits[i]
		}
	}
	return best
}

// earlier orders lines by time, lines without one (stdout) last.
func earlier(a, b *model.LogLine) bool {
	if a.Time.IsZero() != b.Time.IsZero() {
		return !a.Time.IsZero()
	}
	return a.Time.Before(b.Time)
}

// bootstrapFinding reports a failed bootstrap action, but only when the
// cluster says so: EMR logs "bootstrap action 1 failed" for the primary
// node even on clusters with no bootstrap actions.
func bootstrapFinding(c *ctx, r *model.Report) {
	if r.Cluster == nil || r.Cluster.StateCode != "BOOTSTRAP_FAILURE" {
		return
	}
	ev := []model.Evidence{{Text: "EMR DescribeCluster: " + r.Cluster.StateReason}}
	for _, h := range c.logs.hits {
		if h.l.Kind == model.LogBootstrap && len(ev) < 5 {
			ev = append(ev, h.evidence(h.who()+": "+h.l.Text))
		}
	}
	c.add(model.Finding{Rule: "bootstrap-failed", Severity: model.Critical, Section: "nodes", Title: "A bootstrap action failed, so the cluster could not start",
		Explanation: "EMR runs the bootstrap actions on all nodes before it starts Hadoop and Spark. One action exited with an error, so EMR stopped the cluster.",
		Evidence:    ev, Fix: "Read the stderr of the action in the log bucket, under node/<instance>/bootstrap-actions/<n>/.\nTest the script on one node."})
}

// clock shows a time of day in the report's time zone, labelled.
func (c *ctx) clock(t time.Time) string {
	loc, err := time.LoadLocation(c.in.TimeZone)
	if err != nil || c.in.TimeZone == "" {
		loc = time.UTC
	}
	return t.In(loc).Format("15:04:05 MST")
}

// finding returns the finding with this rule, if one was added.
func (c *ctx) finding(rule string) *model.Finding {
	for i := range c.findings {
		if c.findings[i].Rule == rule {
			return &c.findings[i]
		}
	}
	return nil
}
