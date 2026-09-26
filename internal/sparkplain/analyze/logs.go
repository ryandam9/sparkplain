package analyze

import (
	"fmt"
	"path"
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
		if !strings.Contains(l.Text, root) && l.Kind != model.LogTraceback {
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
	firstFailure(c, r)
	memoryKillFindings(c, r)
	lostExecutorCauses(c, r)
	accessFindings(c, r)
	connectionFindings(c)
	stepAndAttemptFindings(c, r)
	bootstrapFinding(c, r)
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
	// Without the event log, the first container of each attempt is the
	// application master, which is the driver in cluster mode.
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
		case f.Container != "" && strings.HasSuffix(f.Container, "_000001") && !c.has():
			f.Executor = "am"
			if cluster {
				f.Executor = "driver"
			}
		case f.Instance != "":
			f.Host = instHost[f.Instance]
		}
	}
}

// applicationFromLogs fills in the application's name, user, queue,
// status and times from YARN's records when there is no event log.
func applicationFromLogs(c *ctx, r *model.Report) {
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

// causeKinds are the lines that can be a failure's cause; exits, reports
// and summaries only say that something failed.
var causeKinds = map[model.LogKind]int{
	model.LogOutOfMemory: 0, model.LogMemoryKill: 0, model.LogAccess: 0, model.LogKerberos: 0, model.LogMetastore: 0, model.LogHBase: 0,
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
		if _, ok := causeKinds[h.l.Kind]; ok && h.l.Severity == model.Critical {
			cands = append(cands, h)
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
	attempts := 0
	var lastExit *hit
	for i, h := range c.logs.hits {
		if h.l.Kind == model.LogAppExit && h.l.Fields["attempt"] != "" && h.l.Severity == model.Critical {
			attempts++
			lastExit = &c.logs.hits[i]
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
	expl := fmt.Sprintf("This is the earliest error in the logs, in %s%s, before %s. Later errors usually follow from it.", first.who(), when, what)
	if attempts > 1 {
		expl += fmt.Sprintf(" YARN started the application %d times and each attempt failed the same way.", attempts)
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
		return "See the memory finding below for what to change."
	case l.Kind == model.LogAccess:
		return "See the access finding below for the permission to grant."
	case strings.Contains(root, "FileNotFoundException") && eventLogDirIn(c, r, msg):
		return "Spark could not open its event log folder, so SparkContext never started. On S3 the folder must already hold an object: create one (for example an empty spark-events/ marker) or point spark.eventLog.dir at a prefix that exists."
	case strings.Contains(root, "FileNotFoundException"):
		return "Check that the path exists and that the job reads the one it should; an input written by an earlier step that failed is a common cause."
	case strings.Contains(root, "ClassNotFoundException") || strings.Contains(root, "NoClassDefFoundError"):
		return "A class was missing at run time: ship its jar with --jars or --packages, or check the versions of the jars on the cluster."
	case strings.Contains(root, "ModuleNotFoundError") || strings.Contains(root, "ImportError"):
		return "A Python module was missing on the node that needed it: ship it with --py-files or install it on every node with a bootstrap action."
	case strings.Contains(root, "AnalysisException"):
		return "Spark SQL rejected the query: a table, column or function name is wrong or missing."
	}
	return "Fix this error first; the evidence shows where it happened. The lines after it in the same log say what it led to."
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
	var kills []hit
	containers := map[string]bool{}
	var usage string
	for _, h := range c.logs.hits {
		l := h.l
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
				f.Explanation += " YARN's own message: “" + usage + "”."
			}
		} else {
			n := len(containers)
			if n == 0 {
				n = len(kills)
			}
			expl := "YARN stops a container that uses more memory than it was given: the Java heap plus the overhead, which also has to hold Python workers and off-heap buffers. Exit code 137 means it was killed with SIGKILL."
			if usage != "" {
				expl += " YARN's own message: “" + usage + "”."
			}
			c.add(model.Finding{Rule: "executor-memory-kill", Severity: model.Critical, Section: "executors",
				Title:       fmt.Sprintf("YARN killed %s for using too much memory", model.Plural(n, "container", "containers")),
				Explanation: expl, Evidence: ev,
				Fix: "Raise spark.executor.memoryOverhead (PySpark jobs often need 20–40% of the heap), or run fewer cores per executor so fewer tasks share the memory."})
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
	fix := "Give each task more memory: raise spark.executor.memory, run fewer cores per executor, or split the work into more partitions (spark.sql.shuffle.partitions) so each task holds less."
	switch {
	case driver && len(order) == 1:
		title = "The driver ran out of memory"
		fix = "Avoid bringing large results to the driver (collect, toPandas, large broadcasts), or raise spark.driver.memory."
	case order[0] == "am" && len(order) == 1:
		title = "The application master ran out of memory"
		fix = "In cluster mode the application master is the driver: avoid bringing large results to it (collect, toPandas, large broadcasts), or raise spark.driver.memory. In client mode raise spark.yarn.am.memory."
	}
	if kind != "" {
		title += " (" + kind + ")"
	}
	if strings.Contains(kind, "Metaspace") {
		fix = "Metaspace holds loaded classes: raise -XX:MaxMetaspaceSize in spark.executor.extraJavaOptions, or load fewer jars."
	}
	c.add(model.Finding{Rule: "out-of-memory", Severity: model.Critical, Section: "memory", Title: title,
		Explanation: "The Java process asked for more memory than it had and threw OutOfMemoryError; the task, and often the executor, failed with it. The container logs say so even when the event log only shows a lost executor or a failed task.",
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
		}
		if rule == "executor-lost" {
			f.Fix = "The evidence includes the last error in each lost executor's own log, when it wrote one. A lost executor with no error of its own usually lost its node (spot reclaim, a node failure) or stopped sending heartbeats under long garbage collection."
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
		Title:       fmt.Sprintf("%s lost during the run", model.Plural(len(seen), "executor was", "executors were")),
		Explanation: "The driver lost contact with these executors (the process crashed, stopped sending heartbeats, or its node went away). Their running tasks and shuffle data had to be recomputed.",
		Evidence:    ev,
		Fix:         "Read each executor's own log around that time for the cause; the event log, when supplied, adds which tasks were affected."})
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
		if h.l.Kind != model.LogAccess {
			continue
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
		Title:       fmt.Sprintf("AWS refused access %s: %s", model.Plural(total, "time", "times"), clip(strings.Join(order, "; "), 200)),
		Explanation: "A request to S3, Glue, Lake Formation or KMS was denied, so the task or query that needed it failed. On EMR, jobs use " + role + " unless they carry other credentials or a runtime role.",
		Evidence:    ev,
		Fix:         "Grant " + role + " the refused action on that resource (or add the Lake Formation permission), then rerun. Check the bucket policy and any KMS key policy too."})
}

// connectionFindings reports Kerberos, metastore and HBase failures.
func connectionFindings(c *ctx) {
	for _, k := range []struct {
		kind              model.LogKind
		rule, title, expl string
		fix               string
	}{
		{model.LogKerberos, "kerberos-failure", "Kerberos authentication failed", "A process could not get or use a Kerberos ticket, so the Hadoop service it called refused it.",
			"Check that the principal and keytab are right and not expired (kinit -kt), that clocks agree, and that the KDC is reachable from every node."},
		{model.LogMetastore, "metastore-failure", "The table catalog could not be reached", "Spark could not talk to the Hive metastore or the Glue Data Catalog, so queries that name tables failed.",
			"Check that the metastore is up and reachable from every node (security groups, hive.metastore.uris), or, for Glue, the role's glue: permissions."},
		{model.LogHBase, "hbase-failure", "HBase could not be reached", "Calls to HBase (or its ZooKeeper) failed, so reads or writes to HBase did not complete.",
			"Check hbase.zookeeper.quorum, that the HBase and ZooKeeper nodes are reachable from every node, and HBase's own logs for region server problems."},
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
					Explanation: fmt.Sprintf("Spark reported success, but step %s ended with exit code %s, so EMR marked the step failed (and may have acted on it, such as cancelling later steps or terminating the cluster).", h.f.Step, h.l.Fields["exitCode"]),
					Evidence:    []model.Evidence{h.evidence(h.who() + ": " + h.l.Text)},
					Fix:         "Look at the step's stderr after the application finished: code that runs after the Spark session ends, or the script's own exit code, failed the step."})
			}
		}
	}
	var failedAttempts []hit
	final := ""
	for _, h := range c.logs.hits {
		switch h.l.Kind {
		case model.LogAppExit:
			if h.l.Fields["attempt"] != "" && (h.l.Fields["status"] == "FAILED" || h.l.Fields["status"] == "KILLED") {
				failedAttempts = append(failedAttempts, h)
			}
		case model.LogAppSummary:
			final = h.l.Fields["finalStatus"]
		}
	}
	if len(failedAttempts) > 0 && final == "SUCCEEDED" {
		h := failedAttempts[0]
		c.add(model.Finding{Rule: "app-retried", Severity: model.Warning, Section: "summary",
			Title:       fmt.Sprintf("YARN restarted the application after %s", model.Plural(len(failedAttempts), "failed attempt", "failed attempts")),
			Explanation: fmt.Sprintf("The first attempt's application master exited with code %s (%s). YARN started it again (spark.yarn.maxAppAttempts), and a later attempt finished. The event log describes the last attempt only.", h.l.Fields["exitCode"], h.l.Fields["meaning"]),
			Evidence:    []model.Evidence{h.evidence(h.who() + ": " + h.l.Text)},
			Fix:         "Find the first attempt's error in its driver log (container …_01_000001). Work the application repeats on retry can double writes that are not idempotent."})
	}
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
		Explanation: "EMR runs bootstrap actions on every node before it starts Hadoop and Spark. One exited with an error, so EMR terminated the cluster.",
		Evidence:    ev, Fix: "Read the action's stderr under node/<instance>/bootstrap-actions/<n>/ in the log bucket, and test the script on a single node."})
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
