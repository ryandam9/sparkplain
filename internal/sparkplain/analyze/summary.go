package analyze

import (
	"fmt"
	"strings"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

func analyzeIdentity(c *ctx, r *model.Report) {
	s := &r.Identity
	s.Missing = []string{
		"AWS roles: instance profile, EMR service role and any runtime role (needs the EMR API, phase 3)",
		"EMR security configuration: encryption, Kerberos, Lake Formation (phase 3)",
		"AWS calls made by the job's role and every AccessDenied (needs CloudTrail, phase 3)",
		"Whether Hive metastore, HBase and Kerberos connections succeeded (needs container logs, phase 2)",
	}
	if !c.has() {
		s.Coverage = model.NeedsEventLog
		return
	}
	s.Coverage = model.Partial
	a := c.log.Application
	src := c.confSrc
	add := func(label, value, explain string, srcs ...model.Source) {
		f := model.Fact{Label: label, Value: value, Explain: explain}
		if len(srcs) > 0 {
			f.Source = srcs[0]
		}
		s.Facts = append(s.Facts, f)
	}
	add("Ran as", a.User, "The Hadoop user Spark recorded when the application started.", a.Source)
	if u := c.sys["user.name"]; u != "" && u != a.User {
		add("Operating system user", u, "The Linux account the driver process ran under.", src)
	}
	queue := c.conf["spark.yarn.queue"]
	if queue == "" {
		queue = "default"
	}
	if strings.HasPrefix(a.Master, "yarn") {
		add("YARN queue", queue, "The queue that decided how much of the cluster this application could use.", src)
	}
	auth := c.conf["hadoop.security.authentication"]
	if auth == "" {
		auth = "simple"
	}
	principal := c.conf["spark.kerberos.principal"]
	if principal == "" {
		principal = c.conf["spark.yarn.principal"]
	}
	switch {
	case strings.EqualFold(auth, "kerberos") && principal != "":
		add("Kerberos", "on, principal "+principal, "Hadoop services checked this Kerberos identity.", src)
	case strings.EqualFold(auth, "kerberos"):
		add("Kerberos", "on", "Hadoop services require Kerberos. The principal was not in the Spark settings, so it came from a ticket cache or keytab on the node.", src)
	default:
		add("Kerberos", "off (simple authentication)", "Hadoop services trust the user name they are given, without a Kerberos ticket.", src)
	}
	factory := firstOf(c.conf, "hive.metastore.client.factory.class", "spark.hadoop.hive.metastore.client.factory.class")
	uris := firstOf(c.conf, "hive.metastore.uris", "spark.hadoop.hive.metastore.uris", "spark.sql.hive.metastore.uris")
	catalog := c.conf["spark.sql.catalogImplementation"]
	switch {
	case strings.Contains(factory, "AWSGlueDataCatalog"):
		add("Table catalog", "AWS Glue Data Catalog", "Table definitions come from Glue, so access is controlled by the role's IAM permissions (and Lake Formation if enabled).", src)
	case uris != "":
		add("Table catalog", "Hive metastore at "+uris, "Table definitions come from a Hive metastore server.", src)
	case catalog == "hive":
		add("Table catalog", "Hive (local or default metastore)", "Hive support is on, but no metastore URI or Glue factory is set, so Spark used the default metastore.", src)
	default:
		add("Table catalog", "Spark in-memory catalog", "No Hive or Glue catalog was configured; tables created here last only for this run unless written to a warehouse path.", src)
	}
	if q := firstOf(c.conf, "hbase.zookeeper.quorum", "spark.hadoop.hbase.zookeeper.quorum"); q != "" {
		hauth := firstOf(c.conf, "hbase.security.authentication", "spark.hadoop.hbase.security.authentication")
		if hauth == "" {
			hauth = "simple"
		}
		add("HBase", "ZooKeeper "+q+", "+hauth+" authentication", "From the HBase settings the application carried.", src)
	}
	var on, off []string
	for _, sec := range []struct{ key, label string }{
		{"spark.authenticate", "RPC authentication"},
		{"spark.network.crypto.enabled", "RPC encryption"},
		{"spark.io.encryption.enabled", "local disk encryption"},
		{"spark.ssl.enabled", "TLS for Spark UIs"},
	} {
		if c.confBool(sec.key, false) {
			on = append(on, sec.label)
		} else {
			off = append(off, sec.label)
		}
	}
	val := "on: " + strings.Join(on, ", ")
	if len(on) == 0 {
		val = "none of Spark's own protections"
	}
	add("Spark security settings", val, "Off: "+strings.Join(off, ", ")+". EMR can also encrypt traffic and disks through its security configuration, which needs the EMR API to show.", src)
	var creds []string
	for _, k := range []string{"spark.hadoop.fs.s3a.access.key", "spark.hadoop.fs.s3a.secret.key", "fs.s3a.access.key", "fs.s3a.secret.key", "spark.hadoop.fs.s3.awsAccessKeyId", "spark.hadoop.fs.s3.awsSecretAccessKey", "spark.executorEnv.AWS_SECRET_ACCESS_KEY", "spark.executorEnv.AWS_ACCESS_KEY_ID", "spark.yarn.appMasterEnv.AWS_SECRET_ACCESS_KEY"} {
		if _, ok := c.conf[k]; ok {
			creds = append(creds, k)
		}
	}
	if len(creds) > 0 {
		add("AWS credentials in settings", strings.Join(creds, ", "), "Values hidden. Static keys set here override the node's instance profile role.", src)
		c.add(model.Finding{
			Rule: "access-static-keys", Severity: model.Warning, Section: "access",
			Title:       "Static AWS credentials are set in the Spark configuration",
			Explanation: "The application carried AWS keys in its settings. Anyone who can read the event log, the Spark UI or the History Server can see settings, and static keys do not expire. sparkplain hides the values, but Spark only hides them if spark.redaction.regex matches.",
			Evidence:    []model.Evidence{{Source: src, Text: strings.Join(creds, ", ") + " (values hidden)"}},
			Fix:         "Remove the keys and let EMR's instance profile or an EMR runtime role provide credentials. Rotate the keys if the logs were shared.",
		})
	}
}

func firstOf(m map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := m[k]; v != "" {
			return v
		}
	}
	return ""
}

func analyzeSources(c *ctx, r *model.Report) {
	ev := c.in.EventSource
	if ev.Name == "" {
		ev = model.SourceStatus{Name: "Spark event log", Status: "not-supplied", Detail: "No event log was given."}
	}
	r.Sources = append(r.Sources, ev)
	notYet := func(name, detail string) {
		r.Sources = append(r.Sources, model.SourceStatus{Name: name, Status: "not-yet", Detail: detail})
	}
	notYet("Container logs", "Not read: this version of sparkplain reads the event log only. Planned for phase 2 (S3 fetching).")
	notYet("Step logs", "Not read: planned for phase 2.")
	notYet("Node logs", "Not read: planned for phase 2.")
	notYet("EMR API", "Not called: planned for phase 3 (instance types, roles, security configuration).")
	notYet("CloudWatch", "Not called: planned for phase 3 (host CPU, memory and disk).")
	notYet("CloudTrail", "Not called: planned for phase 3 (AWS calls and AccessDenied).")
}

func analyzeCoverage(c *ctx, r *model.Report) {
	row := func(id, title string, cov model.Coverage, shown string, missing []string) {
		r.Coverage = append(r.Coverage, model.SectionStatus{ID: id, Title: title, Coverage: cov, Shown: shown, Missing: missing})
	}
	if !c.has() {
		for _, s := range []struct{ id, title string }{{"summary", "Application summary"}, {"nodes", "Cluster and nodes"}, {"executors", "Executors"}, {"memory", "Memory"}, {"cpu", "CPU"}, {"io", "Storage and I/O"}, {"stages", "Jobs, stages, tasks"}, {"config", "Configuration"}, {"access", "Identity and access"}} {
			row(s.id, s.title, model.NeedsEventLog, "Nothing: this version reads only the event log", []string{"The event log"})
		}
		return
	}
	var appMissing []string
	if r.Application.Status == model.StatusIncomplete {
		appMissing = []string{"The end of the run: the log stops before the application ended"}
	}
	appMissing = append(appMissing, "EMR release and step ID (needs the EMR API and step logs)")
	row("summary", "Application summary", model.Partial, "Name, ID, Spark version, user, start, end, duration, final status", appMissing)
	row("nodes", "Cluster and nodes", r.Nodes.Coverage, "Every host that ran the driver or executors, with the work done on each", r.Nodes.Missing)
	row("executors", "Executors", r.Executors.Coverage, "How many, where, size, lifetime, why each ended, tasks and data per executor", r.Executors.Missing)
	row("memory", "Memory", r.Memory.Coverage, "Configured sizes, peak heap and off-heap per executor, spill, garbage collection", r.Memory.Missing)
	row("cpu", "CPU", r.CPU.Coverage, "CPU time against run time per executor and stage, and how busy executor cores were", r.CPU.Missing)
	row("io", "Storage and I/O", r.IO.Coverage, "Bytes and rows read, written and shuffled per stage, cached data, tables and paths", r.IO.Missing)
	row("stages", "Jobs, stages, tasks", r.Jobs.Coverage, "Every job and stage, task time spread, skew, retries, failures, SQL plans", r.Jobs.Missing)
	row("config", "Configuration", r.Config.Coverage, "The full effective configuration, grouped, with key settings explained", r.Config.Missing)
	row("access", "Identity and access", r.Identity.Coverage, "User, queue, Kerberos, table catalog, Spark security settings, credentials in settings", r.Identity.Missing)
	row("findings", "Findings", model.Partial, "Rules that read the event log: skew, spill, GC, CPU, memory size, lost executors, failures", []string{"Rules that need container logs and AWS APIs: OOM messages, spot interruptions, AccessDenied"})
}

func analyzeSummary(c *ctx, r *model.Report) {
	s := &r.Summary
	if !c.has() {
		s.Sentences = []string{"sparkplain could not read the event log, so it has nothing to report about this run yet. The Sources panel says why."}
		return
	}
	a := r.Application
	name := a.Name
	if name == "" {
		name = a.ID
	}
	var verb string
	switch a.Status {
	case model.StatusSucceeded:
		verb = "and finished"
	case model.StatusFailed:
		verb = "and failed"
	case model.StatusIncomplete:
		verb = "and had not finished when the log ends"
	default:
		verb = "with an unknown outcome"
	}
	var tasks int64
	for _, st := range c.log.Stages {
		tasks += st.Totals.Tasks
	}
	hosts := 0
	for _, h := range r.Nodes.Hosts {
		if len(h.Executors) > 0 {
			hosts++
		}
	}
	first := fmt.Sprintf("%s ran for %s as %s %s.", name, model.Duration(a.DurationMs), orUnknown(a.User), verb)
	if a.Status == model.StatusFailed || a.Status == model.StatusIncomplete {
		first += " " + a.StatusReason
	}
	s.Sentences = append(s.Sentences, first)
	s.Sentences = append(s.Sentences, fmt.Sprintf("It ran %s (%s, %s tasks) on %s across %s. Tasks used %s of CPU time; executors held %s of core time.",
		model.Plural(len(c.log.Jobs), "job", "jobs"), model.Plural(len(c.log.Stages), "stage", "stages"), model.Num(tasks),
		model.Plural(len(c.log.Executors), "executor", "executors"), model.Plural(hosts, "host", "hosts"),
		model.Duration(r.CPU.CPUMs), model.Duration(r.CPU.AllocatedCoreMs)))
	var problems []string
	for _, f := range r.Findings {
		if f.Severity == model.Info || len(problems) == 3 {
			continue
		}
		problems = append(problems, lowerFirst(strings.TrimSuffix(f.Title, ".")))
	}
	if len(problems) > 0 {
		s.Sentences = append(s.Sentences, "What needs attention: "+joinAnd(problems)+".")
	} else {
		s.Sentences = append(s.Sentences, "No warnings or critical findings came up.")
	}
	io := r.IO.Totals
	s.Sentences = append(s.Sentences, fmt.Sprintf("It read %s, wrote %s and moved %s between executors in shuffles.",
		model.Bytes(io.InputBytes), model.Bytes(io.OutputBytes), model.Bytes(io.ShuffleWriteBytes)))

	crit, warn, info := 0, 0, 0
	for _, f := range r.Findings {
		switch f.Severity {
		case model.Critical:
			crit++
		case model.Warning:
			warn++
		default:
			info++
		}
	}
	mem := r.Memory.Config
	var peakHeap int64
	for _, e := range r.Memory.Executors {
		peakHeap = max(peakHeap, e.PeakHeap)
	}
	killed := 0
	for _, x := range c.log.Executors {
		if x.RemovalKind == model.RemovalMemoryKill || x.RemovalKind == model.RemovalLost || x.RemovalKind == model.RemovalDecommissioned {
			killed++
		}
	}
	execTone := ""
	if killed > 0 {
		execTone = "crit"
	}
	findTone := ""
	switch {
	case crit > 0:
		findTone = "crit"
	case warn > 0:
		findTone = "warn"
	}
	s.KPIs = []model.KPI{
		{Label: "Ran for", Value: model.Duration(a.DurationMs), Explain: "From application start to end" + map[bool]string{true: " (or to the last event, as it had not ended).", false: "."}[a.End.IsZero()]},
		{Label: "Hosts", Value: fmt.Sprint(hosts), Unit: "ran executors", Explain: "Machines that ran executors. Instance details need the EMR API."},
		{Label: "Executors", Value: fmt.Sprint(len(c.log.Executors)), Unit: fmt.Sprintf("started · %d peak", r.Executors.Peak), Explain: fmt.Sprintf("Worker processes Spark launched, and the most at once. %d ended early.", killed), Tone: execTone},
		{Label: "Executor size", Value: fmt.Sprint(mem.Cores), Unit: "cores · " + model.Bytes(mem.ContainerBytes), Explain: fmt.Sprintf("%s heap plus %s overhead, per executor.", model.Bytes(mem.HeapBytes), model.Bytes(mem.OverheadBytes))},
		cpuKPI(r.CPU),
		heapKPI(r.Memory, peakHeap),
		{Label: "Data read", Value: model.Bytes(io.InputBytes), Explain: fmt.Sprintf("%s rows from files and tables.", model.Num(io.InputRecords))},
		{Label: "Data written", Value: model.Bytes(io.OutputBytes), Explain: fmt.Sprintf("%s rows. Shuffles moved %s more.", model.Num(io.OutputRecords), model.Bytes(io.ShuffleWriteBytes))},
		{Label: "Findings", Value: fmt.Sprint(crit + warn + info), Unit: fmt.Sprintf("%d critical · %d warning", crit, warn), Explain: "Problems and notes found by the rules below.", Tone: findTone},
	}
}

func orUnknown(s string) string {
	if s == "" {
		return "an unknown user"
	}
	return s
}

func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	// Keep proper nouns and identifiers such as "Stage 18" or "AWS" readable.
	w, _, _ := strings.Cut(s, " ")
	if w == strings.ToUpper(w) || w == "Stage" || w == "Job" || w == "Executor" {
		return s
	}
	return strings.ToLower(s[:1]) + s[1:]
}

func joinAnd(items []string) string {
	switch len(items) {
	case 0:
		return ""
	case 1:
		return items[0]
	}
	return strings.Join(items[:len(items)-1], "; ") + "; and " + items[len(items)-1]
}

func cpuKPI(c model.CPUSection) model.KPI {
	k := model.KPI{Label: "CPU time", Value: model.Duration(c.CPUMs), Explain: fmt.Sprintf("Task CPU time, %s of task run time.", model.Percent(c.Share))}
	if c.CPUMs >= 3_600_000 {
		k.Value, k.Unit = fmt.Sprintf("%.1f", float64(c.CPUMs)/3.6e6), "hours"
	}
	return k
}

// heapKPI shows the highest executor heap sample, or says none was recorded
// rather than showing 0 B.
func heapKPI(m model.MemorySection, peak int64) model.KPI {
	if !m.HeapKnown {
		return model.KPI{Label: "Peak heap", Value: "—", Unit: "not recorded · " + model.Bytes(m.Config.HeapBytes) + " heap",
			Explain: "The event log holds no executor memory samples. Set spark.eventLog.logStageExecutorMetrics=true to record them."}
	}
	return model.KPI{Label: "Peak heap", Value: model.Bytes(peak), Unit: "of " + model.Bytes(m.Config.HeapBytes), Explain: "Highest Java heap use sampled on any executor."}
}
