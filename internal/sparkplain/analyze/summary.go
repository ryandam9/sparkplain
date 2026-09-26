package analyze

import (
	"fmt"
	"strings"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

func analyzeIdentity(c *ctx, r *model.Report) {
	s := &r.Identity
	s.Missing = []string{"AWS calls made by the job's role and every AccessDenied (needs CloudTrail, phase 3)"}
	if r.Cluster == nil {
		s.Missing = append(s.Missing, "AWS roles: instance profile and EMR service role (needs -cluster-id)", "EMR security configuration (needs -cluster-id)")
	} else {
		s.Missing = append(s.Missing, "Runtime roles of individual steps, and what the security configuration turns on (phase 3)")
	}
	if c.logs == nil {
		s.Missing = append(s.Missing, "Whether Hive metastore, HBase and Kerberos connections succeeded (needs the container logs: -cluster-id or -from)")
	}
	add := func(label, value, explain string, srcs ...model.Source) {
		f := model.Fact{Label: label, Value: value, Explain: explain}
		if len(srcs) > 0 {
			f.Source = srcs[0]
		}
		s.Facts = append(s.Facts, f)
	}
	if !c.has() {
		s.Coverage = model.NeedsEventLog
		if c.logs != nil || r.Cluster != nil {
			s.Coverage = model.Partial
			if a := r.Application; a.User != "" {
				add("Ran as", a.User, "The user YARN ran the application as, from its own records.", a.Source)
				if a.Queue != "" {
					add("YARN queue", a.Queue, "The queue that decided how much of the cluster this application could use.", a.Source)
				}
			}
			identityFromLogs(c, r, add)
		}
		return
	}
	s.Coverage = model.Partial
	defer identityFromLogs(c, r, add)
	a := c.log.Application
	src := c.confSrc
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

// identityFromLogs adds what the EMR API and the logs say about who the
// application ran as and what it connected to.
func identityFromLogs(c *ctx, r *model.Report, add func(label, value, explain string, srcs ...model.Source)) {
	if cl := r.Cluster; cl != nil {
		api := model.Source{File: cl.Source}
		add("EC2 instance profile", orNone(cl.InstanceProfile), "The IAM role every node's processes use for AWS calls, including S3 reads and writes, unless the job carries other credentials.", api)
		add("EMR service role", orNone(cl.ServiceRole), "The role EMR itself uses to create and manage the cluster's instances.", api)
		add("EMR security configuration", orNone(cl.SecurityConfig), "Where EMR's encryption, Kerberos and Lake Formation settings are defined.", api)
		if p := cl.Security; p != nil {
			sec := model.Source{File: p.Source}
			rest := "off"
			if p.AtRestEncryption {
				rest = "on"
				var parts []string
				if p.S3Encryption != "" {
					parts = append(parts, "S3 "+p.S3Encryption)
				}
				if p.LocalDiskEncryption {
					disk := "local disks"
					if p.EBSEncryption {
						disk += " and EBS volumes"
					}
					parts = append(parts, disk)
				}
				if len(parts) > 0 {
					rest += ": " + strings.Join(parts, ", ")
				}
			}
			add("Encryption at rest", rest, "Whether EMRFS data on S3 and the nodes' disks are encrypted by the security configuration.", sec)
			add("Encryption in transit", onOff(p.InTransitEncryption), "Whether traffic between the cluster's services is encrypted with TLS.", sec)
			if p.Kerberos != "" {
				v := p.Kerberos
				if cl.KerberosRealm != "" {
					v += ", realm " + cl.KerberosRealm
				}
				add("EMR Kerberos", v, "Hadoop services on the cluster require Kerberos tickets.", sec)
			}
			add("Lake Formation", onOff(p.LakeFormation), "Whether Lake Formation grants control access to tables, on top of IAM.", sec)
			add("Runtime roles", onOff(p.RuntimeRoles), "Whether steps can run as their own IAM role instead of the instance profile.", sec)
		}
	}
	for _, st := range r.Steps {
		if st.AppID != "" && st.ExecutionRole != "" {
			add("Step runtime role", st.ExecutionRole, "Step "+st.ID+" ran as this role, so the application's AWS calls used it rather than the instance profile.", model.Source{File: "EMR DescribeStep " + st.ID})
		}
	}
	if c.logs == nil {
		return
	}
	seen := map[string]bool{}
	for _, f := range r.Identity.Facts {
		seen[f.Label+"\x00"+f.Value] = true
	}
	once := func(label, value, explain string, src model.Source) {
		if value == "" || seen[label+"\x00"+value] {
			return
		}
		seen[label+"\x00"+value] = true
		add(label, value, explain, src)
	}
	var metaOK, metaFail, hbase []string
	var metaSrc, hbaseSrc model.Source
	for _, h := range c.logs.hits {
		l := h.l
		switch l.Kind {
		case model.LogIdentity:
			if u := l.Fields["user"]; u != "" && u != r.Application.User {
				once("YARN user", u, "The user YARN recorded for the application ("+l.Fields["via"]+"), which differs from the one Spark recorded.", l.Source)
			}
			if p := l.Fields["principal"]; p != "" {
				once("Kerberos login", p+" (keytab "+l.Fields["keytab"]+")", "The principal a process logged in as, from its own log.", l.Source)
			}
		case model.LogMetastore:
			if l.Severity == model.Critical {
				metaFail = append(metaFail, h.who())
			} else if u := l.Fields["uri"]; u != "" {
				metaOK = append(metaOK, u)
			}
			if metaSrc.IsZero() {
				metaSrc = l.Source
			}
		case model.LogHBase:
			if q := l.Fields["quorum"]; q != "" {
				hbase = append(hbase, q)
				if hbaseSrc.IsZero() {
					hbaseSrc = l.Source
				}
			}
		}
	}
	switch {
	case len(metaFail) > 0:
		once("Metastore connection", fmt.Sprintf("failed (%s)", model.Plural(len(metaFail), "error", "errors")), "The logs show Spark failing to reach the Hive metastore or Glue catalog; the findings list each error.", metaSrc)
	case len(metaOK) > 0:
		once("Metastore connection", metaOK[0]+" (connected)", "The metastore Spark connected to, from the logs.", metaSrc)
	}
	if len(hbase) > 0 {
		once("HBase connection", "ZooKeeper "+hbase[0], "The ZooKeeper quorum a process connected to for HBase, from the logs.", hbaseSrc)
	}
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
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
		for _, s := range r.Sources {
			if s.Name == name {
				return // this run read it
			}
		}
		r.Sources = append(r.Sources, model.SourceStatus{Name: name, Status: "not-yet", Detail: detail})
	}
	if c.in.LogsRead {
		r.Sources = append(r.Sources, c.in.LogSources...)
	} else {
		for _, name := range []string{"Container logs", "Step logs", "Node logs"} {
			r.Sources = append(r.Sources, model.SourceStatus{Name: name, Status: "not-requested",
				Detail: "Not read: pass -cluster-id with -profile to read them from S3, or -from with a local copy of the cluster's log folder."})
		}
		r.Sources = append(r.Sources, model.SourceStatus{Name: "EMR API", Status: "not-requested", Detail: "Not called: pass -cluster-id with -profile."})
	}
	if c.in.Cluster == nil {
		r.Sources = append(r.Sources, model.SourceStatus{Name: "CloudWatch", Status: "not-requested", Detail: "Not called: pass -cluster-id with -profile for the cluster's and its nodes' metrics."})
	}
	notYet("CloudWatch", "Not called.")
	notYet("CloudTrail", "Not called: planned for phase 3 (AWS calls and AccessDenied).")
}

func analyzeCoverage(c *ctx, r *model.Report) {
	row := func(id, title string, cov model.Coverage, shown string, missing []string) {
		r.Coverage = append(r.Coverage, model.SectionStatus{ID: id, Title: title, Coverage: cov, Shown: shown, Missing: missing})
	}
	if !c.has() {
		for _, s := range []struct{ id, title string }{{"summary", "Application summary"}, {"nodes", "Cluster and nodes"}, {"executors", "Executors"}, {"memory", "Memory"}, {"cpu", "CPU"}, {"io", "Storage and I/O"}, {"stages", "Jobs, stages, tasks"}, {"config", "Configuration"}, {"access", "Identity and access"}} {
			switch {
			case s.id == "summary" && c.logs != nil:
				row(s.id, s.title, model.Partial, "Name, user, queue, final status and times from YARN's records", []string{"Spark version, jobs, stages and resource use (the event log)"})
			case s.id == "nodes" && r.Nodes.Coverage != model.NeedsEventLog:
				row(s.id, s.title, r.Nodes.Coverage, "The cluster's nodes from the EMR API, and CloudWatch's view of them while the application ran", r.Nodes.Missing)
			case s.id == "access" && r.Identity.Coverage == model.Partial:
				row(s.id, s.title, model.Partial, "User and queue from YARN, AWS roles, and the connections the logs show", r.Identity.Missing)
			default:
				row(s.id, s.title, model.NeedsEventLog, "Nothing: this section is built from the event log", []string{"The event log"})
			}
		}
		if c.logs != nil {
			row("findings", "Findings", model.Partial, "Rules that read the container, step and node logs: first error, memory kills, out-of-memory, access, Kerberos, metastore and HBase errors", []string{"Rules that read the event log"})
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
	if c.logs != nil {
		row("findings", "Findings", model.Partial, "Rules that read the event log (skew, spill, GC, CPU, memory size, lost executors, failures) and the container, step and node logs (first error, memory kills, out-of-memory, access, Kerberos, metastore and HBase errors)", []string{"Rules that need CloudWatch and CloudTrail: host pressure, spot interruptions, every AWS call and AccessDenied (phase 3)"})
	} else {
		row("findings", "Findings", model.Partial, "Rules that read the event log: skew, spill, GC, CPU, memory size, lost executors, failures", []string{"Rules that need the container logs (-cluster-id or -from): the error behind a failure, out-of-memory messages, access errors"})
	}
}

func analyzeSummary(c *ctx, r *model.Report) {
	s := &r.Summary
	if !c.has() && c.logs != nil {
		summaryFromLogs(c, r)
		return
	}
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

// summaryFromLogs writes "What happened" from YARN's records and the logs
// when there is no event log.
func summaryFromLogs(c *ctx, r *model.Report) {
	a := r.Application
	name := a.ID
	if a.Name != "" {
		name = a.Name + " (" + a.ID + ")"
	}
	var first string
	switch a.Status {
	case model.StatusSucceeded:
		first = name + " finished"
	case model.StatusFailed:
		first = name + " failed"
	default:
		first = name + " ran with an outcome the logs do not show"
	}
	if a.User != "" {
		first += " as " + a.User
	}
	if a.DurationMs > 0 {
		first += " after " + model.Duration(a.DurationMs)
	}
	first += "."
	if a.StatusReason != "" {
		first += " " + a.StatusReason
	}
	sentences := []string{first}
	if h := c.logs.first; h != nil {
		sentences = append(sentences, fmt.Sprintf("The first error in the logs was %s, in %s.", strings.TrimSuffix(h.says(), "."), h.who()))
	}
	var problems []string
	for _, f := range r.Findings {
		if f.Severity == model.Info || f.Rule == "log-first-failure" || len(problems) == 3 {
			continue
		}
		problems = append(problems, lowerFirst(strings.TrimSuffix(f.Title, ".")))
	}
	if len(problems) > 0 {
		sentences = append(sentences, "Also: "+joinAnd(problems)+".")
	}
	sentences = append(sentences, "There is no event log, so jobs, stages and resource use are not shown; this summary comes from YARN's records and the container, step and node logs.")
	r.Summary.Sentences = sentences
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
	if w == strings.ToUpper(w) || w == "Stage" || w == "Job" || w == "Executor" || w == "Spark" || w == "Python" {
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
