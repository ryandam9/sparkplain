package analyze

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// hbaseGroup is every client line that shows one HBase problem.
type hbaseGroup struct {
	hits                []hit
	n                   int // lines, counting repeats
	tables, servers, zk []string
	attempt, attempts   int // the furthest retry the client logged, of how many
	limit               string
	callQueue           bool // CallQueueTooBigException rather than a full memstore
}

func (g *hbaseGroup) add(h hit) {
	l := h.l
	g.hits = append(g.hits, h)
	g.n += l.Count
	for _, kv := range []struct {
		to *[]string
		v  string
	}{{&g.tables, l.Fields["table"]}, {&g.servers, l.Fields["server"]}, {&g.zk, l.Fields["zookeeper"]}} {
		if kv.v != "" && !slices.Contains(*kv.to, kv.v) {
			*kv.to = append(*kv.to, kv.v)
		}
	}
	if a, _ := strconv.Atoi(l.Fields["attempt"]); a > g.attempt {
		g.attempt = a
		g.attempts, _ = strconv.Atoi(l.Fields["attempts"])
	}
	if v := l.Fields["limit"]; v != "" {
		g.limit = v
	}
	if strings.Contains(l.Text+strings.Join(l.Detail, " "), "CallQueueTooBigException") {
		g.callQueue = true
	}
}

// evidence cites up to four of the group's lines.
func (g *hbaseGroup) evidence() []model.Evidence {
	var ev []model.Evidence
	for _, h := range g.hits {
		if len(ev) == 4 {
			break
		}
		ev = append(ev, h.evidence(h.who()+": "+h.says()))
	}
	return ev
}

// hbaseFindings reports each HBase problem the client logged, apart,
// because each has its own fix (HISTORY.md, phase 5 step 2). The problem is
// the classifier's "hbase" field.
func hbaseFindings(c *ctx) {
	groups := map[string]*hbaseGroup{}
	for _, h := range c.logs.hits {
		p := h.l.Fields["hbase"]
		if h.l.Kind != model.LogHBase || p == "" {
			continue
		}
		if groups[p] == nil {
			groups[p] = &hbaseGroup{}
		}
		groups[p].add(h)
	}
	// A client that gave up writing to a table that does not exist says
	// "retries exhausted"; that is the missing table, not a second problem.
	if tm, rg := groups["table-missing"], groups["retries"]; tm != nil && rg != nil {
		rest := &hbaseGroup{}
		for _, h := range rg.hits {
			if slices.Contains(tm.tables, h.l.Fields["table"]) {
				tm.add(h)
			} else {
				rest.add(h)
			}
		}
		groups["retries"] = rest
		if len(rest.hits) == 0 {
			delete(groups, "retries")
		}
	}
	errs := func(g *hbaseGroup) string { return model.Plural(g.n, "error", "errors") }
	if g := groups["table-missing"]; g != nil {
		c.add(model.Finding{Rule: "hbase-table-missing", Severity: model.Critical, Section: "access",
			Title:       fmt.Sprintf("HBase table %s does not exist (%s in the logs)", strings.Join(g.tables, ", "), errs(g)),
			Explanation: "The job read or wrote an HBase table that HBase does not have. As a result, all calls to it failed. HBase finds a table by its full name, with the namespace (ns:table). If the name has no namespace, HBase uses the default namespace.",
			Evidence:    g.evidence(),
			Fix:         "Do one of these:\n- Create the table and its column families before the job runs. In the hbase shell, use create '<table>', '<family>'.\n- Correct the table name that the job uses."})
	}
	if g := groups["zookeeper"]; g != nil {
		title, expl := "The HBase client could not reach ZooKeeper", "An HBase client first asks ZooKeeper for the location of HBase. This client could not connect. As a result, it did not reach HBase, and its calls failed."
		if len(g.zk) > 0 {
			title += " at " + strings.Join(g.zk, ", ")
			if _, port, ok := strings.Cut(g.zk[0], ":"); ok && port != "2181" {
				expl += " ZooKeeper listens on port 2181, if the cluster did not change it. This client used port " + port + "."
			}
		}
		c.add(model.Finding{Rule: "hbase-zookeeper", Severity: model.Critical, Section: "access",
			Title: fmt.Sprintf("%s (%s in the logs)", title, errs(g)), Explanation: expl, Evidence: g.evidence(),
			Fix: "Compare hbase.zookeeper.quorum and hbase.zookeeper.property.clientPort in two files:\n- The hbase-site.xml that the job uses. The job often sends it with --files.\n- /etc/hbase/conf/hbase-site.xml on the cluster.\nMake sure that ZooKeeper runs and that all nodes can reach it."})
	}
	if g := groups["server"]; g != nil {
		title := "HBase region servers did not answer"
		if len(g.servers) > 0 {
			title = "HBase region server " + strings.Join(g.servers, ", ") + " did not answer"
		}
		c.add(model.Finding{Rule: "hbase-server", Severity: model.Critical, Section: "stages",
			Title:       fmt.Sprintf("%s (%s in the logs)", title, errs(g)),
			Explanation: "Calls to a region server timed out, or the server closed the connection or was stopping. As a result, reads and writes to its regions stopped or failed until HBase moved the regions.",
			Evidence:    append(g.evidence(), serverRefs(g.servers)...),
			Fix:         "Read the log of that region server. Examine the memory, the garbage collection pauses and the disks of its node at that time.\nhbase.rpc.timeout and hbase.client.operation.timeout set how long the client waits."})
	}
	if g := groups["retries"]; g != nil {
		title := "HBase calls gave up after all their retries"
		if len(g.tables) > 0 {
			title += " on " + strings.Join(g.tables, ", ")
		}
		c.add(model.Finding{Rule: "hbase-retries", Severity: model.Critical, Section: "stages",
			Title:       fmt.Sprintf("%s (%s in the logs)", title, errs(g)),
			Explanation: "The HBase client tried a call again until it used all its retries (hbase.client.retries.number). As a result, that read or write failed. The HBase lines before it in the same log usually show why it failed again and again.",
			Evidence:    g.evidence(),
			Fix:         "Find the first HBase error before it. For example, a region server stopped, a region was busy or a region moved. Correct that error.\nThe logs of HBase for that time show the problem on the server."})
	}
	if g := groups["busy"]; g != nil {
		on := ""
		if len(g.tables) > 0 {
			on = " to " + strings.Join(g.tables, ", ")
		}
		why := "the memstore of the region was more than its limit"
		if g.limit != "" {
			why += " (" + g.limit + ")"
		}
		why += ". The memstore is the memory that holds writes until HBase flushes them to disk"
		if g.callQueue {
			why = "its queue of requests was full"
		}
		where := "The region server"
		if len(g.servers) > 0 {
			where = "The region server on " + strings.Join(g.servers, ", ")
		}
		retried := "The client waited and tried again. This made the tasks that wrote slower."
		if g.attempt > 0 && g.attempts > 0 {
			retried = fmt.Sprintf("The client waited and tried again, up to attempt %d of %d. This made the tasks that wrote slower.", g.attempt, g.attempts)
		}
		c.add(model.Finding{Rule: "hbase-busy", Severity: model.Warning, Section: "stages",
			Title:       fmt.Sprintf("HBase refused writes%s for a short time (%s in the logs)", on, model.Plural(g.n, "time", "times")),
			Explanation: fmt.Sprintf("%s refused writes because %s. %s\nWrites go to one region when row keys share a prefix or come in order, or when the table has few regions.", where, why, retried),
			Evidence:    append(g.evidence(), serverRefs(g.servers)...),
			Fix:         "Spread the writes:\n- Pre-split the table into more regions.\n- Do not use row keys that share a prefix or always increase. Add a salt or a hash as a prefix.\n- Use fewer tasks that write at the same time.\nThe log of the region server shows if flushes or compactions stopped it."})
	}
	if g := groups["scanner"]; g != nil {
		c.add(model.Finding{Rule: "hbase-scanner-expired", Severity: model.Warning, Section: "stages",
			Title:       fmt.Sprintf("HBase scanner leases expired (%s in the logs)", model.Plural(g.n, "time", "times")),
			Explanation: "Between two fetches of rows, a task used more time than the scanner lease (hbase.client.scanner.timeout.period, 60 s by default). As a result, the region server stopped its scan. The client opened the scan again and continued. But the task lost the time that it waited, and it ran longer.",
			Evidence:    g.evidence(),
			Fix:         "Fetch fewer rows in each call, so that the task completes each batch within the lease. Decrease one of these:\n- hbase.client.scanner.caching\n- Scan.setCaching\n- hbase.mapreduce.scan.cachedrows, for TableInputFormat\nOr make the work on each row faster.\nA longer hbase.client.scanner.timeout.period helps only if the region servers also get it."})
	}
	if g := groups["moved"]; g != nil {
		c.add(model.Finding{Rule: "hbase-region-moved", Severity: model.Warning, Section: "stages",
			Title:       fmt.Sprintf("HBase regions were moving or opening during the run (%s in the logs)", errs(g)),
			Explanation: "The client reached a region server that did not serve the region at that time. HBase moved, split or opened the region again. The client found the new location and tried again. This makes the tasks slower.",
			Evidence:    g.evidence(),
			Fix:         "Look for region moves and splits at that time in the log of the HBase Master.\nDo not run the balancer or major compactions during large jobs.\nPre-split the tables that split during writes."})
	}
	if g := groups["other"]; g != nil {
		c.add(model.Finding{Rule: "hbase-error", Severity: model.Critical, Section: "stages",
			Title:       fmt.Sprintf("HBase calls failed (%s in the logs)", errs(g)),
			Explanation: "The HBase client logged errors of a type that sparkplain does not know. The evidence shows them.",
			Evidence:    g.evidence(),
			Fix:         "Find the cause in the first error.\nRead the logs of HBase for that time."})
	}
	hbaseAccessFinding(c)
}

// serverRefs points at region servers' nodes, so the at-a-glance diagram
// marks them.
func serverRefs(servers []string) []model.Evidence {
	var ev []model.Evidence
	for _, s := range servers {
		ev = append(ev, model.Evidence{Ref: model.NodeRef(s), Text: "region server " + s})
	}
	return ev
}

// hbaseAccessFinding reports HBase's own access control refusing the job,
// which is not an AWS refusal.
func hbaseAccessFinding(c *ctx) {
	g := &hbaseGroup{}
	for _, h := range c.logs.hits {
		if h.l.Kind == model.LogAccess && h.l.Fields["service"] == "HBase" {
			g.add(h)
		}
	}
	if len(g.hits) == 0 {
		return
	}
	on := ""
	if len(g.tables) > 0 {
		on = " on " + strings.Join(g.tables, ", ")
	}
	c.add(model.Finding{Rule: "hbase-access-denied", Severity: model.Critical, Section: "access",
		Title:       fmt.Sprintf("HBase refused access%s (%s)", on, model.Plural(g.n, "time", "times")),
		Explanation: "The access control of HBase refused the user of the job. As a result, the read or write failed. This is a permission in HBase, not in AWS.",
		Evidence:    g.evidence(),
		Fix:         "Do one of these:\n- Give that user the necessary permission on the table. In the hbase shell, use grant '<user>', 'RW', '<table>'.\n- Run the job as a user that has the permission."})
}

// classpathFinding reports classes missing, or of another version, at run
// time: the jar that needed each, and where the usual provider is.
func classpathFinding(c *ctx) {
	type missing struct {
		class, err, neededBy string
		hits                 []hit
		n                    int
	}
	var order []string
	byClass := map[string]*missing{}
	for _, h := range c.logs.hits {
		l := h.l
		cls := l.Fields["class"]
		if cls == "" || l.Severity != model.Critical || (l.Kind != model.LogClasspath && l.Fields["cause"] != "missing class") {
			continue
		}
		m := byClass[cls]
		if m == nil {
			m = &missing{class: cls, err: l.Fields["classError"]}
			byClass[cls] = m
			order = append(order, cls)
		}
		if m.neededBy == "" {
			m.neededBy = l.Fields["neededBy"]
		}
		m.hits = append(m.hits, h)
		m.n += l.Count
	}
	if len(order) == 0 {
		return
	}
	var expl, hints []string
	var ev []model.Evidence
	for _, cls := range order {
		m := byClass[cls]
		who := "The code"
		if m.neededBy != "" {
			who = m.neededBy
		}
		switch m.err {
		case "NoSuchMethodError", "NoSuchFieldError", "AbstractMethodError", "IncompatibleClassChangeError":
			expl = append(expl, fmt.Sprintf("%s found a version of %s that is different from the version it was built with (%s, %s in the logs).", who, cls, m.err, model.Plural(m.n, "line", "lines")))
		default:
			expl = append(expl, fmt.Sprintf("%s needed %s, but no jar on the classpath had it (%s, %s in the logs).", who, cls, m.err, model.Plural(m.n, "line", "lines")))
		}
		if hint := classHint(cls); hint != "" && !slices.Contains(hints, hint) {
			hints = append(hints, hint)
		}
		for _, h := range m.hits {
			if len(ev) == 4 {
				break
			}
			ev = append(ev, h.evidence(h.who()+": "+h.says()))
		}
	}
	title := "Class " + order[0] + " was missing at run time"
	if len(order) > 1 {
		title = fmt.Sprintf("%d classes were missing at run time (%s)", len(order), clip(strings.Join(order, ", "), 160))
	}
	fix := "Do one of these:\n- Add the jar that has the class with --jars or spark.jars. Then the driver and all executors load it.\n- If two versions conflict, keep only one version of the library on the classpath."
	if len(hints) > 0 {
		fix += "\n" + strings.Join(hints, "\n")
	}
	c.add(model.Finding{Rule: "classpath-clash", Severity: model.Critical, Section: "config", Title: title,
		Explanation: strings.Join(expl, "\n") + "\nThe code that used the class failed at that point.", Evidence: ev, Fix: fix})
}

// classHint says which jar usually provides a class the HBase client or
// its Spark connector needs, and where EMR keeps it. Checked on EMR 7.3.0
// (HBase 2.4.17, Spark 3.5.1, hbase-spark connector 1.0.1).
func classHint(cls string) string {
	switch {
	case strings.HasPrefix(cls, "com.google.protobuf."):
		return "The HBase 2 client loads protobuf-java 2.5, but Spark 3.5 does not include it. On EMR, it is /usr/lib/hadoop/lib/protobuf-java-2.5.0.jar."
	case cls == "org.slf4j.impl.StaticLoggerBinder":
		return "The jar that needed it uses the SLF4J 1 API. Spark 3.5 includes SLF4J 2, which has no StaticLoggerBinder. Add an SLF4J 1.7 binding, for example /usr/lib/hadoop/lib/slf4j-reload4j-1.7.36.jar on EMR."
	case strings.HasPrefix(cls, "org.apache.hadoop.hbase.spark."):
		return "It is in the hbase-spark connector, which EMR 7 does not include. Add hbase-spark and hbase-spark-protocol-shaded (Maven group org.apache.hbase.connectors.spark)."
	case strings.HasPrefix(cls, "org.apache.hbase.thirdparty."):
		return "It is in the shaded third-party jars of HBase (hbase-shaded-miscellaneous, hbase-shaded-netty, hbase-shaded-protobuf). On EMR, they are in /usr/lib/hbase/lib."
	case strings.HasPrefix(cls, "org.apache.hadoop.hbase."):
		return "It is in the HBase client. On EMR, add the HBase jars from /usr/lib/hbase/lib: hbase-client, hbase-common, hbase-mapreduce and the jars that they use."
	case strings.HasPrefix(cls, "org.apache.htrace."):
		return "It is in htrace-core4. On EMR, it is in /usr/lib/hbase/lib/client-facing-thirdparty."
	}
	return ""
}
