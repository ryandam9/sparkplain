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
			Explanation: "The job read or wrote an HBase table that HBase does not have, so every call to it failed. HBase finds a table by its full name, namespace included (ns:table, or the default namespace when none is given).",
			Evidence:    g.evidence(),
			Fix:         "Create the table, with its column families, before the job runs (in the hbase shell: create '<table>', '<family>'), or correct the table name the job uses."})
	}
	if g := groups["zookeeper"]; g != nil {
		title, expl := "HBase's ZooKeeper could not be reached", "An HBase client first asks ZooKeeper where HBase is. It could not connect, so it never reached HBase and its calls failed."
		if len(g.zk) > 0 {
			title += " at " + strings.Join(g.zk, ", ")
			if _, port, ok := strings.Cut(g.zk[0], ":"); ok && port != "2181" {
				expl += " ZooKeeper listens on port 2181 unless the cluster changed it; this client dialled port " + port + "."
			}
		}
		c.add(model.Finding{Rule: "hbase-zookeeper", Severity: model.Critical, Section: "access",
			Title: fmt.Sprintf("%s (%s in the logs)", title, errs(g)), Explanation: expl, Evidence: g.evidence(),
			Fix: "Check hbase.zookeeper.quorum and hbase.zookeeper.property.clientPort in the hbase-site.xml the job uses (often shipped with --files) against the cluster's own /etc/hbase/conf/hbase-site.xml, and that ZooKeeper is running and reachable from every node."})
	}
	if g := groups["server"]; g != nil {
		title := "HBase region servers did not answer"
		if len(g.servers) > 0 {
			title = "HBase region server " + strings.Join(g.servers, ", ") + " did not answer"
		}
		c.add(model.Finding{Rule: "hbase-server", Severity: model.Critical, Section: "stages",
			Title:       fmt.Sprintf("%s (%s in the logs)", title, errs(g)),
			Explanation: "Calls to a region server timed out, or it closed the connection or was stopping, so reads and writes to the regions it serves stalled or failed until HBase moved them.",
			Evidence:    append(g.evidence(), serverRefs(g.servers)...),
			Fix:         "Read that region server's own log, and check its node's memory, garbage-collection pauses and disks at that time. hbase.rpc.timeout and hbase.client.operation.timeout set how long the client waits."})
	}
	if g := groups["retries"]; g != nil {
		title := "HBase calls gave up after all their retries"
		if len(g.tables) > 0 {
			title += " on " + strings.Join(g.tables, ", ")
		}
		c.add(model.Finding{Rule: "hbase-retries", Severity: model.Critical, Section: "stages",
			Title:       fmt.Sprintf("%s (%s in the logs)", title, errs(g)),
			Explanation: "The HBase client retried a call until hbase.client.retries.number ran out, so that read or write failed. The HBase lines before it in the same log usually say why it kept failing.",
			Evidence:    g.evidence(),
			Fix:         "Find the first HBase error before it (a region server down, a busy region, a region moving) and fix that; HBase's own logs for that time show the server's side."})
	}
	if g := groups["busy"]; g != nil {
		on := ""
		if len(g.tables) > 0 {
			on = " to " + strings.Join(g.tables, ", ")
		}
		why := "the region's memstore, the memory that holds writes until they are flushed to disk, was over its limit"
		if g.limit != "" {
			why += " (" + g.limit + ")"
		}
		if g.callQueue {
			why = "its queue of waiting requests was full"
		}
		where := "The region server"
		if len(g.servers) > 0 {
			where = "The region server on " + strings.Join(g.servers, ", ")
		}
		retried := "The client waited and retried, which slowed the tasks writing."
		if g.attempt > 0 && g.attempts > 0 {
			retried = fmt.Sprintf("The client waited and retried, up to attempt %d of %d, which slowed the tasks writing.", g.attempt, g.attempts)
		}
		c.add(model.Finding{Rule: "hbase-busy", Severity: model.Warning, Section: "stages",
			Title:       fmt.Sprintf("HBase pushed back on writes%s (%s in the logs)", on, model.Plural(g.n, "time", "times")),
			Explanation: fmt.Sprintf("%s refused writes because %s. %s Writes pile onto one region when row keys share a prefix or arrive in order, or when the table has few regions.", where, why, retried),
			Evidence:    append(g.evidence(), serverRefs(g.servers)...),
			Fix:         "Spread the writes: pre-split the table into more regions, and avoid row keys that share a prefix or keep increasing (salt or hash a prefix). Fewer tasks writing at once also helps. The region server's own log shows whether flushes or compactions held it up."})
	}
	if g := groups["scanner"]; g != nil {
		c.add(model.Finding{Rule: "hbase-scanner-expired", Severity: model.Warning, Section: "stages",
			Title:       fmt.Sprintf("HBase scanner leases expired (%s in the logs)", model.Plural(g.n, "time", "times")),
			Explanation: "A task took longer than the scanner lease (hbase.client.scanner.timeout.period, 60 s unless changed) between two fetches of rows, so the region server dropped its scan. The client opened the scan again and carried on, but the task lost the time it had waited and ran longer.",
			Evidence:    g.evidence(),
			Fix:         "Fetch fewer rows per call, so each batch is processed within the lease: lower hbase.client.scanner.caching, Scan.setCaching, or for TableInputFormat hbase.mapreduce.scan.cachedrows. Or make the work done per row faster. A longer hbase.client.scanner.timeout.period only helps if the region servers get it too."})
	}
	if g := groups["moved"]; g != nil {
		c.add(model.Finding{Rule: "hbase-region-moved", Severity: model.Warning, Section: "stages",
			Title:       fmt.Sprintf("HBase regions were moving or opening during the run (%s in the logs)", errs(g)),
			Explanation: "The client reached a region server that no longer, or not yet, served a region, because HBase was moving, splitting or reopening it. The client found the new place and retried, which slows the tasks involved.",
			Evidence:    g.evidence(),
			Fix:         "Check the HBase Master's log for region moves and splits at that time. Avoid running the balancer or major compactions during heavy jobs, and pre-split tables that split while being written."})
	}
	if g := groups["other"]; g != nil {
		c.add(model.Finding{Rule: "hbase-error", Severity: model.Critical, Section: "stages",
			Title:       fmt.Sprintf("HBase calls failed (%s in the logs)", errs(g)),
			Explanation: "The HBase client logged errors of a kind the report does not recognise; the evidence shows them.",
			Evidence:    g.evidence(),
			Fix:         "Read the first error for its cause, and HBase's own logs for that time."})
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
		Explanation: "HBase's own access control refused the user the job ran as, so the read or write failed. This is HBase's permission, not an AWS one.",
		Evidence:    g.evidence(),
		Fix:         "Grant that user what it needs on the table (in the hbase shell: grant '<user>', 'RW', '<table>'), or run the job as a user that has it."})
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
			expl = append(expl, fmt.Sprintf("%s found a different version of %s than it was built against (%s; %s in the logs).", who, cls, m.err, model.Plural(m.n, "line", "lines")))
		default:
			expl = append(expl, fmt.Sprintf("%s needed %s, but no jar on the classpath provided it (%s; %s in the logs).", who, cls, m.err, model.Plural(m.n, "line", "lines")))
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
		title = fmt.Sprintf("%d classes were missing at run time: %s", len(order), clip(strings.Join(order, ", "), 160))
	}
	fix := "Ship the jar that provides it with --jars (or spark.jars) so the driver and every executor load it, or, for a version clash, keep one version of the library on the classpath."
	if len(hints) > 0 {
		fix += " " + strings.Join(hints, " ")
	}
	c.add(model.Finding{Rule: "classpath-clash", Severity: model.Critical, Section: "config", Title: title,
		Explanation: strings.Join(expl, " ") + " Whatever used it failed there.", Evidence: ev, Fix: fix})
}

// classHint says which jar usually provides a class the HBase client or
// its Spark connector needs, and where EMR keeps it. Checked on EMR 7.3.0
// (HBase 2.4.17, Spark 3.5.1, hbase-spark connector 1.0.1).
func classHint(cls string) string {
	switch {
	case strings.HasPrefix(cls, "com.google.protobuf."):
		return "HBase 2's client still loads protobuf-java 2.5, which Spark 3.5 no longer ships; on EMR it is /usr/lib/hadoop/lib/protobuf-java-2.5.0.jar."
	case cls == "org.slf4j.impl.StaticLoggerBinder":
		return "The jar that needed it calls SLF4J 1's API, and Spark 3.5 ships SLF4J 2, which has no StaticLoggerBinder; add an SLF4J 1.7 binding such as /usr/lib/hadoop/lib/slf4j-reload4j-1.7.36.jar on EMR."
	case strings.HasPrefix(cls, "org.apache.hadoop.hbase.spark."):
		return "It is the hbase-spark connector, which EMR 7 does not ship: add hbase-spark and hbase-spark-protocol-shaded (Maven group org.apache.hbase.connectors.spark)."
	case strings.HasPrefix(cls, "org.apache.hbase.thirdparty."):
		return "It is in HBase's shaded third-party jars (hbase-shaded-miscellaneous, hbase-shaded-netty, hbase-shaded-protobuf), in /usr/lib/hbase/lib on EMR."
	case strings.HasPrefix(cls, "org.apache.hadoop.hbase."):
		return "It is part of HBase's client: add HBase's jars from /usr/lib/hbase/lib on EMR (hbase-client, hbase-common, hbase-mapreduce and the ones they need)."
	case strings.HasPrefix(cls, "org.apache.htrace."):
		return "It is htrace-core4, in /usr/lib/hbase/lib/client-facing-thirdparty on EMR."
	}
	return ""
}
