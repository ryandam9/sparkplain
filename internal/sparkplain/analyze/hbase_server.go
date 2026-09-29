package analyze

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// hbaseServer gathers what HBase's Master and region servers logged while
// the run went on (phase 5 step 7). Their logs are the cluster's, so an
// event is tied to this run only by what it names: one of the run's
// tables, one of its executors' hosts, or a whole region server, which
// every client depends on. Events tied to the run go to the HBase stage
// running then, and give the findings the clients' own logs cannot.
func hbaseServer(c *ctx, h *model.HBaseSection) {
	if c.logs == nil {
		return
	}
	tables, read := map[string]bool{}, map[string]bool{}
	for _, t := range h.Tables {
		tables[t.Name] = true
		read[t.Name] = t.Read
	}
	hosts := map[string]bool{}
	busyHosts := map[string]bool{} // region servers the clients saw refuse writes
	for _, x := range c.logs.hits {
		if x.f.Container != "" && x.f.Host != "" {
			hosts[shortHost(x.f.Host)] = true
		}
		if x.l.Kind == model.LogHBase && x.l.Fields["hbase"] == "busy" && x.l.Fields["server"] != "" {
			busyHosts[shortHost(x.l.Fields["server"])] = true
		}
	}
	if c.log != nil {
		for _, e := range c.log.Executors {
			if e.Host != "" {
				hosts[shortHost(e.Host)] = true
			}
		}
	}
	type key struct{ event, host, table, detail string }
	rows := map[key]*model.HBaseServerEvent{}
	var order []key
	mine := map[string][]hit{} // event -> hits tied to the run
	for _, x := range c.logs.hits {
		l := x.l
		if l.Kind != model.LogHBaseServer {
			continue
		}
		ev, tbl, host := l.Fields["event"], l.Fields["table"], l.Fields["host"]
		ok := false
		switch ev {
		case "server-lost", "server-stopped", "pause":
			ok = true
		case "scanner":
			// A lease is a scan's: the run must have read the table, from
			// one of its executors' hosts.
			ok = read[tbl] && (len(hosts) == 0 || hosts[shortHost(l.Fields["client"])])
		case "busy", "store-files":
			ok = busyHosts[shortHost(host)] || ev == "store-files" && tables[tbl]
		case "flush":
		default:
			ok = tbl != "" && tables[tbl]
		}
		detail := ""
		switch ev {
		case "server-lost":
			detail = "held " + l.Fields["regions"] + " regions"
			if l.Fields["meta"] == "true" {
				detail += ", including hbase:meta"
			}
			host = l.Fields["server"]
		case "server-stopped":
			detail = l.Fields["reason"]
		case "pause":
			detail = l.Fields["ms"] + " ms"
		case "slow-call", "large-response":
			detail = strings.TrimSpace(l.Fields["method"] + " " + l.Fields["ms"] + " ms")
		case "scanner":
			detail = "client " + l.Fields["client"]
		}
		k := key{ev, host, tbl, detail}
		if rows[k] == nil {
			rows[k] = &model.HBaseServerEvent{Event: ev, Host: host, Table: tbl, Detail: detail, Mine: ok, Source: l.Source}
			order = append(order, k)
		}
		rows[k].Count += l.Count
		if ok {
			mine[ev] = append(mine[ev], x)
		}
	}
	for _, k := range order {
		h.ServerEvents = append(h.ServerEvents, *rows[k])
	}
	sort.SliceStable(h.ServerEvents, func(i, j int) bool {
		a, b := h.ServerEvents[i], h.ServerEvents[j]
		if a.Mine != b.Mine {
			return a.Mine
		}
		return a.Count > b.Count
	})
	// Each event tied to the run goes to the HBase stage that started last
	// before it.
	if c.log != nil {
		stageOf := map[[2]int]*model.Stage{}
		for _, s := range c.log.Stages {
			stageOf[[2]int{s.ID, s.Attempt}] = s
		}
		for ev, xs := range mine {
			for _, x := range xs {
				best := -1
				for i, hs := range h.Stages {
					s := stageOf[[2]int{hs.StageID, hs.Attempt}]
					if s != nil && during(s, x.l.Time) && (best < 0 || s.Submitted.After(stageOf[[2]int{h.Stages[best].StageID, h.Stages[best].Attempt}].Submitted)) {
						best = i
					}
				}
				if best >= 0 {
					hs := &h.Stages[best]
					if hs.ServerEvents == nil {
						hs.ServerEvents = map[string]int{}
					}
					hs.ServerEvents[ev] += x.l.Count
				}
			}
		}
	}
	hbaseServerFindings(c, h, mine)
}

// stagesWith names the HBase stages that saw an event.
func stagesWith(h *model.HBaseSection, events ...string) []string {
	var out []string
	for _, s := range h.Stages {
		for _, ev := range events {
			if s.ServerEvents[ev] > 0 {
				out = append(out, stageName(s))
				break
			}
		}
	}
	return out
}

func whileRan(names []string) string {
	if len(names) == 0 {
		return ""
	}
	if len(names) > 3 {
		names = append(names[:3], fmt.Sprintf("%d more", len(names)-3))
	}
	return " It happened while " + strings.Join(names, ", ") + " ran."
}

func serverEvidence(xs []hit, n int) []model.Evidence {
	var ev []model.Evidence
	for _, x := range xs {
		if len(ev) == n {
			break
		}
		ev = append(ev, model.Evidence{Source: x.l.Source, Text: fmt.Sprintf("HBase %s on %s: %s", strings.TrimPrefix(x.f.Kind, "hbase-"), x.l.Fields["host"], x.says())})
	}
	return ev
}

// hbaseServerFindings are what only HBase's own logs show: a region server
// lost, regions moved or split, a server paused, slow calls; and the
// server's side of the clients' busy and scanner findings.
func hbaseServerFindings(c *ctx, h *model.HBaseSection, mine map[string][]hit) {
	count := func(ev string) int {
		n := 0
		for _, x := range mine[ev] {
			n += x.l.Count
		}
		return n
	}
	// A region server lost or stopped.
	lost := append(slices.Clone(mine["server-lost"]), mine["server-stopped"]...)
	if len(lost) > 0 {
		servers, reasons, held := []string{}, []string{}, ""
		var at time.Time
		for _, x := range lost {
			s := x.l.Fields["server"]
			if s != "" && !slices.Contains(servers, s) {
				servers = append(servers, s)
			}
			if r := x.l.Fields["reason"]; r != "" && !slices.Contains(reasons, r) {
				reasons = append(reasons, r)
			}
			if x.l.Fields["event"] == "server-lost" {
				held = fmt.Sprintf(" The Master moved the %s regions it held", x.l.Fields["regions"])
				if x.l.Fields["meta"] == "true" {
					held += ", including hbase:meta, which every client reads to find regions,"
				}
				held += " to other region servers."
			}
			if at.IsZero() || x.l.Time.Before(at) {
				at = x.l.Time
			}
		}
		expl := fmt.Sprintf("It stopped at %s.", c.clock(at))
		for _, r := range reasons {
			if strings.HasPrefix(r, "Called by admin client") {
				expl += " Its log gives the reason \"" + r + "\": a client or an administrator asked it to stop."
			} else {
				expl += " Its log gives the reason \"" + r + "\"."
			}
		}
		expl += held + " Until each region reopened, calls to it waited and retried; HBase's client logs nothing about this at its default settings, so only the servers' logs show it." + whileRan(stagesWith(h, "server-lost", "server-stopped"))
		c.add(model.Finding{Rule: "hbase-server-lost", Severity: model.Warning, Section: "stages",
			Title:       fmt.Sprintf("Region server %s stopped while the run was using HBase", strings.Join(servers, ", ")),
			Explanation: expl, Evidence: serverEvidence(lost, 4),
			Fix: "Read that region server's log before it stopped for why, and its node's health at that time. Restarts and rolling upgrades of HBase belong outside the hours heavy jobs run."})
	}
	// Regions of the run's tables moved or split.
	if moved, split := count("moved"), count("split"); moved+split > 0 {
		var tbls []string
		for _, x := range append(slices.Clone(mine["moved"]), mine["split"]...) {
			if t := x.l.Fields["table"]; !slices.Contains(tbls, t) {
				tbls = append(tbls, t)
			}
		}
		var what []string
		if moved > 0 {
			what = append(what, "moved "+model.Plural(moved, "region", "regions"))
		}
		if split > 0 {
			what = append(what, "split "+model.Plural(split, "region", "regions"))
		}
		c.add(model.Finding{Rule: "hbase-regions-changed", Severity: model.Warning, Section: "stages",
			Title:       fmt.Sprintf("HBase %s of %s while the run used %s", strings.Join(what, " and "), strings.Join(tbls, ", "), map[bool]string{true: "it", false: "them"}[len(tbls) == 1]),
			Explanation: "While a region moves or splits it is closed for a moment, and calls to it wait and retry; HBase's client logs nothing about this at its default settings, so the tasks using it only look slower." + whileRan(stagesWith(h, "moved", "split")),
			Evidence:    serverEvidence(append(slices.Clone(mine["moved"]), mine["split"]...), 4),
			Fix:         "The Master's log says what moved them: the balancer, an administrator, or regions splitting as they grow. Avoid balancing and major compactions while heavy jobs run, and pre-split tables that split while they are written."})
	}
	// A region server paused.
	if xs := mine["pause"]; len(xs) > 0 {
		longest, on := 0, ""
		for _, x := range xs {
			if ms, _ := strconv.Atoi(x.l.Fields["ms"]); ms > longest {
				longest, on = ms, x.l.Fields["host"]
			}
		}
		c.add(model.Finding{Rule: "hbase-server-pause", Severity: model.Warning, Section: "stages",
			Title:       fmt.Sprintf("Region server %s paused for %s while the run was using HBase", on, model.Duration(int64(longest))),
			Explanation: fmt.Sprintf("HBase's JVM pause monitor saw the whole region server stop for that long (%s in all), usually for Java garbage collection, and every call to it waited.", model.Plural(len(xs), "pause", "pauses")) + whileRan(stagesWith(h, "pause")),
			Evidence:    serverEvidence(xs, 4),
			Fix:         "Check the region server's heap and garbage collector (HBASE_REGIONSERVER_OPTS) against its node's memory. Long pauses can also make ZooKeeper think the server died."})
	}
	// Calls the region servers logged as slow or too large.
	if xs := append(slices.Clone(mine["slow-call"]), mine["large-response"]...); len(xs) > 0 {
		n := count("slow-call") + count("large-response")
		c.add(model.Finding{Rule: "hbase-slow-calls", Severity: model.Warning, Section: "stages",
			Title:       fmt.Sprintf("Region servers logged %s from the run's tables as too slow or too large", model.Plural(n, "call", "calls")),
			Explanation: "A region server logs a call that took longer than hbase.ipc.warn.response.time (10 s unless changed), or answered with more than hbase.ipc.warn.response.size, with its table and client." + whileRan(stagesWith(h, "slow-call", "large-response")),
			Evidence:    serverEvidence(xs, 4),
			Fix:         "For scans, fetch fewer rows per call (scanner caching) and filter on the server; for writes, send smaller batches. The region server's log around those calls says whether it was busy flushing, compacting or paused."})
	}
	// The server's side of what the clients retried.
	if f := c.finding("hbase-busy"); f != nil {
		if n := count("busy"); n > 0 {
			var flushes, compactions, stores int
			servers := hostsOf(mine["busy"])
			for _, x := range c.logs.hits {
				if x.l.Kind == model.LogHBaseServer && servers[shortHost(x.l.Fields["host"])] {
					switch x.l.Fields["event"] {
					case "flush":
						flushes += x.l.Count
					case "compaction":
						compactions += x.l.Count
					case "store-files":
						stores += x.l.Count
					}
				}
			}
			s := fmt.Sprintf(" The region server's own log shows %s while the run wrote", model.Plural(n, "refusal", "refusals"))
			if flushes+compactions > 0 {
				s += ", and " + model.HBaseServerEventsText(map[string]int{"flush": flushes, "compaction": compactions}) + ": it was flushing as fast as it could"
			}
			if stores > 0 {
				s += fmt.Sprintf(", and held back flushes %s because a region had too many store files", model.Plural(stores, "time", "times"))
			}
			f.Explanation += s + "."
			f.Evidence = append(f.Evidence, serverEvidence(mine["busy"], 1)...)
		}
	}
	if f := c.finding("hbase-scanner-expired"); f != nil {
		if xs := mine["scanner"]; len(xs) > 0 {
			f.Explanation += fmt.Sprintf(" The region servers' logs confirm %s on %s from this run's executors.", model.Plural(count("scanner"), "expired lease", "expired leases"), xs[0].l.Fields["table"])
			f.Evidence = append(f.Evidence, serverEvidence(xs, 2)...)
		}
	}
}

// hostsOf is the servers that logged xs.
func hostsOf(xs []hit) map[string]bool {
	out := map[string]bool{}
	for _, x := range xs {
		out[shortHost(x.l.Fields["host"])] = true
	}
	return out
}

// shortHost is a host name without its domain, and an IP address as the
// name EMR gives it, so ip-10-0-2-10.ec2.internal and 10.0.2.10 match.
func shortHost(h string) string {
	h = strings.ToLower(strings.TrimSpace(h))
	if strings.Count(h, ".") == 3 && strings.Trim(h, "0123456789.") == "" {
		return "ip-" + strings.ReplaceAll(h, ".", "-")
	}
	if i := strings.IndexByte(h, '.'); i > 0 {
		return h[:i]
	}
	return h
}
