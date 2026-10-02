package analyze

import (
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// hbaseJarRE reads the HBase client and connector jars YARN localized, for
// runs without an event log: hbase-client-2.4.17-amzn-7.jar, hbase-spark-1.0.1.jar.
var hbaseJarRE = regexp.MustCompile(`^(hbase-(?:shaded-)?client|hbase-spark)-(\d+(?:\.\d+)+(?:[-.][\w.-]+)?)\.jar$`)

// analyzeHBase says what the run did with HBase (phase 5 step 3): tables
// from the connector's SQL plans and from the MapReduce API's log lines,
// the regions each scan read per region server, the ZooKeeper quorum and
// how many connections each process opened, and the HBase libraries.
func analyzeHBase(c *ctx, r *model.Report) {
	h := &model.HBaseSection{}
	tables := map[string]*model.HBaseTable{}
	regions := map[string]map[string]int{} // table -> server -> regions
	table := func(name string, src model.Source) *model.HBaseTable {
		t := tables[name]
		if t == nil {
			t = &model.HBaseTable{Name: name, Source: src}
			tables[name] = t
		}
		return t
	}
	use := func(t *model.HBaseTable, access, api string) {
		switch access {
		case "read":
			t.Read = true
		case "write":
			t.Written = true
		}
		if !slices.Contains(t.APIs, api) {
			t.APIs = append(t.APIs, api)
		}
	}
	for _, d := range r.IO.Data {
		if d.Format == "hbase" {
			use(table(d.Name, d.Source), d.Access, "hbase-spark connector")
		}
	}
	perProcess := map[string]int{}
	var jars []model.Component
	if c.logs != nil {
		for _, x := range c.logs.hits {
			l := x.l
			switch l.Kind {
			case model.LogHBaseUse:
				t := table(l.Fields["table"], l.Source)
				use(t, l.Fields["access"], l.Fields["api"])
				if s := l.Fields["server"]; s != "" {
					if regions[t.Name] == nil {
						regions[t.Name] = map[string]int{}
					}
					regions[t.Name][s] += l.Count
				}
			case model.LogHBase:
				if q := l.Fields["quorum"]; q != "" && l.Severity == model.Info {
					if h.Quorum == "" {
						h.Quorum, h.QuorumSource = q, l.Source
					}
					h.Sessions += l.Count
					perProcess[processOf(x)] += l.Count
				}
			case model.LogLocalized:
				if m := hbaseJarRE.FindStringSubmatch(l.Fields["name"]); m != nil {
					name := "HBase client"
					if m[1] == "hbase-spark" {
						name = "HBase Spark connector"
					}
					jars = append(jars, model.Component{Name: name, Artifact: m[1], Version: m[2], Path: l.Fields["name"], Source: l.Source})
				}
			}
		}
	}
	for who, n := range perProcess {
		if n > h.MostSessions || n == h.MostSessions && who < h.MostSessionsBy {
			h.MostSessions, h.MostSessionsBy = n, who
		}
	}
	// Library versions: the event log's, else the jars YARN localized.
	if c.log != nil {
		for _, x := range c.log.Components {
			if strings.HasPrefix(x.Name, "HBase ") {
				h.Libraries = append(h.Libraries, x)
			}
		}
	}
	for _, j := range jars {
		if !slices.ContainsFunc(h.Libraries, func(x model.Component) bool { return x.Name == j.Name }) {
			h.Libraries = append(h.Libraries, j)
		}
	}
	for _, t := range tables {
		for s, n := range regions[t.Name] {
			t.Regions = append(t.Regions, model.HBaseServerRegions{Server: s, Regions: n})
		}
		sort.Slice(t.Regions, func(i, j int) bool {
			a, b := t.Regions[i], t.Regions[j]
			return a.Regions > b.Regions || a.Regions == b.Regions && a.Server < b.Server
		})
		h.Tables = append(h.Tables, *t)
	}
	sort.Slice(h.Tables, func(i, j int) bool { return h.Tables[i].Name < h.Tables[j].Name })
	if len(h.Tables) == 0 && h.Quorum == "" && len(h.Libraries) == 0 {
		return
	}
	hbaseStages(c, r, h)
	hbaseLocality(c, h)
	hbaseTasks(c, r, h, hbaseScans(c, r, h))
	hbaseRegionEvents(c, r, h)
	hbaseLoad(c, h)
	hbaseScanFindings(c, h)
	if slices.ContainsFunc(h.Tables, func(t model.HBaseTable) bool { return t.Read && slices.Contains(t.APIs, "hbase-spark connector") }) {
		h.Missing = append(h.Missing, "Regions per region server for tables the hbase-spark connector read: the connector does not log its regions (HBase's own logs, step 7, will).")
	}
	if c.log == nil {
		h.Missing = append(h.Missing, "Tables the hbase-spark connector read or wrote: they are named in the SQL plans, which need the event log.")
	}
	if c.logs == nil {
		h.Missing = append(h.Missing, "Tables read or written with TableInputFormat or TableOutputFormat, their regions, and the ZooKeeper connections: these are in the container logs (-cluster-id or -from).")
	}
	r.HBase = h
	hbaseSlowFindings(c, h)
	hbaseServer(c, h)
}
