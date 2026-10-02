package model

import (
	"sort"
	"strings"
)

// HBaseSection is what the run did with HBase (phase 5): the tables it read
// and wrote and how, where the regions it read were, the ZooKeeper quorum
// its clients dialled and how often, and the HBase libraries it carried.
// It is set only when the event log or the logs show the run used HBase.
type HBaseSection struct {
	Tables []HBaseTable `json:"tables,omitempty"`
	// Quorum is the ZooKeeper address the clients connected to, from their
	// logs.
	Quorum       string `json:"quorum,omitempty"`
	QuorumSource Source `json:"quorumSource,omitzero"`
	// Sessions counts the ZooKeeper connections the run's processes opened;
	// MostSessions is the most one process opened, and MostSessionsBy that
	// process.
	Sessions       int         `json:"sessions"`
	MostSessions   int         `json:"mostSessions"`
	MostSessionsBy string      `json:"mostSessionsBy,omitempty"`
	Libraries      []Component `json:"libraries,omitempty"`
	// Stages are the stages that read or wrote HBase (phase 5 step 4), in
	// order. TimeMs is the wall-clock time at least one of them ran, of the
	// run's RunMs; RunTimeMs and CPUTimeNs add up their tasks.
	Stages    []HBaseStage `json:"stages,omitempty"`
	TimeMs    int64        `json:"timeMs"`
	RunMs     int64        `json:"runMs"`
	RunTimeMs int64        `json:"runTimeMs"`
	CPUTimeNs int64        `json:"cpuTimeNs"`
	// LocalRegions and RemoteRegions count the TableInputFormat regions an
	// executor read on the region server's own node, or from another one.
	LocalRegions  int `json:"localRegions"`
	RemoteRegions int `json:"remoteRegions"`
	// ServerEvents are what HBase's Master and region servers logged while
	// the run went on (phase 5 step 7), one row per event, server and
	// table. Mine marks those tied to this run: its tables, its executors'
	// hosts, or a whole region server. The others may be other work on the
	// cluster at the same time.
	ServerEvents []HBaseServerEvent `json:"serverEvents,omitempty"`
	// Scans are the TableInputFormat scan stages, region by region.
	Scans   []HBaseScanRead `json:"scans,omitempty"`
	Missing []string        `json:"missing,omitempty"`
}

// HBaseServerEvent is one kind of thing an HBase server logged, how often,
// and where: moved, split, server-lost, server-stopped, busy, scanner,
// pause, slow-call, large-response, store-files, flush or compaction.
type HBaseServerEvent struct {
	Event  string `json:"event"`
	Host   string `json:"host"`            // the server that logged it
	Table  string `json:"table,omitempty"` // the table it names
	Detail string `json:"detail,omitempty"`
	Count  int    `json:"count"`
	Mine   bool   `json:"mine"`
	Source Source `json:"source"`
}

// HBaseStage is one stage that read or wrote HBase, what its tasks spent
// their time on, and what the HBase clients logged while it ran.
type HBaseStage struct {
	StageID     int      `json:"stageId"`
	Attempt     int      `json:"attempt"`
	Description string   `json:"description,omitempty"` // its job's description, or the stage's name
	Reads       []string `json:"reads,omitempty"`
	Writes      []string `json:"writes,omitempty"`
	API         string   `json:"api"`
	Tasks       int      `json:"tasks"`
	DurationMs  int64    `json:"durationMs"`
	RunTimeMs   int64    `json:"runTimeMs"`
	CPUTimeNs   int64    `json:"cpuTimeNs"`
	// Retries counts the HBase warnings and errors logged while it ran,
	// by problem (scanner, busy, moved, server, …), and Servers the region
	// servers they named.
	Retries map[string]int `json:"retries,omitempty"`
	Servers []string       `json:"servers,omitempty"`
	// ServerEvents counts what HBase's servers logged while it ran, of the
	// events tied to this run.
	ServerEvents map[string]int `json:"serverEvents,omitempty"`
	// RetrySources and ServerEventSources are a line behind each count,
	// the first read, so a reader can check it.
	RetrySources       map[string]Source `json:"retrySources,omitempty"`
	ServerEventSources map[string]Source `json:"serverEventSources,omitempty"`
	// Slowest is its slowest task, and MedianMs the median task's time.
	Slowest  *TaskRef `json:"slowest,omitempty"`
	MedianMs int64    `json:"medianMs"`
	Source   Source   `json:"source"`
}

// HBaseTable is one table the run read or wrote.
type HBaseTable struct {
	Name    string `json:"name"`
	Read    bool   `json:"read"`
	Written bool   `json:"written"`
	// APIs are how: "hbase-spark connector", "TableInputFormat",
	// "TableOutputFormat".
	APIs []string `json:"apis"`
	// Regions counts the regions a TableInputFormat scan read on each region
	// server; the connector logs none.
	Regions []HBaseServerRegions `json:"regions,omitempty"`
	Source  Source               `json:"source"`
}

// HBaseServerRegions is how many of a table's regions were read from one
// region server.
type HBaseServerRegions struct {
	Server  string `json:"server"`
	Regions int    `json:"regions"`
}

// hbaseProblemNames says in words what the HBase clients logged, by the
// classifier's problem.
var hbaseProblemNames = map[string][2]string{
	"scanner":       {"expired scanner lease", "expired scanner leases"},
	"busy":          {"region-busy retry", "region-busy retries"},
	"moved":         {"moved-region retry", "moved-region retries"},
	"server":        {"region server timeout", "region server timeouts"},
	"retries":       {"call that ran out of retries", "calls that ran out of retries"},
	"table-missing": {"missing-table error", "missing-table errors"},
	"zookeeper":     {"ZooKeeper connection failure", "ZooKeeper connection failures"},
	"other":         {"other HBase error", "other HBase errors"},
}

// HBaseRetriesText says what the HBase clients logged, by problem,, as "6 expired scanner leases
// and 4 region-busy retries".
func HBaseRetriesText(n map[string]int) string {
	var keys []string
	for k := range n {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return n[keys[i]] > n[keys[j]] || n[keys[i]] == n[keys[j]] && keys[i] < keys[j] })
	var parts []string
	for _, k := range keys {
		w := hbaseProblemNames[k]
		parts = append(parts, Plural(n[k], w[0], w[1]))
	}
	switch len(parts) {
	case 0:
		return ""
	case 1:
		return parts[0]
	}
	return strings.Join(parts[:len(parts)-1], ", ") + " and " + parts[len(parts)-1]
}

// hbaseServerEventNames says what an HBase server event is, in words.
var hbaseServerEventNames = map[string][2]string{
	"moved":          {"region moved", "regions moved"},
	"split":          {"region split", "regions split"},
	"server-lost":    {"region server lost", "region servers lost"},
	"server-stopped": {"region server stopped", "region servers stopped"},
	"busy":           {"write refused (memstore full)", "writes refused (memstore full)"},
	"scanner":        {"scanner lease expired", "scanner leases expired"},
	"pause":          {"JVM pause", "JVM pauses"},
	"slow-call":      {"slow call", "slow calls"},
	"large-response": {"large response", "large responses"},
	"store-files":    {"flush held back (too many store files)", "flushes held back (too many store files)"},
	"flush":          {"memstore flush", "memstore flushes"},
	"compaction":     {"compaction", "compactions"},
}

// HBaseServerEventName names one HBase server event.
func HBaseServerEventName(ev string) string {
	if w, ok := hbaseServerEventNames[ev]; ok {
		return strings.ToUpper(w[0][:1]) + w[0][1:]
	}
	return ev
}

// HBaseServerEventsText says what HBase's servers logged, by event, as
// "291 writes refused (memstore full) and 74 compactions".
func HBaseServerEventsText(n map[string]int) string {
	keys := make([]string, 0, len(n))
	for k := range n {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return n[keys[i]] > n[keys[j]] || n[keys[i]] == n[keys[j]] && keys[i] < keys[j] })
	var parts []string
	for _, k := range keys {
		if n[k] == 0 {
			continue
		}
		w, ok := hbaseServerEventNames[k]
		if !ok {
			w = [2]string{k, k}
		}
		parts = append(parts, Plural(n[k], w[0], w[1]))
	}
	switch len(parts) {
	case 0:
		return ""
	case 1:
		return parts[0]
	}
	return strings.Join(parts[:len(parts)-1], ", ") + " and " + parts[len(parts)-1]
}
