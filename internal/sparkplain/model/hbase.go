package model

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
	Missing        []string    `json:"missing,omitempty"`
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
