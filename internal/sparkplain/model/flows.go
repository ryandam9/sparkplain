package model

import "time"

// FlowSection is the data the run moved and the memory it used over
// time, from what the executors logged (TaskStories): shuffle data read
// from each task's own node and over the network, data spilled to disk,
// data cached in memory and the storage memory left, and the broadcast
// variables read. Shuffle sizes are Spark's estimates from the map
// outputs.
type FlowSection struct {
	// From and StepMs place the series: each point adds up its step.
	From   time.Time `json:"from"`
	StepMs int64     `json:"stepMs"`
	Local  []Point   `json:"local"`  // shuffle bytes read on the task's own node, per step
	Remote []Point   `json:"remote"` // shuffle bytes read over the network, per step
	Spill  []Point   `json:"spill"`  // bytes spilled to disk, per step
	Cached []Point   `json:"cached"` // bytes cached in memory, per step
	// Totals of the above.
	LocalBytes  int64 `json:"localBytes"`
	RemoteBytes int64 `json:"remoteBytes"`
	SpillBytes  int64 `json:"spillBytes"`
	CachedBytes int64 `json:"cachedBytes"`
	Dropped     int   `json:"dropped"`
	NotCached   int   `json:"notCached"`
	// Executors, busiest over the network first.
	Executors  []ExecFlow      `json:"executors"`
	Broadcasts []BroadcastRead `json:"broadcasts,omitempty"`
	Spills     []StageSpillLog `json:"spills,omitempty"`
}

// ExecFlow is one executor's data moved and memory over time.
type ExecFlow struct {
	Executor    string `json:"executor"`
	Host        string `json:"host,omitempty"`
	LocalBytes  int64  `json:"localBytes"`
	RemoteBytes int64  `json:"remoteBytes"`
	SpillBytes  int64  `json:"spillBytes"`
	CachedBytes int64  `json:"cachedBytes"`
	Dropped     int    `json:"dropped,omitempty"`
	NotCached   int    `json:"notCached,omitempty"`
	// Remote is its shuffle bytes over the network per step; Free its
	// storage memory free after each block it cached or dropped, and
	// MinFree the least, when.
	Remote    []Point   `json:"remote,omitempty"`
	Free      []Point   `json:"free,omitempty"`
	MinFree   int64     `json:"minFree,omitempty"`
	MinFreeAt time.Time `json:"minFreeAt,omitzero"`
	// MinFreeSource is the line that logged the least free memory.
	MinFreeSource Source `json:"minFreeSource,omitzero"`
	// RemoteSource is its largest shuffle read over the network, and
	// EvictSource its first block dropped or not cached.
	RemoteSource Source `json:"remoteSource,omitzero"`
	EvictSource  Source `json:"evictSource,omitzero"`
	Source       Source `json:"source"` // its log
}

// BroadcastRead is one broadcast variable as the executors read it.
type BroadcastRead struct {
	Name      string    `json:"name"` // "broadcast 7"
	Bytes     int64     `json:"bytes"`
	Pieces    int       `json:"pieces"`
	Executors int       `json:"executors"` // that logged reading it
	ReadMs    int64     `json:"readMs"`    // all reads together
	MaxMs     int64     `json:"maxMs"`     // the slowest read
	SlowestOn string    `json:"slowestOn,omitempty"`
	First     time.Time `json:"first"`
	Source    Source    `json:"source"` // the first read
	MaxSource Source    `json:"maxSource,omitzero"`
}

// StageSpillLog is what one stage's tasks spilled to disk, as logged.
type StageSpillLog struct {
	Stage     int    `json:"stage"` // -1 when its lines named no task
	Attempt   int    `json:"attempt"`
	Tasks     int    `json:"tasks"` // tasks that spilled
	Spills    int    `json:"spills"`
	Bytes     int64  `json:"bytes"`
	MostBytes int64  `json:"mostBytes"` // the task that spilled most
	MostTask  int64  `json:"mostTask"`
	Source    Source `json:"source"`
}
