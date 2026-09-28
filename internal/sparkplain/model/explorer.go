package model

import "time"

// Explorer holds what explorer.html needs beyond the report (SPEC §6,
// Explorer page): per-stage detail, a task sample, running tasks over time and
// SQL operator metrics. The parser collects it while streaming, only when the
// explorer page is wanted, and it never goes into report.json.
type Explorer struct {
	Limits  ExplorerLimits `json:"limits"`
	Stages  []StageDetail  `json:"stages"` // same order as EventLog.Stages
	Running RunningTasks   `json:"running"`
	SQL     []SQLGraph     `json:"sql"` // same order as EventLog.SQL, plans that were kept
	// SampleShrinks counts how often the app-wide task budget halved every
	// stage's sample. Zero means each stage kept its full sample.
	SampleShrinks int `json:"sampleShrinks"`
	// CellsCapped is true when stage × executor totals hit their cap, so
	// later stages have no per-executor breakdown.
	CellsCapped bool `json:"cellsCapped"`
}

// ExplorerLimits bound the explorer's memory and file size. Zero values mean
// the defaults from DefaultExplorerLimits.
type ExplorerLimits struct {
	SlowestPerStage       int `json:"slowestPerStage" yaml:"slowest-per-stage"`
	SamplePerStage        int `json:"samplePerStage" yaml:"sample-per-stage"`
	MaxSampledTasks       int `json:"maxSampledTasks" yaml:"max-sampled-tasks"`
	MaxStageExecutorCells int `json:"maxStageExecutorCells" yaml:"max-stage-executor-cells"`
}

// DefaultExplorerLimits are the SPEC §6 defaults.
func DefaultExplorerLimits() ExplorerLimits {
	return ExplorerLimits{SlowestPerStage: 100, SamplePerStage: 1000, MaxSampledTasks: 100_000, MaxStageExecutorCells: 1_000_000}
}

// WithDefaults fills zero fields from DefaultExplorerLimits.
func (l ExplorerLimits) WithDefaults() ExplorerLimits {
	d := DefaultExplorerLimits()
	if l.SlowestPerStage <= 0 {
		l.SlowestPerStage = d.SlowestPerStage
	}
	if l.SamplePerStage <= 0 {
		l.SamplePerStage = d.SamplePerStage
	}
	if l.MaxSampledTasks <= 0 {
		l.MaxSampledTasks = d.MaxSampledTasks
	}
	if l.MaxStageExecutorCells <= 0 {
		l.MaxStageExecutorCells = d.MaxStageExecutorCells
	}
	return l
}

// Quartiles summarise one task metric over a stage's successful tasks, like
// the History Server's "Summary Metrics". Values past the first 64 tasks
// come from a log-scale histogram, so they are within about 3%.
type Quartiles struct {
	Count int64 `json:"count"`
	Sum   int64 `json:"sum"`
	Min   int64 `json:"min"`
	P25   int64 `json:"p25"`
	P50   int64 `json:"p50"`
	P75   int64 `json:"p75"`
	Max   int64 `json:"max"`
}

// HistBin is one bar of a histogram: values in [Lo, Hi].
type HistBin struct {
	Lo    int64 `json:"lo"`
	Hi    int64 `json:"hi"`
	Count int64 `json:"count"`
}

// Stage metric names used as keys of StageDetail.Metrics.
const (
	MetricDuration        = "durationMs"
	MetricRunTime         = "runTimeMs"
	MetricGCTime          = "gcTimeMs"
	MetricDeserialize     = "deserializeMs"
	MetricRecordsRead     = "recordsRead"
	MetricInputBytes      = "inputBytes"
	MetricShuffleRead     = "shuffleReadBytes"
	MetricFetchWait       = "shuffleFetchWaitMs"
	MetricShuffleWrite    = "shuffleWriteBytes"
	MetricMemorySpill     = "memorySpillBytes"
	MetricDiskSpill       = "diskSpillBytes"
	MetricPeakExecMemory  = "peakExecutionMemory"
	MetricOutputBytes     = "outputBytes"
	MetricShuffleRecsRead = "shuffleRecordsRead"
	MetricSchedulerDelay  = "schedulerDelayMs"
	MetricResultSize      = "resultSizeBytes"
	MetricResultSer       = "resultSerializationMs"
	MetricGettingResult   = "gettingResultMs"
	MetricShuffleWriteMs  = "shuffleWriteTimeMs"
	MetricShuffleRemote   = "shuffleRemoteBytes"
	MetricRemoteToDisk    = "shuffleRemoteToDiskBytes"
	MetricFetchReqs       = "shuffleRemoteRequestsMs"
	MetricDeserializeCPU  = "deserializeCpuMs"
)

// StageDetail is one stage attempt as the explorer shows it.
type StageDetail struct {
	ID       int                  `json:"id"`
	Attempt  int                  `json:"attempt"`
	Metrics  map[string]Quartiles `json:"metrics"`
	Duration []HistBin            `json:"durationHistogram"`
	Slowest  []TaskSample         `json:"slowest"` // slowest first
	Sample   []TaskSample         `json:"sample"`  // uniform over all finished task attempts, by task ID
	// SampledFrom is how many finished task attempts the sample was drawn from.
	SampledFrom int64               `json:"sampledFrom"`
	Executors   []StageExecutorCell `json:"executors,omitempty"`
}

// TaskSample is one finished task attempt. Times are Unix milliseconds.
type TaskSample struct {
	TaskID        int64  `json:"taskId"`
	Index         int    `json:"index"`
	Attempt       int    `json:"attempt"`
	ExecutorID    string `json:"executorId"`
	Status        string `json:"status"` // succeeded, failed or killed
	Speculative   bool   `json:"speculative,omitempty"`
	LaunchMs      int64  `json:"launchMs"`
	DurationMs    int64  `json:"durationMs"`
	RunTimeMs     int64  `json:"runTimeMs"`
	GCTimeMs      int64  `json:"gcTimeMs"`
	DeserializeMs int64  `json:"deserializeMs"`
	FetchWaitMs   int64  `json:"fetchWaitMs"`
	RecordsRead   int64  `json:"recordsRead"`
	InputBytes    int64  `json:"inputBytes"`
	ShuffleRead   int64  `json:"shuffleReadBytes"`
	ShuffleWrite  int64  `json:"shuffleWriteBytes"`
	Spill         int64  `json:"spillBytes"` // memory plus disk
	PartitionID   int    `json:"partitionId"`
	Locality      string `json:"locality,omitempty"` // PROCESS_LOCAL, NODE_LOCAL, RACK_LOCAL, ANY, NO_PREF
	SchedDelayMs  int64  `json:"schedulerDelayMs"`
	ResultSize    int64  `json:"resultSizeBytes"`
	PeakExec      int64  `json:"peakExecutionMemory"` // execution memory the task held at its peak
	Source        Source `json:"source"`
}

// StageExecutorCell is one executor's share of one stage attempt, like the
// History Server's "Aggregated Metrics by Executor", plus the executor's
// peak memory while the stage ran.
type StageExecutorCell struct {
	ExecutorID string     `json:"executorId"`
	Tasks      TaskTotals `json:"tasks"`
	Peak       PeakMemory `json:"peak"`
}

// RunningTasks is the number of tasks running over time, folded into equal
// buckets. BusyMs[i] is task-milliseconds run in bucket i, so the average
// number of running tasks in it is BusyMs[i] / BucketMs.
type RunningTasks struct {
	Start    time.Time `json:"start,omitzero"`
	BucketMs int64     `json:"bucketMs"`
	BusyMs   []int64   `json:"busyMs"`
}

// SQLGraph is a query's final physical plan (after adaptive re-planning) as
// a tree of operators with their metrics.
type SQLGraph struct {
	QueryID   int64     `json:"queryId"`
	Nodes     []SQLNode `json:"nodes"` // Nodes[0] is the root
	Truncated bool      `json:"truncated,omitempty"`
	// Adaptive are metrics adaptive execution added after planning, which
	// the log does not tie to an operator.
	Adaptive []SQLMetric `json:"adaptive,omitempty"`
}

// SQLNode is one plan operator.
type SQLNode struct {
	ID       int         `json:"id"`
	Name     string      `json:"name"`
	Detail   string      `json:"detail,omitempty"`
	Children []int       `json:"children,omitempty"`
	Metrics  []SQLMetric `json:"metrics,omitempty"`
}

// SQLMetric is one operator metric. Value is Spark's total across tasks;
// Type says how to read it (sum, size, timing in ms, nsTiming, average).
// Known is false when the log recorded no value for it.
type SQLMetric struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Value int64  `json:"value"`
	Known bool   `json:"known"`
	// Per-task spread, from each task's update (as Spark's SQL tab shows):
	// how many tasks reported it, the min, median and max, and the task and
	// stage that hit the max. Tasks is 0 when no task reported it.
	Tasks     int64 `json:"tasks,omitempty"`
	Min       int64 `json:"min,omitempty"`
	Median    int64 `json:"median,omitempty"`
	Max       int64 `json:"max,omitempty"`
	MaxTaskID int64 `json:"maxTaskId,omitempty"`
	MaxStage  int   `json:"maxStage,omitempty"`
}
