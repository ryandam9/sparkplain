package model

import "time"

// EventLog is everything read from one Spark event log, already redacted.
type EventLog struct {
	Stats            EventLogStats     `json:"stats"`
	Application      Application       `json:"application"`
	ResourceProfiles []ResourceProfile `json:"resourceProfiles,omitempty"`
	Executors        []*Executor       `json:"executors"`
	Driver           *Executor         `json:"driver,omitempty"`
	Jobs             []*Job            `json:"jobs"`
	Stages           []*Stage          `json:"stages"`
	SQL              []*SQLQuery       `json:"sql,omitempty"`
	RDDs             []*CachedRDD      `json:"cachedRdds,omitempty"`
	Config           []ConfigEntry     `json:"config"`
	CatalogEvents    []DataRef         `json:"catalogEvents,omitempty"`
	Components       []Component       `json:"components,omitempty"`
	// Explorer is collected only when the explorer page is wanted.
	Explorer *Explorer `json:"-"`
}

// Component is a library version read from a jar name on the driver's
// classpath, such as hadoop-client-api-3.3.4.jar.
type Component struct {
	Name     string `json:"name"`
	Artifact string `json:"artifact"`
	Version  string `json:"version"`
	Path     string `json:"path"`
	Source   Source `json:"source"`
}

// EventLogStats describes how the event log was read.
type EventLogStats struct {
	Input          string           `json:"input"`
	Layout         string           `json:"layout"` // single, rolling, zip, zip-rolling
	Codec          string           `json:"codec"`
	Files          []FileRead       `json:"files"`
	Lines          int64            `json:"lines"`
	Events         int64            `json:"events"`
	ByType         map[string]int64 `json:"byType"`
	UnknownEvents  map[string]int64 `json:"unknownEvents,omitempty"`
	UnknownFields  map[string]int64 `json:"unknownFields,omitempty"`
	Malformed      int64            `json:"malformedLines,omitempty"`
	FirstMalformed Source           `json:"firstMalformed,omitzero"`
	InProgress     bool             `json:"inProgress,omitempty"`
	Truncated      bool             `json:"truncated,omitempty"`
	Notes          []string         `json:"notes,omitempty"`
	Redacted       int64            `json:"redactedValues,omitempty"`
}

// FileRead is one physical file (or zip entry) that was read.
type FileRead struct {
	Name         string `json:"name"`
	Codec        string `json:"codec"`
	Bytes        int64  `json:"bytes"`
	Decompressed int64  `json:"decompressedBytes"`
	Lines        int64  `json:"lines"`
	Error        string `json:"error,omitempty"`
}

// Application is the run as a whole.
type Application struct {
	ID           string    `json:"id"`
	AttemptID    string    `json:"attemptId,omitempty"`
	Name         string    `json:"name"`
	User         string    `json:"user"`
	SparkVersion string    `json:"sparkVersion"`
	VersionSrc   Source    `json:"sparkVersionSource,omitzero"`
	Master       string    `json:"master,omitempty"`
	DeployMode   string    `json:"deployMode,omitempty"`
	Queue        string    `json:"queue,omitempty"`
	Start        time.Time `json:"start,omitzero"`
	End          time.Time `json:"end,omitzero"`
	DurationMs   int64     `json:"durationMs"`
	Status       string    `json:"status"`
	StatusReason string    `json:"statusReason"`
	ExitCode     *int      `json:"exitCode,omitempty"`
	Source       Source    `json:"source"`
	EndSource    Source    `json:"endSource,omitzero"`
}

// ResourceProfile is what each executor of a profile asked YARN for.
type ResourceProfile struct {
	ID               int     `json:"id"`
	ExecutorCores    int     `json:"executorCores,omitempty"`
	ExecutorMemoryMB int64   `json:"executorMemoryMiB,omitempty"`
	OverheadMB       int64   `json:"memoryOverheadMiB,omitempty"`
	OffHeapMB        int64   `json:"offHeapMiB,omitempty"`
	PySparkMemoryMB  int64   `json:"pysparkMemoryMiB,omitempty"`
	TaskCPUs         float64 `json:"taskCpus,omitempty"`
	Source           Source  `json:"source"`
}

// PeakMemory holds the highest sampled executor memory metrics, in bytes.
// Spark samples these on heartbeats and at task end, so short spikes can be missed.
type PeakMemory struct {
	JVMHeap          int64  `json:"jvmHeap"`
	JVMOffHeap       int64  `json:"jvmOffHeap"`
	OnHeapExecution  int64  `json:"onHeapExecution"`
	OnHeapStorage    int64  `json:"onHeapStorage"`
	OffHeapExecution int64  `json:"offHeapExecution"`
	OffHeapStorage   int64  `json:"offHeapStorage"`
	DirectPool       int64  `json:"directPool"`
	MappedPool       int64  `json:"mappedPool"`
	ProcessJVMRSS    int64  `json:"processTreeJvmRss"`
	ProcessPythonRSS int64  `json:"processTreePythonRss"`
	ProcessOtherRSS  int64  `json:"processTreeOtherRss"`
	TotalGCTimeMs    int64  `json:"totalGcTimeMs"`
	HeapSource       Source `json:"heapSource,omitzero"`
	RSSSource        Source `json:"rssSource,omitzero"`
}

// Merge keeps the larger of each metric.
func (p *PeakMemory) Merge(o PeakMemory) {
	if o.JVMHeap > p.JVMHeap {
		p.JVMHeap, p.HeapSource = o.JVMHeap, o.HeapSource
	}
	rss := func(x PeakMemory) int64 { return x.ProcessJVMRSS + x.ProcessPythonRSS + x.ProcessOtherRSS }
	if rss(o) > rss(*p) {
		p.RSSSource = o.RSSSource
	}
	p.JVMOffHeap = max(p.JVMOffHeap, o.JVMOffHeap)
	p.OnHeapExecution = max(p.OnHeapExecution, o.OnHeapExecution)
	p.OnHeapStorage = max(p.OnHeapStorage, o.OnHeapStorage)
	p.OffHeapExecution = max(p.OffHeapExecution, o.OffHeapExecution)
	p.OffHeapStorage = max(p.OffHeapStorage, o.OffHeapStorage)
	p.DirectPool = max(p.DirectPool, o.DirectPool)
	p.MappedPool = max(p.MappedPool, o.MappedPool)
	p.ProcessJVMRSS = max(p.ProcessJVMRSS, o.ProcessJVMRSS)
	p.ProcessPythonRSS = max(p.ProcessPythonRSS, o.ProcessPythonRSS)
	p.ProcessOtherRSS = max(p.ProcessOtherRSS, o.ProcessOtherRSS)
	p.TotalGCTimeMs = max(p.TotalGCTimeMs, o.TotalGCTimeMs)
}

// Executor removal kinds, classified from Spark's removal reason text.
const (
	RemovalNone           = ""               // still running when the log ends
	RemovalMemoryKill     = "memory-kill"    // exit 137 / killed for exceeding memory
	RemovalLost           = "lost"           // heartbeat timeout, crash, lost node
	RemovalDecommissioned = "decommissioned" // node decommissioned (often spot reclaim)
	RemovalKilledByDriver = "killed-by-driver"
	RemovalIdle           = "idle"
	RemovalOther          = "other"
)

// Executor is one Spark executor (or the driver, with ID "driver").
type Executor struct {
	ID                string     `json:"id"`
	Host              string     `json:"host"`
	Cores             int        `json:"cores"`
	ResourceProfileID int        `json:"resourceProfileId"`
	Added             time.Time  `json:"added,omitzero"`
	Removed           time.Time  `json:"removed,omitzero"`
	RemovedReason     string     `json:"removedReason,omitempty"`
	RemovalKind       string     `json:"removalKind,omitempty"`
	MaxOnHeapStorage  int64      `json:"maxOnHeapStorageBytes"`
	MaxOffHeapStorage int64      `json:"maxOffHeapStorageBytes"`
	Peak              PeakMemory `json:"peak"`
	Tasks             TaskTotals `json:"tasks"`
	AddedSource       Source     `json:"addedSource,omitzero"`
	RemovedSource     Source     `json:"removedSource,omitzero"`
}

// TaskTotals sums task metrics. Times are milliseconds except CPU time (ns),
// which is how Spark records them.
type TaskTotals struct {
	Tasks               int64 `json:"tasks"`
	Succeeded           int64 `json:"succeeded"`
	Failed              int64 `json:"failed"`
	Killed              int64 `json:"killed"`
	Speculative         int64 `json:"speculative"`
	DurationMs          int64 `json:"durationMs"`
	RunTimeMs           int64 `json:"runTimeMs"`
	CPUTimeNs           int64 `json:"cpuTimeNs"`
	GCTimeMs            int64 `json:"gcTimeMs"`
	DeserializeMs       int64 `json:"deserializeMs"`
	InputBytes          int64 `json:"inputBytes"`
	InputRecords        int64 `json:"inputRecords"`
	OutputBytes         int64 `json:"outputBytes"`
	OutputRecords       int64 `json:"outputRecords"`
	ShuffleReadBytes    int64 `json:"shuffleReadBytes"`
	ShuffleRemoteBytes  int64 `json:"shuffleRemoteBytes"`
	ShuffleReadRecords  int64 `json:"shuffleReadRecords"`
	ShuffleFetchWaitMs  int64 `json:"shuffleFetchWaitMs"`
	ShuffleWriteBytes   int64 `json:"shuffleWriteBytes"`
	ShuffleWriteRecords int64 `json:"shuffleWriteRecords"`
	MemorySpillBytes    int64 `json:"memorySpillBytes"`
	DiskSpillBytes      int64 `json:"diskSpillBytes"`
	PeakExecutionMemory int64 `json:"peakExecutionMemoryMax"`
}

// Add folds o into t.
func (t *TaskTotals) Add(o TaskTotals) {
	t.Tasks += o.Tasks
	t.Succeeded += o.Succeeded
	t.Failed += o.Failed
	t.Killed += o.Killed
	t.Speculative += o.Speculative
	t.DurationMs += o.DurationMs
	t.RunTimeMs += o.RunTimeMs
	t.CPUTimeNs += o.CPUTimeNs
	t.GCTimeMs += o.GCTimeMs
	t.DeserializeMs += o.DeserializeMs
	t.InputBytes += o.InputBytes
	t.InputRecords += o.InputRecords
	t.OutputBytes += o.OutputBytes
	t.OutputRecords += o.OutputRecords
	t.ShuffleReadBytes += o.ShuffleReadBytes
	t.ShuffleRemoteBytes += o.ShuffleRemoteBytes
	t.ShuffleReadRecords += o.ShuffleReadRecords
	t.ShuffleFetchWaitMs += o.ShuffleFetchWaitMs
	t.ShuffleWriteBytes += o.ShuffleWriteBytes
	t.ShuffleWriteRecords += o.ShuffleWriteRecords
	t.MemorySpillBytes += o.MemorySpillBytes
	t.DiskSpillBytes += o.DiskSpillBytes
	t.PeakExecutionMemory = max(t.PeakExecutionMemory, o.PeakExecutionMemory)
}

// Dist summarises a per-task distribution. P50 and P95 are exact for up to
// 64 tasks and within about 3% above that (fixed log-scale histogram).
type Dist struct {
	Count int64 `json:"count"`
	Sum   int64 `json:"sum"`
	Min   int64 `json:"min"`
	Max   int64 `json:"max"`
	P50   int64 `json:"p50"`
	P95   int64 `json:"p95"`
}

// TaskRef identifies one notable task.
type TaskRef struct {
	TaskID           int64  `json:"taskId"`
	Index            int    `json:"index"`
	Attempt          int    `json:"attempt"`
	ExecutorID       string `json:"executorId"`
	Host             string `json:"host"`
	DurationMs       int64  `json:"durationMs"`
	InputBytes       int64  `json:"inputBytes"`
	ShuffleReadBytes int64  `json:"shuffleReadBytes"`
	RecordsRead      int64  `json:"recordsRead"` // input plus shuffle rows
	Source           Source `json:"source"`
}

// TaskFailure groups failed tasks of a stage by reason.
type TaskFailure struct {
	Kind      string   `json:"kind"` // Spark's Task End Reason, e.g. ExceptionFailure
	Message   string   `json:"message"`
	Count     int64    `json:"count"`
	Executors []string `json:"executors"`
	Source    Source   `json:"source"`
}

// Stage is one stage attempt.
type Stage struct {
	ID            int           `json:"id"`
	Attempt       int           `json:"attempt"`
	Name          string        `json:"name"`
	NumTasks      int           `json:"numTasks"`
	JobIDs        []int         `json:"jobIds"`
	ParentIDs     []int         `json:"parentIds,omitempty"`
	Submitted     time.Time     `json:"submitted,omitzero"`
	Completed     time.Time     `json:"completed,omitzero"`
	Status        string        `json:"status"`
	FailureReason string        `json:"failureReason,omitempty"`
	Totals        TaskTotals    `json:"totals"`
	TaskDuration  Dist          `json:"taskDurationMs"`
	TaskInput     Dist          `json:"taskInputBytes"`
	TaskShuffle   Dist          `json:"taskShuffleReadBytes"`
	TaskRecords   Dist          `json:"taskRecordsRead"` // input plus shuffle rows per task
	Slowest       *TaskRef      `json:"slowestTask,omitempty"`
	Failures      []TaskFailure `json:"failures,omitempty"`
	CachedRDDs    []int         `json:"cachedRdds,omitempty"`
	Source        Source        `json:"source"`
	TaskSource    Source        `json:"taskSource,omitzero"`
	EndSource     Source        `json:"endSource,omitzero"`
}

// DurationMs is the wall-clock time of the stage attempt, or 0 if unknown.
func (s *Stage) DurationMs() int64 {
	if s.Submitted.IsZero() || s.Completed.IsZero() {
		return 0
	}
	return s.Completed.Sub(s.Submitted).Milliseconds()
}

// Job is one Spark job (an action such as count or write).
type Job struct {
	ID             int       `json:"id"`
	Name           string    `json:"name"`
	Description    string    `json:"description,omitempty"`
	Group          string    `json:"group,omitempty"`
	StageIDs       []int     `json:"stageIds"`
	SQLExecutionID *int64    `json:"sqlExecutionId,omitempty"`
	Submitted      time.Time `json:"submitted,omitzero"`
	Completed      time.Time `json:"completed,omitzero"`
	Status         string    `json:"status"`
	Failure        string    `json:"failure,omitempty"`
	Source         Source    `json:"source"`
	EndSource      Source    `json:"endSource,omitzero"`
}

// DurationMs is the wall-clock time of the job, or 0 if unknown.
func (j *Job) DurationMs() int64 {
	if j.Submitted.IsZero() || j.Completed.IsZero() {
		return 0
	}
	return j.Completed.Sub(j.Submitted).Milliseconds()
}

// DataRef is a table or path read or written.
type DataRef struct {
	Kind   string `json:"kind"`   // table or path
	Access string `json:"access"` // read, write or create
	Name   string `json:"name"`
	Format string `json:"format,omitempty"`
	Source Source `json:"source"`
}

// SQLQuery is one SQL execution (DataFrame actions count too).
type SQLQuery struct {
	ID            int64     `json:"id"`
	Description   string    `json:"description"`
	Start         time.Time `json:"start,omitzero"`
	End           time.Time `json:"end,omitzero"`
	Error         string    `json:"error,omitempty"`
	Plan          string    `json:"plan,omitempty"`
	PlanTruncated bool      `json:"planTruncated,omitempty"`
	Reads         []DataRef `json:"reads,omitempty"`
	Writes        []DataRef `json:"writes,omitempty"`
	JobIDs        []int     `json:"jobIds,omitempty"`
	Source        Source    `json:"source"`
}

// CachedRDD is an RDD or DataFrame that was persisted.
type CachedRDD struct {
	ID           int    `json:"id"`
	Name         string `json:"name"`
	StorageLevel string `json:"storageLevel"`
	Partitions   int    `json:"partitions"`
	FirstStage   int    `json:"firstStage"`
	Unpersisted  bool   `json:"unpersisted"`
	// Sizes are only known when spark.eventLog.logBlockUpdates.enabled was on.
	MemoryBytes int64  `json:"memoryBytes"`
	DiskBytes   int64  `json:"diskBytes"`
	SizeKnown   bool   `json:"sizeKnown"`
	Source      Source `json:"source"`
}

// ConfigEntry is one effective setting.
type ConfigEntry struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	Group    string `json:"group"`  // Spark, Hadoop, Hive, HBase, JVM, System
	Origin   string `json:"origin"` // the event log section it came from
	Redacted bool   `json:"redacted,omitempty"`
	Source   Source `json:"source"`
}
