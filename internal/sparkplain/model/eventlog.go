package model

import (
	"strings"
	"time"
)

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

	Exclusions   []Exclusion   `json:"exclusions,omitempty"`
	RunningTasks []RunningTask `json:"runningTasks,omitempty"` // only in logs that end before their tasks
	// RunningCapped is true when more tasks were running than are listed.
	RunningCapped bool        `json:"runningCapped,omitempty"`
	BlockKinds    []BlockKind `json:"blockKinds,omitempty"`
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
	// DriverLogs are the driver's stdout and stderr links, and
	// DriverAttributes its YARN container attributes (redacted).
	DriverLogs       map[string]string `json:"driverLogs,omitempty"`
	DriverAttributes map[string]string `json:"driverAttributes,omitempty"`
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
	// Other resources the profile requests, such as gpu, with amounts.
	ExecutorOther map[string]int64   `json:"executorOther,omitempty"`
	TaskOther     map[string]float64 `json:"taskOther,omitempty"`
	Source        Source             `json:"source"`
}

// PeakMemory holds the highest sampled executor memory metrics, in bytes.
// Spark samples these on heartbeats and at task end, so short spikes can be missed.
type PeakMemory struct {
	JVMHeap          int64 `json:"jvmHeap"`
	JVMOffHeap       int64 `json:"jvmOffHeap"`
	OnHeapExecution  int64 `json:"onHeapExecution"`
	OnHeapStorage    int64 `json:"onHeapStorage"`
	OffHeapExecution int64 `json:"offHeapExecution"`
	OffHeapStorage   int64 `json:"offHeapStorage"`
	DirectPool       int64 `json:"directPool"`
	MappedPool       int64 `json:"mappedPool"`
	ProcessJVMRSS    int64 `json:"processTreeJvmRss"`
	ProcessPythonRSS int64 `json:"processTreePythonRss"`
	ProcessOtherRSS  int64 `json:"processTreeOtherRss"`
	TotalGCTimeMs    int64 `json:"totalGcTimeMs"`
	// JVM garbage collector counters (running totals) and unified memory.
	MinorGCCount     int64  `json:"minorGcCount"`
	MinorGCTimeMs    int64  `json:"minorGcTimeMs"`
	MajorGCCount     int64  `json:"majorGcCount"`
	MajorGCTimeMs    int64  `json:"majorGcTimeMs"`
	OnHeapUnified    int64  `json:"onHeapUnified"`
	OffHeapUnified   int64  `json:"offHeapUnified"`
	ProcessJVMVMem   int64  `json:"processTreeJvmVmem"`
	ProcessPyVMem    int64  `json:"processTreePythonVmem"`
	ProcessOtherVMem int64  `json:"processTreeOtherVmem"`
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
	p.MinorGCCount = max(p.MinorGCCount, o.MinorGCCount)
	p.MinorGCTimeMs = max(p.MinorGCTimeMs, o.MinorGCTimeMs)
	p.MajorGCCount = max(p.MajorGCCount, o.MajorGCCount)
	p.MajorGCTimeMs = max(p.MajorGCTimeMs, o.MajorGCTimeMs)
	p.OnHeapUnified = max(p.OnHeapUnified, o.OnHeapUnified)
	p.OffHeapUnified = max(p.OffHeapUnified, o.OffHeapUnified)
	p.ProcessJVMVMem = max(p.ProcessJVMVMem, o.ProcessJVMVMem)
	p.ProcessPyVMem = max(p.ProcessPyVMem, o.ProcessPyVMem)
	p.ProcessOtherVMem = max(p.ProcessOtherVMem, o.ProcessOtherVMem)
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
	// Launch detail from SparkListenerExecutorAdded. StartupMs is from the
	// resource request to registration with the driver, when both are known.
	Requested  time.Time         `json:"requested,omitzero"`
	Registered time.Time         `json:"registered,omitzero"`
	StartupMs  int64             `json:"startupMs,omitempty"`
	LogURLs    map[string]string `json:"logUrls,omitempty"`    // stdout, stderr
	Attributes map[string]string `json:"attributes,omitempty"` // YARN container attributes, redacted
	Resources  map[string]int    `json:"resources,omitempty"`  // extra resources (such as gpu) and how many addresses each
	// BlockManagerRemoved is when the executor's block manager left, which can
	// differ from the executor's own removal.
	BlockManagerRemoved time.Time `json:"blockManagerRemoved,omitzero"`
}

// Exclusion is Spark's excludeOnFailure (formerly blacklist) taking an
// executor or a whole node out of scheduling, for one stage or the whole
// application. Spark writes each under both names; they are merged.
type Exclusion struct {
	Kind         string    `json:"kind"`  // executor or node
	Scope        string    `json:"scope"` // application or stage
	Target       string    `json:"target"`
	StageID      int       `json:"stageId,omitempty"`
	StageAttempt int       `json:"stageAttempt,omitempty"`
	Time         time.Time `json:"time,omitzero"`
	Failures     int       `json:"failures"` // failed tasks (executor) or excluded executors (node)
	Lifted       time.Time `json:"lifted,omitzero"`
	Source       Source    `json:"source"`
	LiftedSource Source    `json:"liftedSource,omitzero"`
}

// RunningTask is a task that started but had not ended when the log ended.
type RunningTask struct {
	TaskID       int64     `json:"taskId"`
	StageID      int       `json:"stageId"`
	StageAttempt int       `json:"stageAttempt"`
	Index        int       `json:"index"`
	Partition    int       `json:"partition"`
	Attempt      int       `json:"attempt"`
	ExecutorID   string    `json:"executorId"`
	Host         string    `json:"host"`
	Launched     time.Time `json:"launched,omitzero"`
	Locality     string    `json:"locality,omitempty"`
	Speculative  bool      `json:"speculative,omitempty"`
	Source       Source    `json:"source"`
}

// BlockKind totals the storage-block updates of one kind of block (RDD
// partitions, broadcast pieces, large task results) when block updates
// were logged.
type BlockKind struct {
	Kind      string `json:"kind"`
	Updates   int64  `json:"updates"`
	Blocks    int64  `json:"blocks"`
	MaxMemory int64  `json:"maxMemoryBytes"` // largest in-memory size seen for one block
	MaxDisk   int64  `json:"maxDiskBytes"`
}

// TaskTotals sums task metrics. Times are milliseconds except CPU time (ns),
// which is how Spark records them.
type TaskTotals struct {
	Tasks               int64 `json:"tasks"`
	Succeeded           int64 `json:"succeeded"`
	Failed              int64 `json:"failed"`
	Killed              int64 `json:"killed"`
	Speculative         int64 `json:"speculative"`
	SpeculativeWon      int64 `json:"speculativeWon"` // speculative attempts that succeeded
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

	// Result handling. SchedulerDelayMs is what the Spark UI calls scheduler
	// delay: task duration not spent deserializing, running, serializing the
	// result or fetching it (launch overhead and waiting on the driver).
	ResultSizeBytes       int64 `json:"resultSizeBytes"`
	ResultSerializationMs int64 `json:"resultSerializationMs"`
	GettingResultMs       int64 `json:"gettingResultMs"`
	SchedulerDelayMs      int64 `json:"schedulerDelayMs"`
	DeserializeCPUNs      int64 `json:"deserializeCpuNs"`
	// Shuffle detail.
	ShuffleWriteTimeNs       int64 `json:"shuffleWriteTimeNs"`
	ShuffleLocalBlocks       int64 `json:"shuffleLocalBlocks"`
	ShuffleRemoteBlocks      int64 `json:"shuffleRemoteBlocks"`
	ShuffleRemoteToDiskBytes int64 `json:"shuffleRemoteToDiskBytes"`
	ShuffleRemoteReqsMs      int64 `json:"shuffleRemoteRequestsMs"`
	// Push-based shuffle: blocks and bytes read from merged shuffle files.
	PushMergedLocalBlocks  int64 `json:"pushMergedLocalBlocks"`
	PushMergedLocalBytes   int64 `json:"pushMergedLocalBytes"`
	PushMergedLocalChunks  int64 `json:"pushMergedLocalChunks"`
	PushMergedRemoteBlocks int64 `json:"pushMergedRemoteBlocks"`
	PushMergedRemoteBytes  int64 `json:"pushMergedRemoteBytes"`
	PushMergedRemoteChunks int64 `json:"pushMergedRemoteChunks"`
	PushMergedRemoteReqsMs int64 `json:"pushMergedRemoteRequestsMs"`
	PushFallbacks          int64 `json:"pushFallbacks"`
	PushCorruptChunks      int64 `json:"pushCorruptChunks"`
	// Cache blocks the tasks wrote (only tracked when
	// spark.taskMetrics.trackUpdatedBlockStatuses is on).
	UpdatedBlocks     int64 `json:"updatedBlocks"`
	UpdatedBlockBytes int64 `json:"updatedBlockBytes"`
	// Data locality of the tasks.
	LocalityProcess int64 `json:"localityProcess"`
	LocalityNode    int64 `json:"localityNode"`
	LocalityRack    int64 `json:"localityRack"`
	LocalityAny     int64 `json:"localityAny"`
	LocalityNoPref  int64 `json:"localityNoPref"`
}

// Add folds o into t.
func (t *TaskTotals) Add(o TaskTotals) {
	t.Tasks += o.Tasks
	t.Succeeded += o.Succeeded
	t.Failed += o.Failed
	t.Killed += o.Killed
	t.Speculative += o.Speculative
	t.SpeculativeWon += o.SpeculativeWon
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
	t.ResultSizeBytes += o.ResultSizeBytes
	t.ResultSerializationMs += o.ResultSerializationMs
	t.GettingResultMs += o.GettingResultMs
	t.SchedulerDelayMs += o.SchedulerDelayMs
	t.DeserializeCPUNs += o.DeserializeCPUNs
	t.ShuffleWriteTimeNs += o.ShuffleWriteTimeNs
	t.ShuffleLocalBlocks += o.ShuffleLocalBlocks
	t.ShuffleRemoteBlocks += o.ShuffleRemoteBlocks
	t.ShuffleRemoteToDiskBytes += o.ShuffleRemoteToDiskBytes
	t.ShuffleRemoteReqsMs += o.ShuffleRemoteReqsMs
	t.PushMergedLocalBlocks += o.PushMergedLocalBlocks
	t.PushMergedLocalBytes += o.PushMergedLocalBytes
	t.PushMergedLocalChunks += o.PushMergedLocalChunks
	t.PushMergedRemoteBlocks += o.PushMergedRemoteBlocks
	t.PushMergedRemoteBytes += o.PushMergedRemoteBytes
	t.PushMergedRemoteChunks += o.PushMergedRemoteChunks
	t.PushMergedRemoteReqsMs += o.PushMergedRemoteReqsMs
	t.PushFallbacks += o.PushFallbacks
	t.PushCorruptChunks += o.PushCorruptChunks
	t.UpdatedBlocks += o.UpdatedBlocks
	t.UpdatedBlockBytes += o.UpdatedBlockBytes
	t.LocalityProcess += o.LocalityProcess
	t.LocalityNode += o.LocalityNode
	t.LocalityRack += o.LocalityRack
	t.LocalityAny += o.LocalityAny
	t.LocalityNoPref += o.LocalityNoPref
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
	// StackTrace is the first full stack trace logged for this reason,
	// redacted and capped.
	StackTrace string `json:"stackTrace,omitempty"`
	// ExitCausedByApp says, for a lost executor, whether Spark blamed the
	// application for the exit (nil when Spark did not say).
	ExitCausedByApp *bool `json:"exitCausedByApp,omitempty"`
}

// Stage is one stage attempt.
type Stage struct {
	ID            int        `json:"id"`
	Attempt       int        `json:"attempt"`
	Name          string     `json:"name"`
	NumTasks      int        `json:"numTasks"`
	JobIDs        []int      `json:"jobIds"`
	ParentIDs     []int      `json:"parentIds,omitempty"`
	Submitted     time.Time  `json:"submitted,omitzero"`
	Completed     time.Time  `json:"completed,omitzero"`
	Status        string     `json:"status"`
	FailureReason string     `json:"failureReason,omitempty"`
	Totals        TaskTotals `json:"totals"`
	TaskType      string     `json:"taskType,omitempty"` // ResultTask or ShuffleMapTask
	// What the stage computes: its RDDs (capped), the long call site, its
	// resource profile, and push-based shuffle settings.
	RDDs []StageRDD `json:"rdds,omitempty"`
	// Code is where in the application the stage came from, innermost
	// first; empty when Spark recorded no user code.
	Code            []CodeLocation    `json:"code,omitempty"`
	RDDsCapped      bool              `json:"rddsCapped,omitempty"`
	Details         string            `json:"details,omitempty"`
	ResourceProfile int               `json:"resourceProfile,omitempty"`
	ShufflePush     bool              `json:"shufflePush,omitempty"`
	PushMergers     int               `json:"pushMergers,omitempty"`
	Properties      map[string]string `json:"properties,omitempty"` // only where they differ from the job's
	TaskDuration    Dist              `json:"taskDurationMs"`
	TaskInput       Dist              `json:"taskInputBytes"`
	TaskShuffle     Dist              `json:"taskShuffleReadBytes"`
	TaskRecords     Dist              `json:"taskRecordsRead"` // input plus shuffle rows per task
	Slowest         *TaskRef          `json:"slowestTask,omitempty"`
	Failures        []TaskFailure     `json:"failures,omitempty"`
	CachedRDDs      []int             `json:"cachedRdds,omitempty"`
	Source          Source            `json:"source"`
	TaskSource      Source            `json:"taskSource,omitzero"`
	EndSource       Source            `json:"endSource,omitzero"`
	// ScanTasks are the successful tasks of a stage that read with
	// newAPIHadoopRDD, one per partition (the first to succeed), up to
	// MaxScanTasks: for a TableInputFormat scan, partition i read the
	// i-th region in key order. ScanTasksCapped says some were left out.
	ScanTasks       []ScanTask `json:"-"`
	ScanTasksCapped bool       `json:"-"`
}

// MaxScanTasks caps the tasks kept per scan stage.
const MaxScanTasks = 20000

// ScanTask is one task of a newAPIHadoopRDD stage: its partition, where it
// ran, how long, and the rows it read.
type ScanTask struct {
	Index      int       `json:"index"`
	TaskID     int64     `json:"taskId"`
	Attempt    int       `json:"attempt"`
	ExecutorID string    `json:"executorId"`
	Host       string    `json:"host"`
	Launch     time.Time `json:"launch,omitzero"`
	DurationMs int64     `json:"durationMs"`
	RunTimeMs  int64     `json:"runTimeMs"`
	Rows       int64     `json:"rows"`
	Source     Source    `json:"source"`
}

// IsHadoopScan reports whether the stage reads with newAPIHadoopRDD, as a
// TableInputFormat scan does.
func (s *Stage) IsHadoopScan() bool {
	for _, r := range s.RDDs {
		if r.Name == "NewHadoopRDD" && strings.HasPrefix(r.Callsite, "newAPIHadoopRDD") {
			return true
		}
	}
	return false
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
	FailureStack   string    `json:"failureStack,omitempty"` // stack trace of the job's exception, redacted
	// Properties are the job's local properties that differ from the
	// application's settings (scheduler pool, job group, settings changed in
	// the session), redacted.
	Properties map[string]string `json:"properties,omitempty"`
	Source     Source            `json:"source"`
	EndSource  Source            `json:"endSource,omitzero"`
	Code       []CodeLocation    `json:"code,omitempty"` // where in the application it ran
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
	// RootID is the query this one ran inside (a sub-query or nested
	// command); nil for a top-level query.
	RootID  *int64   `json:"rootId,omitempty"`
	JobTags []string `json:"jobTags,omitempty"`
	// Details is the long call site; ModifiedConfigs the session settings
	// in force that differ from defaults, redacted.
	Details         string            `json:"details,omitempty"`
	ModifiedConfigs map[string]string `json:"modifiedConfigs,omitempty"`
	// AdaptiveMetrics are metrics adaptive execution registered for the
	// query after planning; the explorer resolves their values.
	AdaptiveMetrics []PlanMetric `json:"adaptiveMetrics,omitempty"`
	// Optimizer is EMR's report of optimizer time per rule.
	Optimizer *OptimizerStats `json:"optimizer,omitempty"`
	Code      []CodeLocation  `json:"code,omitempty"`
}

// PlanMetric names one SQL metric by its accumulator.
type PlanMetric struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	AccumulatorID int64  `json:"accumulatorId"`
}

// OptimizerStats is EMR's per-query optimizer report
// (SparkListenerQueryExecutionMetrics): time and runs per rule.
type OptimizerStats struct {
	TotalNs     int64             `json:"totalNs"`
	RulesRun    int               `json:"rulesRun"`
	RulesUseful int               `json:"rulesUseful"` // rules that changed the plan at least once
	Rules       []OptimizerRule   `json:"rules"`       // slowest first, capped
	RulesCapped bool              `json:"rulesCapped,omitempty"`
	Other       map[string]string `json:"other,omitempty"` // counters, timers and stats, when EMR fills them
	Source      Source            `json:"source"`
}

// OptimizerRule is one optimizer rule's work on a query.
type OptimizerRule struct {
	Name            string `json:"name"`
	TimeNs          int64  `json:"timeNs"`
	Runs            int64  `json:"runs"`
	EffectiveRuns   int64  `json:"effectiveRuns"`
	EffectiveTimeNs int64  `json:"effectiveTimeNs"`
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
	// Placement: where the cached partitions were at the end, per executor.
	Executors []CachedPlacement `json:"executors,omitempty"`
}

// CachedPlacement is one executor's share of a cached RDD.
type CachedPlacement struct {
	ExecutorID   string `json:"executorId"`
	Host         string `json:"host"`
	Blocks       int    `json:"blocks"`
	MemoryBytes  int64  `json:"memoryBytes"`
	DiskBytes    int64  `json:"diskBytes"`
	StorageLevel string `json:"storageLevel"`
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

// StageRDD is one RDD a stage computes, as the Spark UI's stage graph shows
// it: the operation (scope) that made it, its parents, and where in the
// code it came from.
type StageRDD struct {
	ID               int    `json:"id"`
	Name             string `json:"name"`
	Operation        string `json:"operation,omitempty"` // scope name, e.g. Exchange or WholeStageCodegen (1)
	OperationID      string `json:"operationId,omitempty"`
	Callsite         string `json:"callsite,omitempty"`
	Parents          []int  `json:"parents,omitempty"`
	Partitions       int    `json:"partitions"`
	CachedPartitions int    `json:"cachedPartitions,omitempty"`
	StorageLevel     string `json:"storageLevel,omitempty"`
	Barrier          bool   `json:"barrier,omitempty"`
	Deterministic    string `json:"deterministic,omitempty"` // DETERMINATE, UNORDERED or INDETERMINATE
}

// CodeLocation is a place in the application's code that Spark recorded:
// a file and line, with the function (from a JVM stack frame) or the action
// (from a short call site such as "collect at etl.py:32").
type CodeLocation struct {
	File     string `json:"file"`
	Line     int    `json:"line"`
	Function string `json:"function,omitempty"`
	Action   string `json:"action,omitempty"`
}
