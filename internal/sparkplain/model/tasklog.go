package model

import "time"

// TaskLog is one task attempt's story, told by its executor's log: Spark
// 3.5's own lines from Executor (start, end, result, failure, kill),
// TorrentBroadcast (broadcasts read), MemoryStore (blocks cached, memory
// left, blocks dropped), ShuffleBlockFetcherIterator (shuffle blocks
// read), UnsafeExternalSorter and Spillable (spills), HadoopRDD,
// NewHadoopRDD and FileScanRDD (input), and SparkHadoopMapRedUtil (output
// commits). A line is the task's when its thread names the task (a layout
// that prints the thread: Spark names it "Executor task launch worker for
// task <partition>.<attempt> in stage <stage>.<attempt> (TID <tid>)"),
// when the line names the task's TID, or when the task was the only one
// its executor was running. Rows read and CPU time are only in the event
// log.
//
// The same type adds up an executor's lines that could not be told
// apart (LogFile.Untied), with TaskID -1.
type TaskLog struct {
	TaskID       int64     `json:"taskId"`
	Partition    int       `json:"partition"`
	Attempt      int       `json:"attempt"`
	Stage        int       `json:"stage"`
	StageAttempt int       `json:"stageAttempt"`
	Executor     string    `json:"executor,omitempty"`
	Host         string    `json:"host,omitempty"`
	Start        time.Time `json:"start,omitzero"`
	End          time.Time `json:"end,omitzero"`
	// Outcome is "finished", "failed", "killed", or empty when the log
	// holds no end for it.
	Outcome     string `json:"outcome,omitempty"`
	ResultBytes int64  `json:"resultBytes,omitempty"`
	ResultVia   string `json:"resultVia,omitempty"` // "driver", or "BlockManager" for a large result
	Error       string `json:"error,omitempty"`     // the failure's or kill's message, redacted
	// Broadcast variables read (their estimated size, and the time to
	// read them).
	Broadcasts     int   `json:"broadcasts,omitempty"`
	BroadcastBytes int64 `json:"broadcastBytes,omitempty"`
	BroadcastMs    int64 `json:"broadcastMs,omitempty"`
	// Shuffle blocks read: local covers local, host-local and push-merged
	// blocks, remote came over the network. FetchStartMs is the time
	// Spark took to start the remote fetches; the wait for them is only in
	// the event log.
	ShuffleReads       int   `json:"shuffleReads,omitempty"`
	ShuffleBlocks      int   `json:"shuffleBlocks,omitempty"`
	ShuffleBytes       int64 `json:"shuffleBytes,omitempty"`
	ShuffleLocalBytes  int64 `json:"shuffleLocalBytes,omitempty"`
	ShuffleRemoteBytes int64 `json:"shuffleRemoteBytes,omitempty"`
	RemoteBlocks       int   `json:"remoteBlocks,omitempty"`
	RemoteFetches      int   `json:"remoteFetches,omitempty"`
	FetchStartMs       int64 `json:"fetchStartMs,omitempty"`
	// Data spilled from memory to disk (the in-memory size Spark wrote
	// out).
	Spills     int   `json:"spills,omitempty"`
	SpillBytes int64 `json:"spillBytes,omitempty"`
	// Cached blocks it stored in memory (RDD partitions; broadcast pieces
	// count under broadcasts), blocks dropped from memory to make room,
	// blocks that did not fit, and the storage memory free after its last
	// block.
	CachedBlocks int   `json:"cachedBlocks,omitempty"`
	CachedBytes  int64 `json:"cachedBytes,omitempty"`
	Dropped      int   `json:"dropped,omitempty"`
	NotCached    int   `json:"notCached,omitempty"`
	MemoryFree   int64 `json:"memoryFree,omitempty"`
	// Input it read: files or file ranges (InputBytes adds up the
	// ranges' lengths, not bytes read: a columnar file is read in part),
	// and the first one, redacted.
	Inputs     int    `json:"inputs,omitempty"`
	Input      string `json:"input,omitempty"`
	InputBytes int64  `json:"inputBytes,omitempty"`
	// Output commits and their time.
	Commits  int   `json:"commits,omitempty"`
	CommitMs int64 `json:"commitMs,omitempty"`
	// Other warnings and errors logged as its, and the first one.
	Warnings int    `json:"warnings,omitempty"`
	Errors   int    `json:"errors,omitempty"`
	Problem  string `json:"problem,omitempty"`
	Lines    int    `json:"lines"`
	// TiedBy says how its lines were told apart: "thread" (every line
	// names it), or "tid" (its own lines name its TID, and others are its
	// when it ran alone).
	TiedBy string `json:"tiedBy,omitempty"`
	// Source is its first line (the Running line), EndSource its last
	// (Finished, Exception or killed). Steps are what it did, in order,
	// at most MaxTaskSteps, each with its line in Source.File.
	Source    Source     `json:"source"`
	EndSource Source     `json:"endSource,omitzero"`
	Steps     []TaskStep `json:"steps,omitempty"`
	CutSteps  int        `json:"cutSteps,omitempty"`
}

// MaxTaskSteps caps the steps kept per task.
const MaxTaskSteps = 40

// TaskStep is one thing a task did, as its log line says: a kind, the
// sizes and times the line gives, and the line.
type TaskStep struct {
	T     time.Time `json:"t"`
	Kind  string    `json:"kind"`
	Bytes int64     `json:"bytes,omitempty"`
	N     int       `json:"n,omitempty"`
	Ms    int64     `json:"ms,omitempty"`
	Name  string    `json:"name,omitempty"` // a block, broadcast or file, redacted
	Line  int64     `json:"line"`
}

// The kinds of TaskStep.
const (
	StepStart     = "start"     // Running task
	StepBroadcast = "broadcast" // Started reading broadcast variable N (Bytes: its estimated size, N: pieces)
	StepBcastRead = "broadcast-read"
	StepShuffle   = "shuffle"        // Getting N non-empty blocks (Bytes: all, N: blocks)
	StepFetch     = "fetch"          // Started N remote fetches in M ms
	StepSpill     = "spill"          // spilling … of Bytes to disk
	StepCache     = "cache"          // Block rdd_… stored in memory (Bytes; free memory after)
	StepDrop      = "drop"           // After dropping N blocks, free memory is Bytes
	StepNoRoom    = "no-room"        // Not enough space to cache a block in memory
	StepInput     = "input"          // Input split or file read (Bytes: its range's length)
	StepCommit    = "commit"         // Committed. Elapsed time: M ms
	StepProblem   = "problem"        // a warning or error logged as the task's
	StepEnd       = "end"            // Finished task (Bytes: the result's size)
	StepFailed    = "failed"         // Exception in task
	StepKilled    = "killed"         // Executor killed task
	StepBigResult = "result-too-big" // Result is larger than maxResultSize
)

// DurationMs is how long the task ran, from its Running to its end line,
// or 0 when either is missing.
func (t TaskLog) DurationMs() int64 {
	if t.Start.IsZero() || t.End.IsZero() {
		return 0
	}
	return t.End.Sub(t.Start).Milliseconds()
}

// TaskStorySection is what the executors' logs tell of the run's tasks:
// each task attempt's story, and per executor the totals of its tasks'
// lines, those no task could be found for included.
type TaskStorySection struct {
	Coverage Coverage `json:"coverage"`
	Missing  []string `json:"missing,omitempty"`
	// Tasks are every task attempt with a story, in the order they
	// started; Cut counts those past the per-file cap.
	Tasks []TaskLog `json:"tasks"`
	Cut   int       `json:"cut,omitempty"`
	// ByThread and ByTID count the tasks told apart by their thread's
	// name, or by the lines that name their TID.
	ByThread int `json:"byThread"`
	ByTID    int `json:"byTid"`
	// Executors add up each executor's lines; Totals and Untied the run's.
	Executors []TaskStoryExec `json:"executors"`
	Totals    TaskLog         `json:"totals"`
	Untied    TaskLog         `json:"untied"`
}

// TaskStoryExec adds up one executor's (or the driver's) task lines.
type TaskStoryExec struct {
	Executor string  `json:"executor"`
	Host     string  `json:"host,omitempty"`
	Tasks    int     `json:"tasks"`
	Totals   TaskLog `json:"totals"` // every line, tied or not
	Untied   TaskLog `json:"untied"` // the lines no task could be found for
	Source   Source  `json:"source"` // its log file
}

// Add adds o's counts and sizes into t (not its times, steps or text).
func (t *TaskLog) Add(o TaskLog) {
	t.ResultBytes += o.ResultBytes
	t.Broadcasts += o.Broadcasts
	t.BroadcastBytes += o.BroadcastBytes
	t.BroadcastMs += o.BroadcastMs
	t.ShuffleReads += o.ShuffleReads
	t.ShuffleBlocks += o.ShuffleBlocks
	t.ShuffleBytes += o.ShuffleBytes
	t.ShuffleLocalBytes += o.ShuffleLocalBytes
	t.ShuffleRemoteBytes += o.ShuffleRemoteBytes
	t.RemoteBlocks += o.RemoteBlocks
	t.RemoteFetches += o.RemoteFetches
	t.FetchStartMs += o.FetchStartMs
	t.Spills += o.Spills
	t.SpillBytes += o.SpillBytes
	t.CachedBlocks += o.CachedBlocks
	t.CachedBytes += o.CachedBytes
	t.Dropped += o.Dropped
	t.NotCached += o.NotCached
	t.Inputs += o.Inputs
	t.InputBytes += o.InputBytes
	t.Commits += o.Commits
	t.CommitMs += o.CommitMs
	t.Warnings += o.Warnings
	t.Errors += o.Errors
	t.Lines += o.Lines
}
