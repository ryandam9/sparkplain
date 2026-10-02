package model

import (
	"strings"
	"time"
)

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
	// Output files it wrote (EMRFS logs each one as it closes it), the
	// bytes it uploaded for them, and the first one, redacted.
	Outputs     int    `json:"outputs,omitempty"`
	Output      string `json:"output,omitempty"`
	OutputBytes int64  `json:"outputBytes,omitempty"`
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

// MaxTaskSteps caps the steps kept per task, and MaxUntiedSteps those of
// an executor's lines no task could be found for.
const (
	MaxTaskSteps   = 40
	MaxUntiedSteps = 20_000
)

// TaskStep is one thing a task did, as its log line says: a kind, the
// sizes and times the line gives, and the line.
type TaskStep struct {
	T     time.Time `json:"t"`
	Kind  string    `json:"kind"`
	Bytes int64     `json:"bytes,omitempty"`
	N     int       `json:"n,omitempty"`
	Ms    int64     `json:"ms,omitempty"`
	Name  string    `json:"name,omitempty"` // a block, broadcast or file, redacted
	// Remote is a shuffle read's bytes over the network (the rest came
	// from the task's own node); Free the storage memory free after a
	// block was cached or blocks were dropped.
	Remote int64 `json:"remote,omitempty"`
	Free   int64 `json:"free,omitempty"`
	Line   int64 `json:"line"`
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
	StepOutput    = "output"         // an output file written (closed)
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
	t.Outputs += o.Outputs
	t.OutputBytes += o.OutputBytes
	t.Commits += o.Commits
	t.CommitMs += o.CommitMs
	t.Warnings += o.Warnings
	t.Errors += o.Errors
	t.Lines += o.Lines
}

// StageData is a folder or table one stage attempt read or wrote: from
// its executors' logs (the files and file ranges its tasks read, the files
// they wrote), its SQL plan, its RDDs' names, or its HBase scan or write.
type StageData struct {
	Stage        int    `json:"stage"`
	StageAttempt int    `json:"stageAttempt"`
	Access       string `json:"access"` // read or write
	// Kind is "path" (a folder: a file's path loses its own name, its
	// partition folders such as year=2024 and any staging folder), "table",
	// or "hbase" (an HBase table).
	Kind   string `json:"kind"`
	Name   string `json:"name"`
	Format string `json:"format,omitempty"`
	// Parts counts the files, file ranges or HBase regions the logs name,
	// Sized those whose size they give, adding up to Bytes: for a read, a
	// range's length (a columnar file is read in part); for a write, the
	// bytes uploaded.
	Parts int   `json:"parts,omitempty"`
	Sized int   `json:"sized,omitempty"`
	Bytes int64 `json:"bytes,omitempty"`
	// Paths are the files in the folder a SQL plan named, when it named
	// files (up to MaxDataPaths).
	Paths []string `json:"paths,omitempty"`
	// From says how it is known: "executor logs", "SQL plan", "RDD",
	// "HBase"; Source is the first line that says so.
	From   []string `json:"from"`
	Source Source   `json:"source"`
}

// MaxDataPaths caps StageData.Paths.
const MaxDataPaths = 10

// Data kinds and accesses of StageData.
const (
	DataPath  = "path"
	DataTable = "table"
	DataHBase = "hbase"
	DataRead  = "read"
	DataWrite = "write"
)

// DataFolder is the folder a file's data belongs to: its path without the
// file's own name, any staging or temporary folder (a name that starts
// with "." or "_", such as _temporary or .emrfs_staging_…), or partition
// folders (name=value) at its end.
func DataFolder(path string) string {
	start := 0
	if i := strings.Index(path, "://"); i >= 0 {
		start = i + 3
		if j := strings.IndexByte(path[start:], '/'); j >= 0 {
			start += j + 1
		} else {
			return path
		}
	}
	segs := strings.Split(path[start:], "/")
	segs = segs[:len(segs)-1] // the file's own name
	for i, s := range segs {
		if strings.HasPrefix(s, ".") || strings.HasPrefix(s, "_") {
			segs = segs[:i]
			break
		}
	}
	for len(segs) > 0 && (segs[len(segs)-1] == "" || strings.Contains(segs[len(segs)-1], "=")) {
		segs = segs[:len(segs)-1]
	}
	if dir := strings.Join(segs, "/"); dir != "" {
		return path[:start] + dir
	}
	return strings.TrimSuffix(path[:start], "/")
}
