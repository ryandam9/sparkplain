package yarnlog

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/redact"
)

// A task's own lines in an executor's log, as Spark 3.5.1 writes them
// (checked against its source and the fixtures). Sizes are
// Utils.bytesToString's ("3.0 KiB"), times Utils.getUsedTimeNs's ("12 ms").
var (
	tlRunningRE   = regexp.MustCompile(`^Running task (\d+)\.(\d+) in stage (\d+)\.(\d+) \(TID (\d+)\)`)
	tlFinishedRE  = regexp.MustCompile(`^Finished task \S+ in stage \S+ \(TID (\d+)\)\. (\d+) bytes result sent (to driver|via BlockManager)`)
	tlTooBigRE    = regexp.MustCompile(`^Finished task \S+ in stage \S+ \(TID (\d+)\)\. Result is larger than maxResultSize \((.+?) > (.+?)\)`)
	tlExcRE       = regexp.MustCompile(`^Exception in task \S+ in stage \S+ \(TID (\d+)\)(?:: (.*))?$`)
	tlKilledRE    = regexp.MustCompile(`^Executor (?:interrupted and )?killed task \S+ in stage \S+ \(TID (\d+)\), reason: (.*)$`)
	tlBcastRE     = regexp.MustCompile(`^Started reading broadcast variable (\d+) with (\d+) pieces \(estimated total size (.+?)\)`)
	tlBcastTookRE = regexp.MustCompile(`^Reading broadcast variable (\d+) took (\d+) ms`)
	// EMR adds the actual size of a serialized block: "(estimated size
	// 4.0 KiB, actual size: 4.0 KiB, free 1.1 GiB)".
	tlStoredRE  = regexp.MustCompile(`^Block (\S+) stored as (?:values|bytes) in memory \(estimated size (.+?), (?:actual size: .+?, )?free (.+?)\)`)
	tlDroppedRE = regexp.MustCompile(`^After dropping (\d+) blocks, free memory is (.+)$`)
	tlNoRoomRE  = regexp.MustCompile(`^Not enough space to cache (\S+) in memory! \(computed (.+?) so far\)`)
	tlShuffleRE = regexp.MustCompile(`^Getting (\d+) \((.+?)\) non-empty blocks including (\d+) \((.+?)\) local and (\d+) \((.+?)\) host-local and (\d+) \((.+?)\) push-merged-local and (\d+) \((.+?)\) remote blocks`)
	tlFetchRE   = regexp.MustCompile(`^Started (\d+) remote fetches in (\d+) ms`)
	tlSpillRE   = regexp.MustCompile(`spilling (?:sort data|in-memory map) of (.+?) to disk`)
	// FileScanRDD: Spark's "Reading File path: …, range: 0-1024, …", and
	// EMR's "TID: 12 - Reading current file: path: …, range: 0-1024, …".
	tlFileRE   = regexp.MustCompile(`^(?:TID: (\d+) - )?Reading (?:File|current file:) path: (.*?), range: (\d+)-(\d+),`)
	tlSplitRE  = regexp.MustCompile(`^Input split: (.*):(\d+)\+(\d+)$`) // a FileSplit: path:start+length
	tlRegionRE = regexp.MustCompile(`^Input split: Split\(tablename=([\w:.-]+), .*\bregionname=([^,)]+)`)
	tlCommitRE = regexp.MustCompile(`: Committed\. Elapsed time: (\d+) ms\.`)

	// What EMRFS logs as a task writes to S3 (EMR 7.3, checked against the
	// fixtures): each file as the task closes it, each part as it uploads
	// it (a file's first part is "partNum 1"), and, with EMR's optimized
	// committer, the folder each task's files are published to, with the
	// task's attempt ID, whose last number is the task's TID (Spark makes
	// it so).
	tlCloseRE   = regexp.MustCompile(`^close closed:false (\S+)$`)
	tlUploadRE  = regexp.MustCompile(`^uploadPart: partNum (\d+) of '([^']+)' from local file '[^']*', (\d+) bytes in `)
	tlPublishRE = regexp.MustCompile(`^Publishing staging directory at (\S+) named \S*_(\d+)$`)
)

// maxStageData caps the folders and tables kept per log file.
const maxStageData = 5000

// maxTaskLogs caps the tasks kept per log file; lines of tasks past it
// add to the file's untied totals.
const maxTaskLogs = 200_000

// taskStory folds a line into the story of the task it belongs to: the
// one its thread names, the one it names by TID, or, in a layout that
// prints no thread, the only task its executor was running. Lines that
// cannot be told apart add to the file's untied totals.
func (c *classifier) taskStory(h header) {
	lg := shortLogger(h.logger)
	msg := h.msg
	problem := h.level == "WARN" || h.level == "ERROR" || h.level == "FATAL"
	tid := int64(-1)
	step := model.TaskStep{T: h.time, Line: c.n}
	var apply func(t *model.TaskLog)
	// data is the folder or table the line says was read or written, and
	// quiet a line that adds to a task's totals without being a step.
	var data *model.StageData
	quiet := false
	switch lg {
	case "Executor":
		switch {
		case strings.HasPrefix(msg, "Running task "):
			m := tlRunningRE.FindStringSubmatch(msg)
			if m == nil {
				return
			}
			tid = atoi64(m[5])
			t := c.task(tid, h)
			t.Partition, t.Attempt, t.Stage, t.StageAttempt = atoi(m[1]), atoi(m[2]), atoi(m[3]), atoi(m[4])
			if t.Start.IsZero() {
				t.Start, t.Source = h.time, model.Source{File: c.res.Name, Line: c.n}
			}
			if c.running == nil {
				c.running = map[int64]bool{}
			}
			c.running[tid] = true
			step.Kind = model.StepStart
			addStep(t, step)
			t.Lines++
			return
		case strings.HasPrefix(msg, "Finished task "):
			if m := tlFinishedRE.FindStringSubmatch(msg); m != nil {
				tid = atoi64(m[1])
				step.Kind, step.Bytes = model.StepEnd, atoi64(m[2])
				apply = func(t *model.TaskLog) {
					t.ResultBytes, t.ResultVia, t.Outcome = step.Bytes, strings.TrimPrefix(strings.TrimPrefix(m[3], "to "), "via "), "finished"
				}
			} else if m := tlTooBigRE.FindStringSubmatch(msg); m != nil {
				tid = atoi64(m[1])
				step.Kind, step.Bytes = model.StepBigResult, SparkBytes(m[2])
				apply = func(t *model.TaskLog) {
					t.ResultBytes, t.ResultVia, t.Outcome = step.Bytes, "dropped", "finished"
					t.Error = clip(redact.Text(msg), maxDetailLine)
				}
			} else {
				return
			}
		case strings.HasPrefix(msg, "Exception in task "):
			m := tlExcRE.FindStringSubmatch(msg)
			if m == nil {
				return
			}
			tid, step.Kind = atoi64(m[1]), model.StepFailed
			apply = func(t *model.TaskLog) {
				t.Outcome = "failed"
				if m[2] != "" {
					t.Error = clip(redact.Text(m[2]), maxDetailLine)
				} else {
					c.errTask = t.TaskID // its exception follows on the next line
				}
			}
		case strings.Contains(msg, "killed task "):
			m := tlKilledRE.FindStringSubmatch(msg)
			if m == nil {
				return
			}
			tid, step.Kind = atoi64(m[1]), model.StepKilled
			apply = func(t *model.TaskLog) {
				t.Outcome, t.Error = "killed", clip(redact.Text(m[2]), maxDetailLine)
			}
		}
	case "TorrentBroadcast":
		if m := tlBcastRE.FindStringSubmatch(msg); m != nil {
			step.Kind, step.N, step.Bytes, step.Name = model.StepBroadcast, atoi(m[2]), SparkBytes(m[3]), "broadcast "+m[1]
			apply = func(t *model.TaskLog) { t.Broadcasts++; t.BroadcastBytes += step.Bytes }
		} else if m := tlBcastTookRE.FindStringSubmatch(msg); m != nil {
			step.Kind, step.Ms, step.Name = model.StepBcastRead, atoi64(m[2]), "broadcast "+m[1]
			apply = func(t *model.TaskLog) { t.BroadcastMs += step.Ms }
		}
	case "MemoryStore":
		if m := tlStoredRE.FindStringSubmatch(msg); m != nil {
			if !strings.HasPrefix(m[1], "rdd_") {
				return // a broadcast's pieces: counted with the broadcast
			}
			step.Kind, step.Bytes, step.Name, step.Free = model.StepCache, SparkBytes(m[2]), m[1], SparkBytes(m[3])
			apply = func(t *model.TaskLog) { t.CachedBlocks++; t.CachedBytes += step.Bytes; t.MemoryFree = step.Free }
		} else if m := tlDroppedRE.FindStringSubmatch(msg); m != nil {
			step.Kind, step.N, step.Free = model.StepDrop, atoi(m[1]), SparkBytes(m[2])
			apply = func(t *model.TaskLog) { t.Dropped += step.N; t.MemoryFree = step.Free }
		} else if m := tlNoRoomRE.FindStringSubmatch(msg); m != nil {
			step.Kind, step.Bytes, step.Name = model.StepNoRoom, SparkBytes(m[2]), m[1]
			apply = func(t *model.TaskLog) { t.NotCached++ }
			problem = false
		}
	case "ShuffleBlockFetcherIterator":
		if m := tlShuffleRE.FindStringSubmatch(msg); m != nil {
			step.Kind, step.N, step.Bytes = model.StepShuffle, atoi(m[1]), SparkBytes(m[2])
			local := SparkBytes(m[4]) + SparkBytes(m[6]) + SparkBytes(m[8])
			remote, rblocks := SparkBytes(m[10]), atoi(m[9])
			step.Remote = remote
			apply = func(t *model.TaskLog) {
				t.ShuffleReads++
				t.ShuffleBlocks += step.N
				t.ShuffleBytes += step.Bytes
				t.ShuffleLocalBytes += local
				t.ShuffleRemoteBytes += remote
				t.RemoteBlocks += rblocks
			}
		} else if m := tlFetchRE.FindStringSubmatch(msg); m != nil {
			step.Kind, step.N, step.Ms = model.StepFetch, atoi(m[1]), atoi64(m[2])
			apply = func(t *model.TaskLog) { t.RemoteFetches += step.N; t.FetchStartMs += step.Ms }
		}
	case "UnsafeExternalSorter", "ExternalSorter", "ExternalAppendOnlyMap", "Spillable":
		if m := tlSpillRE.FindStringSubmatch(msg); m != nil {
			step.Kind, step.Bytes = model.StepSpill, SparkBytes(m[1])
			apply = func(t *model.TaskLog) { t.Spills++; t.SpillBytes += step.Bytes }
		}
	case "FileScanRDD":
		if m := tlFileRE.FindStringSubmatch(msg); m != nil {
			if m[1] != "" {
				tid = atoi64(m[1])
			}
			from, to := atoi64(m[3]), atoi64(m[4])
			step.Kind, step.Bytes, step.Name = model.StepInput, to-from, clip(redact.Text(m[2]), maxDetailLine)
			apply = func(t *model.TaskLog) { input(t, step) }
			data = &model.StageData{Access: model.DataRead, Kind: model.DataPath, Name: model.DataFolder(redact.Text(m[2])), Parts: 1, Sized: 1, Bytes: step.Bytes}
		}
	case "HadoopRDD", "NewHadoopRDD":
		if m := tlRegionRE.FindStringSubmatch(msg); m != nil {
			step.Kind, step.Name = model.StepInput, m[1]+" region "+m[2]
			apply = func(t *model.TaskLog) { input(t, step) }
			data = &model.StageData{Access: model.DataRead, Kind: model.DataHBase, Name: m[1], Parts: 1}
		} else if m := tlSplitRE.FindStringSubmatch(msg); m != nil {
			step.Kind, step.Bytes, step.Name = model.StepInput, atoi64(m[3]), clip(redact.Text(m[1]), maxDetailLine)
			apply = func(t *model.TaskLog) { input(t, step) }
			data = &model.StageData{Access: model.DataRead, Kind: model.DataPath, Name: model.DataFolder(redact.Text(m[1])), Parts: 1, Sized: 1, Bytes: step.Bytes}
		}
	case "MultipartUploadOutputStream":
		if m := tlCloseRE.FindStringSubmatch(msg); m != nil {
			step.Kind, step.Name = model.StepOutput, clip(redact.Text(m[1]), maxDetailLine)
			apply = func(t *model.TaskLog) {
				t.Outputs++
				if t.Output == "" {
					t.Output = step.Name
				}
			}
			data = &model.StageData{Access: model.DataWrite, Kind: model.DataPath, Name: model.DataFolder(redact.Text(m[1])), Parts: 1}
		} else if m := tlUploadRE.FindStringSubmatch(msg); m != nil {
			n := atoi64(m[3])
			apply, quiet = func(t *model.TaskLog) { t.OutputBytes += n }, true
			data = &model.StageData{Access: model.DataWrite, Kind: model.DataPath, Name: model.DataFolder(redact.Text(m[2])), Bytes: n}
			if m[1] == "1" {
				data.Sized = 1
			}
		}
	case "FileSystemOptimizedCommitter":
		if m := tlPublishRE.FindStringSubmatch(msg); m != nil {
			// Names the folder its files went to, and the task by its TID,
			// when the files' own lines could not be tied to it.
			tid, quiet = atoi64(m[2]), true
			apply = func(t *model.TaskLog) {}
			data = &model.StageData{Access: model.DataWrite, Kind: model.DataPath, Name: model.DataFolder(redact.Text(m[1]) + "/")}
		}
	case "SparkHadoopMapRedUtil":
		if m := tlCommitRE.FindStringSubmatch(msg); m != nil {
			step.Kind, step.Ms = model.StepCommit, atoi64(m[1])
			apply = func(t *model.TaskLog) { t.Commits++; t.CommitMs += step.Ms }
		}
	}
	if apply == nil && problem {
		step.Kind, step.Name = model.StepProblem, clip(redact.Text(msg), maxDetailLine)
		level := h.level
		apply = func(t *model.TaskLog) {
			if level == "WARN" {
				t.Warnings++
			} else {
				t.Errors++
			}
			if t.Problem == "" {
				t.Problem = step.Name
			}
		}
	}
	named := tid >= 0 || strings.Contains(h.thread, "(TID ")
	switch {
	case apply == nil && !named:
		return // nothing to tell, and no task named
	case step.Kind == model.StepProblem && !named && (h.thread != "" || len(c.running) != 1):
		return // a warning of no task: the Logs section has it
	case data != nil && data.Access == model.DataWrite && !named && len(c.running) == 0 && threadTask(h.thread) == nil:
		return // a file written with no task running: the driver's own, such as its event log
	}
	t, direct := c.owner(h, tid)
	if direct || apply != nil {
		t.Lines++ // the lines read as its: on its thread, naming it, or one of the above
	}
	if apply == nil {
		return
	}
	apply(t)
	if data != nil {
		c.stageData(t, *data)
	}
	if quiet {
		return
	}
	addStep(t, step)
	switch step.Kind {
	case model.StepEnd, model.StepBigResult, model.StepFailed, model.StepKilled:
		if t.TaskID >= 0 {
			t.End, t.EndSource = h.time, model.Source{File: c.res.Name, Line: c.n}
			delete(c.running, t.TaskID)
		}
	}
}

// owner is the task a line belongs to, or the file's untied totals;
// direct says the line itself named the task (its thread or its TID).
func (c *classifier) owner(h header, tid int64) (*model.TaskLog, bool) {
	if tt := threadTask(h.thread); tt != nil {
		t := c.task(tt.TaskID, h)
		t.Partition, t.Attempt, t.Stage, t.StageAttempt, t.TiedBy = tt.Partition, tt.Attempt, tt.Stage, tt.StageAttempt, "thread"
		return t, true
	}
	if tid >= 0 {
		return c.task(tid, h), true
	}
	if h.thread == "" && len(c.running) == 1 {
		for id := range c.running {
			return c.task(id, h), false
		}
	}
	return c.untied(), false
}

// task finds or starts the story of task tid.
func (c *classifier) task(tid int64, h header) *model.TaskLog {
	if i, ok := c.tasks[tid]; ok {
		return &c.res.TaskLogs[i]
	}
	if len(c.res.TaskLogs) == maxTaskLogs {
		c.res.TaskLogsCut++
		return c.untied()
	}
	if c.tasks == nil {
		c.tasks = map[int64]int{}
	}
	c.tasks[tid] = len(c.res.TaskLogs)
	tiedBy := "tid"
	if h.thread != "" {
		tiedBy = "thread"
	}
	c.res.TaskLogs = append(c.res.TaskLogs, model.TaskLog{TaskID: tid, Partition: -1, Stage: -1, TiedBy: tiedBy,
		Source: model.Source{File: c.res.Name, Line: c.n}})
	return &c.res.TaskLogs[len(c.res.TaskLogs)-1]
}

// untied is the file's totals of lines no task could be found for.
func (c *classifier) untied() *model.TaskLog {
	if c.res.Untied == nil {
		c.res.Untied = &model.TaskLog{TaskID: -1, Partition: -1, Stage: -1, Source: model.Source{File: c.res.Name, Line: c.n}}
	}
	return c.res.Untied
}

// stageData adds a folder or table read or written to the stage of task
// t; for a line no task could be found for, to the stage of the tasks
// running, when they all belong to one.
func (c *classifier) stageData(t *model.TaskLog, d model.StageData) {
	stage, attempt, ok := t.Stage, t.StageAttempt, t.TaskID >= 0 && t.Stage >= 0
	if !ok {
		stage, attempt, ok = c.runningStage()
	}
	if !ok || d.Name == "" {
		c.res.DataUntied += d.Parts
		return
	}
	k := strconv.Itoa(stage) + "." + strconv.Itoa(attempt) + " " + d.Access + " " + d.Kind + " " + d.Name
	if i, has := c.data[k]; has {
		e := &c.res.StageData[i]
		e.Parts += d.Parts
		e.Sized += d.Sized
		e.Bytes += d.Bytes
		return
	}
	if len(c.res.StageData) == maxStageData {
		c.res.DataUntied += d.Parts
		return
	}
	if c.data == nil {
		c.data = map[string]int{}
	}
	c.data[k] = len(c.res.StageData)
	d.Stage, d.StageAttempt = stage, attempt
	d.From, d.Source = []string{"executor logs"}, model.Source{File: c.res.Name, Line: c.n}
	c.res.StageData = append(c.res.StageData, d)
}

// runningStage is the stage attempt of every task running, when they
// all belong to one.
func (c *classifier) runningStage() (stage, attempt int, ok bool) {
	for id := range c.running {
		i, has := c.tasks[id]
		if !has {
			return 0, 0, false
		}
		x := &c.res.TaskLogs[i]
		if x.Stage < 0 || ok && (x.Stage != stage || x.StageAttempt != attempt) {
			return 0, 0, false
		}
		stage, attempt, ok = x.Stage, x.StageAttempt, true
	}
	return stage, attempt, ok
}

func addStep(t *model.TaskLog, s model.TaskStep) {
	limit := model.MaxTaskSteps
	if t.TaskID < 0 {
		limit = model.MaxUntiedSteps
	}
	if len(t.Steps) == limit {
		t.CutSteps++
		return
	}
	t.Steps = append(t.Steps, s)
}

func input(t *model.TaskLog, s model.TaskStep) {
	t.Inputs++
	t.InputBytes += s.Bytes
	if t.Input == "" {
		t.Input = s.Name
	}
}

func atoi(s string) int { n, _ := strconv.Atoi(s); return n }

func atoi64(s string) int64 { n, _ := strconv.ParseInt(s, 10, 64); return n }

// taskError gives a failed task the exception on the line after its
// "Exception in task" line.
func (c *classifier) taskError(line string) {
	i, ok := c.tasks[c.errTask]
	c.errTask = -1
	if ok && c.res.TaskLogs[i].Error == "" {
		c.res.TaskLogs[i].Error = clip(redact.Text(strings.TrimSpace(line)), maxDetailLine)
	}
}
