package eventlog

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/redact"
)

// Options tune Parse.
type Options struct {
	MaxPlanBytes int // physical plan text kept per SQL query; default 64 KiB
	MaxPlans     int // queries whose plan text is kept; default 500
}

type stageKey struct{ id, attempt int }

type stageAcc struct {
	st       *model.Stage
	dur      distAcc
	input    distAcc
	shuffle  distAcc
	failures map[string]*model.TaskFailure
	order    []string
}

type blockSize struct{ mem, disk int64 }

type parser struct {
	opt       Options
	log       *model.EventLog
	execs     map[string]*model.Executor
	jobs      map[int]*model.Job
	stages    map[stageKey]*stageAcc
	stageJobs map[int][]int
	sql       map[int64]*model.SQLQuery
	sqlJobs   map[int64][]int
	plans     int
	rdds      map[int]*model.CachedRDD
	blocks    map[string]blockSize
	sawEnd    bool
	lastMs    int64
	partial   bool // the previous line was cut off by the end of a file
}

// Parse streams the event log and folds it into the model. It returns an
// error only when ctx ends; everything else is recorded in the stats.
func Parse(ctx context.Context, in *Input, opt Options) (*model.EventLog, error) {
	if opt.MaxPlanBytes <= 0 {
		opt.MaxPlanBytes = 64 << 10
	}
	if opt.MaxPlans <= 0 {
		opt.MaxPlans = 500
	}
	p := &parser{
		opt: opt,
		log: &model.EventLog{Stats: model.EventLogStats{
			Input: in.Location, Layout: in.Layout, InProgress: in.InProgress,
			ByType: map[string]int64{}, UnknownEvents: map[string]int64{}, UnknownFields: map[string]int64{},
			Notes: append([]string(nil), in.Notes...),
		}},
		execs: map[string]*model.Executor{}, jobs: map[int]*model.Job{}, stages: map[stageKey]*stageAcc{},
		stageJobs: map[int][]int{}, sql: map[int64]*model.SQLQuery{}, sqlJobs: map[int64][]int{},
		rdds: map[int]*model.CachedRDD{}, blocks: map[string]blockSize{},
	}
	st := &p.log.Stats
	var lastPartial model.Source
	files, truncated, notes := in.eachLine(ctx, func(line []byte, src model.Source, partial bool) error {
		st.Lines++
		if partial {
			lastPartial = src
		}
		if err := p.line(line, src); err != nil {
			if partial {
				return nil // a half-written last line; counted below
			}
			st.Malformed++
			if st.FirstMalformed.IsZero() {
				st.FirstMalformed = src
			}
		}
		return nil
	}, func(src model.Source) {
		st.Lines++
		st.Malformed++
		if st.FirstMalformed.IsZero() {
			st.FirstMalformed = src
		}
		st.Notes = append(st.Notes, fmt.Sprintf("Skipped an event over the line size limit at %s.", src))
	})
	st.Files = files
	st.Notes = append(st.Notes, notes...)
	st.Truncated = truncated
	if !lastPartial.IsZero() {
		st.Truncated = true
		st.Notes = append(st.Notes, fmt.Sprintf("The last line (%s) is cut off, so the log was still being written or was copied mid-write.", lastPartial))
	}
	if len(files) > 0 {
		st.Codec = files[0].Codec
	}
	p.finish()
	return p.log, ctx.Err()
}

func (p *parser) line(line []byte, src model.Source) error {
	name := eventName(line)
	if name == "" {
		var e struct {
			Event string `json:"Event"`
		}
		if err := json.Unmarshal(line, &e); err != nil || e.Event == "" {
			return fmt.Errorf("not an event")
		}
		name = e.Event
	}
	st := &p.log.Stats
	st.Events++
	st.ByType[name]++
	if keys, ok := knownKeys[name]; ok {
		topLevelKeys(line, func(k []byte) {
			if !keys[string(k)] {
				st.UnknownFields[name+"."+string(k)]++
			}
		})
	}
	switch name {
	case evTaskEnd:
		var e taskEndEvent
		return p.decode(line, &e, func() { p.taskEnd(&e, src) })
	case evStageExecMetric:
		var e stageExecMetricsEvent
		return p.decode(line, &e, func() { p.peak(e.ExecutorID, e.Metrics, src) })
	case evMetricsUpdate:
		var e metricsUpdateEvent
		return p.decode(line, &e, func() {
			for _, u := range e.Updated {
				p.peak(e.ExecutorID, u.Metrics, src)
			}
		})
	case evStageSubmitted, evStageCompleted:
		var e stageEvent
		return p.decode(line, &e, func() { p.stage(&e.Info, name == evStageCompleted, src) })
	case evJobStart:
		var e jobStartEvent
		return p.decode(line, &e, func() { p.jobStart(&e, src) })
	case evJobEnd:
		var e jobEndEvent
		return p.decode(line, &e, func() { p.jobEnd(&e, src) })
	case evExecAdded:
		var e executorAddedEvent
		return p.decode(line, &e, func() {
			x := p.executor(e.ExecutorID, e.Info.Host)
			x.Cores, x.ResourceProfileID = e.Info.Cores, e.Info.ResourceProfileID
			x.Added, x.AddedSource = ms(e.Timestamp), src
			p.seen(e.Timestamp)
		})
	case evExecRemoved:
		var e executorRemovedEvent
		return p.decode(line, &e, func() {
			x := p.executor(e.ExecutorID, "")
			x.Removed, x.RemovedSource = ms(e.Timestamp), src
			x.RemovedReason = redact.Text(e.Reason)
			x.RemovalKind = removalKind(e.Reason)
			p.seen(e.Timestamp)
		})
	case evBMAdded:
		var e blockManagerAddedEvent
		return p.decode(line, &e, func() {
			x := p.executor(e.ID.ExecutorID, e.ID.Host)
			x.MaxOnHeapStorage, x.MaxOffHeapStorage = e.MaxOnHeap, e.MaxOffHeap
			if x.Added.IsZero() && e.ID.ExecutorID == "driver" {
				x.Added, x.AddedSource = ms(e.Timestamp), src
			}
		})
	case evResourceProfile:
		var e resourceProfileEvent
		return p.decode(line, &e, func() { p.resourceProfile(&e, src) })
	case evEnv:
		var e envEvent
		return p.decode(line, &e, func() { p.environment(&e, src) })
	case evAppStart:
		var e appStartEvent
		return p.decode(line, &e, func() {
			a := &p.log.Application
			a.ID, a.Name, a.User, a.AttemptID = redact.Text(e.ID), redact.Text(e.Name), redact.Text(e.User), redact.Text(e.AttemptID)
			a.Start, a.Source = ms(e.Timestamp), src
			p.seen(e.Timestamp)
		})
	case evAppEnd:
		var e appEndEvent
		return p.decode(line, &e, func() {
			a := &p.log.Application
			a.End, a.EndSource, a.ExitCode = ms(e.Timestamp), src, e.ExitCode
			p.sawEnd = true
			p.seen(e.Timestamp)
		})
	case evLogStart:
		var e logStartEvent
		return p.decode(line, &e, func() {
			p.log.Application.SparkVersion, p.log.Application.VersionSrc = redact.Text(e.Version), src
		})
	case evBlockUpdated:
		var e blockUpdatedEvent
		return p.decode(line, &e, func() {
			if strings.HasPrefix(e.Info.BlockID, "rdd_") {
				p.blocks[e.Info.BlockManager.ExecutorID+"/"+e.Info.BlockID] = blockSize{e.Info.MemorySize, e.Info.DiskSize}
			}
		})
	case evUnpersist:
		var e unpersistEvent
		return p.decode(line, &e, func() {
			if r := p.rdds[e.RDDID]; r != nil {
				r.Unpersisted = true
			}
		})
	case evSQLStart:
		var e sqlStartEvent
		return p.decode(line, &e, func() { p.sqlStart(&e, src) })
	case evSQLAdaptive:
		var e sqlStartEvent
		return p.decode(line, &e, func() { p.sqlPlan(e.ID, e.Plan, e.PlanInfo, src) })
	case evSQLEnd:
		var e sqlEndEvent
		return p.decode(line, &e, func() {
			q := p.query(e.ID, src)
			q.End = ms(e.Time)
			q.Error = redact.Text(truncate(e.Error, 4000))
			p.seen(e.Time)
		})
	case evBMRemoved:
		return nil
	}
	if strings.HasPrefix(name, catalogPrefix) {
		var e catalogEvent
		return p.decode(line, &e, func() { p.catalog(strings.TrimPrefix(name, catalogPrefix), &e, src) })
	}
	if !ignored[name] {
		st.UnknownEvents[name]++
	}
	return nil
}

func (p *parser) decode(line []byte, v any, apply func()) error {
	if err := json.Unmarshal(line, v); err != nil {
		return err
	}
	apply()
	return nil
}

func ms(v int64) time.Time {
	if v <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(v).UTC()
}

func (p *parser) seen(v int64) {
	if v > p.lastMs {
		p.lastMs = v
	}
}

func (p *parser) executor(id, host string) *model.Executor {
	x := p.execs[id]
	if x == nil {
		x = &model.Executor{ID: redact.Text(id)}
		p.execs[id] = x
	}
	if host != "" && x.Host == "" {
		x.Host = redact.Text(host)
	}
	return x
}

func (p *parser) peak(execID string, m execMetrics, src model.Source) {
	x := p.executor(execID, "")
	x.Peak.Merge(model.PeakMemory{
		JVMHeap: m.JVMHeapMemory, JVMOffHeap: m.JVMOffHeapMemory,
		OnHeapExecution: m.OnHeapExecutionMemory, OnHeapStorage: m.OnHeapStorageMemory,
		OffHeapExecution: m.OffHeapExecutionMemory, OffHeapStorage: m.OffHeapStorageMemory,
		DirectPool: m.DirectPoolMemory, MappedPool: m.MappedPoolMemory,
		ProcessJVMRSS: m.ProcessTreeJVMRSSMemory, ProcessPythonRSS: m.ProcessTreePythonRSSMemory,
		ProcessOtherRSS: m.ProcessTreeOtherRSSMemory, TotalGCTimeMs: m.TotalGCTime,
		HeapSource: src, RSSSource: src,
	})
}

func (p *parser) stageAcc(id, attempt int, src model.Source) *stageAcc {
	k := stageKey{id, attempt}
	a := p.stages[k]
	if a == nil {
		a = &stageAcc{st: &model.Stage{ID: id, Attempt: attempt, Status: StatusPending, Source: src}, failures: map[string]*model.TaskFailure{}}
		p.stages[k] = a
	}
	return a
}

// StatusPending marks a stage listed by a job but not (yet) submitted. Stages
// still pending when the log ends are reported as skipped.
const StatusPending = "pending"

func (p *parser) taskEnd(e *taskEndEvent, src model.Source) {
	a := p.stageAcc(e.StageID, e.StageAttempt, src)
	st := a.st
	st.TaskSource.Extend(src)
	var t model.TaskTotals
	t.Tasks = 1
	ok := e.Reason.Reason == "Success"
	switch {
	case ok:
		t.Succeeded = 1
	case e.Reason.Reason == "TaskKilled" || e.Info.Killed:
		t.Killed = 1
	default:
		t.Failed = 1
	}
	if e.Info.Speculative {
		t.Speculative = 1
	}
	dur := int64(0)
	if e.Info.FinishTime > 0 && e.Info.LaunchTime > 0 {
		dur = e.Info.FinishTime - e.Info.LaunchTime
		p.seen(e.Info.FinishTime)
	}
	t.DurationMs = dur
	var input, shuffle int64
	if m := e.Metrics; m != nil {
		t.RunTimeMs, t.CPUTimeNs, t.GCTimeMs, t.DeserializeMs = m.RunTime, m.CPUTime, m.GCTime, m.DeserializeTime
		t.InputBytes, t.InputRecords = m.Input.Bytes, m.Input.Records
		t.OutputBytes, t.OutputRecords = m.Output.Bytes, m.Output.Records
		shuffle = m.ShuffleRead.RemoteBytes + m.ShuffleRead.LocalBytes
		input = m.Input.Bytes
		t.ShuffleReadBytes, t.ShuffleRemoteBytes, t.ShuffleReadRecords = shuffle, m.ShuffleRead.RemoteBytes, m.ShuffleRead.RecordsRead
		t.ShuffleFetchWaitMs = m.ShuffleRead.FetchWait
		t.ShuffleWriteBytes, t.ShuffleWriteRecords = m.ShuffleWrite.Bytes, m.ShuffleWrite.Records
		t.MemorySpillBytes, t.DiskSpillBytes = m.MemorySpilled, m.DiskSpilled
		t.PeakExecutionMemory = m.PeakExecutionMemory
	}
	st.Totals.Add(t)
	x := p.executor(e.Info.ExecutorID, e.Info.Host)
	x.Tasks.Add(t)
	if e.ExecMetrics != nil {
		p.peak(e.Info.ExecutorID, *e.ExecMetrics, src)
	}
	if ok {
		a.dur.add(dur)
		a.input.add(input)
		a.shuffle.add(shuffle)
		if st.Slowest == nil || dur > st.Slowest.DurationMs {
			st.Slowest = &model.TaskRef{
				TaskID: e.Info.TaskID, Index: e.Info.Index, Attempt: e.Info.Attempt,
				ExecutorID: redact.Text(e.Info.ExecutorID), Host: redact.Text(e.Info.Host),
				DurationMs: dur, InputBytes: input, ShuffleReadBytes: shuffle, Source: src,
			}
		}
		return
	}
	kind, msg := failureOf(e.Reason)
	key := kind + "|" + msg
	f := a.failures[key]
	if f == nil {
		if len(a.failures) >= 20 {
			key = "other|"
			if f = a.failures[key]; f == nil {
				f = &model.TaskFailure{Kind: "Other", Message: "Other failure reasons (more than 20 kinds)", Source: src}
				a.failures[key] = f
				a.order = append(a.order, key)
			}
		} else {
			f = &model.TaskFailure{Kind: kind, Message: msg, Source: src}
			a.failures[key] = f
			a.order = append(a.order, key)
		}
	}
	f.Count++
	if eid := redact.Text(e.Info.ExecutorID); !contains(f.Executors, eid) && len(f.Executors) < 20 {
		f.Executors = append(f.Executors, eid)
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// failureOf turns a Task End Reason into a kind and a one-line message.
func failureOf(r taskEndReason) (string, string) {
	var msg string
	switch r.Reason {
	case "ExceptionFailure":
		msg = r.ClassName
		if d := exceptionLine(r.ClassName, r.Description); d != "" {
			msg += ": " + d
		}
	case "ExecutorLostFailure":
		msg = fmt.Sprintf("Executor %s was lost: %s", r.ExecutorID, firstLine(r.LossReason))
	case "TaskKilled":
		msg = "Killed: " + firstLine(r.KillReason)
	case "FetchFailed":
		msg = "Could not fetch shuffle data: " + firstLine(r.Message)
	case "TaskResultLost":
		msg = "The task result was lost before the driver fetched it"
	case "TaskCommitDenied":
		msg = "Output commit denied (another attempt committed first)"
	case "Resubmitted":
		msg = "Re-run because the executor holding its output was lost"
	default:
		msg = r.Reason
	}
	return r.Reason, redact.Text(truncate(msg, 400))
}

// exceptionLine picks the informative line of an exception description.
// Python tracebacks end with the error; JVM exceptions start with it.
func exceptionLine(class, desc string) string {
	desc = strings.TrimSpace(desc)
	if strings.Contains(class, "PythonException") || strings.HasPrefix(desc, "Traceback") {
		lines := strings.Split(desc, "\n")
		for i := len(lines) - 1; i >= 0; i-- {
			if l := strings.TrimSpace(lines[i]); l != "" {
				return l
			}
		}
	}
	return firstLine(desc)
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	l, _, _ := strings.Cut(s, "\n")
	return strings.TrimSpace(l)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && cut < len(s) && s[cut]&0xc0 == 0x80 {
		cut--
	}
	return s[:cut] + "…"
}

// callSite shortens Spark's "collect at /some/dir/job.py:32" to "collect at
// job.py:32". On YARN the directory is the container's scratch folder, which
// is long and says nothing. Names not ending in a path:line are left alone.
func callSite(s string) string {
	i := strings.LastIndex(s, " at ")
	if i < 0 {
		return s
	}
	loc := s[i+len(" at "):]
	slash := strings.LastIndexAny(loc, `/\`)
	if slash < 0 || strings.ContainsAny(loc, " \t") {
		return s
	}
	base := loc[slash+1:]
	colon := strings.LastIndexByte(base, ':')
	if colon <= 0 {
		return s
	}
	if _, err := strconv.Atoi(base[colon+1:]); err != nil {
		return s
	}
	return s[:i+len(" at ")] + base
}

func (p *parser) stage(si *stageInfo, completed bool, src model.Source) {
	a := p.stageAcc(si.ID, si.Attempt, src)
	st := a.st
	st.Name = redact.Text(callSite(si.Name))
	st.NumTasks = si.NumTasks
	st.ParentIDs = si.ParentIDs
	if si.SubmissionTime != nil {
		st.Submitted = ms(*si.SubmissionTime)
		p.seen(*si.SubmissionTime)
	}
	if !completed {
		st.Status = model.StatusRunning
		st.Source = src
	} else {
		st.EndSource = src
		if si.CompletionTime != nil {
			st.Completed = ms(*si.CompletionTime)
			p.seen(*si.CompletionTime)
		}
		st.Status = model.StatusSucceeded
		if si.FailureReason != nil {
			st.Status = model.StatusFailed
			st.FailureReason = redact.Text(truncate(*si.FailureReason, 4000))
		}
	}
	for _, r := range si.RDDs {
		lvl := r.StorageLevel
		if !lvl.UseDisk && !lvl.UseMemory && !lvl.UseOffHeap {
			continue
		}
		c := p.rdds[r.ID]
		if c == nil {
			c = &model.CachedRDD{ID: r.ID, Name: redact.Text(truncate(firstLine(r.Name), 300)), StorageLevel: levelName(lvl), Partitions: r.NumPartitions, FirstStage: si.ID, Source: src}
			p.rdds[r.ID] = c
		}
		if r.MemorySize > 0 || r.DiskSize > 0 {
			c.MemoryBytes, c.DiskBytes, c.SizeKnown = max(c.MemoryBytes, r.MemorySize), max(c.DiskBytes, r.DiskSize), true
		}
		if !contains(intsToStrings(st.CachedRDDs), strconv.Itoa(r.ID)) {
			st.CachedRDDs = append(st.CachedRDDs, r.ID)
		}
	}
}

func intsToStrings(v []int) []string {
	out := make([]string, len(v))
	for i, x := range v {
		out[i] = strconv.Itoa(x)
	}
	return out
}

func levelName(l storageLevel) string {
	var parts []string
	if l.UseMemory {
		if l.Deserialized {
			parts = append(parts, "memory")
		} else {
			parts = append(parts, "memory (serialized)")
		}
	}
	if l.UseOffHeap {
		parts = append(parts, "off-heap")
	}
	if l.UseDisk {
		parts = append(parts, "disk")
	}
	s := strings.Join(parts, " + ")
	if l.Replication > 1 {
		s += fmt.Sprintf(", %d copies", l.Replication)
	}
	return s
}

func (p *parser) jobStart(e *jobStartEvent, src model.Source) {
	j := &model.Job{ID: e.JobID, StageIDs: e.StageIDs, Submitted: ms(e.Submitted), Status: model.StatusRunning, Source: src}
	p.seen(e.Submitted)
	j.Description = redact.Text(truncate(e.Properties["spark.job.description"], 500))
	j.Group = redact.Text(e.Properties["spark.jobGroup.id"])
	if id, err := strconv.ParseInt(e.Properties["spark.sql.execution.id"], 10, 64); err == nil {
		j.SQLExecutionID = &id
		p.sqlJobs[id] = append(p.sqlJobs[id], e.JobID)
	}
	last := -1
	for _, si := range e.StageInfos {
		a := p.stageAcc(si.ID, si.Attempt, src)
		if a.st.Name == "" {
			a.st.Name = redact.Text(callSite(si.Name))
			a.st.NumTasks = si.NumTasks
			a.st.ParentIDs = si.ParentIDs
		}
		if si.ID > last {
			last, j.Name = si.ID, redact.Text(callSite(si.Name))
		}
	}
	for _, id := range e.StageIDs {
		p.stageJobs[id] = append(p.stageJobs[id], e.JobID)
	}
	p.jobs[e.JobID] = j
}

func (p *parser) jobEnd(e *jobEndEvent, src model.Source) {
	j := p.jobs[e.JobID]
	if j == nil {
		j = &model.Job{ID: e.JobID, Source: src}
		p.jobs[e.JobID] = j
	}
	j.Completed, j.EndSource = ms(e.Completed), src
	p.seen(e.Completed)
	if e.Result.Result == "JobSucceeded" {
		j.Status = model.StatusSucceeded
	} else {
		j.Status = model.StatusFailed
		if e.Result.Exception != nil {
			j.Failure = redact.Text(truncate(e.Result.Exception.Message, 4000))
		} else {
			j.Failure = redact.Text(e.Result.Result)
		}
	}
}

func (p *parser) resourceProfile(e *resourceProfileEvent, src model.Source) {
	rp := model.ResourceProfile{ID: e.ID, Source: src}
	rp.ExecutorCores = int(e.Executor["cores"].Amount)
	rp.ExecutorMemoryMB = e.Executor["memory"].Amount
	rp.OverheadMB = e.Executor["memoryOverhead"].Amount
	rp.OffHeapMB = e.Executor["offHeap"].Amount
	rp.PySparkMemoryMB = e.Executor["pyspark.memory"].Amount
	rp.TaskCPUs = e.Task["cpus"].Amount
	p.log.ResourceProfiles = append(p.log.ResourceProfiles, rp)
}

func (p *parser) environment(e *envEvent, src model.Source) {
	add := func(origin string, m map[string]string) {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			v, hidden := redact.Value(k, m[k])
			if hidden {
				p.log.Stats.Redacted++
			}
			p.log.Config = append(p.log.Config, model.ConfigEntry{
				Key: redact.Clean(k), Value: truncate(v, 4000), Group: configGroup(origin, k), Origin: origin, Redacted: hidden, Source: src,
			})
		}
	}
	p.log.Config = p.log.Config[:0]
	add("JVM Information", e.JVM)
	add("Spark Properties", e.Spark)
	add("Hadoop Properties", e.Hadoop)
	add("System Properties", e.System)
	add("Metrics Properties", e.Metrics)
	p.log.Components = components(e.Classpath, src)
	if n := len(e.Classpath); n > 0 {
		p.log.Stats.Notes = append(p.log.Stats.Notes, fmt.Sprintf("The environment lists %d classpath entries; only the library versions named in the runtime table are shown.", n))
	}
	a := &p.log.Application
	get := func(k string) string {
		v, _ := redact.Value(k, e.Spark[k])
		return v
	}
	a.Master, a.DeployMode, a.Queue = get("spark.master"), get("spark.submit.deployMode"), get("spark.yarn.queue")
	if a.Name == "" {
		a.Name = get("spark.app.name")
	}
}

// configGroup sorts a setting into one of the report's groups.
func configGroup(origin, key string) string {
	if origin == "JVM Information" || origin == "System Properties" {
		return "JVM"
	}
	lk := strings.ToLower(key)
	switch {
	case strings.Contains(lk, "hbase"):
		return "HBase"
	case strings.Contains(lk, "hive") || strings.Contains(lk, "metastore"):
		return "Hive"
	case origin == "Hadoop Properties" || strings.HasPrefix(key, "spark.hadoop."):
		return "Hadoop"
	case strings.HasSuffix(lk, "extrajavaoptions"):
		return "JVM"
	}
	return "Spark"
}

func (p *parser) query(id int64, src model.Source) *model.SQLQuery {
	q := p.sql[id]
	if q == nil {
		q = &model.SQLQuery{ID: id, Source: src}
		p.sql[id] = q
	}
	return q
}

func (p *parser) sqlStart(e *sqlStartEvent, src model.Source) {
	q := p.query(e.ID, src)
	q.Source = src
	q.Description = redact.Text(truncate(callSite(e.Description), 500))
	q.Start = ms(e.Time)
	p.seen(e.Time)
	p.sqlPlan(e.ID, e.Plan, e.PlanInfo, src)
}

func (p *parser) sqlPlan(id int64, plan string, info planNode, src model.Source) {
	q := p.query(id, src)
	reads, writes := planData(info, plan, src)
	q.Reads = dedupe(append(q.Reads, reads...))
	q.Writes = dedupe(append(q.Writes, writes...))
	if plan == "" {
		return
	}
	if q.Plan == "" {
		if p.plans >= p.opt.MaxPlans {
			return
		}
		p.plans++
	}
	q.Plan = redact.Text(truncate(plan, p.opt.MaxPlanBytes))
	q.PlanTruncated = len(plan) > p.opt.MaxPlanBytes
}

func (p *parser) catalog(kind string, e *catalogEvent, src model.Source) {
	access := ""
	switch kind {
	case "CreateTableEvent":
		access = "create"
	case "DropTableEvent":
		access = "drop"
	case "RenameTableEvent":
		access = "rename"
	case "AlterTableEvent":
		access = "alter"
	default:
		return // pre-events, databases and functions
	}
	name := e.Name
	if e.Database != "" {
		name = e.Database + "." + name
	}
	if e.NewName != "" {
		name += " → " + e.NewName
	}
	p.log.CatalogEvents = append(p.log.CatalogEvents, model.DataRef{Kind: "table", Access: access, Name: redact.Text(name), Source: src})
}

// finish sorts everything and settles statuses once the log is read.
func (p *parser) finish() {
	l := p.log
	for id, x := range p.execs {
		if id == "driver" {
			l.Driver = x
			continue
		}
		l.Executors = append(l.Executors, x)
	}
	sort.Slice(l.Executors, func(i, j int) bool { return lessNumeric(l.Executors[i].ID, l.Executors[j].ID) })

	for _, j := range p.jobs {
		l.Jobs = append(l.Jobs, j)
	}
	sort.Slice(l.Jobs, func(i, j int) bool { return l.Jobs[i].ID < l.Jobs[j].ID })

	for _, a := range p.stages {
		st := a.st
		st.JobIDs = p.stageJobs[st.ID]
		st.TaskDuration, st.TaskInput, st.TaskShuffle = a.dur.dist(), a.input.dist(), a.shuffle.dist()
		for _, k := range a.order {
			st.Failures = append(st.Failures, *a.failures[k])
		}
		if st.Status == StatusPending {
			st.Status = model.StatusSkipped
			if st.Totals.Tasks > 0 {
				st.Status = model.StatusRunning
			}
		}
		if st.Status == model.StatusRunning && p.sawEnd {
			st.Status = model.StatusIncomplete
		}
		// A stage whose job never started a task and that has no submission
		// time was skipped because its output was already available.
		l.Stages = append(l.Stages, st)
	}
	sort.Slice(l.Stages, func(i, j int) bool {
		if l.Stages[i].ID != l.Stages[j].ID {
			return l.Stages[i].ID < l.Stages[j].ID
		}
		return l.Stages[i].Attempt < l.Stages[j].Attempt
	})

	for id, q := range p.sql {
		q.JobIDs = p.sqlJobs[id]
		l.SQL = append(l.SQL, q)
	}
	sort.Slice(l.SQL, func(i, j int) bool { return l.SQL[i].ID < l.SQL[j].ID })

	for key, b := range p.blocks {
		// key is <executor>/rdd_<rdd>_<partition>
		_, blk, _ := strings.Cut(key, "/")
		parts := strings.Split(blk, "_")
		if len(parts) != 3 {
			continue
		}
		id, err := strconv.Atoi(parts[1])
		if err != nil {
			continue
		}
		if r := p.rdds[id]; r != nil {
			r.MemoryBytes += b.mem
			r.DiskBytes += b.disk
			r.SizeKnown = true
		}
	}
	for _, r := range p.rdds {
		l.RDDs = append(l.RDDs, r)
	}
	sort.Slice(l.RDDs, func(i, j int) bool { return l.RDDs[i].ID < l.RDDs[j].ID })

	p.settleApplication()
	if len(l.Stats.UnknownEvents) == 0 {
		l.Stats.UnknownEvents = nil
	}
	if len(l.Stats.UnknownFields) == 0 {
		l.Stats.UnknownFields = nil
	}
}

func lessNumeric(a, b string) bool {
	x, errA := strconv.Atoi(a)
	y, errB := strconv.Atoi(b)
	if errA == nil && errB == nil {
		return x < y
	}
	if (errA == nil) != (errB == nil) {
		return errA == nil
	}
	return a < b
}

// settleApplication works out the final status. Spark 3.5 does not log the
// driver's exit code, so the status comes from the end event and the jobs.
func (p *parser) settleApplication() {
	a := &p.log.Application
	var failed, last *model.Job
	nFailed := 0
	for _, j := range p.log.Jobs {
		if j.Status == model.StatusFailed {
			nFailed++
			failed = j
		}
		if last == nil || j.Completed.After(last.Completed) || (j.Completed.Equal(last.Completed) && j.ID > last.ID) {
			last = j
		}
	}
	switch {
	case a.Start.IsZero() && p.log.Stats.Events == 0:
		a.Status, a.StatusReason = model.StatusUnknown, "The event log holds no events."
	case !p.sawEnd:
		a.Status = model.StatusIncomplete
		a.StatusReason = "The log has no application end event: the application was still running when the log was copied, or the driver stopped without closing the log."
		if p.lastMs > 0 && !a.Start.IsZero() {
			a.DurationMs = p.lastMs - a.Start.UnixMilli()
		}
	case a.ExitCode != nil && *a.ExitCode != 0:
		a.Status = model.StatusFailed
		a.StatusReason = fmt.Sprintf("The driver exited with code %d.", *a.ExitCode)
	case last != nil && last.Status == model.StatusFailed:
		a.Status = model.StatusFailed
		a.StatusReason = fmt.Sprintf("The last job (job %d) failed, and the application ended after it.", last.ID)
	default:
		a.Status = model.StatusSucceeded
		switch {
		case last == nil:
			a.StatusReason = "The application ended normally and ran no Spark jobs."
		case nFailed > 0:
			a.StatusReason = fmt.Sprintf("The application ended after its last job succeeded. %d earlier job(s) failed along the way (last: job %d).", nFailed, failed.ID)
		default:
			a.StatusReason = "The application ended and every job succeeded."
		}
		if a.ExitCode == nil {
			a.StatusReason += " Spark 3.5 does not record the driver's exit code, so an error after the last job would not show here."
		}
	}
	if p.sawEnd && !a.Start.IsZero() {
		a.DurationMs = a.End.Sub(a.Start).Milliseconds()
	}
}
