package eventlog

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// LayoutLogs marks an event log rebuilt from a driver's log (Rebuild): its
// jobs, stages, tasks and executors are Spark's own, but tasks carry no
// metrics (rows, bytes, CPU, GC, memory), which only the event log holds.
const LayoutLogs = model.LayoutRebuilt

// Rebuild turns a driver's log lines about jobs, stages, tasks and
// executors (yarnlog's DriverEvents, in file order) into the listener
// events an event log would hold, and folds them as Parse does, so every
// part of sparkplain reads the run as from an event log. Each value keeps
// the driver log line it came from. location names the driver's log;
// appID and attempt identify the application.
//
// What the log does not say is left out: task metrics, stage RDD graphs
// beyond the stage's own RDD, SQL plans, and Spark's configuration. A
// stage belongs to the job whose final stage or final stage's parent it
// is; any other stage to the job started last and not yet ended when it
// was submitted.
func Rebuild(ctx context.Context, events []model.DriverEvent, location, appID, attempt string, opt Options) *model.EventLog {
	if opt.MaxPlanBytes <= 0 {
		opt.MaxPlanBytes = 64 << 10
	}
	if opt.MaxPlans <= 0 {
		opt.MaxPlans = 500
	}
	p := newParser(opt)
	p.log.Stats.Input, p.log.Stats.Layout = location, LayoutLogs
	r := newRebuilder(events)
	emit := func(v any, src model.Source) {
		b, err := json.Marshal(v)
		if err != nil {
			return
		}
		p.log.Stats.Lines++
		if p.line(b, src) != nil {
			p.log.Stats.Malformed++
		}
	}
	if len(events) == 0 {
		p.finish()
		return p.log
	}
	first := events[0]
	if r.version.Name != "" {
		emit(map[string]any{"Event": evLogStart, "Spark Version": r.version.Name}, r.version.Source)
	}
	start := map[string]any{"Event": evAppStart, "App Name": r.appName.Name, "App ID": appID, "Timestamp": msOf(first.Time)}
	if attempt != "" {
		start["App Attempt ID"] = attempt
	}
	emit(start, firstSource(r.appName.Source, first.Source))

	execContainer := map[string]string{}
	for c, id := range r.containerExec {
		execContainer[id] = c
	}
	ended := false
	jobEnded := map[int]bool{}
	for i, e := range events {
		if i%4096 == 0 && ctx.Err() != nil {
			break
		}
		switch e.Kind {
		case model.DrvBlockManager:
			emit(map[string]any{"Event": evBMAdded, "Block Manager ID": map[string]any{"Executor ID": e.Executor, "Host": r.host(e.Executor, e.Host)},
				"Timestamp": msOf(e.Time), "Maximum Onheap Memory": e.Bytes, "Maximum Offheap Memory": 0}, e.Source)
		case model.DrvExecAdded:
			info := map[string]any{"Host": r.host(e.Executor, hostOf(e.Host)), "Total Cores": r.cores, "Resource Profile Id": 0}
			if c := execContainer[e.Executor]; c != "" { // ties the executor to its container's log
				info["Attributes"] = map[string]string{"CONTAINER_ID": c}
			}
			emit(map[string]any{"Event": evExecAdded, "Timestamp": msOf(e.Time), "Executor ID": e.Executor, "Executor Info": info}, e.Source)
		case model.DrvJobStart:
			stages := r.jobStages[e.Job]
			infos := make([]map[string]any, 0, len(stages))
			for _, s := range stages {
				infos = append(infos, r.stageInfo(s, 0, nil, nil, nil))
			}
			emit(map[string]any{"Event": evJobStart, "Job ID": e.Job, "Submission Time": msOf(e.Time), "Stage Infos": infos, "Stage IDs": stages,
				"Properties": map[string]string{"callSite.short": e.Name}}, e.Source)
		case model.DrvStageTasks:
			att := r.attemptAt[i]
			sub := msOf(e.Time)
			emit(map[string]any{"Event": evStageSubmitted, "Stage Info": r.stageInfoN(e.Stage, att, &sub, nil, e.N)}, e.Source)
		case model.DrvTaskStart:
			emit(map[string]any{"Event": evTaskStart, "Stage ID": e.Stage, "Stage Attempt ID": e.StageAttempt, "Task Info": r.taskInfo(e, msOf(e.Time), 0)}, e.Source)
		case model.DrvTaskEnd:
			launch := msOf(e.Time) - e.Ms
			if st, ok := r.starts[e.TaskID]; ok {
				launch = msOf(st.Time)
			}
			emit(map[string]any{"Event": evTaskEnd, "Stage ID": e.Stage, "Stage Attempt ID": e.StageAttempt, "Task Type": r.taskType(e.Stage),
				"Task End Reason": map[string]any{"Reason": "Success"}, "Task Info": r.taskInfo(e, launch, launch+e.Ms)}, e.Source)
		case model.DrvTaskLost:
			launch := msOf(e.Time)
			if st, ok := r.starts[e.TaskID]; ok {
				launch = msOf(st.Time)
			}
			reason, killed := lostReason(e)
			info := r.taskInfo(e, launch, msOf(e.Time))
			info["Failed"], info["Killed"] = !killed, killed
			emit(map[string]any{"Event": evTaskEnd, "Stage ID": e.Stage, "Stage Attempt ID": e.StageAttempt, "Task Type": r.taskType(e.Stage),
				"Task End Reason": reason, "Task Info": info}, e.Source)
		case model.DrvStageDone, model.DrvStageFailed:
			att := r.lastAttempt[e.Stage]
			done := msOf(e.Time)
			// "finished in 3.956 s" is exact where line times may have
			// whole seconds: the stage started that long before it ended.
			sub := done - e.Ms
			if t, ok := r.submitted[[2]int{e.Stage, att}]; ok && e.Ms == 0 {
				sub = t
			}
			var reason *string
			if e.Kind == model.DrvStageFailed {
				reason = &e.Text
			}
			emit(map[string]any{"Event": evStageCompleted, "Stage Info": r.stageInfo(e.Stage, att, &sub, &done, reason)}, e.Source)
			// A map stage job ends with its final stage, and logs no end.
			for _, j := range r.mapJobs[e.Stage] {
				if !jobEnded[j] {
					jobEnded[j] = true
					res := map[string]any{"Result": "JobSucceeded"}
					if reason != nil {
						res = map[string]any{"Result": "JobFailed", "Exception": map[string]any{"Message": *reason}}
					}
					emit(map[string]any{"Event": evJobEnd, "Job ID": j, "Completion Time": done, "Job Result": res}, e.Source)
				}
			}
		case model.DrvJobDone, model.DrvJobFailed:
			if jobEnded[e.Job] {
				break
			}
			jobEnded[e.Job] = true
			res := map[string]any{"Result": "JobSucceeded"}
			if e.Kind == model.DrvJobFailed {
				res = map[string]any{"Result": "JobFailed", "Exception": map[string]any{"Message": r.jobFailure(e.Job)}}
			}
			emit(map[string]any{"Event": evJobEnd, "Job ID": e.Job, "Completion Time": msOf(e.Time), "Job Result": res}, e.Source)
		case model.DrvExecLost:
			if r.removed(e.Executor) {
				emit(map[string]any{"Event": evExecRemoved, "Timestamp": msOf(e.Time), "Executor ID": e.Executor, "Removed Reason": e.Text}, e.Source)
			}
		case model.DrvExecIdle:
			for _, id := range strings.Split(e.Name, ",") {
				if id = strings.TrimSpace(id); id != "" && r.removed(id) {
					emit(map[string]any{"Event": evExecRemoved, "Timestamp": msOf(e.Time), "Executor ID": id, "Removed Reason": "Executor idle for longer than the dynamic allocation timeout."}, e.Source)
				}
			}
		case model.DrvContainerDone:
			if id := r.containerExec[e.Name]; id != "" && r.removed(id) {
				emit(map[string]any{"Event": evExecRemoved, "Timestamp": msOf(e.Time), "Executor ID": id,
					"Removed Reason": fmt.Sprintf("Container %s exited with status %d.", e.Name, e.N)}, e.Source)
			}
		case model.DrvAppStopped, model.DrvAppFinal:
			// The application master logs the driver's exit code after
			// SparkContext stops, or alone when it never stopped cleanly.
			if !ended {
				end := map[string]any{"Event": evAppEnd, "Timestamp": msOf(e.Time)}
				if r.final != nil {
					end["ExitCode"] = r.final.N
				}
				emit(end, e.Source)
				ended = true
			}
		}
	}
	p.finish()
	st := &p.log.Stats
	st.Notes = append(st.Notes, "Rebuilt from the driver's log ("+location+"): jobs, stages, tasks and executors as Spark logged them. Task metrics (rows, bytes, CPU, GC, memory), SQL plans and Spark's configuration are only in the event log.")
	return p.log
}

// rebuilder holds what the driver's log says across lines: which stages
// each job ran, each stage's name, type, task count and attempts, when
// each task started, and each executor's host and cores.
type rebuilder struct {
	version, appName model.DriverEvent
	final            *model.DriverEvent // Final app status: …, exitCode: N
	jobStages        map[int][]int
	stageName        map[int]string
	stageType        map[int]string
	stageRDD         map[int]string
	stageTasks       map[int]int
	parents          map[int][]int
	attemptAt        map[int]int // a stage-tasks event's index -> its attempt
	lastAttempt      map[int]int // stage -> its latest attempt
	submitted        map[[2]int]int64
	stageFailure     map[int]string // stage -> why it failed
	jobFinal         map[int]int    // job -> its final stage
	mapJobs          map[int][]int  // a map stage job\'s final stage -> the job
	starts           map[int64]model.DriverEvent
	execHost         map[string]string
	containerExec    map[string]string
	gone             map[string]bool
	cores            int
}

func newRebuilder(events []model.DriverEvent) *rebuilder {
	r := &rebuilder{jobStages: map[int][]int{}, stageName: map[int]string{}, stageType: map[int]string{}, stageRDD: map[int]string{},
		stageTasks: map[int]int{}, parents: map[int][]int{}, attemptAt: map[int]int{}, lastAttempt: map[int]int{},
		submitted: map[[2]int]int64{}, stageFailure: map[int]string{}, jobFinal: map[int]int{}, mapJobs: map[int][]int{}, starts: map[int64]model.DriverEvent{},
		execHost: map[string]string{}, containerExec: map[string]string{}, gone: map[string]bool{}}
	var active []int // jobs started and not yet ended, oldest first
	lastJob := -1
	inJob := map[[2]int]bool{}
	addStage := func(job, stage int) {
		if !inJob[[2]int{job, stage}] {
			inJob[[2]int{job, stage}] = true
			r.jobStages[job] = append(r.jobStages[job], stage)
		}
	}
	submissions := map[int]int{}
	mapJob := map[int]bool{}
	for i, e := range events {
		switch e.Kind {
		case model.DrvSparkVersion:
			if r.version.Name == "" {
				r.version = e
			}
		case model.DrvAppName:
			if r.appName.Name == "" {
				r.appName = e
			}
		case model.DrvAppFinal:
			if r.final == nil {
				r.final = &events[i]
			}
		case model.DrvJobStart:
			active, lastJob = append(active, e.Job), e.Job
			mapJob[e.Job] = e.Text == "map stage"
		case model.DrvFinalStage:
			if lastJob >= 0 {
				r.jobFinal[lastJob] = e.Stage
				addStage(lastJob, e.Stage)
			}
			r.noteStage(e)
		case model.DrvParents:
			if lastJob >= 0 {
				final := r.jobFinal[lastJob]
				r.parents[final] = e.Parents
				for _, s := range e.Parents {
					addStage(lastJob, s)
				}
			}
		case model.DrvStageTasks:
			r.noteStage(e)
			r.stageRDD[e.Stage] = e.Name
			if _, ok := r.stageTasks[e.Stage]; !ok {
				r.stageTasks[e.Stage] = e.N
			}
			att := submissions[e.Stage]
			submissions[e.Stage]++
			r.attemptAt[i], r.lastAttempt[e.Stage] = att, att
			r.submitted[[2]int{e.Stage, att}] = msOf(e.Time)
			if job := owner(r, active, e.Stage); job >= 0 {
				addStage(job, e.Stage)
			}
		case model.DrvStageDone:
			r.noteStage(e)
		case model.DrvStageFailed:
			r.noteStage(e)
			r.stageFailure[e.Stage] = e.Text
		case model.DrvJobDone, model.DrvJobFailed:
			for k, j := range active {
				if j == e.Job {
					active = append(active[:k], active[k+1:]...)
					break
				}
			}
		case model.DrvTaskStart:
			r.starts[e.TaskID] = e
			if e.Host != "" {
				r.execHost[e.Executor] = e.Host
			}
		case model.DrvContainer:
			r.containerExec[e.Name] = e.Executor
			r.execHost[e.Executor] = e.Host
		case model.DrvBlockManager:
			if _, ok := r.execHost[e.Executor]; !ok && e.Host != "" {
				r.execHost[e.Executor] = e.Host
			}
		case model.DrvExecResources:
			r.cores = e.N
		}
	}
	for j, stages := range r.jobStages {
		sort.Ints(stages)
		if mapJob[j] {
			r.mapJobs[r.jobFinal[j]] = append(r.mapJobs[r.jobFinal[j]], j)
		}
	}
	return r
}

// owner is the job a submitted stage belongs to: the active job that
// already names it, else the active job started last.
func owner(r *rebuilder, active []int, stage int) int {
	for k := len(active) - 1; k >= 0; k-- {
		for _, s := range r.jobStages[active[k]] {
			if s == stage {
				return active[k]
			}
		}
	}
	if len(active) == 0 {
		return -1
	}
	return active[len(active)-1]
}

// noteStage keeps a stage's name and type from the line naming it.
func (r *rebuilder) noteStage(e model.DriverEvent) {
	if e.StageType != "" {
		r.stageType[e.Stage] = e.StageType
	}
	if e.Kind == model.DrvStageTasks {
		if _, ok := r.stageName[e.Stage]; !ok {
			r.stageName[e.Stage] = rddCallSite(e.Name)
		}
		return
	}
	if e.Name != "" {
		r.stageName[e.Stage] = e.Name
	}
}

// rddRE reads a stage's RDD as DAGScheduler prints it:
// "MapPartitionsRDD[3] at count at NativeMethodAccessorImpl.java:0".
var rddRE = regexp.MustCompile(`^(\w+)\[(\d+)\] at (.*)$`)

// rddCallSite is the call site in an RDD's description, or the description.
func rddCallSite(rdd string) string {
	if m := rddRE.FindStringSubmatch(rdd); m != nil {
		return m[3]
	}
	return rdd
}

func (r *rebuilder) stageInfo(stage, attempt int, sub, done *int64, reason *string) map[string]any {
	return r.stageInfoN(stage, attempt, sub, done, r.stageTasks[stage], reason)
}

func (r *rebuilder) stageInfoN(stage, attempt int, sub, done *int64, n int, reason ...*string) map[string]any {
	info := map[string]any{"Stage ID": stage, "Stage Attempt ID": attempt, "Stage Name": r.stageName[stage], "Number of Tasks": n,
		"Parent IDs": orEmptyInts(r.parents[stage])}
	if m := rddRE.FindStringSubmatch(r.stageRDD[stage]); m != nil {
		var id int
		fmt.Sscan(m[2], &id)
		info["RDD Info"] = []map[string]any{{"RDD ID": id, "Name": m[1], "Callsite": m[3], "Number of Partitions": n}}
	}
	if sub != nil {
		info["Submission Time"] = *sub
	}
	if done != nil {
		info["Completion Time"] = *done
	}
	if len(reason) > 0 && reason[0] != nil {
		info["Failure Reason"] = *reason[0]
	}
	return info
}

func (r *rebuilder) taskType(stage int) string {
	if r.stageType[stage] == "ShuffleMapStage" {
		return "ShuffleMapTask"
	}
	return "ResultTask"
}

func (r *rebuilder) taskInfo(e model.DriverEvent, launch, finish int64) map[string]any {
	info := map[string]any{"Task ID": e.TaskID, "Index": e.Index, "Attempt": e.Attempt, "Launch Time": launch, "Executor ID": e.Executor,
		"Host": e.Host, "Finish Time": finish, "Partition ID": e.Partition, "Locality": e.Locality}
	if st, ok := r.starts[e.TaskID]; ok {
		info["Partition ID"], info["Locality"] = st.Partition, st.Locality
		if e.Host == "" {
			info["Host"] = st.Host
		}
	}
	return info
}

// host is an executor's host name: from its container or its tasks, else
// what the line gives (an address for a registration).
func (r *rebuilder) host(exec, given string) string {
	if h := r.execHost[exec]; h != "" {
		return h
	}
	return given
}

// removed reports an executor's first removal, so it is removed once.
func (r *rebuilder) removed(exec string) bool {
	if r.gone[exec] {
		return false
	}
	r.gone[exec] = true
	return true
}

// jobFailure is why a job failed: its failed stage's reason.
func (r *rebuilder) jobFailure(job int) string {
	for _, s := range r.jobStages[job] {
		if why := r.stageFailure[s]; why != "" {
			return why
		}
	}
	return "The job failed (the driver's log names no failed stage for it)."
}

// lostReason turns a "Lost task" line's reason into the task end reason
// Spark's event log would hold, and whether the task was killed.
func lostReason(e model.DriverEvent) (map[string]any, bool) {
	t := e.Text
	switch {
	case strings.HasPrefix(t, "TaskKilled"):
		return map[string]any{"Reason": "TaskKilled", "Kill Reason": strings.Trim(strings.TrimPrefix(t, "TaskKilled"), " ()")}, true
	case strings.HasPrefix(t, "ExecutorLostFailure"):
		return map[string]any{"Reason": "ExecutorLostFailure", "Executor ID": e.Executor, "Loss Reason": strings.TrimSpace(strings.TrimPrefix(t, "ExecutorLostFailure"))}, false
	case strings.HasPrefix(t, "FetchFailed"):
		return map[string]any{"Reason": "FetchFailed", "Message": t}, false
	case strings.HasPrefix(t, "TaskCommitDenied"):
		return map[string]any{"Reason": "TaskCommitDenied"}, false
	case strings.HasPrefix(t, "Resubmitted"):
		return map[string]any{"Reason": "Resubmitted"}, false
	}
	class, desc, _ := strings.Cut(t, ": ")
	if strings.ContainsAny(class, " (") {
		class, desc = "", t
	}
	return map[string]any{"Reason": "ExceptionFailure", "Class Name": class, "Description": desc}, false
}

// hostOf drops the port from an address.
func hostOf(addr string) string {
	if h, _, ok := strings.Cut(addr, ":"); ok {
		return h
	}
	return addr
}

func msOf(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func firstSource(a, b model.Source) model.Source {
	if !a.IsZero() {
		return a
	}
	return b
}

func orEmptyInts(v []int) []int {
	if v == nil {
		return []int{}
	}
	return v
}
