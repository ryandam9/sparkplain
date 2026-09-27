package analyze

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

func analyzeJobs(c *ctx, r *model.Report) {
	s := &r.Jobs
	if !c.has() {
		s.Coverage = model.NeedsEventLog
		s.Missing = []string{"Every job and stage on a timeline", "Slow tasks, skew and retries", "The SQL queries run, with their plans and tables"}
		return
	}
	s.Coverage = model.Complete
	s.Jobs, s.Stages, s.SQL = c.log.Jobs, c.log.Stages, c.log.SQL
	s.RunningTasks, s.RunningCapped = c.log.RunningTasks, c.log.RunningCapped
	if s.Jobs == nil {
		s.Jobs = []*model.Job{}
	}
	if s.Stages == nil {
		s.Stages = []*model.Stage{}
	}
	if s.SQL == nil {
		s.SQL = []*model.SQLQuery{}
	}
	for _, j := range s.Jobs {
		if j.Status == model.StatusFailed {
			s.Failed++
		}
	}
	s.CriticalJob, s.CriticalPath = criticalPath(c)
	s.DriverGaps, s.DriverGapMs = driverGaps(c)
	driverGapFinding(c, s)
	skewFindings(c)
	failureFindings(c)
}

// criticalPath finds the longest job and, within it, the chain of dependent
// stages with the most wall-clock time.
func criticalPath(c *ctx) (int, []int) {
	var long *model.Job
	for _, j := range c.log.Jobs {
		if long == nil || j.DurationMs() > long.DurationMs() {
			long = j
		}
	}
	if long == nil {
		return -1, nil
	}
	stages := map[int]*model.Stage{} // latest attempt of each stage in the job
	for _, st := range c.log.Stages {
		for _, jid := range st.JobIDs {
			if jid == long.ID && !st.Submitted.IsZero() {
				if cur := stages[st.ID]; cur == nil || st.Attempt > cur.Attempt {
					stages[st.ID] = st
				}
			}
		}
	}
	best := map[int]int64{}
	prev := map[int]int{}
	var visit func(id int) int64
	visit = func(id int) int64 {
		if v, ok := best[id]; ok {
			return v
		}
		st := stages[id]
		best[id] = 0 // guards against cycles in malformed logs
		var up int64
		prev[id] = -1
		for _, p := range st.ParentIDs {
			if stages[p] == nil {
				continue
			}
			if v := visit(p); v > up {
				up, prev[id] = v, p
			}
		}
		best[id] = up + st.DurationMs()
		return best[id]
	}
	end, endV := -1, int64(-1)
	ids := make([]int, 0, len(stages))
	for id := range stages {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		if v := visit(id); v > endV {
			end, endV = id, v
		}
	}
	var path []int
	for id := end; id >= 0; id = prev[id] {
		path = append([]int{id}, path...)
		if len(path) > len(stages) {
			break
		}
	}
	return long.ID, path
}

func skewFindings(c *ctx) {
	t := c.t
	type skew struct {
		st     *model.Stage
		excess int64
	}
	var found []skew
	for _, st := range c.log.Stages {
		d := st.TaskDuration
		if d.Count < int64(t.SkewMinTasks) || d.Max < t.SkewMinTask.Milliseconds() || d.P50 <= 0 || st.Slowest == nil {
			continue
		}
		if float64(d.Max) > t.SkewRatio*float64(d.P50) && dataSkewed(st, t.SkewRatio) {
			found = append(found, skew{st, d.Max - d.P50})
		}
	}
	sort.Slice(found, func(i, j int) bool { return found[i].excess > found[j].excess })
	for i, f := range found {
		if i == 5 {
			break
		}
		st, d := f.st, f.st.TaskDuration
		expl := fmt.Sprintf("In stage %d (%s), the slowest of %s tasks took %s, %.0f× the median task (%s).",
			st.ID, st.Name, model.Num(d.Count), model.Duration(d.Max), float64(d.Max)/float64(d.P50), model.Duration(d.P50))
		var read []string
		if sl := st.Slowest; sl.RecordsRead > 0 {
			read = append(read, fmt.Sprintf("%s rows against a median of %s", model.Num(sl.RecordsRead), model.Num(st.TaskRecords.P50)))
		}
		if sl := st.Slowest; sl.ShuffleReadBytes > 0 {
			read = append(read, fmt.Sprintf("%s of shuffle data against %s", model.Bytes(sl.ShuffleReadBytes), model.Bytes(st.TaskShuffle.P50)))
		} else if sl.InputBytes > 0 {
			read = append(read, fmt.Sprintf("%s of input against %s", model.Bytes(sl.InputBytes), model.Bytes(st.TaskInput.P50)))
		}
		expl += " It read " + strings.Join(read, ", and ") + ", so one key or partition held far more data than the rest. The stage could not finish until that one task did."
		fix := "Find the hot key (for example, count rows per join key) and salt it, filter out null or default keys before the join, or broadcast the smaller side."
		if c.conf["spark.sql.adaptive.skewJoin.enabled"] == "false" || c.conf["spark.sql.adaptive.enabled"] == "false" {
			fix = "Adaptive skew handling is switched off in this run; set spark.sql.adaptive.enabled=true and spark.sql.adaptive.skewJoin.enabled=true so Spark splits oversized join partitions. " + fix
		}
		sev := model.Warning
		if st.DurationMs() > 0 && d.Max*2 < st.DurationMs() {
			sev = model.Info // the slow task was not what held the stage up
		}
		c.add(model.Finding{
			Rule: "stage-skew", Severity: sev, Section: "stages",
			Title:       fmt.Sprintf("Stage %d is skewed: one task ran %.0f× longer than the median", st.ID, float64(d.Max)/float64(d.P50)),
			Explanation: expl,
			Evidence: []model.Evidence{{Source: st.Slowest.Source, Ref: model.StageRef(st.ID, st.Attempt), Text: fmt.Sprintf("task %d (partition %d) on executor %s, %s",
				st.Slowest.TaskID, st.Slowest.Index, st.Slowest.ExecutorID, model.Duration(st.Slowest.DurationMs))}},
			Fix: fix,
		})
	}
}

// dataSkewed reports whether the stage's slowest task also read far more data
// than its median task: more than ratio times the median rows, or the median
// shuffle or input bytes. A slow task that read the usual amount (typically
// the first task on a fresh executor, warming up the JVM) is not skew.
func dataSkewed(st *model.Stage, ratio float64) bool {
	sl := st.Slowest
	over := func(v, median int64) bool { return v > 0 && float64(v) > ratio*float64(median) }
	return over(sl.RecordsRead, st.TaskRecords.P50) || over(sl.ShuffleReadBytes, st.TaskShuffle.P50) || over(sl.InputBytes, st.TaskInput.P50)
}

func failureFindings(c *ctx) {
	app := c.log.Application
	var failed []*model.Job
	for _, j := range c.log.Jobs {
		if j.Status == model.StatusFailed {
			failed = append(failed, j)
		}
	}
	maxFailures := c.conf["spark.task.maxFailures"]
	if maxFailures == "" {
		maxFailures = "4"
	}
	for i, j := range failed {
		if i == 5 {
			break
		}
		fatal := app.Status == model.StatusFailed && i == len(failed)-1
		sev, title := model.Warning, fmt.Sprintf("Job %d (%s) failed", j.ID, jobLabel(j))
		if fatal {
			sev, title = model.Critical, fmt.Sprintf("Job %d (%s) failed and the application ended", j.ID, jobLabel(j))
		}
		expl := "Spark stops a job when one of its tasks fails " + maxFailures + " times (spark.task.maxFailures). "
		if !fatal {
			expl += "The application carried on afterwards, so the code probably caught the error; check that its output is still complete. "
		}
		expl += "Error: " + shortError(j.Failure)
		ev := []model.Evidence{{Source: j.EndSource, Ref: model.JobRef(j.ID), Text: truncateText(firstLineOf(j.Failure), 300)}}
		if st := failedStageOf(c, j); st != nil {
			for _, f := range st.Failures {
				ev = append(ev, model.Evidence{Source: f.Source, Ref: model.StageRef(st.ID, st.Attempt), Text: fmt.Sprintf("stage %d: %s × %s", st.ID, model.Num(f.Count), f.Message)})
			}
		}
		c.add(model.Finding{Rule: "job-failed", Severity: sev, Section: "stages", Title: title, Explanation: expl, Evidence: ev,
			Fix: "Read the error above: it comes from the task that failed last. If it is a data error, fix or filter the bad rows; if it is memory or a lost executor, see the memory findings."})
	}
	if len(failed) > 5 {
		c.add(model.Finding{Rule: "job-failed", Severity: model.Warning, Section: "stages",
			Title: fmt.Sprintf("%d more jobs failed", len(failed)-5), Explanation: "Only the first five failed jobs are listed in detail. The Jobs table shows all of them.",
			Evidence: []model.Evidence{{Source: failed[5].EndSource, Ref: model.JobRef(failed[5].ID), Text: fmt.Sprintf("job %d", failed[5].ID)}}})
	}
	// Stage retries.
	for _, st := range c.log.Stages {
		if st.Attempt == 0 {
			continue
		}
		reason := "an earlier attempt failed"
		for _, prev := range c.log.Stages {
			if prev.ID == st.ID && prev.Attempt == st.Attempt-1 && prev.FailureReason != "" {
				reason = "the previous attempt failed: " + shortError(prev.FailureReason)
			}
		}
		c.add(model.Finding{Rule: "stage-retried", Severity: model.Warning, Section: "stages",
			Title:       fmt.Sprintf("Stage %d ran again (attempt %d)", st.ID, st.Attempt+1),
			Explanation: fmt.Sprintf("Spark re-ran stage %d because %s. This usually follows a lost executor or failed shuffle fetch, and it repeats all the stage's work.", st.ID, reason),
			Evidence:    []model.Evidence{{Source: st.Source, Ref: model.StageRef(st.ID, st.Attempt), Text: fmt.Sprintf("Stage Attempt ID %d", st.Attempt)}},
			Fix:         "Look for lost executors at the same time. Enabling the external shuffle service keeps shuffle files when executors die."})
	}
	// Tasks that failed in stages that still succeeded.
	var n int64
	reasons := map[string]*model.TaskFailure{}
	var order []string
	for _, st := range c.log.Stages {
		if st.Status != model.StatusSucceeded {
			continue
		}
		for _, f := range st.Failures {
			n += f.Count
			k := f.Kind + "|" + f.Message
			if reasons[k] == nil {
				ff := f
				reasons[k] = &ff
				order = append(order, k)
			} else {
				reasons[k].Count += f.Count
			}
		}
	}
	if n > 0 {
		sort.SliceStable(order, func(i, j int) bool { return reasons[order[i]].Count > reasons[order[j]].Count })
		var ev []model.Evidence
		for i, k := range order {
			if i == 3 {
				break
			}
			f := reasons[k]
			ev = append(ev, model.Evidence{Source: f.Source, Text: fmt.Sprintf("%s × %s", model.Num(f.Count), f.Message)})
		}
		c.add(model.Finding{Rule: "task-retries", Severity: model.Info, Section: "stages",
			Title:       fmt.Sprintf("%s failed and were retried successfully", model.Plural(int(n), "task attempt", "task attempts")),
			Explanation: "These failures did not stop their stages, because a retry succeeded, but each one repeated work.",
			Evidence:    ev,
			Fix:         "If the reason is a lost executor, see the executor findings; if it is an exception, it may point at bad data that a retry happened to get past."})
	}
}

func failedStageOf(c *ctx, j *model.Job) *model.Stage {
	var out *model.Stage
	for _, st := range c.log.Stages {
		if st.Status != model.StatusFailed {
			continue
		}
		for _, id := range st.JobIDs {
			if id == j.ID {
				out = st
			}
		}
	}
	return out
}

var pyErrorLine = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.]*(Error|Exception|Exit|Interrupt|Warning)(: .*)?$`)

// shortError reduces a Spark failure message to a line a person can act on:
// the summary, plus the Python error line when there is a traceback.
func shortError(msg string) string {
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return "no reason recorded"
	}
	lines := strings.Split(msg, "\n")
	head := strings.TrimSpace(lines[0])
	if i := strings.Index(head, ", most recent failure:"); i > 0 {
		head = head[:i]
	}
	head = strings.TrimSuffix(head, "Traceback (most recent call last):")
	head = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(head), ":"))
	for i, l := range lines {
		if !strings.Contains(l, "Traceback (most recent call last)") {
			continue
		}
		for _, next := range lines[i+1:] {
			if pyErrorLine.MatchString(strings.TrimRight(next, " \t")) {
				return truncateText(head+" — "+strings.TrimSpace(next), 300)
			}
		}
	}
	return truncateText(head, 300)
}

func firstLineOf(s string) string {
	l, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	return l
}

func truncateText(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}
