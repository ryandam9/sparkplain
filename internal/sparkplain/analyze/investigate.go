package analyze

import (
	"fmt"
	"sort"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// Findings from the fields phase 1c reads (SPEC §8): exclusions, scheduler
// delay, data locality, large results, executor startup, speculation, and
// tasks still running when the log ended.
func investigateFindings(c *ctx) {
	if !c.has() {
		return
	}
	exclusionFindings(c)
	schedulerDelayFinding(c)
	localityFinding(c)
	resultSizeFinding(c)
	startupFinding(c)
	speculationFinding(c)
	runningAtEndFinding(c)
}

func exclusionFindings(c *ctx) {
	var app, node, stage []model.Exclusion
	for _, x := range c.log.Exclusions {
		switch {
		case x.Kind == "node":
			node = append(node, x)
		case x.Scope == "application":
			app = append(app, x)
		default:
			stage = append(stage, x)
		}
	}
	if len(app)+len(node)+len(stage) == 0 {
		return
	}
	var ev []model.Evidence
	for _, x := range append(append(node, app...), stage...) {
		if len(ev) == 5 {
			ev = append(ev, model.Evidence{Text: fmt.Sprintf("… and %d more", len(app)+len(node)+len(stage)-5)})
			break
		}
		what := "executor " + x.Target
		ref := model.ExecutorRef(x.Target)
		if x.Kind == "node" {
			what, ref = "node "+x.Target, ""
		}
		scope := "for the rest of the application"
		if x.Scope == "stage" {
			scope, ref = fmt.Sprintf("for stage %d", x.StageID), model.StageRef(x.StageID, x.StageAttempt)
		}
		lifted := ""
		if !x.Lifted.IsZero() {
			lifted = fmt.Sprintf(", lifted after %s", model.Duration(x.Lifted.Sub(x.Time).Milliseconds()))
		}
		ev = append(ev, model.Evidence{Source: x.Source, Ref: ref, Text: fmt.Sprintf("%s excluded %s after %d failures%s", what, scope, x.Failures, lifted)})
	}
	sev, title := model.Info, fmt.Sprintf("Spark stopped using %s for a stage after task failures", model.Plural(len(stage), "executor", "executors"))
	switch {
	case len(node) > 0:
		sev, title = model.Warning, fmt.Sprintf("Spark excluded %s after task failures", model.Plural(len(node), "whole node", "whole nodes"))
	case len(app) > 0:
		sev, title = model.Warning, fmt.Sprintf("Spark excluded %s from the application after task failures", model.Plural(len(app), "executor", "executors"))
	}
	c.add(model.Finding{Rule: "executors-excluded", Severity: sev, Section: "executors", Title: title,
		Explanation: "With spark.excludeOnFailure.enabled, Spark stops scheduling tasks on an executor, or a whole node, where tasks keep failing. It protects the job from a bad machine, but it also takes capacity away, and if every executor is excluded the job cannot run at all.",
		Evidence:    ev,
		Fix:         "Look at why the tasks failed on those executors (see the task failures). If one host keeps failing, replace it; if the failures are in the data or the code, excluding executors only delays the job failing."})
}

func schedulerDelayFinding(c *ctx) {
	var delay, dur int64
	type st struct {
		s     *model.Stage
		delay int64
	}
	var worst []st
	for _, s := range c.log.Stages {
		delay += s.Totals.SchedulerDelayMs
		dur += s.Totals.DurationMs
		if s.Totals.SchedulerDelayMs > 0 {
			worst = append(worst, st{s, s.Totals.SchedulerDelayMs})
		}
	}
	if dur < c.t.MinRunTime.Milliseconds() || share(delay, dur) <= c.t.SchedDelayShare {
		return
	}
	sort.Slice(worst, func(i, j int) bool { return worst[i].delay > worst[j].delay })
	var ev []model.Evidence
	for i, w := range worst {
		if i == 3 {
			break
		}
		ev = append(ev, model.Evidence{Source: w.s.TaskSource, Ref: model.StageRef(w.s.ID, w.s.Attempt),
			Text: fmt.Sprintf("stage %d: %s scheduler delay over %s tasks (%s of their time)", w.s.ID, model.Duration(w.delay), model.Num(w.s.Totals.Tasks), model.Percent(share(w.delay, w.s.Totals.DurationMs)))})
	}
	c.add(model.Finding{Rule: "scheduler-delay", Severity: model.Warning, Section: "stages",
		Title:       fmt.Sprintf("Tasks spent %s of their time waiting to start or to report back", model.Percent(share(delay, dur))),
		Explanation: "Scheduler delay is task time not spent running, unpacking the task or returning its result: launching it, shipping its code and data, and waiting on the driver. A large share usually means very many short tasks, a busy driver, or large task closures.",
		Evidence:    ev,
		Fix:         "Use fewer, larger tasks (fewer partitions), avoid capturing large objects in closures (broadcast them), and give the driver more cores if it is busy."})
}

func localityFinding(c *ctx) {
	type st struct {
		s   *model.Stage
		far int64
	}
	var bad []st
	var far, known int64
	for _, s := range c.log.Stages {
		t := s.Totals
		if t.InputBytes == 0 || t.Tasks < int64(c.t.SkewMinTasks) {
			continue
		}
		f := t.LocalityRack + t.LocalityAny
		k := f + t.LocalityProcess + t.LocalityNode
		far, known = far+f, known+k
		if share(f, k) > c.t.LocalityAnyShare {
			bad = append(bad, st{s, f})
		}
	}
	if len(bad) == 0 {
		return
	}
	sort.Slice(bad, func(i, j int) bool { return bad[i].far > bad[j].far })
	var ev []model.Evidence
	for i, b := range bad {
		if i == 3 {
			break
		}
		t := b.s.Totals
		ev = append(ev, model.Evidence{Source: b.s.TaskSource, Ref: model.StageRef(b.s.ID, b.s.Attempt),
			Text: fmt.Sprintf("stage %d: %s of %s input tasks ran away from their data (%s rack-local, %s any)", b.s.ID, model.Num(b.far), model.Num(t.Tasks), model.Num(t.LocalityRack), model.Num(t.LocalityAny))})
	}
	c.add(model.Finding{Rule: "poor-locality", Severity: model.Info, Section: "stages",
		Title:       fmt.Sprintf("%s of tasks that read input ran on a different host from their data", model.Percent(share(far, known))),
		Explanation: "Spark tries to run each input task where its data is. When it cannot within spark.locality.wait, the task runs elsewhere and reads its data over the network. On S3 every read is remote anyway, so this matters for HDFS and cached data.",
		Evidence:    ev,
		Fix:         "If the data is on HDFS, check that executors run on the nodes holding it; raising spark.locality.wait trades waiting for locality against starting sooner."})
}

func resultSizeFinding(c *ctx) {
	limit := parseSize(c.conf["spark.driver.maxResultSize"], 1)
	if _, set := c.conf["spark.driver.maxResultSize"]; !set {
		limit = 1 << 30
	}
	if limit <= 0 {
		return // unlimited: config-unlimited-result covers it
	}
	type st struct {
		s     *model.Stage
		bytes int64
	}
	var big []st
	for _, s := range c.log.Stages {
		if s.TaskType == "ResultTask" && float64(s.Totals.ResultSizeBytes) >= c.t.ResultShare*float64(limit) {
			big = append(big, st{s, s.Totals.ResultSizeBytes})
		}
	}
	if len(big) == 0 {
		return
	}
	sort.Slice(big, func(i, j int) bool { return big[i].bytes > big[j].bytes })
	var ev []model.Evidence
	for i, b := range big {
		if i == 3 {
			break
		}
		ev = append(ev, model.Evidence{Source: b.s.TaskSource, Ref: model.StageRef(b.s.ID, b.s.Attempt),
			Text: fmt.Sprintf("stage %d (%s): %s of results sent to the driver", b.s.ID, b.s.Name, model.Bytes(b.bytes))})
	}
	c.add(model.Finding{Rule: "large-results", Severity: model.Warning, Section: "stages",
		Title:       fmt.Sprintf("A stage sent %s of results to the driver, near spark.driver.maxResultSize (%s)", model.Bytes(big[0].bytes), model.Bytes(limit)),
		Explanation: "Results of actions such as collect() and toPandas() all land in the driver's memory. Close to spark.driver.maxResultSize, the job fails; before that, the driver can run out of memory.",
		Evidence:    ev,
		Fix:         "Write large results to storage instead of collecting them, or collect an aggregate or a sample. Raise spark.driver.maxResultSize and the driver's memory only if the result really must come back."})
}

func startupFinding(c *ctx) {
	var slow []*model.Executor
	for _, x := range c.log.Executors {
		if x.StartupMs > c.t.SlowStartup.Milliseconds() {
			slow = append(slow, x)
		}
	}
	if len(slow) == 0 {
		return
	}
	sort.Slice(slow, func(i, j int) bool { return slow[i].StartupMs > slow[j].StartupMs })
	var ev []model.Evidence
	for i, x := range slow {
		if i == 3 {
			break
		}
		ev = append(ev, model.Evidence{Source: x.AddedSource, Ref: model.ExecutorRef(x.ID), Text: fmt.Sprintf("executor %s on %s took %s to start", x.ID, x.Host, model.Duration(x.StartupMs))})
	}
	c.add(model.Finding{Rule: "slow-executor-startup", Severity: model.Info, Section: "executors",
		Title:       fmt.Sprintf("%s took over %s to start", model.Plural(len(slow), "executor", "executors"), model.Duration(c.t.SlowStartup.Milliseconds())),
		Explanation: "Startup is the time from Spark asking the cluster manager for an executor to it registering with the driver: waiting for capacity, launching the container and starting the JVM. Tasks wait while it happens.",
		Evidence:    ev,
		Fix:         "On EMR, slow starts usually mean the cluster is waiting to scale out or containers are queued in YARN; keep a minimum number of executors warm (spark.dynamicAllocation.minExecutors) or size the cluster for the peak."})
}

func speculationFinding(c *ctx) {
	var spec, won, stages int64
	var first *model.Stage
	for _, s := range c.log.Stages {
		if s.Totals.Speculative == 0 {
			continue
		}
		spec += s.Totals.Speculative
		won += s.Totals.SpeculativeWon
		stages++
		if first == nil {
			first = s
		}
	}
	if spec == 0 {
		return
	}
	c.add(model.Finding{Rule: "speculation", Severity: model.Info, Section: "stages",
		Title: fmt.Sprintf("Spark ran %s of slow tasks in %s; %s finished first", model.Plural(int(spec), "speculative copy", "speculative copies"),
			model.Plural(int(stages), "stage", "stages"), model.Num(won)),
		Explanation: "With spark.speculation on, Spark starts a second copy of a task that runs much slower than its siblings, on another host, and keeps whichever finishes first. It hides slow machines but costs extra work, and it does not help when the task is slow because of its data (skew).",
		Evidence:    []model.Evidence{{Source: first.TaskSource, Ref: model.StageRef(first.ID, first.Attempt), Text: fmt.Sprintf("stage %d: %s speculative attempts", first.ID, model.Num(first.Totals.Speculative))}},
		Fix:         "If the same hosts keep being slow, look at them; if the slow tasks read more data than others, fix the skew instead."})
}

func runningAtEndFinding(c *ctx) {
	n := len(c.log.RunningTasks)
	if n == 0 {
		return
	}
	stages := map[[2]int]bool{}
	for _, t := range c.log.RunningTasks {
		stages[[2]int{t.StageID, t.StageAttempt}] = true
	}
	t := c.log.RunningTasks[0]
	more := ""
	if c.log.RunningCapped {
		more = " (at least)"
	}
	title := fmt.Sprintf("%s%s still running in %s when the log ended", model.Plural(n, "task was", "tasks were"), more, model.Plural(len(stages), "stage", "stages"))
	expl := "The log has a start but no end for these tasks. Either the application was still running when the log was copied, or it stopped without closing the log (a crash, a kill, or the driver running out of memory), so what these tasks did is unknown."
	if !c.log.Application.End.IsZero() {
		title = fmt.Sprintf("%s%s in %s never logged an end", model.Plural(n, "task", "tasks"), more, model.Plural(len(stages), "stage", "stages"))
		expl = "The application ended while these tasks were running, and Spark logged no end for them. This happens when a job is aborted (for example after too many failures) or the application shuts down with work in flight; their results were thrown away."
	}
	c.add(model.Finding{Rule: "tasks-running-at-end", Severity: model.Warning, Section: "stages",
		Title:       title,
		Explanation: expl,
		Evidence:    []model.Evidence{{Source: t.Source, Ref: model.StageRef(t.StageID, t.StageAttempt), Text: fmt.Sprintf("task %d of stage %d on executor %s, launched %s", t.TaskID, t.StageID, t.ExecutorID, t.Launched.UTC().Format(time.RFC3339))}},
		Fix:         "If the application should have finished, check the driver's log for why it stopped; phase 2 reads it automatically."})
}
