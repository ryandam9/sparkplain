package main

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strconv"

	"github.com/ryandam9/sparkplain/internal/sparkplain/eventlog"
	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// rebuiltSource names the Sources row of a run rebuilt from its driver's
// log.
const rebuiltSource = "Driver log rebuild"

// rebuildFromLogs rebuilds the run's jobs, stages, tasks and executors
// from its driver's log when there is no event log: the log file with the
// most driver lines in the latest YARN attempt (in cluster mode the
// application master's container, _000001). It returns nil when no log
// shows a job started, as when the driver failed before running one: the
// logs-only report says more about that than an empty run would.
func rebuildFromLogs(ctx context.Context, files []model.LogFile, appID string, opt eventlog.Options) (*model.EventLog, *model.SourceStatus) {
	best := -1
	for i, f := range files {
		if !slices.ContainsFunc(f.DriverEvents, func(e model.DriverEvent) bool { return e.Kind == model.DrvJobStart }) {
			continue
		}
		if best < 0 {
			best = i
			continue
		}
		b := files[best]
		if a, ab := attemptOf(f.Container), attemptOf(b.Container); a > ab || a == ab && len(f.DriverEvents) > len(b.DriverEvents) {
			best = i
		}
	}
	if best < 0 {
		return nil, nil
	}
	f := files[best]
	attempt := ""
	if a := attemptOf(f.Container); a > 0 {
		attempt = strconv.Itoa(a)
	}
	log := eventlog.Rebuild(ctx, f.DriverEvents, f.Location, appID, attempt, opt)
	if f.Container != "" {
		// The driver ran in the application master's container: cluster
		// mode. Naming it joins the container's log to the driver.
		log.Application.DeployMode = "cluster"
		log.Application.DriverAttributes = map[string]string{"CONTAINER_ID": f.Container}
	}
	tasks := 0
	for _, s := range log.Stages {
		tasks += int(s.Totals.Tasks)
	}
	row := &model.SourceStatus{Name: rebuiltSource, Status: "read", Location: f.Location,
		Detail: fmt.Sprintf("No event log, so the run was rebuilt from the %s lines in which the driver logged its jobs, stages, tasks and executors: %s, %s, %s and %s. Task metrics (rows, bytes, CPU and GC time, spill, memory), SQL plans and Spark's configuration are only in the event log.",
			model.Num(int64(len(f.DriverEvents))), model.Plural(len(log.Jobs), "job", "jobs"), model.Plural(len(log.Stages), "stage attempt", "stage attempts"),
			model.Plural(tasks, "task attempt", "task attempts"), model.Plural(len(log.Executors), "executor", "executors")),
		Brief: fmt.Sprintf("%s, %s, %s", model.Plural(len(log.Jobs), "job", "jobs"), model.Plural(len(log.Stages), "stage", "stages"), model.Plural(tasks, "task", "tasks"))}
	return log, row
}

var attemptRE = regexp.MustCompile(`^container_(?:e\d+_)?\d+_\d+_(\d+)_\d+$`)

// attemptOf is the YARN attempt in a container ID, or 0.
func attemptOf(container string) int {
	m := attemptRE.FindStringSubmatch(container)
	if m == nil {
		return 0
	}
	n, _ := strconv.Atoi(m[1])
	return n
}
