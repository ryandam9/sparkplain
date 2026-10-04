package analyze

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// analyzeTaskStories gathers what the executors' logs tell of each task
// (yarnlog's task stories): every task attempt's story with its executor
// and host, and per executor the totals of its task lines, those that
// could not be tied to a task included. Earlier attempts of the
// application are left out, since the final attempt reuses their task IDs.
func analyzeTaskStories(c *ctx, r *model.Report) {
	if r.Logs == nil {
		return
	}
	s := &model.TaskStorySection{Tasks: []model.TaskLog{}, Executors: []model.TaskStoryExec{}}
	earlier := 0
	for _, f := range r.Logs.Files {
		if len(f.TaskLogs) == 0 && f.Untied == nil {
			continue
		}
		if f.EarlierAttempt > 0 {
			earlier += len(f.TaskLogs)
			continue
		}
		x := model.TaskStoryExec{Executor: f.Executor, Host: f.Host, Tasks: len(f.TaskLogs), Source: model.Source{File: f.Location}}
		if x.Executor == "" {
			x.Executor = f.Container
		}
		for _, t := range f.TaskLogs {
			t.Executor, t.Host = f.Executor, f.Host
			x.Totals.Add(t)
			if t.TiedBy == "thread" {
				s.ByThread++
			} else {
				s.ByTID++
			}
			s.Tasks = append(s.Tasks, t)
		}
		if f.Untied != nil {
			x.Untied = *f.Untied
			x.Untied.Executor, x.Untied.Host = f.Executor, f.Host
			x.Totals.Add(*f.Untied)
			s.Untied.Add(*f.Untied)
		}
		s.Cut += f.TaskLogsCut
		s.Totals.Add(x.Totals)
		s.Executors = append(s.Executors, x)
	}
	if len(s.Executors) == 0 {
		return
	}
	s.Totals.TaskID, s.Untied.TaskID = -1, -1
	sort.SliceStable(s.Tasks, func(i, j int) bool {
		a, b := s.Tasks[i], s.Tasks[j]
		if !a.Start.Equal(b.Start) {
			return a.Start.Before(b.Start)
		}
		return a.TaskID < b.TaskID
	})
	sort.SliceStable(s.Executors, func(i, j int) bool { return execLess(s.Executors[i].Executor, s.Executors[j].Executor) })

	s.Coverage = model.Complete
	if u := s.Untied; u.Lines > 0 {
		s.Coverage = model.Partial
		s.Missing = append(s.Missing, fmt.Sprintf("Which task logged %s of the task lines of the executors (shuffle reads, broadcasts, cached blocks, spills, commits). The log layout prints no thread name, and the executors ran many tasks at the same time. As a result, sparkplain counts these lines only for each executor. Add %%t to the log4j pattern of the executors, and each line shows its task.",
			model.Num(int64(u.Lines))))
	}
	if s.Cut > 0 {
		s.Coverage = model.Partial
		s.Missing = append(s.Missing, fmt.Sprintf("The stories of %s after the first %s in each executor log. sparkplain counts their lines only for each executor", model.Plural(s.Cut, "task", "tasks"), model.Num(200_000)))
	}
	if earlier > 0 {
		s.Missing = append(s.Missing, fmt.Sprintf("%s from earlier attempts of the application. The final attempt uses their task IDs again", model.Plural(earlier, "task story", "task stories")))
	}
	if c.has() {
		var want int64
		for _, st := range c.log.Stages {
			want += st.Totals.Tasks
		}
		if n := want - int64(len(s.Tasks)) - int64(s.Cut); n > 0 {
			s.Coverage = model.Partial
			s.Missing = append(s.Missing, fmt.Sprintf("%s of the %s that the run records. sparkplain did not read the logs of their executors", model.Plural(int(n), "task attempt", "task attempts"), model.Num(want)))
		}
	}
	if !c.metrics() {
		s.Missing = append(s.Missing, "Rows read and written, CPU and GC time for each task. Only the event log has them")
	}
	r.TaskStories = s
}

// execLess orders executor IDs as numbers, the driver and names last.
func execLess(a, b string) bool {
	x, ea := strconv.Atoi(a)
	y, eb := strconv.Atoi(b)
	switch {
	case ea == nil && eb == nil:
		return x < y
	case ea == nil:
		return true
	case eb == nil:
		return false
	}
	return a < b
}
