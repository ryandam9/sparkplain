package analyze

import (
	"sort"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// hbaseTasks lists every TableInputFormat task attempt the executors
// logged a split for, across all stages: the region from its split line;
// the task from the thread that logged it, or from the scan's tie
// (placed); its time from its Running and Finished lines, or from the
// event log when it records the task, which also gives its rows. Then it
// sums them per stage.
func hbaseTasks(r *model.Report, h *model.HBaseSection, placed map[model.Source]placement) {
	if r.Logs == nil {
		return
	}
	for _, f := range r.Logs.Files {
		for _, sp := range f.HBaseSplits {
			t := model.HBaseTaskRead{Stage: -1, Partition: -1, TaskID: -1, Table: sp.Table, StartRow: sp.StartRow, EndRow: sp.EndRow,
				Region: sp.Region, Server: sp.Server, SizeBytes: sp.SizeBytes, ExecutorID: f.Executor, Host: f.Host,
				Source: sp.Source, SizeSource: sp.SizeSource}
			if k := sp.Task; k != nil {
				t.Stage, t.StageAttempt, t.Partition, t.Attempt, t.TaskID = k.Stage, k.StageAttempt, k.Partition, k.Attempt, k.TaskID
				t.Start, t.End, t.EndSource = k.Start, k.End, k.EndSource
				switch {
				case k.Failed:
					t.Outcome = "failed"
				case k.End.IsZero():
					t.Outcome = "no end logged"
				default:
					t.Outcome = "finished"
				}
				if !k.End.IsZero() && !k.Start.IsZero() {
					t.DurationMs, t.Timed, t.TimeFrom = k.End.Sub(k.Start).Milliseconds(), true, "executor log"
				}
			}
			pl := placed[sp.Source]
			if pl.stage != nil && sp.Task == nil {
				t.Stage, t.StageAttempt = pl.stage.ID, pl.stage.Attempt
			}
			if k := pl.task; k != nil {
				if sp.Task == nil {
					t.Partition, t.Attempt, t.TaskID, t.Outcome = k.Index, k.Attempt, k.TaskID, "finished"
				}
				t.Rows, t.RowsKnown = k.Rows, true
				t.DurationMs, t.Timed, t.TimeFrom = k.DurationMs, true, "event log"
				t.ExecutorID, t.Host, t.TaskSource = k.ExecutorID, k.Host, k.Source
			}
			h.Tasks = append(h.Tasks, t)
		}
	}
	// Stage, partition and attempt order; what is not known goes last.
	last := func(v int64) int64 {
		if v < 0 {
			return 1 << 62
		}
		return v
	}
	sort.SliceStable(h.Tasks, func(i, j int) bool {
		a, b := h.Tasks[i], h.Tasks[j]
		for _, d := range [][2]int64{
			{last(int64(a.Stage)), last(int64(b.Stage))}, {int64(a.StageAttempt), int64(b.StageAttempt)},
			{last(int64(a.Partition)), last(int64(b.Partition))}, {int64(a.Attempt), int64(b.Attempt)},
		} {
			if d[0] != d[1] {
				return d[0] < d[1]
			}
		}
		if a.Source.File != b.Source.File {
			return a.Source.File < b.Source.File
		}
		return a.Source.Line < b.Source.Line
	})
	h.TaskStages = taskStages(h.Tasks)
}

// taskStages sums the tasks table per stage, in its order; tasks whose
// stage is not known make one last row with stage -1.
func taskStages(tasks []model.HBaseTaskRead) []model.HBaseTaskStage {
	var out []model.HBaseTaskStage
	var regions, servers, tables map[string]bool
	for _, t := range tasks {
		if n := len(out); n == 0 || out[n-1].Stage != t.Stage || out[n-1].StageAttempt != t.StageAttempt {
			out = append(out, model.HBaseTaskStage{Stage: t.Stage, StageAttempt: t.StageAttempt})
			regions, servers, tables = map[string]bool{}, map[string]bool{}, map[string]bool{}
		}
		s := &out[len(out)-1]
		s.Tasks++
		if t.Outcome == "failed" {
			s.Failed++
		}
		if !regions[t.Table+"\x00"+t.StartRow+"\x00"+t.EndRow] {
			regions[t.Table+"\x00"+t.StartRow+"\x00"+t.EndRow] = true
			s.Regions++
		}
		if !servers[t.Server] {
			servers[t.Server] = true
			s.Servers++
		}
		if !tables[t.Table] {
			tables[t.Table] = true
			s.Tables = append(s.Tables, t.Table)
		}
		if !t.Start.IsZero() && (s.Start.IsZero() || t.Start.Before(s.Start)) {
			s.Start = t.Start
		}
		if t.End.After(s.End) {
			s.End = t.End
		}
	}
	for i := range out {
		sort.Strings(out[i].Tables)
	}
	return out
}
