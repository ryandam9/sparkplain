package report

import (
	"fmt"
	"html/template"
	"sort"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// flowChart shows, per step of time, the shuffle data tasks read on their
// own node and over the network, and what they spilled and cached, from
// the executors' logs.
func flowChart(r *model.Report, loc *time.Location) template.HTML {
	f := r.Flows
	if f == nil || len(f.Local) == 0 {
		return ""
	}
	series := []line{
		{label: "Shuffle read, own node", color: cShRead, points: f.Local, step: true},
		{label: "Shuffle read, over the network", color: cShWrite, points: f.Remote, step: true, area: true},
	}
	legend := []legendItem{{cShRead, "Shuffle read on the task's own node"}, {cShWrite, "Shuffle read over the network"}}
	if f.SpillBytes > 0 {
		series = append(series, line{label: "Spilled to disk", color: cSpill, points: f.Spill, step: true})
		legend = append(legend, legendItem{cSpill, "Spilled to disk"})
	}
	if f.CachedBytes > 0 {
		series = append(series, line{label: "Cached in memory", color: cInput, points: f.Cached, step: true, dash: true})
		legend = append(legend, legendItem{cInput, "Cached in memory (dashed)"})
	}
	end := f.From.Add(time.Duration(int64(len(f.Local))*f.StepMs) * time.Millisecond)
	return chartBox("Data moved over time", chartGuide{
		Run: runNotes(r)["flows"],
		Axes: [][2]string{
			{"Across", "Time of day, while the tasks ran."},
			{"Up", fmt.Sprintf("Bytes in each %s step, as the executors logged them: shuffle data asked for when each task began reading it (Spark's estimate from the map outputs), and data spilled or cached when it was.", model.Duration(f.StepMs))},
		},
		Read: []string{
			"The network line high next to the own-node line: most shuffle data came from other nodes, which costs network time and their disks' reads.",
			"Spikes of spill: tasks ran out of execution memory at those times; the stage running then needed more memory or more partitions.",
			"Caching that stops while the job still runs, with drops in the memory chart below: storage memory was full.",
		},
	}, lines(series, f.From, end, bytesF, loc), legend)
}

// memoryChart shows each executor's storage memory free after each block
// it cached or dropped, for the executors that came closest to running
// out.
func memoryChart(r *model.Report, loc *time.Location) template.HTML {
	f := r.Flows
	if f == nil {
		return ""
	}
	var execs []model.ExecFlow
	for _, e := range f.Executors {
		if len(e.Free) > 0 {
			execs = append(execs, e)
		}
	}
	if len(execs) == 0 {
		return ""
	}
	sort.SliceStable(execs, func(i, j int) bool { return execs[i].MinFree < execs[j].MinFree })
	colors := []string{cInput, cShRead, cShWrite, cOutput, cSpill}
	var series []line
	var legend []legendItem
	from, to := execs[0].Free[0].T, execs[0].Free[0].T
	for i, e := range execs {
		if i == len(colors) {
			break
		}
		series = append(series, line{label: "Executor " + e.Executor, color: colors[i], points: e.Free, step: true})
		legend = append(legend, legendItem{colors[i], "Executor " + e.Executor})
		for _, p := range e.Free {
			if p.T.Before(from) {
				from = p.T
			}
			if p.T.After(to) {
				to = p.T
			}
		}
	}
	note := ""
	if len(execs) > len(colors) {
		note = fmt.Sprintf("The %d executors with the least storage memory left, of %d that cached data.", len(colors), len(execs))
	}
	return chartBox("Storage memory left over time", chartGuide{
		Run: runNotes(r)["memory"],
		Axes: [][2]string{
			{"Across", "Time of day, from each executor's first cached block to its last."},
			{"Up", "Storage memory free on the executor after each block it cached or dropped, as its MemoryStore logged it. Storage shares the heap with execution, so free storage can also shrink while tasks sort and join."},
		},
		Read: []string{
			"A line that falls towards zero: that executor's cache filled up; the next blocks pushed older ones out or were not cached.",
			"A line that jumps back up: blocks were dropped to make room, and will be computed again when used.",
			"Lines that stay high: the cache had room to spare.",
		},
		Note: note,
	}, lines(series, from, to.Add(time.Second), bytesF, loc), legend)
}
