package analyze

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/yarnlog"
)

// hbaseScans builds a scan stage's regions from its executors' split lines
// and the event log. TableInputFormat makes one split per region the scan
// overlaps, in key order, and Spark's partition i is split i; so sorting a
// stage's splits by start row ties each to its task, which is checked
// against the executor that logged the split. Checked on the 0084 run: the
// executor that ran partitions 1 and 2 logged the second and third splits
// in key order. Without the event log, a scan is built from split lines
// that name their tasks (logScanStages), with the task times their
// executors logged and no rows.
func hbaseScans(c *ctx, r *model.Report, h *model.HBaseSection) {
	if r.Logs == nil {
		return
	}
	var stages []*model.Stage
	fromLogs := c.log == nil
	if fromLogs {
		stages = logScanStages(r)
		if untasked := countUntasked(r); untasked > 0 {
			h.Missing = append(h.Missing, fmt.Sprintf("Scans for %s logged with no task: without the event log, a split is placed in its stage only when the executors' log layout prints the thread (log4j %%t), which names the task.",
				model.Plural(untasked, "split", "splits")))
		}
		if len(stages) > 0 {
			h.Missing = append(h.Missing, "Rows each scan region returned: Spark records them only in the event log.")
		}
	} else {
		stages = c.log.Stages
	}
	type split struct {
		s model.HBaseSplit
		f *model.LogFile
	}
	byStage := map[*model.Stage][]split{}
	for i := range r.Logs.Files {
		f := &r.Logs.Files[i]
		for _, sp := range f.HBaseSplits {
			// A split goes to the stage its thread names; without a thread,
			// to the scan stage that started last of those running then
			// (log times have whole seconds).
			var best *model.Stage
			for _, s := range stages {
				switch {
				case sp.Task != nil:
					if s.ID == sp.Task.Stage && s.Attempt == sp.Task.StageAttempt {
						best = s
					}
				case s.IsHadoopScan() && during(s, sp.Time) && (best == nil || s.Submitted.After(best.Submitted)):
					best = s
				}
			}
			if best != nil {
				byStage[best] = append(byStage[best], split{sp, f})
			}
		}
	}
	scans := printedScans(c, r)
	for _, s := range stages {
		xs := byStage[s]
		if len(xs) == 0 {
			continue
		}
		// A task that ran again logs its split again.
		seen := map[string]int{}
		var regions []model.HBaseRegionRead
		executors := map[int]string{}
		parts := map[int]int{} // region -> the partition its split's thread names
		tables := map[string]bool{}
		for _, x := range xs {
			key := x.s.Table + "\x00" + x.s.StartRow + "\x00" + x.s.EndRow
			if i, ok := seen[key]; ok {
				if regions[i].SizeBytes == 0 && x.s.SizeBytes > 0 {
					regions[i].SizeBytes, regions[i].SizeSource = x.s.SizeBytes, x.s.SizeSource
				}
				continue
			}
			seen[key] = len(regions)
			executors[len(regions)] = x.f.Executor
			if x.s.Task != nil {
				parts[len(regions)] = x.s.Task.Partition
			}
			tables[x.s.Table] = true
			regions = append(regions, model.HBaseRegionRead{Region: x.s.Region, StartRow: x.s.StartRow, EndRow: x.s.EndRow,
				Server: x.s.Server, SizeBytes: x.s.SizeBytes, Source: x.s.Source, SizeSource: x.s.SizeSource})
		}
		order := make([]int, len(regions))
		for i := range order {
			order[i] = i
		}
		sort.SliceStable(order, func(a, b int) bool {
			return bytes.Compare(unbinary(regions[order[a]].StartRow), unbinary(regions[order[b]].StartRow)) < 0
		})
		sorted := make([]model.HBaseRegionRead, len(regions))
		execOf := make([]string, len(regions))
		partOf := make([]int, len(regions))
		for i, k := range order {
			sorted[i], execOf[i] = regions[k], executors[k]
			partOf[i] = -1
			if p, ok := parts[k]; ok {
				partOf[i] = p
			}
		}
		var names []string
		for t := range tables {
			names = append(names, t)
		}
		sort.Strings(names)
		sc := model.HBaseScanRead{StageID: s.ID, Attempt: s.Attempt, Table: strings.Join(names, ", "), Regions: sorted,
			Tasks: s.NumTasks, TotalRows: s.Totals.InputRecords, FromLogs: fromLogs, Source: s.Source}
		if len(sorted) > 0 {
			sc.Rows = model.HBaseScan{StartRow: sorted[0].StartRow, StopRow: sorted[len(sorted)-1].EndRow, IncludeStart: true}.Rows()
		}
		if tieByTask(s, sc.Regions, partOf) {
			sc.Tied, sc.TiedBy = true, "task"
		} else if sc.Tied, sc.Untied = tieRegions(s, sc.Regions, execOf, len(names)); sc.Tied {
			sc.TiedBy = "key order"
		}
		sc.Scan = scanFor(scans, sc.Table, s)
		servers := map[string]*model.HBaseServerRead{}
		for _, g := range sc.Regions {
			sv := servers[g.Server]
			if sv == nil {
				sv = &model.HBaseServerRead{Server: g.Server}
				servers[g.Server] = sv
			}
			sv.Regions++
			sv.SizeBytes += g.SizeBytes
			if g.SizeBytes > 0 {
				sc.SizeBytes += g.SizeBytes
				sc.SizedRegions++
			}
			if g.Task != nil {
				sv.Rows += g.Task.Rows
				sv.TaskMs += g.Task.DurationMs
			}
		}
		for _, sv := range servers {
			sc.Servers = append(sc.Servers, *sv)
		}
		sort.Slice(sc.Servers, func(i, j int) bool {
			a, b := sc.Servers[i], sc.Servers[j]
			if a.Rows != b.Rows {
				return a.Rows > b.Rows
			}
			if a.TaskMs != b.TaskMs {
				return a.TaskMs > b.TaskMs
			}
			if a.Regions != b.Regions {
				return a.Regions > b.Regions
			}
			return a.Server < b.Server
		})
		h.Scans = append(h.Scans, sc)
		scanSkew(c, sc, s)
	}
}

// logScanStages are the scan stages the executors' split lines name, for
// a run with no event log: each holds a task per partition, timed from its
// Running to its Finished line. A partition that ran more than once keeps
// the attempt that finished; a task with no end, or only failed ones, is
// left out, so its region shows no time rather than a wrong one.
func logScanStages(r *model.Report) []*model.Stage {
	type key struct{ id, attempt int }
	byKey := map[key]*model.Stage{}
	parts := map[key]map[int]bool{}
	at := map[key]map[int]int{} // partition -> its task's place in ScanTasks
	for _, f := range r.Logs.Files {
		for _, sp := range f.HBaseSplits {
			t := sp.Task
			if t == nil {
				continue
			}
			k := key{t.Stage, t.StageAttempt}
			s := byKey[k]
			if s == nil {
				s = &model.Stage{ID: t.Stage, Attempt: t.StageAttempt, Source: sp.Source}
				byKey[k], parts[k], at[k] = s, map[int]bool{}, map[int]int{}
			}
			if !t.Start.IsZero() && (s.Submitted.IsZero() || t.Start.Before(s.Submitted)) {
				s.Submitted = t.Start
			}
			if t.End.After(s.Completed) {
				s.Completed = t.End
			}
			parts[k][t.Partition] = true
			if t.End.IsZero() || t.Failed {
				continue
			}
			st := model.ScanTask{Index: t.Partition, TaskID: t.TaskID, Attempt: t.Attempt, ExecutorID: f.Executor, Host: f.Host,
				DurationMs: t.End.Sub(t.Start).Milliseconds(), Source: t.EndSource}
			if i, ok := at[k][t.Partition]; ok {
				if t.Attempt > s.ScanTasks[i].Attempt {
					s.ScanTasks[i] = st
				}
				continue
			}
			at[k][t.Partition] = len(s.ScanTasks)
			s.ScanTasks = append(s.ScanTasks, st)
		}
	}
	var out []*model.Stage
	for k, s := range byKey {
		s.NumTasks = len(parts[k])
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID || out[i].ID == out[j].ID && out[i].Attempt < out[j].Attempt
	})
	return out
}

// countUntasked counts the split lines that name no task.
func countUntasked(r *model.Report) int {
	n := 0
	for _, f := range r.Logs.Files {
		for _, sp := range f.HBaseSplits {
			if sp.Task == nil {
				n++
			}
		}
	}
	return n
}

// scanSkew reports a scan stage that waited on one region: a scan reads
// one region per task, so the stage lasts as long as its slowest region.
// It fires when the slowest region's task took at least twice the median
// region's and at least skew-min-task longer, which catches a scan of only
// a few regions as well as a large one.
func scanSkew(c *ctx, sc model.HBaseScanRead, s *model.Stage) {
	var tied []model.HBaseRegionRead
	for _, g := range sc.Regions {
		if g.Task != nil {
			tied = append(tied, g)
		}
	}
	if len(tied) < 2 {
		return
	}
	durs := make([]int64, len(tied))
	var rows int64
	slow := tied[0]
	for i, g := range tied {
		durs[i] = g.Task.DurationMs
		rows += g.Task.Rows
		if g.Task.DurationMs > slow.Task.DurationMs {
			slow = g
		}
	}
	sort.Slice(durs, func(i, j int) bool { return durs[i] < durs[j] })
	median := durs[len(durs)/2]
	if len(durs)%2 == 0 {
		median = (durs[len(durs)/2-1] + durs[len(durs)/2]) / 2
	}
	t := slow.Task
	if t.DurationMs < 2*median || t.DurationMs-median < c.t.SkewMinTask.Milliseconds() {
		return
	}
	keys := model.HBaseScan{StartRow: slow.StartRow, StopRow: slow.EndRow, IncludeStart: true}.Rows()
	why := fmt.Sprintf(" It returned %s of the scan's %s rows (%s), so it had the most to read.", model.Num(t.Rows), model.Num(rows), model.Percent(share(t.Rows, rows)))
	if rows > 0 && float64(t.Rows) < 1.5*float64(rows)/float64(len(tied)) {
		why = fmt.Sprintf(" It returned %s of the scan's %s rows (%s), no more than its share, so its time is not explained by the rows it returned: the region server's own log for that time, or rows its filters read and skipped, may say why.", model.Num(t.Rows), model.Num(rows), model.Percent(share(t.Rows, rows)))
	}
	took := fmt.Sprintf("%s, %s rows", model.Duration(t.DurationMs), model.Num(t.Rows))
	ref := fmt.Sprintf("stage:%d.%d", s.ID, s.Attempt)
	if sc.FromLogs {
		// No event log: no rows, and no stage page to link to.
		why = " Rows per region need the event log."
		if slow.SizeBytes > 0 && sc.SizedRegions == len(sc.Regions) {
			why = fmt.Sprintf(" Its region holds %s of the scan's %s (%s, HBase's estimate), so it had the most to read if that share is large; rows per region need the event log.",
				model.Bytes(slow.SizeBytes), model.Bytes(sc.SizeBytes), model.Percent(share(slow.SizeBytes, sc.SizeBytes)))
		}
		took = model.Duration(t.DurationMs) + ", from its Running to its Finished line"
		ref = ""
	}
	c.add(model.Finding{Rule: "hbase-scan-skew", Severity: model.Warning, Section: "stages",
		Title: fmt.Sprintf("Stage %d's scan of %s waited on one region: %s took %s, the median region %s", s.ID, sc.Table, slow.Server, model.Duration(t.DurationMs), model.Duration(median)),
		Explanation: fmt.Sprintf("A TableInputFormat scan reads one region per task, so the stage lasts as long as its slowest region. Region %s (rows %s), on region server %s, was read by task %d (partition %d) on executor %s.%s",
			slow.Region, keys, slow.Server, t.TaskID, t.Index, t.ExecutorID, why),
		Evidence: []model.Evidence{
			{Source: slow.Source, Text: fmt.Sprintf("the split: region %s of %s, rows %s, on %s", slow.Region, sc.Table, keys, slow.Server)},
			{Source: t.Source, Ref: ref, Text: fmt.Sprintf("task %d: %s", t.TaskID, took)},
		},
		Fix: fmt.Sprintf("Split the region so its rows become several tasks (HBase shell: split '%s', '<a row key inside %s>'), or pre-split the table along its key range. Row keys that start with a date or a counter put the newest rows in one region: salting or hashing a prefix spreads them. If the table cannot change, scan narrower key ranges.", sc.Table, keys)})
}

// tieByTask gives each region the task its split's thread names (the
// partition it ran; a retried partition read the same region), when every
// split names one: exact, whatever ran at once.
func tieByTask(s *model.Stage, regions []model.HBaseRegionRead, partOf []int) bool {
	if len(regions) == 0 {
		return false
	}
	for _, p := range partOf {
		if p < 0 {
			return false
		}
	}
	tasks := map[int]*model.ScanTask{}
	for i := range s.ScanTasks {
		tasks[s.ScanTasks[i].Index] = &s.ScanTasks[i]
	}
	for i := range regions {
		regions[i].Task = tasks[partOf[i]]
	}
	return true
}

// tieRegions gives each region, in key order, the task of the same
// partition, when the stage's splits and partitions match one for one and
// every split was logged by the executor that ran that partition. It says
// why not otherwise, and ties none: a guess would put rows on the wrong
// region server.
func tieRegions(s *model.Stage, regions []model.HBaseRegionRead, execOf []string, tables int) (bool, string) {
	switch {
	case tables > 1:
		return false, "The stage read more than one table, so its splits cannot be put in its partitions' order."
	case s.ScanTasksCapped:
		return false, fmt.Sprintf("The stage ran more than %s tasks; sparkplain keeps the first %s.", model.Num(model.MaxScanTasks), model.Num(model.MaxScanTasks))
	case len(regions) != s.NumTasks:
		return false, fmt.Sprintf("%s were logged for its %s, so they cannot be matched one for one: an executor's log may be missing, or TableInputFormat split regions further (hbase.mapreduce.tableinput.mappers.per.region).",
			model.Plural(len(regions), "split", "splits"), model.Plural(s.NumTasks, "task", "tasks"))
	}
	tasks := map[int]*model.ScanTask{}
	for i := range s.ScanTasks {
		tasks[s.ScanTasks[i].Index] = &s.ScanTasks[i]
	}
	for i := range regions {
		if t := tasks[i]; t != nil && execOf[i] != "" && t.ExecutorID != execOf[i] {
			return false, fmt.Sprintf("Its splits in key order did not match the executors that ran its partitions (partition %d ran on executor %s, but executor %s logged that split).", i, t.ExecutorID, execOf[i])
		}
	}
	for i := range regions {
		regions[i].Task = tasks[i]
	}
	return true, ""
}

// unbinary undoes Bytes.toStringBinary, so row keys sort as HBase sorts
// them: by their bytes.
func unbinary(s string) []byte {
	var b []byte
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) && s[i+1] == 'x' {
			if v, err := strconv.ParseUint(s[i+2:i+4], 16, 8); err == nil {
				b = append(b, byte(v))
				i += 3
				continue
			}
		}
		b = append(b, s[i])
	}
	return b
}

// printedScans are the scans the job printed (sparkplain-scan lines, in
// file order), and one set as a Spark property, decoded.
func printedScans(c *ctx, r *model.Report) []model.HBaseScan {
	var out []model.HBaseScan
	for _, f := range r.Logs.Files {
		out = append(out, f.HBaseScans...)
	}
	for _, k := range []string{"hbase.mapreduce.scan", "spark.hadoop.hbase.mapreduce.scan"} {
		if v := c.conf[k]; v != "" {
			if sc, err := yarnlog.DecodeScan(v); err == nil {
				sc.Source = c.confSrc
				out = append(out, *sc)
			}
		}
	}
	return out
}

// scanFor picks the scan a stage ran: of those for its table (or naming
// none), the last printed before the stage started; a scan with no time
// (a Spark property) when none was printed.
func scanFor(scans []model.HBaseScan, table string, s *model.Stage) *model.HBaseScan {
	var best *model.HBaseScan
	for i := range scans {
		sc := &scans[i]
		if sc.Table != "" && sc.Table != table {
			continue
		}
		switch {
		case sc.Time.IsZero():
			if best == nil {
				best = sc
			}
		case s.Submitted.IsZero() || !sc.Time.After(s.Submitted.Add(time.Second)):
			if best == nil || best.Time.IsZero() || !sc.Time.Before(best.Time) {
				best = sc
			}
		}
	}
	if best == nil {
		return nil
	}
	cp := *best
	return &cp
}
