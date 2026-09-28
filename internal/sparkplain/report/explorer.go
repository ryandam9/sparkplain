package report

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

var (
	//go:embed assets/explorer.css
	explorerCSS string
	//go:embed assets/explorer.js
	explorerJS string
	// D3 7.9.0 from npm (integrity checked; ISC licence in
	// assets/vendor/d3-LICENSE), for the anatomy diagram's zoom and hover.
	// Its data loaders (d3.json and the like) are never called.
	//go:embed assets/vendor/d3-7.9.0.min.js
	d3JS string
	//go:embed templates/explorer.html.tmpl
	explorerTmpl string
)

// ExplorerOptions control the explorer page.
type ExplorerOptions struct {
	// ReportHref links to report.html; empty when it was not written.
	ReportHref string
	// Sources are the application's files from -source, already redacted.
	Sources     []SourceFile
	SourceNotes []string
}

// Caps on text that would otherwise dominate the page's size.
const (
	maxStackText     = 8 << 10  // per stack trace
	maxStackTraces   = 3        // per stage
	maxPlanText      = 16 << 10 // per query
	maxPlanTextTotal = 4 << 20  // all queries together
)

// WriteExplorer renders explorer.html (SPEC §6, Explorer page): the page's
// own script draws everything from the data embedded as JSON. x may be nil
// when the explorer data was not collected or the event log was unreadable.
func WriteExplorer(w io.Writer, r *model.Report, x *model.Explorer, opt ExplorerOptions) error {
	data, err := json.Marshal(explorerData(r, x, opt)) // escapes <, > and &, so no value can close the script tag
	if err != nil {
		return err
	}
	t, err := template.New("explorer").Parse(explorerTmpl)
	if err != nil {
		return err
	}
	title := r.Application.Name
	if title == "" {
		title = r.Application.ID
	}
	var buf bytes.Buffer
	err = t.Execute(&buf, struct {
		Title string
		CSS   template.CSS
		JS    template.JS
		D3    template.JS
		Data  template.HTML
	}{title, template.CSS(css + "\n" + explorerCSS), template.JS(explorerJS), template.JS(d3JS),
		template.HTML(`<script type="application/json" id="sp-data">` + string(data) + `</script>`)})
	if err != nil {
		return err
	}
	_, err = w.Write(buf.Bytes())
	return err
}

// table is rows of values under column names. The page's script reads
// columns by name, so their order can change without breaking it.
type table struct {
	Cols []string `json:"cols"`
	Rows [][]any  `json:"rows"`
}

func newTable(cols ...string) table { return table{Cols: cols, Rows: [][]any{}} }

func (t *table) add(v ...any) {
	if len(v) != len(t.Cols) {
		panic(fmt.Sprintf("row has %d values for %d columns", len(v), len(t.Cols)))
	}
	t.Rows = append(t.Rows, v)
}

// Column names of task and stage × executor rows, which are the bulk of the
// page and so are sent once instead of on every row.
var (
	taskCols = []string{"task", "index", "attempt", "exec", "status", "spec", "launch", "dur", "run", "gc", "deser",
		"fetch", "rows", "input", "shRead", "shWrite", "spill", "file", "line", "part", "loc", "sched", "result"}
	// localities are the task "loc" codes, in order.
	localities = []string{"PROCESS_LOCAL", "NODE_LOCAL", "RACK_LOCAL", "ANY", "NO_PREF"}
	cellCols   = []string{"exec", "tasks", "ok", "failed", "killed", "dur", "gc", "input", "shRead", "shWrite",
		"diskSpill", "peakHeap", "peakExec"}
)

type xData struct {
	V          int                  `json:"v"`
	Tool       string               `json:"tool"`
	Generated  int64                `json:"generated"`
	ReportHref string               `json:"reportHref,omitempty"`
	T0         int64                `json:"t0"` // Unix ms that task launch offsets count from
	App        xApp                 `json:"app"`
	Summary    []string             `json:"summary"`
	KPIs       []model.KPI          `json:"kpis"`
	Findings   []xFinding           `json:"findings"`
	Anatomy    string               `json:"anatomy,omitempty"` // the diagram's SVG, drawn in Go
	Files      []string             `json:"files"`
	Execs      []string             `json:"execs"` // executor IDs; task and cell rows use their index
	Executors  table                `json:"executors"`
	HeapBytes  int64                `json:"heapBytes"`
	Jobs       table                `json:"jobs"`
	Stages     table                `json:"stages"`
	Detail     map[string]xDetail   `json:"detail"` // by "id.attempt"
	TaskCols   []string             `json:"taskCols"`
	CellCols   []string             `json:"cellCols"`
	Running    *xRunning            `json:"running,omitempty"`
	SQL        table                `json:"sql"`
	Graphs     map[string][]xNode   `json:"graphs"`      // by query ID
	PlanLays   map[string]xLayout   `json:"planLayouts"` // by query ID, for graphs small enough to draw
	JobDags    map[string]xJobDag   `json:"jobDags"`     // by job ID, for jobs with 2 to maxGraphNodes stages
	StageOps   map[string]xStageOps `json:"stageOps"`    // by "id.attempt": the RDDs each stage computes, laid out as a graph
	Adaptive   map[string][][]any   `json:"adaptive"`    // by query ID: metrics adaptive execution added, rows as in a plan node
	RDDs       table                `json:"rdds"`
	Runtime    table                `json:"runtime"`
	Config     []xConfigGroup       `json:"config"`
	Exclusions table                `json:"exclusions"`
	RunTasks   table                `json:"runningTasks"`
	RunCapped  bool                 `json:"runningCapped"`
	Gaps       [][4]any             `json:"gaps"` // driver gaps: start and end (Unix ms), the jobs before and after (null at either end)
	BlockKinds table                `json:"blockKinds"`
	Data       table                `json:"data"`
	Profiles   table                `json:"profiles"`
	Critical   []int                `json:"critical"`
	CritJob    int                  `json:"criticalJob"`
	LogStats   *model.EventLogStats `json:"logStats,omitempty"`
	Sources    []xSource            `json:"sources"`
	SourceNote []string             `json:"sourceNotes"`
	Logs       []xLogFile           `json:"logs"`
	LogCols    []string             `json:"logCols"`
	LogSources []xLogSource         `json:"logSources"`
	Cluster    *xCluster            `json:"cluster,omitempty"`
	AWS        *xAWS                `json:"aws,omitempty"`
	AccessGaps []model.AccessGap    `json:"accessGaps,omitempty"`
	Collected  bool                 `json:"collected"` // explorer data was gathered
	Limits     model.ExplorerLimits `json:"limits"`
	Shrinks    int                  `json:"shrinks"`
	CellsCap   bool                 `json:"cellsCapped"`
	Notes      []string             `json:"notes"`
}

type xApp struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	User     string `json:"user"`
	Attempt  string `json:"attempt"`
	Spark    string `json:"spark"`
	Master   string `json:"master"`
	Deploy   string `json:"deploy"`
	Status   string `json:"status"`
	Reason   string `json:"reason"`
	Start    int64  `json:"start"`
	End      int64  `json:"end"`
	Duration int64  `json:"duration"`
	// DriverLogs are links to the driver's stdout and stderr.
	DriverLogs map[string]string `json:"driverLogs,omitempty"`
}

type xFinding struct {
	Sev      string      `json:"sev"`
	Title    string      `json:"title"`
	Expl     string      `json:"expl"`
	Fix      string      `json:"fix,omitempty"`
	Evidence [][3]string `json:"ev"` // text, ref, file:line
}

type xDetail struct {
	Metrics map[string][7]int64 `json:"m"` // count, sum, min, p25, p50, p75, max
	Hist    [][3]int64          `json:"h"` // lo, hi, count
	Slowest [][]int64           `json:"slow"`
	Sample  [][]int64           `json:"sample"`
	From    int64               `json:"from"`
	Cells   [][]int64           `json:"cells"`
}

// xJobDag is a job's stages as a graph: node i is stage Stages[i].
type xJobDag struct {
	Stages []int   `json:"stages"`
	Layout xLayout `json:"layout"`
}

// maxGraphNodes caps the graphs the page draws; bigger ones are listed as
// tables only.
const maxGraphNodes = 300

// Stage operation graphs are kept for stages of up to maxStageOpNodes RDDs,
// and for at most maxStageOpStages stages, to keep the page small.
const (
	maxStageOpNodes  = 100
	maxStageOpStages = 3000
)

// xStageOps is a stage's RDDs (id, name, operation, call site, partitions,
// cached partitions, storage level, barrier, determinism) and their layout.
type xStageOps struct {
	RDDs   [][]any `json:"rdds"`
	Layout xLayout `json:"layout"`
}

type xRunning struct {
	Start    int64   `json:"start"`
	BucketMs int64   `json:"bucketMs"`
	Busy     []int64 `json:"busy"`
}

type xNode struct {
	Name     string  `json:"n"`
	Detail   string  `json:"d,omitempty"`
	Children []int   `json:"c,omitempty"`
	Metrics  [][]any `json:"m,omitempty"` // name, type, value (null when not recorded), then tasks, min, median, max, max task, max stage when tasks reported it
}

type xConfigGroup struct {
	Name    string  `json:"name"`
	Entries [][]any `json:"entries"` // key, value, default, non-default, explain
}

func unixMs(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func explorerData(r *model.Report, x *model.Explorer, opt ExplorerOptions) xData {
	a := r.Application
	d := xData{
		V: 1, Tool: r.Tool, Generated: unixMs(r.GeneratedAt), ReportHref: opt.ReportHref, T0: unixMs(a.Start),
		App: xApp{ID: a.ID, Name: a.Name, User: a.User, Attempt: a.AttemptID, Spark: a.SparkVersion, Master: a.Master,
			Deploy: a.DeployMode, Status: a.Status, Reason: a.StatusReason, Start: unixMs(a.Start), End: unixMs(a.End), Duration: a.DurationMs,
			DriverLogs: a.DriverLogs},
		Summary: r.Summary.Sentences, KPIs: r.Summary.KPIs, HeapBytes: r.Memory.Config.HeapBytes,
		Detail: map[string]xDetail{}, TaskCols: taskCols, CellCols: cellCols, Graphs: map[string][]xNode{},
		Findings: []xFinding{}, Files: []string{}, Execs: []string{}, Notes: []string{},
		PlanLays: map[string]xLayout{}, JobDags: map[string]xJobDag{}, StageOps: map[string]xStageOps{}, Adaptive: map[string][][]any{},
	}
	d.Anatomy = anatomySVG(buildAnatomy(r), anatLinks{
		Finding: func(n int) string { return fmt.Sprintf("#finding/%d", n) },
		Ref: func(ref string) string {
			kind, id, _ := strings.Cut(ref, ":")
			switch kind {
			case "executor":
				return "#executor/" + url.PathEscape(id)
			case "node":
				if r.Cluster != nil {
					return "#cluster"
				}
				return "#executors"
			}
			return ""
		},
	})
	files := map[string]int{}
	fileIdx := func(s model.Source) int64 {
		if s.File == "" {
			return -1
		}
		i, ok := files[s.File]
		if !ok {
			i = len(d.Files)
			files[s.File] = i
			d.Files = append(d.Files, s.File)
		}
		return int64(i)
	}
	src := func(s model.Source) any {
		if s.File == "" {
			return nil
		}
		return []int64{fileIdx(s), s.Line}
	}
	execs := map[string]int{}
	execIdx := func(id string) int64 {
		i, ok := execs[id]
		if !ok {
			i = len(d.Execs)
			execs[id] = i
			d.Execs = append(d.Execs, id)
		}
		return int64(i)
	}

	d.Logs, d.LogSources, d.Cluster = logData(r)
	d.AWS = awsData(r)
	d.AccessGaps = r.AccessGaps
	d.LogCols = logLineCols
	for _, f := range r.Findings {
		xf := xFinding{Sev: string(f.Severity), Title: f.Title, Expl: f.Explanation, Fix: f.Fix}
		for _, e := range f.Evidence {
			loc := ""
			if !e.Source.IsZero() {
				loc = e.Source.String()
			}
			xf.Evidence = append(xf.Evidence, [3]string{e.Text, e.Ref, loc})
		}
		d.Findings = append(d.Findings, xf)
	}

	d.Executors = newTable("id", "host", "cores", "added", "removed", "reason", "kind", "tasks", "ok", "failed", "killed",
		"dur", "run", "cpuNs", "gc", "input", "output", "shRead", "shWrite", "memSpill", "diskSpill",
		"peakHeap", "peakOffHeap", "peakExec", "peakStorage", "peakRss", "storageMem", "src",
		"minorGc", "minorGcMs", "majorGc", "majorGcMs", "unified", "vmem", "sched", "resultSize",
		"startupMs", "logs", "attrs", "resources", "bmRemoved")
	var all []*model.Executor
	if r.Executors.Driver != nil {
		all = append(all, r.Executors.Driver)
	}
	all = append(all, r.Executors.Executors...)
	for _, e := range all {
		execIdx(e.ID)
		p, t := e.Peak, e.Tasks
		d.Executors.add(e.ID, e.Host, e.Cores, unixMs(e.Added), unixMs(e.Removed), e.RemovedReason, e.RemovalKind,
			t.Tasks, t.Succeeded, t.Failed, t.Killed, t.DurationMs, t.RunTimeMs, t.CPUTimeNs, t.GCTimeMs,
			t.InputBytes, t.OutputBytes, t.ShuffleReadBytes, t.ShuffleWriteBytes, t.MemorySpillBytes, t.DiskSpillBytes,
			p.JVMHeap, p.JVMOffHeap, p.OnHeapExecution+p.OffHeapExecution, p.OnHeapStorage+p.OffHeapStorage,
			p.ProcessJVMRSS+p.ProcessPythonRSS+p.ProcessOtherRSS, e.MaxOnHeapStorage+e.MaxOffHeapStorage, src(e.AddedSource),
			p.MinorGCCount, p.MinorGCTimeMs, p.MajorGCCount, p.MajorGCTimeMs, p.OnHeapUnified+p.OffHeapUnified,
			p.ProcessJVMVMem+p.ProcessPyVMem+p.ProcessOtherVMem, t.SchedulerDelayMs, t.ResultSizeBytes,
			e.StartupMs, orMap(e.LogURLs), orMap(e.Attributes), orMap(e.Resources), unixMs(e.BlockManagerRemoved))
	}

	d.Jobs = newTable("id", "name", "desc", "group", "submitted", "completed", "status", "stages", "sql", "failure", "src", "failureStack", "props", "code")
	for _, j := range r.Jobs.Jobs {
		var sql any
		if j.SQLExecutionID != nil {
			sql = *j.SQLExecutionID
		}
		d.Jobs.add(j.ID, j.Name, j.Description, j.Group, unixMs(j.Submitted), unixMs(j.Completed), j.Status,
			orEmpty(j.StageIDs), sql, capText(j.Failure, 2000), src(j.Source), capText(j.FailureStack, maxStackText), orMap(j.Properties), codeRows(j.Code))
	}

	parents := map[int][]int{}
	for _, st := range r.Jobs.Stages {
		parents[st.ID] = st.ParentIDs
	}
	for _, j := range r.Jobs.Jobs {
		if len(j.StageIDs) < 2 || len(j.StageIDs) > maxGraphNodes {
			continue
		}
		idx := map[int]int{}
		for i, id := range j.StageIDs {
			idx[id] = i
		}
		var edges [][2]int
		for i, id := range j.StageIDs {
			for _, p := range parents[id] {
				if pi, ok := idx[p]; ok {
					edges = append(edges, [2]int{pi, i})
				}
			}
		}
		d.JobDags[strconv.Itoa(j.ID)] = xJobDag{Stages: j.StageIDs, Layout: layered(len(j.StageIDs), edges)}
	}

	d.Stages = newTable("id", "attempt", "name", "status", "submitted", "completed", "numTasks", "jobs", "parents",
		"tasks", "ok", "failed", "killed", "dur", "run", "gc", "input", "inputRows", "output", "outputRows",
		"shRead", "shReadRows", "shWrite", "shWriteRows", "memSpill", "diskSpill", "p50", "max", "failure", "cached", "src",
		"taskType", "loc", "sched", "resultSize", "gettingMs", "shWriteMs", "shRemote", "shRemoteDisk", "shLocalBlocks", "shRemoteBlocks",
		"push", "cacheWrites", "failures", "details", "rp", "pushOn", "pushMergers", "barrier", "props", "cpuNs", "code", "split", "durMin", "p95", "durN")
	for _, st := range r.Jobs.Stages {
		t := st.Totals
		d.Stages.add(st.ID, st.Attempt, st.Name, st.Status, unixMs(st.Submitted), unixMs(st.Completed), st.NumTasks,
			orEmpty(st.JobIDs), orEmpty(st.ParentIDs), t.Tasks, t.Succeeded, t.Failed, t.Killed, t.DurationMs, t.RunTimeMs, t.GCTimeMs,
			t.InputBytes, t.InputRecords, t.OutputBytes, t.OutputRecords, t.ShuffleReadBytes, t.ShuffleReadRecords,
			t.ShuffleWriteBytes, t.ShuffleWriteRecords, t.MemorySpillBytes, t.DiskSpillBytes,
			st.TaskDuration.P50, st.TaskDuration.Max, capText(st.FailureReason, 2000), orEmpty(st.CachedRDDs), src(st.Source),
			st.TaskType, []int64{t.LocalityProcess, t.LocalityNode, t.LocalityRack, t.LocalityAny, t.LocalityNoPref},
			t.SchedulerDelayMs, t.ResultSizeBytes, t.GettingResultMs, t.ShuffleWriteTimeNs/1e6, t.ShuffleRemoteBytes,
			t.ShuffleRemoteToDiskBytes, t.ShuffleLocalBlocks, t.ShuffleRemoteBlocks,
			[]int64{t.PushMergedLocalBlocks, t.PushMergedLocalBytes, t.PushMergedRemoteBlocks, t.PushMergedRemoteBytes, t.PushFallbacks, t.PushCorruptChunks, t.PushMergedRemoteReqsMs},
			[]int64{t.UpdatedBlocks, t.UpdatedBlockBytes}, stageFailures(st),
			st.Details, st.ResourceProfile, st.ShufflePush, st.PushMergers, isBarrier(st), orMap(st.Properties), t.CPUTimeNs, codeRows(st.Code), splitRow(t.TimeSplit()), st.TaskDuration.Min, st.TaskDuration.P95, st.TaskDuration.Count)
		if len(st.RDDs) > 0 && len(st.RDDs) <= maxStageOpNodes && len(d.StageOps) < maxStageOpStages {
			d.StageOps[strconv.Itoa(st.ID)+"."+strconv.Itoa(st.Attempt)] = stageOps(st)
		}
	}

	d.SQL = newTable("id", "desc", "start", "end", "error", "jobs", "reads", "writes", "plan", "planCut", "src",
		"root", "tags", "details", "modified", "optimizer", "code")
	planBudget := maxPlanTextTotal
	for _, q := range r.Jobs.SQL {
		plan, cut := q.Plan, q.PlanTruncated
		if len(plan) > maxPlanText || len(plan) > planBudget {
			plan, cut = capText(plan, min(maxPlanText, max(0, planBudget))), true
		}
		planBudget -= len(plan)
		var root any
		if q.RootID != nil {
			root = *q.RootID
		}
		d.SQL.add(q.ID, q.Description, unixMs(q.Start), unixMs(q.End), capText(q.Error, 2000), orEmpty(q.JobIDs),
			refNames(q.Reads), refNames(q.Writes), plan, cut, src(q.Source),
			root, orEmpty(q.JobTags), q.Details, orMap(q.ModifiedConfigs), optimizerRows(q.Optimizer), codeRows(q.Code))
	}

	d.RDDs = newTable("id", "name", "level", "partitions", "firstStage", "unpersisted", "mem", "disk", "sizeKnown", "executors")
	for _, c := range r.IO.Cached {
		placed := [][]any{}
		for _, pl := range c.Executors {
			placed = append(placed, []any{pl.ExecutorID, pl.Host, pl.Blocks, pl.MemoryBytes, pl.DiskBytes, pl.StorageLevel})
		}
		d.RDDs.add(c.ID, c.Name, c.StorageLevel, c.Partitions, c.FirstStage, c.Unpersisted, c.MemoryBytes, c.DiskBytes, c.SizeKnown, placed)
	}
	d.Data = newTable("kind", "access", "name", "format", "src")
	for _, x := range r.IO.Data {
		d.Data.add(x.Kind, x.Access, x.Name, x.Format, src(x.Source))
	}
	d.Profiles = newTable("id", "cores", "memMiB", "overheadMiB", "offHeapMiB", "pysparkMiB", "taskCpus", "execOther", "taskOther", "src")
	for _, p := range r.Config.ResourceProfiles {
		d.Profiles.add(p.ID, p.ExecutorCores, p.ExecutorMemoryMB, p.OverheadMB, p.OffHeapMB, p.PySparkMemoryMB, p.TaskCPUs, orMap(p.ExecutorOther), orMap(p.TaskOther), src(p.Source))
	}
	d.Critical, d.CritJob, d.LogStats = orEmpty(r.Jobs.CriticalPath), r.Jobs.CriticalJob, r.EventLog
	d.Sources, d.SourceNote = []xSource{}, orEmpty(opt.SourceNotes)
	for _, sf := range opt.Sources {
		d.Sources = append(d.Sources, xSource{Path: sf.Path, Logged: sf.Logged, Lines: sf.Lines, Cut: sf.Cut})
	}
	d.BlockKinds = newTable("kind", "updates", "maxMem", "maxDisk")
	for _, k := range r.IO.BlockKinds {
		d.BlockKinds.add(k.Kind, k.Updates, k.MaxMemory, k.MaxDisk)
	}
	d.Exclusions = newTable("kind", "scope", "target", "stage", "stageAttempt", "time", "lifted", "failures", "src")
	for _, x := range r.Executors.Exclusions {
		d.Exclusions.add(x.Kind, x.Scope, x.Target, x.StageID, x.StageAttempt, unixMs(x.Time), unixMs(x.Lifted), x.Failures, src(x.Source))
	}
	d.RunTasks = newTable("task", "stage", "stageAttempt", "index", "partition", "attempt", "exec", "host", "launched", "locality", "spec", "src")
	for _, t := range r.Jobs.RunningTasks {
		d.RunTasks.add(t.TaskID, t.StageID, t.StageAttempt, t.Index, t.Partition, t.Attempt, t.ExecutorID, t.Host, unixMs(t.Launched), t.Locality, t.Speculative, src(t.Source))
	}
	d.RunCapped = r.Jobs.RunningCapped
	d.Gaps = [][4]any{}
	for _, g := range r.Jobs.DriverGaps {
		var before, after any
		if g.Before != nil {
			before = *g.Before
		}
		if g.After != nil {
			after = *g.After
		}
		d.Gaps = append(d.Gaps, [4]any{unixMs(g.Start), unixMs(g.End), before, after})
	}

	d.Runtime = newTable("group", "label", "value", "from", "explain", "missing")
	for _, row := range r.Config.Runtime {
		d.Runtime.add(row.Group, row.Label, row.Value, row.From, row.Explain, row.Missing)
	}
	for _, g := range r.Config.Groups {
		xg := xConfigGroup{Name: g.Name, Entries: [][]any{}}
		for _, e := range g.Entries {
			xg.Entries = append(xg.Entries, []any{e.Key, e.Value, e.Default, e.NonDefault, e.Explain})
		}
		d.Config = append(d.Config, xg)
	}

	if x == nil {
		d.Notes = append(d.Notes, "Per-task detail was not collected for this run, so stage summaries, samples and charts are missing.")
		return d
	}
	d.Collected, d.Limits, d.Shrinks, d.CellsCap = true, x.Limits, x.SampleShrinks, x.CellsCapped
	if x.SampleShrinks > 0 {
		d.Notes = append(d.Notes, fmt.Sprintf("This run has many tasks, so each stage keeps a smaller sample (the app-wide budget of %s sampled tasks halved every sample %d times). Charts drawn from samples say so.",
			model.Num(int64(x.Limits.MaxSampledTasks)), x.SampleShrinks))
	}
	if x.CellsCapped {
		d.Notes = append(d.Notes, fmt.Sprintf("Per-executor totals stop after %s stage × executor pairs, so later stages have no per-executor table.", model.Num(int64(x.Limits.MaxStageExecutorCells))))
	}
	taskRow := func(t model.TaskSample) []int64 {
		status := int64(0)
		switch t.Status {
		case model.StatusFailed:
			status = 1
		case "killed":
			status = 2
		}
		spec := int64(0)
		if t.Speculative {
			spec = 1
		}
		launch := t.LaunchMs
		if d.T0 > 0 && launch > 0 {
			launch -= d.T0
		}
		loc := int64(-1)
		for i, l := range localities {
			if t.Locality == l {
				loc = int64(i)
			}
		}
		return []int64{t.TaskID, int64(t.Index), int64(t.Attempt), execIdx(t.ExecutorID), status, spec, launch,
			t.DurationMs, t.RunTimeMs, t.GCTimeMs, t.DeserializeMs, t.FetchWaitMs, t.RecordsRead, t.InputBytes,
			t.ShuffleRead, t.ShuffleWrite, t.Spill, fileIdx(t.Source), t.Source.Line, int64(t.PartitionID), loc, t.SchedDelayMs, t.ResultSize}
	}
	for _, sd := range x.Stages {
		xd := xDetail{Metrics: map[string][7]int64{}, Hist: [][3]int64{}, Slowest: [][]int64{}, Sample: [][]int64{}, Cells: [][]int64{}, From: sd.SampledFrom}
		for k, q := range sd.Metrics {
			xd.Metrics[k] = [7]int64{q.Count, q.Sum, q.Min, q.P25, q.P50, q.P75, q.Max}
		}
		for _, b := range sd.Duration {
			xd.Hist = append(xd.Hist, [3]int64{b.Lo, b.Hi, b.Count})
		}
		for _, t := range sd.Slowest {
			xd.Slowest = append(xd.Slowest, taskRow(t))
		}
		for _, t := range sd.Sample {
			xd.Sample = append(xd.Sample, taskRow(t))
		}
		for _, c := range sd.Executors {
			t, p := c.Tasks, c.Peak
			xd.Cells = append(xd.Cells, []int64{execIdx(c.ExecutorID), t.Tasks, t.Succeeded, t.Failed, t.Killed, t.DurationMs, t.GCTimeMs,
				t.InputBytes, t.ShuffleReadBytes, t.ShuffleWriteBytes, t.DiskSpillBytes, p.JVMHeap, p.OnHeapExecution + p.OffHeapExecution})
		}
		d.Detail[strconv.Itoa(sd.ID)+"."+strconv.Itoa(sd.Attempt)] = xd
	}
	if len(x.Running.BusyMs) > 0 {
		d.Running = &xRunning{Start: unixMs(x.Running.Start), BucketMs: x.Running.BucketMs, Busy: x.Running.BusyMs}
	}
	for _, g := range x.SQL {
		nodes := make([]xNode, len(g.Nodes))
		for i, n := range g.Nodes {
			xn := xNode{Name: n.Name, Detail: n.Detail, Children: n.Children}
			xn.Metrics = metricRows(n.Metrics)
			nodes[i] = xn
		}
		d.Graphs[strconv.FormatInt(g.QueryID, 10)] = nodes
		if len(g.Adaptive) > 0 {
			d.Adaptive[strconv.FormatInt(g.QueryID, 10)] = metricRows(g.Adaptive)
		}
		if len(nodes) <= maxGraphNodes {
			var edges [][2]int // data flows from each child up to its parent
			for i, n := range g.Nodes {
				for _, c := range n.Children {
					edges = append(edges, [2]int{c, i})
				}
			}
			d.PlanLays[strconv.FormatInt(g.QueryID, 10)] = layered(len(nodes), edges)
		}
	}
	return d
}

func orEmpty[T any](v []T) []T {
	if v == nil {
		return []T{}
	}
	return v
}

func refNames(refs []model.DataRef) []string {
	out := []string{}
	for _, r := range refs {
		name := r.Name
		if r.Format != "" {
			name += " (" + r.Format + ")"
		}
		out = append(out, name)
	}
	return out
}

// stageFailures lists why a stage's tasks failed: kind, message, count,
// executors, whether Spark blamed the app for a lost executor (null when it
// did not say), and a stack trace for the first few reasons.
func stageFailures(st *model.Stage) [][]any {
	out := [][]any{}
	for i, f := range st.Failures {
		stack := ""
		if i < maxStackTraces {
			stack = capText(f.StackTrace, maxStackText)
		}
		var byApp any
		if f.ExitCausedByApp != nil {
			byApp = *f.ExitCausedByApp
		}
		out = append(out, []any{f.Kind, f.Message, f.Count, orEmpty(f.Executors), byApp, stack})
	}
	return out
}

func orMap[V any](m map[string]V) map[string]V {
	if m == nil {
		return map[string]V{}
	}
	return m
}

func isBarrier(st *model.Stage) bool {
	for _, r := range st.RDDs {
		if r.Barrier {
			return true
		}
	}
	return false
}

// stageOps lays out a stage's RDDs as a graph, each edge from a parent RDD
// to the RDD computed from it.
func stageOps(st *model.Stage) xStageOps {
	idx := map[int]int{}
	for i, r := range st.RDDs {
		idx[r.ID] = i
	}
	var edges [][2]int
	ops := xStageOps{RDDs: [][]any{}}
	for i, r := range st.RDDs {
		for _, p := range r.Parents {
			if pi, ok := idx[p]; ok {
				edges = append(edges, [2]int{pi, i})
			}
		}
		ops.RDDs = append(ops.RDDs, []any{r.ID, r.Name, r.Operation, r.Callsite, r.Partitions, r.CachedPartitions, r.StorageLevel, r.Barrier, r.Deterministic})
	}
	ops.Layout = layered(len(st.RDDs), edges)
	return ops
}

// metricRows encodes SQL metrics as name, type, value (null when not
// recorded), then tasks, min, median, max, max task and max stage when
// tasks reported them.
func metricRows(ms []model.SQLMetric) [][]any {
	var out [][]any
	for _, m := range ms {
		var v any
		if m.Known {
			v = m.Value
		}
		row := []any{m.Name, m.Type, v}
		if m.Tasks > 0 {
			row = append(row, m.Tasks, m.Min, m.Median, m.Max, m.MaxTaskID, m.MaxStage)
		}
		out = append(out, row)
	}
	return out
}

// maxOptimizerRulesShown is how many of EMR's optimizer rules the page lists
// per query, slowest first; report.json keeps them all.
const maxOptimizerRulesShown = 30

// optimizerRows encodes EMR's optimizer report: total ns, rules run, rules
// that changed the plan, the slowest rules (name, ns, runs, effective runs,
// effective ns) and any counters, timers or stats. Nil when absent.
func optimizerRows(o *model.OptimizerStats) any {
	if o == nil {
		return nil
	}
	rules := [][]any{}
	for i, r := range o.Rules {
		if i == maxOptimizerRulesShown {
			break
		}
		rules = append(rules, []any{r.Name, r.TimeNs, r.Runs, r.EffectiveRuns, r.EffectiveTimeNs})
	}
	return []any{o.TotalNs, o.RulesRun, o.RulesUseful, rules, orMap(o.Other)}
}

// xSource is an embedded source file and the logged names that map to it.
type xSource struct {
	Path   string   `json:"path"`
	Logged []string `json:"logged"`
	Lines  []string `json:"lines"`
	Cut    bool     `json:"cut,omitempty"`
}

// codeRows encodes code locations as file, line, function, action.
func codeRows(cs []model.CodeLocation) [][]any {
	out := [][]any{}
	for _, c := range cs {
		out = append(out, []any{c.File, c.Line, c.Function, c.Action})
	}
	return out
}

// splitRow is a stage's model.TimeSplit in the order the page reads it:
// scheduler delay, deserializing, computing, GC, shuffle fetch wait,
// shuffle write, result, other.
func splitRow(p model.TimeSplit) []int64 {
	return []int64{p.SchedulerDelayMs, p.DeserializeMs, p.ComputeMs, p.GCMs, p.ShuffleFetchMs, p.ShuffleWriteMs, p.ResultMs, p.OtherMs}
}
