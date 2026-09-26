package eventlog

import (
	"container/heap"
	"math/rand/v2"
	"sort"
	"strconv"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/redact"
)

// explorerAcc collects the explorer page's data while the log streams
// (SPEC §6, Explorer page). Every part is bounded by model.ExplorerLimits or
// by fixed caps, so memory stays flat however long the log is.
type explorerAcc struct {
	lim         model.ExplorerLimits
	stages      map[stageKey]*stageDetailAcc
	sampleCap   int // per-stage reservoir size, halved when over budget
	slowestCap  int // per-stage slowest list size, halved when over budget
	held        int // tasks held in all samples and slowest lists
	shrinks     int
	cells       int
	cellsCapped bool
	running     runningAcc
	plans       map[int64]*planGraph
	wanted      map[int64]bool  // accumulator IDs of kept plans' metrics
	accVals     map[int64]int64 // latest value of each wanted accumulator
}

// Floors for the per-stage caps: the budget never shrinks a stage below these.
const (
	minSample      = 10
	minSlowest     = 5
	maxPlanNodes   = 2000
	runningBuckets = 2000
	histogramBins  = 40
)

// stageMetrics names the per-stage distributions, in the order of
// stageDetailAcc.metrics and metricValues.
var stageMetrics = []string{
	model.MetricDuration, model.MetricRunTime, model.MetricGCTime, model.MetricDeserialize,
	model.MetricRecordsRead, model.MetricInputBytes, model.MetricShuffleRead, model.MetricShuffleRecsRead,
	model.MetricFetchWait, model.MetricShuffleWrite, model.MetricMemorySpill, model.MetricDiskSpill,
	model.MetricPeakExecMemory, model.MetricOutputBytes,
}

func metricValues(t *model.TaskTotals) [14]int64 {
	return [14]int64{
		t.DurationMs, t.RunTimeMs, t.GCTimeMs, t.DeserializeMs,
		t.InputRecords + t.ShuffleReadRecords, t.InputBytes, t.ShuffleReadBytes, t.ShuffleReadRecords,
		t.ShuffleFetchWaitMs, t.ShuffleWriteBytes, t.MemorySpillBytes, t.DiskSpillBytes,
		t.PeakExecutionMemory, t.OutputBytes,
	}
}

type stageDetailAcc struct {
	metrics [14]distAcc // successful tasks only, as in the History Server
	slowest slowHeap
	sample  []model.TaskSample
	seen    int64
	rng     *rand.Rand
	cells   map[string]*model.StageExecutorCell
}

func newExplorerAcc(lim model.ExplorerLimits) *explorerAcc {
	lim = lim.WithDefaults()
	return &explorerAcc{
		lim: lim, stages: map[stageKey]*stageDetailAcc{},
		sampleCap: lim.SamplePerStage, slowestCap: lim.SlowestPerStage,
		running: runningAcc{width: 100},
		plans:   map[int64]*planGraph{}, wanted: map[int64]bool{}, accVals: map[int64]int64{},
	}
}

func (x *explorerAcc) stage(k stageKey) *stageDetailAcc {
	s := x.stages[k]
	if s == nil {
		// A fixed seed per stage attempt makes reruns produce the same page.
		s = &stageDetailAcc{rng: rand.New(rand.NewPCG(uint64(k.id), uint64(k.attempt))), cells: map[string]*model.StageExecutorCell{}}
		x.stages[k] = s
	}
	return s
}

// task folds one finished task attempt. t holds that task's metrics alone.
func (x *explorerAcc) task(k stageKey, e *taskEndEvent, t *model.TaskTotals, peak *model.PeakMemory, src model.Source) {
	s := x.stage(k)
	if e.Info.LaunchTime > 0 && e.Info.FinishTime >= e.Info.LaunchTime {
		x.running.add(e.Info.LaunchTime, e.Info.FinishTime)
	}
	if t.Succeeded == 1 {
		v := metricValues(t)
		for i := range s.metrics {
			s.metrics[i].add(v[i])
		}
	}
	execID := redact.Text(e.Info.ExecutorID)
	if c := x.cell(s, execID); c != nil {
		c.Tasks.Add(*t)
		if peak != nil {
			c.Peak.Merge(*peak)
		}
	}

	status := model.StatusSucceeded
	switch {
	case t.Failed == 1:
		status = model.StatusFailed
	case t.Killed == 1:
		status = "killed"
	}
	ts := model.TaskSample{
		TaskID: e.Info.TaskID, Index: e.Info.Index, Attempt: e.Info.Attempt, ExecutorID: execID,
		Status: status, Speculative: e.Info.Speculative, LaunchMs: e.Info.LaunchTime,
		DurationMs: t.DurationMs, RunTimeMs: t.RunTimeMs, GCTimeMs: t.GCTimeMs, DeserializeMs: t.DeserializeMs,
		FetchWaitMs: t.ShuffleFetchWaitMs, RecordsRead: t.InputRecords + t.ShuffleReadRecords,
		InputBytes: t.InputBytes, ShuffleRead: t.ShuffleReadBytes, ShuffleWrite: t.ShuffleWriteBytes,
		Spill: t.MemorySpillBytes + t.DiskSpillBytes, Source: src,
	}

	// Slowest tasks: keep the top slowestCap by duration.
	if s.slowest.Len() < x.slowestCap {
		heap.Push(&s.slowest, ts)
		x.held++
	} else if slower(ts, s.slowest[0]) {
		s.slowest[0] = ts
		heap.Fix(&s.slowest, 0)
	}
	// Uniform sample: reservoir sampling (Algorithm R).
	s.seen++
	if len(s.sample) < x.sampleCap {
		s.sample = append(s.sample, ts)
		x.held++
	} else if j := s.rng.Int64N(s.seen); j < int64(x.sampleCap) {
		s.sample[j] = ts
	}
	if x.held > x.lim.MaxSampledTasks {
		x.shrink()
	}
}

// shrink halves every stage's sample and slowest list to get back under the
// app-wide budget. A random half of a uniform sample is still uniform, so
// reservoir sampling carries on correctly with the smaller size.
func (x *explorerAcc) shrink() {
	for x.held > x.lim.MaxSampledTasks && (x.sampleCap > minSample || x.slowestCap > minSlowest) {
		x.sampleCap = max(minSample, x.sampleCap/2)
		x.slowestCap = max(minSlowest, x.slowestCap/2)
		x.shrinks++
		x.held = 0
		for _, s := range x.stages {
			if len(s.sample) > x.sampleCap {
				for i := 0; i < x.sampleCap; i++ { // partial Fisher–Yates
					j := i + s.rng.IntN(len(s.sample)-i)
					s.sample[i], s.sample[j] = s.sample[j], s.sample[i]
				}
				s.sample = append([]model.TaskSample(nil), s.sample[:x.sampleCap]...)
			}
			for s.slowest.Len() > x.slowestCap {
				heap.Pop(&s.slowest)
			}
			x.held += len(s.sample) + s.slowest.Len()
		}
	}
}

func (x *explorerAcc) cell(s *stageDetailAcc, execID string) *model.StageExecutorCell {
	c := s.cells[execID]
	if c == nil {
		if x.cells >= x.lim.MaxStageExecutorCells {
			x.cellsCapped = true
			return nil
		}
		c = &model.StageExecutorCell{ExecutorID: execID}
		s.cells[execID] = c
		x.cells++
	}
	return c
}

// stagePeak merges a SparkListenerStageExecutorMetrics peak into the cell.
func (x *explorerAcc) stagePeak(k stageKey, execID string, peak model.PeakMemory) {
	if c := x.cell(x.stage(k), redact.Text(execID)); c != nil {
		c.Peak.Merge(peak)
	}
}

// accumulables records SQL metric totals from a completed stage.
func (x *explorerAcc) accumulables(list []accumulable) {
	for _, a := range list {
		if a.Metadata != "sql" || !x.wanted[a.ID] {
			continue
		}
		if v, ok := a.int(); ok {
			x.accVals[a.ID] = v
		}
	}
}

// driverAccums records SQL metrics the driver updated itself.
func (x *explorerAcc) driverAccums(updates [][]int64) {
	for _, u := range updates {
		if len(u) == 2 && x.wanted[u[0]] {
			x.accVals[u[0]] = u[1]
		}
	}
}

type planGraph struct {
	nodes     []model.SQLNode
	accIDs    [][]int64 // per node, the accumulator ID of each metric
	truncated bool
}

// plan keeps the latest plan of a query; adaptive execution replaces it.
// At most maxPlans queries keep a graph.
func (x *explorerAcc) plan(id int64, root planNode, maxPlans int) {
	if x.plans[id] == nil && len(x.plans) >= maxPlans {
		return
	}
	g := &planGraph{}
	var walk func(n *planNode) int
	walk = func(n *planNode) int {
		if len(g.nodes) >= maxPlanNodes {
			g.truncated = true
			return -1
		}
		i := len(g.nodes)
		node := model.SQLNode{ID: i, Name: redact.Text(truncate(n.NodeName, 200))}
		if d := redact.Text(truncate(firstLine(n.SimpleString), 300)); d != node.Name {
			node.Detail = d
		}
		ids := make([]int64, 0, len(n.Metrics))
		for _, m := range n.Metrics {
			node.Metrics = append(node.Metrics, model.SQLMetric{Name: m.Name, Type: m.Type})
			ids = append(ids, m.AccumulatorID)
			x.wanted[m.AccumulatorID] = true
		}
		g.nodes = append(g.nodes, node)
		g.accIDs = append(g.accIDs, ids)
		for c := range n.Children {
			if ci := walk(&n.Children[c]); ci >= 0 {
				g.nodes[i].Children = append(g.nodes[i].Children, ci)
			}
		}
		return i
	}
	walk(&root)
	x.plans[id] = g
}

// build turns the accumulators into the model, in the order of l's stages
// and queries.
func (x *explorerAcc) build(l *model.EventLog) *model.Explorer {
	out := &model.Explorer{Limits: x.lim, SampleShrinks: x.shrinks, CellsCapped: x.cellsCapped, Running: x.running.result()}
	for _, st := range l.Stages {
		d := model.StageDetail{ID: st.ID, Attempt: st.Attempt, Metrics: map[string]model.Quartiles{}}
		if s := x.stages[stageKey{st.ID, st.Attempt}]; s != nil {
			for i, name := range stageMetrics {
				if q := s.metrics[i].quartiles(); q.Count > 0 {
					d.Metrics[name] = q
				}
			}
			d.Duration = s.metrics[0].histogram(histogramBins)
			d.Slowest = make([]model.TaskSample, s.slowest.Len())
			copy(d.Slowest, s.slowest)
			sort.Slice(d.Slowest, func(i, j int) bool { return slower(d.Slowest[i], d.Slowest[j]) })
			d.Sample = append([]model.TaskSample(nil), s.sample...)
			sort.Slice(d.Sample, func(i, j int) bool { return d.Sample[i].TaskID < d.Sample[j].TaskID })
			d.SampledFrom = s.seen
			for _, c := range s.cells {
				d.Executors = append(d.Executors, *c)
			}
			sort.Slice(d.Executors, func(i, j int) bool { return lessNumeric(d.Executors[i].ExecutorID, d.Executors[j].ExecutorID) })
		}
		out.Stages = append(out.Stages, d)
	}
	for _, q := range l.SQL {
		g := x.plans[q.ID]
		if g == nil {
			continue
		}
		for i := range g.nodes {
			for j, id := range g.accIDs[i] {
				v, ok := x.accVals[id]
				g.nodes[i].Metrics[j].Value, g.nodes[i].Metrics[j].Known = v, ok
			}
		}
		out.SQL = append(out.SQL, model.SQLGraph{QueryID: q.ID, Nodes: g.nodes, Truncated: g.truncated})
	}
	return out
}

// slower orders tasks by duration, then by task ID so ties are stable.
func slower(a, b model.TaskSample) bool {
	if a.DurationMs != b.DurationMs {
		return a.DurationMs > b.DurationMs
	}
	return a.TaskID < b.TaskID
}

// slowHeap is a min-heap on slower, so the root is the fastest kept task.
type slowHeap []model.TaskSample

func (h slowHeap) Len() int           { return len(h) }
func (h slowHeap) Less(i, j int) bool { return slower(h[j], h[i]) }
func (h slowHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *slowHeap) Push(v any)        { *h = append(*h, v.(model.TaskSample)) }
func (h *slowHeap) Pop() any {
	old := *h
	v := old[len(old)-1]
	*h = old[:len(old)-1]
	return v
}

// runningAcc folds task run intervals into runningBuckets equal time buckets
// that double in width when a task falls past the last one. Per bucket it
// keeps the launches and ends in it and the sum of their offsets into it,
// which is enough to recover the exact task-milliseconds run in each bucket
// and to merge buckets pairwise without losing anything.
type runningAcc struct {
	origin int64 // Unix ms of bucket 0; 0 until known
	width  int64
	nL, nE [runningBuckets]int64
	sL, sE [runningBuckets]int64
	used   bool
}

// setOrigin anchors bucket 0 at the application start, if nothing has
// been added yet.
func (r *runningAcc) setOrigin(ms int64) {
	if !r.used && ms > 0 {
		r.origin = ms
	}
}

func (r *runningAcc) add(launch, finish int64) {
	if r.origin == 0 {
		r.origin = launch
	}
	r.used = true
	launch, finish = max(launch, r.origin)-r.origin, max(finish, r.origin)-r.origin
	for finish/r.width >= runningBuckets {
		r.merge()
	}
	i, j := launch/r.width, finish/r.width
	r.nL[i]++
	r.sL[i] += launch - i*r.width
	r.nE[j]++
	r.sE[j] += finish - j*r.width
}

func (r *runningAcc) merge() {
	w := r.width
	for j := 0; j < runningBuckets/2; j++ {
		a, b := 2*j, 2*j+1
		r.nL[j], r.sL[j] = r.nL[a]+r.nL[b], r.sL[a]+r.sL[b]+w*r.nL[b]
		r.nE[j], r.sE[j] = r.nE[a]+r.nE[b], r.sE[a]+r.sE[b]+w*r.nE[b]
	}
	for j := runningBuckets / 2; j < runningBuckets; j++ {
		r.nL[j], r.sL[j], r.nE[j], r.sE[j] = 0, 0, 0, 0
	}
	r.width *= 2
}

func (r *runningAcc) result() model.RunningTasks {
	if !r.used {
		return model.RunningTasks{}
	}
	last := 0
	for i := range runningBuckets {
		if r.nL[i] > 0 || r.nE[i] > 0 {
			last = i
		}
	}
	w := r.width
	busy := make([]int64, last+1)
	var before int64 // tasks launched minus tasks ended before bucket i
	for i := range busy {
		busy[i] = w*before + (w*r.nL[i] - r.sL[i]) - (w*r.nE[i] - r.sE[i])
		before += r.nL[i] - r.nE[i]
	}
	return model.RunningTasks{Start: ms(r.origin), BucketMs: w, BusyMs: busy}
}

// accumulable is one entry of a stage's "Accumulables". SQL metrics carry
// Metadata "sql" and a string Value; Spark's own task metrics use numbers.
type accumulable struct {
	ID       int64  `json:"ID"`
	Value    any    `json:"Value"`
	Metadata string `json:"Metadata"`
}

func (a accumulable) int() (int64, bool) {
	switch v := a.Value.(type) {
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		return n, err == nil
	case float64:
		return int64(v), true
	}
	return 0, false
}
