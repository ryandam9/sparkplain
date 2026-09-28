package eventlog

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

func parseExplorer(t *testing.T, name, app string, lim model.ExplorerLimits) *model.EventLog {
	t.Helper()
	in, err := Resolve(filepath.Join(fixtures, name), app, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	l, err := Parse(context.Background(), in, Options{Explorer: &lim})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestExplorerOffByDefault(t *testing.T) {
	if l := parseFixture(t, mainApp, mainApp); l.Explorer != nil {
		t.Error("explorer data collected without being asked for")
	}
}

// The explorer's numbers must agree with the report's, which were checked
// against the fixture independently.
func TestExplorerMainFixture(t *testing.T) {
	l := parseExplorer(t, mainApp, mainApp, model.ExplorerLimits{})
	x := l.Explorer
	if x == nil || len(x.Stages) != len(l.Stages) {
		t.Fatalf("want %d stage details, got %+v", len(l.Stages), x)
	}
	var taskMs int64
	peakSeen := false
	for i, st := range l.Stages {
		d := x.Stages[i]
		if d.ID != st.ID || d.Attempt != st.Attempt {
			t.Fatalf("stage %d: detail is for %d.%d", st.ID, d.ID, d.Attempt)
		}
		taskMs += st.Totals.DurationMs
		if st.Totals.Tasks == 0 {
			continue
		}
		if d.SampledFrom != st.Totals.Tasks {
			t.Errorf("stage %d: sampled from %d, stage has %d tasks", st.ID, d.SampledFrom, st.Totals.Tasks)
		}
		if q := d.Metrics[model.MetricDuration]; q.Count != st.TaskDuration.Count || q.P50 != st.TaskDuration.P50 || q.Max != st.TaskDuration.Max {
			t.Errorf("stage %d: duration quartiles %+v disagree with %+v", st.ID, q, st.TaskDuration)
		}
		if q := d.Metrics[model.MetricRecordsRead]; q.P50 != st.TaskRecords.P50 {
			t.Errorf("stage %d: rows median %d, want %d", st.ID, q.P50, st.TaskRecords.P50)
		}
		var cellTasks, bins int64
		for _, c := range d.Executors {
			cellTasks += c.Tasks.Tasks
		}
		for _, b := range d.Duration {
			bins += b.Count
			if b.Lo > b.Hi {
				t.Errorf("stage %d: bad bin %+v", st.ID, b)
			}
		}
		if cellTasks != st.Totals.Tasks {
			t.Errorf("stage %d: executor cells hold %d tasks, want %d", st.ID, cellTasks, st.Totals.Tasks)
		}
		if bins != st.TaskDuration.Count {
			t.Errorf("stage %d: histogram holds %d tasks, want %d", st.ID, bins, st.TaskDuration.Count)
		}
		if int64(len(d.Sample)) != min(st.Totals.Tasks, 1000) || !sort.SliceIsSorted(d.Slowest, func(a, b int) bool { return slower(d.Slowest[a], d.Slowest[b]) }) {
			t.Errorf("stage %d: sample %d, slowest not in order", st.ID, len(d.Sample))
		}
		// Each sampled task carries its own peak execution memory; with
		// every task sampled, the largest is the stage's.
		var peak int64
		for _, ts := range d.Sample {
			peak = max(peak, ts.PeakExec)
		}
		if peak > st.Totals.PeakExecutionMemory || int64(len(d.Sample)) == st.Totals.Tasks && peak != st.Totals.PeakExecutionMemory {
			t.Errorf("stage %d: sampled peak execution memory %d, stage peak %d", st.ID, peak, st.Totals.PeakExecutionMemory)
		}
		if st.Totals.PeakExecutionMemory > 0 {
			peakSeen = true
		}
	}
	if !peakSeen {
		t.Error("no stage recorded peak execution memory, so the per-task check proved nothing")
	}
	var busy int64
	for _, b := range x.Running.BusyMs {
		busy += b
	}
	if busy != taskMs {
		t.Errorf("running buckets hold %d task-ms, tasks ran %d", busy, taskMs)
	}
	if !x.Running.Start.Equal(l.Application.Start) {
		t.Errorf("running buckets start at %v, app at %v", x.Running.Start, l.Application.Start)
	}
	rows := false
	for _, g := range x.SQL {
		for _, n := range g.Nodes {
			for _, m := range n.Metrics {
				if m.Name == "number of output rows" && m.Known && m.Value > 0 {
					rows = true
				}
			}
		}
	}
	if len(x.SQL) == 0 || !rows {
		t.Errorf("no SQL operator row counts resolved (%d graphs)", len(x.SQL))
	}
	b, err := json.Marshal(x)
	if err != nil {
		t.Fatal(err)
	}
	assertNoSecrets(t, string(b))
	if b2, _ := json.Marshal(l); strings.Contains(string(b2), "sampleShrinks") {
		t.Error("explorer data leaked into the event log's JSON")
	}
}

// The bucket arithmetic must give the same task-ms per bucket as
// integrating every interval directly, including after many merges.
func TestRunningMatchesBruteForce(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	var r runningAcc
	r.width = 100
	const origin = 1_790_000_000_000
	r.setOrigin(origin)
	type iv struct{ a, b int64 }
	var ivs []iv
	for range 5000 {
		a := origin + rng.Int64N(3_600_000)
		b := a + rng.Int64N(600_000)
		ivs = append(ivs, iv{a, b})
		r.add(a, b)
	}
	got := r.result()
	if got.BucketMs == 100 {
		t.Fatal("test should force merges")
	}
	for i, busy := range got.BusyMs {
		lo := origin + int64(i)*got.BucketMs
		hi := lo + got.BucketMs
		var want int64
		for _, v := range ivs {
			want += max(0, min(v.b, hi)-max(v.a, lo))
		}
		if busy != want {
			t.Fatalf("bucket %d: %d task-ms, want %d", i, busy, want)
		}
	}
}

func synthTask(id int64, dur int64, exec string) (*taskEndEvent, *model.TaskTotals) {
	e := &taskEndEvent{Info: taskInfo{TaskID: id, ExecutorID: exec, LaunchTime: 1000 + id, FinishTime: 1000 + id + dur}}
	return e, &model.TaskTotals{Tasks: 1, Succeeded: 1, DurationMs: dur, RunTimeMs: dur}
}

// Over budget, every stage's sample halves but stays a uniform sample, the
// slowest list stays exactly the slowest, and reruns are identical.
func TestSampleBudget(t *testing.T) {
	run := func() *explorerAcc {
		x := newExplorerAcc(model.ExplorerLimits{SlowestPerStage: 20, SamplePerStage: 80, MaxSampledTasks: 1000})
		rng := rand.New(rand.NewPCG(3, 4))
		for s := range 20 {
			for i := range 2000 {
				e, tt := synthTask(int64(s*10_000+i), rng.Int64N(100_000), "1")
				x.task(stageKey{s, 0}, e, tt, nil, model.Source{})
			}
		}
		return x
	}
	x := run()
	if x.shrinks == 0 || x.held > 1000 {
		t.Fatalf("shrinks %d, held %d", x.shrinks, x.held)
	}
	var mean float64
	for k, s := range x.stages {
		if len(s.sample) != x.sampleCap || s.slowest.Len() != x.slowestCap {
			t.Errorf("stage %d: sample %d (cap %d), slowest %d (cap %d)", k.id, len(s.sample), x.sampleCap, s.slowest.Len(), x.slowestCap)
		}
		for _, ts := range s.sample {
			mean += float64(ts.TaskID % 10_000)
		}
	}
	mean /= float64(20 * x.sampleCap)
	if mean < 850 || mean > 1150 { // uniform over 0..1999 has mean 999.5
		t.Errorf("sample looks biased: mean index %.0f", mean)
	}

	// The slowest list is exactly the top of each stage.
	rng := rand.New(rand.NewPCG(3, 4))
	for s := range 20 {
		durs := make([]int64, 2000)
		for i := range durs {
			durs[i] = rng.Int64N(100_000)
		}
		sort.Slice(durs, func(i, j int) bool { return durs[i] > durs[j] })
		got := x.build(&model.EventLog{Stages: []*model.Stage{{ID: s}}}).Stages[0].Slowest
		for i, ts := range got {
			if ts.DurationMs != durs[i] {
				t.Fatalf("stage %d: slowest[%d] = %d, want %d", s, i, ts.DurationMs, durs[i])
			}
		}
	}

	y := run()
	for k := range x.stages {
		if !reflect.DeepEqual(x.stages[k].sample, y.stages[k].sample) {
			t.Fatalf("stage %d: sample differs between runs", k.id)
		}
	}
}

func TestStageExecutorCellCap(t *testing.T) {
	x := newExplorerAcc(model.ExplorerLimits{MaxStageExecutorCells: 3})
	for i := range 5 {
		e, tt := synthTask(int64(i), 10, string(rune('a'+i)))
		x.task(stageKey{0, 0}, e, tt, nil, model.Source{})
	}
	if !x.cellsCapped || x.cells != 3 {
		t.Errorf("capped %v with %d cells", x.cellsCapped, x.cells)
	}
}

func TestQuartilesAndHistogram(t *testing.T) {
	var d distAcc
	for v := int64(1); v <= 10_000; v++ {
		d.add(v)
	}
	q := d.quartiles()
	for _, c := range []struct{ got, want int64 }{{q.P25, 2500}, {q.P50, 5000}, {q.P75, 7500}} {
		if diff := float64(c.got-c.want) / float64(c.want); diff > 0.035 || diff < -0.035 {
			t.Errorf("quantile %d, want about %d", c.got, c.want)
		}
	}
	bins := d.histogram(40)
	var n int64
	for i, b := range bins {
		n += b.Count
		if b.Lo > b.Hi || (i > 0 && b.Lo <= bins[i-1].Hi) {
			t.Errorf("bin %d out of order: %+v", i, b)
		}
	}
	if n != 10_000 || len(bins) > 40 || bins[0].Lo != 1 || bins[len(bins)-1].Hi != 10_000 {
		t.Errorf("%d bins holding %d values: first %+v last %+v", len(bins), n, bins[0], bins[len(bins)-1])
	}
}

// TestExplorerBudget checks the SPEC §8 budget with explorer data on. It runs
// only when SPARKPLAIN_BIG_LOG names a log, for example after `make bench-log`:
//
//	SPARKPLAIN_BIG_LOG=$PWD/out/big.log go test -run TestExplorerBudget -v ./internal/sparkplain/eventlog/
func TestExplorerBudget(t *testing.T) {
	path := os.Getenv("SPARKPLAIN_BIG_LOG")
	if path == "" {
		t.Skip("set SPARKPLAIN_BIG_LOG to a large event log")
	}
	app := os.Getenv("SPARKPLAIN_BIG_APP")
	if app == "" {
		app = mainApp
	}
	for _, on := range []bool{false, true} {
		in, err := Resolve(path, app, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		opt := Options{}
		if on {
			opt.Explorer = &model.ExplorerLimits{}
		}
		start := time.Now()
		l, err := Parse(context.Background(), in, opt)
		in.Close()
		if err != nil {
			t.Fatal(err)
		}
		took := time.Since(start)
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		detail := ""
		if x := l.Explorer; x != nil {
			var held, cells int
			for _, d := range x.Stages {
				held += len(d.Sample) + len(d.Slowest)
				cells += len(d.Executors)
			}
			detail = fmt.Sprintf(", %d sampled tasks after %d shrinks, %d stage × executor cells", held, x.SampleShrinks, cells)
		}
		t.Logf("explorer %-5v: %d stages in %s, live heap %d MiB, peak RSS so far %s%s",
			on, len(l.Stages), took.Round(time.Millisecond), ms.HeapAlloc>>20, peakRSS(), detail)
		if took > time.Minute {
			t.Errorf("parse took %s, budget is 60 s", took)
		}
	}
}

// peakRSS reads the process's peak resident set on Linux.
func peakRSS() string {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return "unknown"
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "VmHWM:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "VmHWM:"))
		}
	}
	return "unknown"
}
