package eventlog

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// rawTaskEnds decodes a plain fixture's TaskEnd events generically, so the
// tests below compute expectations without the parser's own structs.
func rawTaskEnds(t *testing.T, name string) []map[string]any {
	t.Helper()
	f, err := os.Open(filepath.Join(fixtures, name))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []map[string]any
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for sc.Scan() {
		var e map[string]any
		if json.Unmarshal(sc.Bytes(), &e) == nil && e["Event"] == "SparkListenerTaskEnd" {
			out = append(out, e)
		}
	}
	return out
}

func num(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	}
	return 0
}

func obj(v any, k string) map[string]any {
	m, _ := v.(map[string]any)
	o, _ := m[k].(map[string]any)
	return o
}

// Scheduler delay, fetch time, result size and locality must match the Spark
// UI's definitions, computed here from the raw events.
func TestTaskTimingSplit(t *testing.T) {
	t.Parallel()
	const name = "application_1790380000000_0046"
	type want struct{ getting, delay, result, process, node int64 }
	exp := map[stageKey]*want{}
	for _, e := range rawTaskEnds(t, name) {
		k := stageKey{int(num(e["Stage ID"])), int(num(e["Stage Attempt ID"]))}
		w := exp[k]
		if w == nil {
			w = &want{}
			exp[k] = w
		}
		info, m := obj(e, "Task Info"), obj(e, "Task Metrics")
		launch, finish, get := num(info["Launch Time"]), num(info["Finish Time"]), num(info["Getting Result Time"])
		var fetch int64
		if get > 0 {
			fetch = finish - get
		}
		w.getting += fetch
		w.delay += max(0, finish-launch-num(m["Executor Run Time"])-num(m["Executor Deserialize Time"])-num(m["Result Serialization Time"])-fetch)
		w.result += num(m["Result Size"])
		switch info["Locality"] {
		case "PROCESS_LOCAL":
			w.process++
		case "NODE_LOCAL":
			w.node++
		}
	}
	l := parseFixture(t, name, name)
	var fetched int64
	for _, st := range l.Stages {
		w := exp[stageKey{st.ID, st.Attempt}]
		if w == nil {
			continue
		}
		got := want{st.Totals.GettingResultMs, st.Totals.SchedulerDelayMs, st.Totals.ResultSizeBytes, st.Totals.LocalityProcess, st.Totals.LocalityNode}
		if got != *w {
			t.Errorf("stage %d.%d: got %+v, want %+v", st.ID, st.Attempt, got, *w)
		}
		fetched += got.getting
		if st.TaskType != "ResultTask" && st.TaskType != "ShuffleMapTask" {
			t.Errorf("stage %d: task type %q", st.ID, st.TaskType)
		}
	}
	if fetched == 0 {
		t.Error("the fixture's large results should show fetch time")
	}
}

// Every distinct failure keeps its first full stack trace, redacted, and a
// lost executor says whether Spark blamed the application.
func TestFailureDetail(t *testing.T) {
	t.Parallel()
	l := parseFixture(t, mainApp, mainApp)
	var traces, lost int
	for _, st := range l.Stages {
		for _, f := range st.Failures {
			if f.StackTrace != "" {
				traces++
				if len(f.StackTrace) > maxStackTrace+8 {
					t.Errorf("stack trace of %d bytes", len(f.StackTrace))
				}
			}
			if f.Kind == "ExecutorLostFailure" {
				lost++
				if f.ExitCausedByApp == nil || !*f.ExitCausedByApp {
					t.Errorf("lost executor failure should record that the app caused the exit: %+v", f)
				}
			}
		}
	}
	if traces == 0 || lost == 0 {
		t.Errorf("%d stack traces, %d lost-executor failures", traces, lost)
	}
	b, _ := json.Marshal(l)
	assertNoSecrets(t, string(b))
}

// Per-operator SQL spreads match the per-task updates in the raw log.
func TestSQLOperatorSpreads(t *testing.T) {
	t.Parallel()
	const name = "application_1790380000000_0049"
	updates := map[int64][]int64{}
	for _, e := range rawTaskEnds(t, name) {
		accs, _ := obj(e, "Task Info")["Accumulables"].([]any)
		for _, a := range accs {
			m := a.(map[string]any)
			if m["Metadata"] == "sql" {
				updates[num(m["ID"])] = append(updates[num(m["ID"])], num(m["Update"]))
			}
		}
	}
	l := parseExplorer(t, name, name, model.ExplorerLimits{})
	checked := 0
	for _, g := range l.Explorer.SQL {
		for _, n := range g.Nodes {
			for _, m := range n.Metrics {
				if m.Tasks == 0 {
					continue
				}
				var vals []int64
				for id, v := range updates { // find the accumulator by its per-task count and max
					if int64(len(v)) == m.Tasks && maxOf(v) == m.Max && minOf(v) == m.Min {
						vals = updates[id]
						break
					}
				}
				if vals == nil {
					t.Errorf("%s/%s: no accumulator in the raw log has %d updates from %d to %d", n.Name, m.Name, m.Tasks, m.Min, m.Max)
					continue
				}
				sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
				if m.Tasks <= 64 && m.Median != vals[(len(vals)+1)/2-1] {
					t.Errorf("%s/%s: median %d, want %d", n.Name, m.Name, m.Median, vals[(len(vals)+1)/2-1])
				}
				checked++
			}
		}
	}
	if checked < 10 {
		t.Errorf("only %d operator metrics had per-task spreads", checked)
	}
}

func maxOf(v []int64) int64 {
	m := v[0]
	for _, x := range v {
		m = max(m, x)
	}
	return m
}

func minOf(v []int64) int64 {
	m := v[0]
	for _, x := range v {
		m = min(m, x)
	}
	return m
}

// GC counters and unified memory reach the executors' peaks.
func TestExecutorGCCounters(t *testing.T) {
	t.Parallel()
	l := parseFixture(t, mainApp, mainApp)
	var minor int64
	for _, x := range l.Executors {
		minor += x.Peak.MinorGCCount
	}
	if minor == 0 {
		t.Error("no minor GC counts recorded")
	}
	if !strings.Contains(stackText([]stackFrame{{Class: "a.B", Method: "c", File: "B.scala", Line: 3}}), "at a.B.c(B.scala:3)") {
		t.Error("stack frames not formatted like the JVM prints them")
	}
}
