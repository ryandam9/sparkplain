package analyze

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

var flowT0 = time.Unix(1_790_000_000, 0).UTC()

// flowTask is a task story on executor exec, with its steps at seconds
// after flowT0.
func flowTask(tid int64, stage int, start, end int, steps ...model.TaskStep) model.TaskLog {
	t := model.TaskLog{TaskID: tid, Stage: stage, Partition: int(tid), Start: flowT0.Add(time.Duration(start) * time.Second), End: flowT0.Add(time.Duration(end) * time.Second),
		Outcome: "finished", TiedBy: "thread", Source: model.Source{Line: tid * 10}}
	for i, s := range steps {
		s.Line = tid*10 + int64(i) + 1
		switch s.Kind {
		case model.StepShuffle:
			t.ShuffleReads++
			t.ShuffleBytes += s.Bytes
			t.ShuffleRemoteBytes += s.Remote
			t.ShuffleLocalBytes += s.Bytes - s.Remote
		case model.StepSpill:
			t.Spills++
			t.SpillBytes += s.Bytes
		case model.StepCommit:
			t.Commits++
			t.CommitMs += s.Ms
		case model.StepCache:
			t.CachedBlocks++
			t.CachedBytes += s.Bytes
		case model.StepDrop:
			t.Dropped += s.N
		case model.StepNoRoom:
			t.NotCached++
		case model.StepBroadcast:
			t.Broadcasts++
			t.BroadcastBytes += s.Bytes
		case model.StepBcastRead:
			t.BroadcastMs += s.Ms
		}
		t.Steps = append(t.Steps, s)
	}
	return t
}

func at(sec int) time.Time { return flowT0.Add(time.Duration(sec) * time.Second) }

// runFlows runs the analysis on executor logs holding these task stories,
// with no event log, and returns the report and its findings by rule.
func runFlows(tasks map[string][]model.TaskLog) (*model.Report, map[string]model.Finding) {
	var files []model.LogFile
	for i, id := range []string{"1", "2", "3", "4"} {
		ts, ok := tasks[id]
		if !ok {
			continue
		}
		container := fmt.Sprintf("container_1_0001_01_%06d", i+2) // _000001 is the driver's
		loc := "containers/c/" + container + "/stderr.gz"
		for i := range ts {
			ts[i].Source.File = loc
		}
		files = append(files, model.LogFile{Location: loc, Kind: "container-stderr", Container: container, Executor: id, Host: "ip-10-0-0-" + id, TaskLogs: ts})
	}
	r := Run(Input{Tool: "t", AppID: "application_1_0001", Logs: files, LogsRead: true, Thresholds: DefaultThresholds()})
	return r, rules(r)
}

func shuffle(sec int, all, remote int64) model.TaskStep {
	return model.TaskStep{T: at(sec), Kind: model.StepShuffle, Bytes: all, Remote: remote, N: 10}
}

// Shuffle reads over the network: a note of how much crossed between
// nodes, a warning when one executor read far more than the others, and
// nothing under network-min. The series add up to the totals.
func TestRuleShuffleNetwork(t *testing.T) {
	t.Parallel()
	even := map[string][]model.TaskLog{
		"1": {flowTask(1, 2, 0, 10, shuffle(1, 1<<30, 600<<20))},
		"2": {flowTask(2, 2, 0, 10, shuffle(2, 1<<30, 600<<20))},
		"3": {flowTask(3, 2, 0, 10, shuffle(3, 1<<30, 600<<20))},
	}
	r, got := runFlows(even)
	f, ok := got["shuffle-network"]
	if !ok || f.Severity != model.Info || !strings.HasPrefix(f.Title, "1.8 GiB of shuffle data crossed the network") {
		t.Fatalf("shuffle-network = %+v", f)
	}
	var local, remote float64
	for i := range r.Flows.Local {
		local += r.Flows.Local[i].V
		remote += r.Flows.Remote[i].V
	}
	if int64(remote) != r.Flows.RemoteBytes || int64(local) != r.Flows.LocalBytes || r.Flows.RemoteBytes != 3*600<<20 || r.Flows.StepMs != 1000 {
		t.Errorf("series %v/%v, totals %d/%d, step %d", local, remote, r.Flows.LocalBytes, r.Flows.RemoteBytes, r.Flows.StepMs)
	}
	if f.Evidence[0].Source.File == "" || f.Evidence[0].Source.Line == 0 {
		t.Errorf("evidence %+v", f.Evidence)
	}
	skewed := map[string][]model.TaskLog{
		"1": {flowTask(1, 2, 0, 10, shuffle(1, 4<<30, 3<<30))},
		"2": {flowTask(2, 2, 0, 10, shuffle(2, 1<<30, 300<<20))},
		"3": {flowTask(3, 2, 0, 10, shuffle(3, 1<<30, 400<<20))},
	}
	if _, got = runFlows(skewed); got["shuffle-network"].Severity != model.Warning || !strings.HasPrefix(got["shuffle-network"].Title, "Executor 1 read 3.0 GiB of shuffle data over the network, 8×") {
		t.Errorf("skewed: %+v", got["shuffle-network"])
	}
	small := map[string][]model.TaskLog{"1": {flowTask(1, 2, 0, 10, shuffle(1, 1<<30, 100<<20))}}
	if _, got = runFlows(small); got["shuffle-network"].Rule != "" {
		t.Errorf("fired under network-min: %+v", got["shuffle-network"])
	}
}

// Cached blocks dropped or not cached are a warning naming each executor
// and its least storage memory free.
func TestRuleCacheEvicted(t *testing.T) {
	t.Parallel()
	_, got := runFlows(map[string][]model.TaskLog{
		"1": {flowTask(1, 1, 0, 5, model.TaskStep{T: at(1), Kind: model.StepCache, Name: "rdd_3_1", Bytes: 900 << 20, Free: 50 << 20},
			model.TaskStep{T: at(2), Kind: model.StepDrop, N: 2, Free: 700 << 20})},
		"2": {flowTask(2, 1, 0, 5, model.TaskStep{T: at(3), Kind: model.StepNoRoom, Name: "rdd_3_2", Bytes: 1 << 30})},
	})
	f, ok := got["cache-evicted"]
	if !ok || f.Severity != model.Warning || f.Title != "Cached data did not fit in memory: 2 cached blocks were dropped from memory to make room, and 1 block did not fit and was not cached" ||
		!strings.Contains(f.Explanation, "executor 1 (2 dropped, 0 did not fit; least storage memory free 50.0 MiB)") || len(f.Evidence) != 2 || f.Evidence[0].Source.Line != 12 {
		t.Errorf("cache-evicted = %+v", f)
	}
}

// A large broadcast, or one slow to read, is a warning.
func TestRuleBroadcastLarge(t *testing.T) {
	t.Parallel()
	bc := func(tid int64, sec int, size, ms int64) model.TaskLog {
		return flowTask(tid, 0, 0, 30, model.TaskStep{T: at(sec), Kind: model.StepBroadcast, Name: "broadcast 7", Bytes: size, N: 3},
			model.TaskStep{T: at(sec + 1), Kind: model.StepBcastRead, Name: "broadcast 7", Ms: ms})
	}
	_, got := runFlows(map[string][]model.TaskLog{"1": {bc(1, 1, 600<<20, 2000)}, "2": {bc(2, 2, 600<<20, 3000)}})
	if f := got["broadcast-large"]; f.Title != "Broadcast variable 7 is 600 MiB, read by 2 executors" || len(f.Evidence) != 2 || !strings.Contains(f.Explanation, "5.0 s in all, the slowest 3.0 s") {
		t.Errorf("large: %+v", f)
	}
	_, got = runFlows(map[string][]model.TaskLog{"1": {bc(1, 1, 4<<20, 12_000)}})
	if f := got["broadcast-large"]; f.Title != "Executor 1 took 12 s to read broadcast variable 7 (4.0 MiB)" {
		t.Errorf("slow: %+v", f)
	}
	_, got = runFlows(map[string][]model.TaskLog{"1": {bc(1, 1, 4<<20, 200)}})
	if f, ok := got["broadcast-large"]; ok {
		t.Errorf("small and quick: %+v", f)
	}
}

// Without the event log, spill of at least spill-min is a warning naming
// the stage that spilled most.
func TestRuleTaskSpill(t *testing.T) {
	t.Parallel()
	spill := func(tid int64, stage int, b int64) model.TaskLog {
		return flowTask(tid, stage, 0, 10, model.TaskStep{T: at(int(tid)), Kind: model.StepSpill, Bytes: b})
	}
	r, got := runFlows(map[string][]model.TaskLog{"1": {spill(1, 4, 800<<20), spill(2, 4, 500<<20)}, "2": {spill(3, 5, 100<<20)}})
	f := got["task-spill"]
	if f.Severity != model.Warning || f.Title != "Tasks spilled 1.4 GiB from memory to disk" || !strings.Contains(f.Explanation, "stage 4 (2 tasks, the most by task 1: 800 MiB): 1.3 GiB") {
		t.Errorf("task-spill = %+v", f)
	}
	if len(r.Flows.Spills) != 2 || r.Flows.Spills[0].Stage != 4 || r.Flows.Spills[0].Spills != 2 {
		t.Errorf("spills by stage %+v", r.Flows.Spills)
	}
	if _, got = runFlows(map[string][]model.TaskLog{"1": {spill(1, 4, 100<<20)}}); got["task-spill"].Rule != "" {
		t.Errorf("fired under spill-min")
	}
}

// Commits taking commit-share of the writing tasks' time, and min-run-time
// in all, are a warning naming the slowest.
func TestRuleCommitSlow(t *testing.T) {
	t.Parallel()
	commit := func(tid int64, ms int64) model.TaskLog {
		return flowTask(tid, 3, 0, 100, model.TaskStep{T: at(99), Kind: model.StepCommit, Ms: ms})
	}
	_, got := runFlows(map[string][]model.TaskLog{"1": {commit(1, 40_000)}, "2": {commit(2, 30_000)}})
	f := got["commit-slow"]
	if f.Title != "Committing output took 35% of the writing tasks' time (1 min 10 s in all)" || f.Evidence[0].Source.Line != 11 || !strings.Contains(f.Evidence[0].Text, "40 s") {
		t.Errorf("commit-slow = %+v", f)
	}
	if _, got = runFlows(map[string][]model.TaskLog{"1": {commit(1, 5_000)}}); got["commit-slow"].Rule != "" {
		t.Errorf("fired on a quick commit")
	}
}
