package yarnlog_test

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/yarnlog"
)

const testdata = "../../../testdata"

// taskEnd is what the event log says of a task, to check the executors'
// logs against.
type taskEnd struct {
	reason, executor string
	result           int64
	local, remote    int64
	blocksLocal      int64
	blocksRemote     int64
	spill            int64
}

func taskEnds(t *testing.T, app string) map[int64]taskEnd {
	t.Helper()
	f, err := os.Open(filepath.Join(testdata, "eventlog", "application_1790380000000_"+app))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[int64]taskEnd{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for sc.Scan() {
		var e struct {
			Event  string
			Reason struct{ Reason string } `json:"Task End Reason"`
			Info   struct {
				TaskID   int64  `json:"Task ID"`
				Executor string `json:"Executor ID"`
			} `json:"Task Info"`
			M struct {
				Result int64 `json:"Result Size"`
				Spill  int64 `json:"Memory Bytes Spilled"`
				SR     struct {
					RemoteBlocks int64 `json:"Remote Blocks Fetched"`
					LocalBlocks  int64 `json:"Local Blocks Fetched"`
					Remote       int64 `json:"Remote Bytes Read"`
					Local        int64 `json:"Local Bytes Read"`
				} `json:"Shuffle Read Metrics"`
			} `json:"Task Metrics"`
		}
		if json.Unmarshal(sc.Bytes(), &e) != nil || e.Event != "SparkListenerTaskEnd" {
			continue
		}
		out[e.Info.TaskID] = taskEnd{e.Reason.Reason, e.Info.Executor, e.M.Result, e.M.SR.Local, e.M.SR.Remote, e.M.SR.LocalBlocks, e.M.SR.RemoteBlocks, e.M.Spill}
	}
	return out
}

func executorLogs(t *testing.T, app string) []yarnlog.Result {
	t.Helper()
	id := "application_1790380000000_" + app
	files, _ := filepath.Glob(filepath.Join(testdata, "emrlogs/*/containers", id, "container_*/stderr.gz"))
	var out []yarnlog.Result
	for _, p := range files {
		fh, err := os.Open(p)
		if err != nil {
			t.Fatal(err)
		}
		zr, err := gzip.NewReader(fh)
		if err != nil {
			t.Fatal(err)
		}
		res, err := yarnlog.Classify(zr, p, yarnlog.File{Kind: yarnlog.ContainerStderr}, yarnlog.Options{})
		fh.Close()
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, res)
	}
	return out
}

// Every task the event log records has its story in its executor's log,
// with the same outcome and, when it finished, the same result size; and
// the executors' logs read as many shuffle blocks as the event log
// counts, tied to tasks or not.
func TestTaskLogsMatchEventLog(t *testing.T) {
	for _, app := range []string{"0049", "0050", "0084", "0088"} {
		ends := taskEnds(t, app)
		seen := map[int64]model.TaskLog{}
		var blocks, want int64
		for _, r := range executorLogs(t, app) {
			for _, x := range r.TaskLogs {
				seen[x.TaskID] = x
				blocks += int64(x.ShuffleBlocks)
			}
			if r.Untied != nil {
				blocks += int64(r.Untied.ShuffleBlocks)
			}
		}
		for tid, e := range ends {
			want += e.blocksLocal + e.blocksRemote
			x, ok := seen[tid]
			outcome := map[string]string{"Success": "finished", "ExceptionFailure": "failed", "TaskKilled": "killed"}[e.reason]
			switch {
			case !ok:
				t.Errorf("%s: TID %d has no story", app, tid)
			case x.Outcome != outcome || outcome == "finished" && x.ResultBytes != e.result:
				t.Errorf("%s: TID %d %s with %d bytes, want %s with %d", app, tid, x.Outcome, x.ResultBytes, outcome, e.result)
			case x.Start.IsZero() || x.End.IsZero() || x.Stage < 0 || x.Source.Line == 0 || x.EndSource.Line == 0:
				t.Errorf("%s: TID %d: %+v", app, tid, x)
			case outcome == "failed" && x.Error == "":
				t.Errorf("%s: TID %d failed with no error", app, tid)
			}
		}
		if blocks != want {
			t.Errorf("%s: %d shuffle blocks in the logs, want %d", app, blocks, want)
		}
	}
}
