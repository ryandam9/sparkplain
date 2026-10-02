package yarnlog

import (
	"fmt"
	"strings"
	"testing"
)

// In a layout that prints the thread, every line a task's thread logs is
// its, however the tasks' lines interleave; its steps keep their order,
// times and lines, and secrets are redacted.
func TestTaskStoriesByThread(t *testing.T) {
	th := func(tid, part int) string {
		return fmt.Sprintf("Executor task launch worker for task %d.0 in stage 3.0 (TID %d)", part, tid)
	}
	line := func(ms int, thread, level, logger, msg string) string {
		return fmt.Sprintf("2026-10-02 09:17:%02d,%03d [%s] %-5s %s  - %s", 10+ms/1000, ms%1000, thread, level, logger, msg)
	}
	a, b := th(40, 0), th(41, 1)
	log := strings.Join([]string{
		line(0, "dispatcher-Executor", "INFO", "org.apache.spark.executor.YarnCoarseGrainedExecutorBackend", "Got assigned task 40"),
		line(1, a, "INFO", "org.apache.spark.executor.Executor", "Running task 0.0 in stage 3.0 (TID 40)"),
		line(2, b, "INFO", "org.apache.spark.executor.Executor", "Running task 1.0 in stage 3.0 (TID 41)"),
		line(3, a, "INFO", "org.apache.spark.broadcast.TorrentBroadcast", "Started reading broadcast variable 7 with 1 pieces (estimated total size 4.0 MiB)"),
		line(9, a, "INFO", "org.apache.spark.broadcast.TorrentBroadcast", "Reading broadcast variable 7 took 6 ms"),
		line(10, b, "INFO", "org.apache.spark.storage.ShuffleBlockFetcherIterator", "Getting 12 (3.5 MiB) non-empty blocks including 4 (1.0 MiB) local and 2 (512.0 KiB) host-local and 0 (0.0 B) push-merged-local and 6 (2.0 MiB) remote blocks"),
		line(11, b, "INFO", "org.apache.spark.storage.ShuffleBlockFetcherIterator", "Started 2 remote fetches in 3 ms"),
		line(20, a, "INFO", "org.apache.spark.rdd.HadoopRDD", "Input split: s3://bucket/in/part-0001.csv:0+134217728"),
		line(400, b, "INFO", "org.apache.spark.util.collection.ExternalSorter", "Thread 81 spilling in-memory map of 256.0 MiB to disk (1 time so far)"),
		line(500, a, "INFO", "org.apache.spark.storage.memory.MemoryStore", "Block rdd_5_0 stored as values in memory (estimated size 64.0 MiB, free 1.2 GiB)"),
		line(600, b, "WARN", "org.apache.spark.storage.BlockManager", "Putting block rdd_5_1 failed due to exception password=hunter2."),
		line(700, b, "ERROR", "org.apache.spark.executor.Executor", "Exception in task 1.0 in stage 3.0 (TID 41)"),
		"java.lang.IllegalStateException: token=abc123secret is bad",
		"\tat org.example.Job.run(Job.java:12)",
		line(800, a, "INFO", "org.apache.spark.mapred.SparkHadoopMapRedUtil", "attempt_202610020917_0003_m_000000_40: Committed. Elapsed time: 42 ms."),
		line(900, a, "INFO", "org.apache.spark.executor.Executor", "Finished task 0.0 in stage 3.0 (TID 40). 2310 bytes result sent to driver"),
	}, "\n") + "\n"
	res, err := Classify(strings.NewReader(log), "stderr", File{Kind: ContainerStderr, Container: "container_1_0001_01_000002"}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.TaskLogs) != 2 || res.Untied != nil {
		t.Fatalf("%d task logs, untied %+v", len(res.TaskLogs), res.Untied)
	}
	x, y := res.TaskLogs[0], res.TaskLogs[1]
	if x.TaskID != 40 || x.Partition != 0 || x.Stage != 3 || x.TiedBy != "thread" || x.Outcome != "finished" || x.ResultBytes != 2310 || x.ResultVia != "driver" ||
		x.Broadcasts != 1 || x.BroadcastBytes != 4<<20 || x.BroadcastMs != 6 || x.Inputs != 1 || x.InputBytes != 128<<20 || x.Input != "s3://bucket/in/part-0001.csv" ||
		x.CachedBlocks != 1 || x.CachedBytes != 64<<20 || x.Commits != 1 || x.CommitMs != 42 || x.DurationMs() != 899 || x.Source.Line != 2 || x.EndSource.Line != 16 {
		t.Errorf("task 40: %+v", x)
	}
	var kinds []string
	for _, s := range x.Steps {
		kinds = append(kinds, s.Kind)
	}
	if got := strings.Join(kinds, " "); got != "start broadcast broadcast-read input cache commit end" {
		t.Errorf("task 40 steps: %s", got)
	}
	if y.TaskID != 41 || y.Outcome != "failed" || y.ShuffleReads != 1 || y.ShuffleBlocks != 12 || y.ShuffleLocalBytes != 1536<<10 || y.ShuffleRemoteBytes != 2<<20 ||
		y.RemoteFetches != 2 || y.FetchStartMs != 3 || y.Spills != 1 || y.SpillBytes != 256<<20 || y.Warnings != 1 || y.Errors != 0 {
		t.Errorf("task 41: %+v", y)
	}
	if !strings.HasPrefix(y.Error, "java.lang.IllegalStateException:") || strings.Contains(y.Error, "abc123secret") || strings.Contains(y.Problem, "hunter2") || y.Problem == "" {
		t.Errorf("task 41: error %q, problem %q", y.Error, y.Problem)
	}
}

// In Spark's default layout, which prints no thread, a line is a task's
// when it names the task's TID or the task was its executor's only one
// running; the rest are counted for the executor, untied.
func TestTaskStoriesWithoutThread(t *testing.T) {
	log := strings.Join([]string{
		"26/10/02 09:12:10 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)",
		"26/10/02 09:12:10 INFO TorrentBroadcast: Started reading broadcast variable 1 with 1 pieces (estimated total size 4.0 KiB)",
		"26/10/02 09:12:10 INFO Executor: Running task 1.0 in stage 0.0 (TID 1)",
		"26/10/02 09:12:11 INFO ShuffleBlockFetcherIterator: Getting 2 (2.0 KiB) non-empty blocks including 2 (2.0 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks",
		"26/10/02 09:12:11 INFO FileScanRDD: TID: 1 - Reading current file: path: s3://bucket/t/part-0.parquet, range: 0-1000, partition values: [empty row], isDataPresent: true",
		"26/10/02 09:12:12 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 1500 bytes result sent to driver",
		"26/10/02 09:12:12 INFO SparkHadoopMapRedUtil: attempt_1_0000_m_000001_1: Committed. Elapsed time: 9 ms.",
		"26/10/02 09:12:13 INFO Executor: Finished task 1.0 in stage 0.0 (TID 1). 1600 bytes result sent to driver",
	}, "\n") + "\n"
	res, err := Classify(strings.NewReader(log), "stderr", File{Kind: ContainerStderr}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.TaskLogs) != 2 || res.Untied == nil {
		t.Fatalf("%d tasks, untied %+v", len(res.TaskLogs), res.Untied)
	}
	x, y, u := res.TaskLogs[0], res.TaskLogs[1], res.Untied
	if x.Broadcasts != 1 || x.ShuffleBlocks != 0 || x.TiedBy != "tid" || x.ResultBytes != 1500 {
		t.Errorf("task 0 (alone at first): %+v", x)
	}
	if y.Inputs != 1 || y.InputBytes != 1000 || y.Commits != 1 || y.ResultBytes != 1600 {
		t.Errorf("task 1 (named by its file line, alone at its commit): %+v", y)
	}
	if u.ShuffleBlocks != 2 || u.Lines != 1 {
		t.Errorf("untied (two tasks running): %+v", u)
	}
}
