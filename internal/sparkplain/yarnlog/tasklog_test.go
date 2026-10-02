package yarnlog

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
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

// What tasks read and wrote adds up per stage and folder: lines naming a
// task go to its stage, others to the stage of the tasks running when
// they all belong to one; a file written with no task running (the
// driver's) is not a task's, and lines logged while tasks of two stages
// run are counted as untied.
func TestStageData(t *testing.T) {
	log := strings.Join([]string{
		"26/10/02 09:12:01 INFO MultipartUploadOutputStream: close closed:false s3://bucket/spark-events/app.inprogress",
		"26/10/02 09:12:10 INFO Executor: Running task 0.0 in stage 5.0 (TID 50)",
		"26/10/02 09:12:10 INFO Executor: Running task 1.0 in stage 5.0 (TID 51)",
		"26/10/02 09:12:11 INFO FileScanRDD: TID: 50 - Reading current file: path: s3://bucket/in/year=2024/part-0.parquet, range: 0-1000, partition values: [2024], isDataPresent: true",
		"26/10/02 09:12:11 INFO FileScanRDD: TID: 51 - Reading current file: path: s3://bucket/in/year=2025/part-1.parquet, range: 0-3000, partition values: [2025], isDataPresent: true",
		"26/10/02 09:12:12 INFO MultipartUploadOutputStream: close closed:false s3://bucket/out/part-00000.parquet",
		"26/10/02 09:12:12 INFO MultipartUploadOutputStream: close closed:false s3://bucket/out/part-00001.parquet",
		"26/10/02 09:12:12 INFO MultipartUploadOutputStream: uploadPart: partNum 1 of 's3://bucket/out/part-00000.parquet' from local file '/mnt/s3/emrfs-1/0000000000', 4000 bytes in 10 ms, md5: x md5hex: y",
		"26/10/02 09:12:12 INFO FileSystemOptimizedCommitter: Publishing staging directory at s3://bucket/out named 0_attempt_202610020912_0005_m_000000_50",
		"26/10/02 09:12:13 INFO Executor: Finished task 0.0 in stage 5.0 (TID 50). 1500 bytes result sent to driver",
		"26/10/02 09:12:13 INFO Executor: Running task 0.0 in stage 6.0 (TID 60)",
		"26/10/02 09:12:14 INFO MultipartUploadOutputStream: close closed:false s3://bucket/other/part-00002.parquet",
	}, "\n") + "\n"
	res, err := Classify(strings.NewReader(log), "stderr", File{Kind: ContainerStderr, Container: "container_1_0001_01_000002"}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, d := range res.StageData {
		got = append(got, fmt.Sprintf("%d.%d %s %s %s %d/%d %d line %d", d.Stage, d.StageAttempt, d.Access, d.Kind, d.Name, d.Sized, d.Parts, d.Bytes, d.Source.Line))
	}
	want := []string{
		"5.0 read path s3://bucket/in 2/2 4000 line 4",
		"5.0 write path s3://bucket/out 1/2 4000 line 6",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("stage data\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if res.DataUntied != 1 {
		t.Errorf("%d untied, want 1 (the file written while stages 5 and 6 ran)", res.DataUntied)
	}
	if x := res.TaskLogs[0]; x.Outputs != 0 || x.TaskID != 50 {
		t.Errorf("task 50 got the files of two tasks running: %+v", x)
	}
	if u := res.Untied; u == nil || u.Outputs != 3 || u.OutputBytes != 4000 {
		t.Errorf("untied outputs: %+v", u)
	}
}

func TestDataFolder(t *testing.T) {
	for in, want := range map[string]string{
		"s3://b/out/part-0.parquet":                       "s3://b/out",
		"s3://b/part-0":                                   "s3://b",
		"s3://b/t/year=2024/month=01/part-0.parquet":      "s3://b/t",
		"s3://b/t/.emrfs_staging_0_attempt_1/part-0":      "s3://b/t",
		"hdfs:///user/x/_temporary/0/_temporary/a/part-0": "hdfs:///user/x",
		"/data/in/f.csv":                                  "/data/in",
		"file:/mnt/lake/t/part-0.parquet":                 "file:/mnt/lake/t",
		"s3://b/out/":                                     "s3://b/out",
	} {
		if got := model.DataFolder(in); got != want {
			t.Errorf("DataFolder(%q) = %q, want %q", in, got, want)
		}
	}
}
