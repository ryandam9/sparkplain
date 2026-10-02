package yarnlog

import (
	"strings"
	"testing"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// The driver's lines about jobs, stages, tasks and executors become driver
// events, in both of Spark's layouts, with secrets in their texts redacted.
func TestDriverEvents(t *testing.T) {
	log := strings.Join([]string{
		"26/10/02 09:12:01 INFO SparkContext: Running Spark version 3.5.1-amzn-0",
		"26/10/02 09:12:01 INFO SparkContext: Submitted application: etl password=hunter2",
		"26/10/02 09:12:05 INFO YarnAllocator: Launching container container_1_0001_01_000002 on host ip-10-0-2-10.ec2.internal for executor with ID 1 for ResourceProfile Id 0",
		"2026-10-02 09:12:09,120 [dispatcher-CoarseGrainedScheduler] INFO  org.apache.spark.scheduler.cluster.YarnClusterSchedulerBackend$YarnDriverEndpoint - Registered executor NettyRpcEndpointRef(spark-client://Executor) (10.0.2.10:41234) with ID 1,  ResourceProfileId 0",
		"26/10/02 09:12:10 INFO DAGScheduler: Got job 0 (count at job.py:12) with 2 output partitions",
		"26/10/02 09:12:10 INFO DAGScheduler: Final stage: ResultStage 1 (count at job.py:12)",
		"26/10/02 09:12:10 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 0)",
		"26/10/02 09:12:10 INFO DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 0 (PythonRDD[3] at count at job.py:12) (first 15 tasks are for partitions Vector(0, 1))",
		"26/10/02 09:12:10 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0) (ip-10-0-2-10.ec2.internal, executor 1, partition 0, PROCESS_LOCAL, 7615 bytes) ",
		"26/10/02 09:12:11 WARN TaskSetManager: Lost task 0.0 in stage 0.0 (TID 0) (ip-10-0-2-10.ec2.internal executor 1): java.lang.RuntimeException: token=abc123secret",
		"26/10/02 09:12:12 INFO TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 812 ms on ip-10-0-2-10.ec2.internal (executor 1) (1/2)",
		"26/10/02 09:12:14 INFO DAGScheduler: ShuffleMapStage 0 (count at job.py:12) finished in 3.956 s",
		"26/10/02 09:12:20 INFO DAGScheduler: Job 0 finished: count at job.py:12, took 9.512 s",
		"26/10/02 09:12:30 INFO ApplicationMaster: Final app status: SUCCEEDED, exitCode: 0",
	}, "\n") + "\n"
	res, err := Classify(strings.NewReader(log), "stderr", File{Kind: ContainerStderr, Container: "container_1_0001_01_000001"}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, e := range res.DriverEvents {
		kinds = append(kinds, e.Kind)
		for _, s := range []string{e.Name, e.Text} {
			if strings.Contains(s, "hunter2") || strings.Contains(s, "abc123secret") {
				t.Errorf("%s event kept a secret: %q", e.Kind, s)
			}
		}
	}
	want := []string{model.DrvSparkVersion, model.DrvAppName, model.DrvContainer, model.DrvExecAdded, model.DrvJobStart, model.DrvFinalStage, model.DrvParents,
		model.DrvStageTasks, model.DrvTaskStart, model.DrvTaskLost, model.DrvTaskEnd, model.DrvStageDone, model.DrvJobDone, model.DrvAppFinal}
	if strings.Join(kinds, " ") != strings.Join(want, " ") {
		t.Fatalf("kinds\n%v\nwant\n%v", kinds, want)
	}
	ev := res.DriverEvents
	if e := ev[10]; e.TaskID != 1 || e.Index != 1 || e.Stage != 0 || e.Ms != 812 || e.Executor != "1" || e.Source.Line != 11 {
		t.Errorf("task end: %+v", e)
	}
	if e := ev[11]; e.Ms != 3956 {
		t.Errorf("stage done in %d ms, want 3956", e.Ms)
	}
	if e := ev[3]; e.Executor != "1" || e.Time.Format("15:04:05.000") != "09:12:09.120" {
		t.Errorf("executor added: %+v", e)
	}
}
