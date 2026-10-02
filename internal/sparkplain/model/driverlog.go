package model

import "time"

// DriverEvent is one line of a driver's log about how Spark ran the
// application: jobs, stages, tasks and executors, as Spark 3.5's
// DAGScheduler, TaskSetManager, CoarseGrainedSchedulerBackend,
// BlockManagerMasterEndpoint, YarnAllocator, TaskSchedulerImpl and
// SparkContext write them. Read in order, they rebuild the run when there
// is no event log (eventlog.Rebuild). Fields a kind does not use are zero.
type DriverEvent struct {
	Kind   string    `json:"kind"`
	Time   time.Time `json:"time"`
	Source Source    `json:"source"`

	Job          int    `json:"job,omitempty"`
	Stage        int    `json:"stage,omitempty"`
	StageAttempt int    `json:"stageAttempt,omitempty"`
	StageType    string `json:"stageType,omitempty"` // ShuffleMapStage or ResultStage
	TaskID       int64  `json:"taskId,omitempty"`
	Index        int    `json:"index,omitempty"` // the task's index in its stage
	Attempt      int    `json:"attempt,omitempty"`
	Partition    int    `json:"partition,omitempty"`
	Executor     string `json:"executor,omitempty"`
	Host         string `json:"host,omitempty"`
	Locality     string `json:"locality,omitempty"`
	N            int    `json:"n,omitempty"`     // output partitions, tasks to run, cores
	Ms           int64  `json:"ms,omitempty"`    // a duration the line gives
	Bytes        int64  `json:"bytes,omitempty"` // a size the line gives
	Name         string `json:"name,omitempty"`  // job call site, stage name, RDD, version, container
	Text         string `json:"text,omitempty"`  // a reason or error, redacted
	Parents      []int  `json:"parents,omitempty"`
}

// LayoutRebuilt is EventLogStats.Layout for a run rebuilt from its
// driver's log: jobs, stages, tasks and executors, but no task metrics.
const LayoutRebuilt = "container-logs"

// The kinds of DriverEvent.
const (
	DrvSparkVersion  = "spark-version"    // Running Spark version 3.5.1-amzn-0 (Name)
	DrvAppName       = "app-name"         // Submitted application: <name> (Name)
	DrvJobStart      = "job-start"        // Got [map stage ]job J (call site) with N output partitions (Text "map stage")
	DrvFinalStage    = "final-stage"      // Final stage: ResultStage S (name)
	DrvParents       = "parents"          // Parents of final stage: List(ShuffleMapStage 5, …)
	DrvStageSubmit   = "stage-submit"     // Submitting ShuffleMapStage S (RDD), which has no missing parents
	DrvStageTasks    = "stage-tasks"      // Submitting N missing tasks from ShuffleMapStage S (RDD)
	DrvStageDone     = "stage-done"       // ShuffleMapStage S (name) finished in X s
	DrvStageFailed   = "stage-failed"     // ShuffleMapStage S (name) failed in X s due to …
	DrvJobDone       = "job-done"         // Job J finished: call site, took X s
	DrvJobFailed     = "job-failed"       // Job J failed: call site, took X s
	DrvTaskStart     = "task-start"       // Starting task I.A in stage S.T (TID n) (host, executor E, partition P, LOCALITY, B bytes)
	DrvTaskEnd       = "task-end"         // Finished task I.A in stage S.T (TID n) in M ms on host (executor E) (k/N)
	DrvTaskLost      = "task-lost"        // Lost task I.A in stage S.T (TID n) (host executor E): reason
	DrvExecResources = "executor-profile" // Launching executor with Xm of heap (plus Ym overhead/off heap) and N cores
	DrvContainer     = "container"        // Launching container C on host H for executor with ID E
	DrvExecAdded     = "executor-added"   // Registered executor … (address) with ID E
	DrvBlockManager  = "block-manager"    // Registering block manager host:port with X RAM, BlockManagerId(E, host, port, …)
	DrvExecLost      = "executor-lost"    // Lost executor E on host: reason
	DrvExecIdle      = "executor-idle"    // Executors 3,4 removed due to idle timeout. (Name: the IDs)
	DrvContainerDone = "container-done"   // Completed container C on host: H (state: COMPLETE, exit status: N)
	DrvAppStopped    = "app-stopped"      // Successfully stopped SparkContext
	DrvAppFinal      = "app-final"        // Final app status: SUCCEEDED, exitCode: 0 (Name, N)
)
