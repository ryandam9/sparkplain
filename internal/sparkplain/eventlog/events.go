package eventlog

import "encoding/json"

// JSON shapes of the Spark 3.5 events sparkplain reads. Only the fields used
// are declared; encoding/json skips the rest. Field names are Spark's
// JsonProtocol names, checked against the fixtures in testdata/eventlog.

type execMetrics struct {
	JVMHeapMemory              int64
	JVMOffHeapMemory           int64
	OnHeapExecutionMemory      int64
	OffHeapExecutionMemory     int64
	OnHeapStorageMemory        int64
	OffHeapStorageMemory       int64
	DirectPoolMemory           int64
	MappedPoolMemory           int64
	ProcessTreeJVMRSSMemory    int64
	ProcessTreePythonRSSMemory int64
	ProcessTreeOtherRSSMemory  int64
	TotalGCTime                int64
	MinorGCCount               int64
	MinorGCTime                int64
	MajorGCCount               int64
	MajorGCTime                int64
	OnHeapUnifiedMemory        int64
	OffHeapUnifiedMemory       int64
	ProcessTreeJVMVMemory      int64
	ProcessTreePythonVMemory   int64
	ProcessTreeOtherVMemory    int64
}

type taskInfo struct {
	TaskID      int64  `json:"Task ID"`
	Index       int    `json:"Index"`
	Attempt     int    `json:"Attempt"`
	LaunchTime  int64  `json:"Launch Time"`
	ExecutorID  string `json:"Executor ID"`
	Host        string `json:"Host"`
	Speculative bool   `json:"Speculative"`
	FinishTime  int64  `json:"Finish Time"`
	Failed      bool   `json:"Failed"`
	Killed      bool   `json:"Killed"`
	PartitionID int    `json:"Partition ID"`
	Locality    string `json:"Locality"`
	GettingTime int64  `json:"Getting Result Time"` // when the driver started fetching a large result; 0 if it did not
	// Accumulables are the task's SQL metric updates; decoded only when the
	// explorer wants them.
	Accumulables json.RawMessage `json:"Accumulables"`
}

type taskEndReason struct {
	Reason      string `json:"Reason"`
	ClassName   string `json:"Class Name"`
	Description string `json:"Description"`
	KillReason  string `json:"Kill Reason"`
	ExecutorID  string `json:"Executor ID"`
	LossReason  string `json:"Loss Reason"`
	Message     string `json:"Message"`
	FullStack   string `json:"Full Stack Trace"`
	ExitByApp   *bool  `json:"Exit Caused By App"`
}

type taskMetrics struct {
	DeserializeTime     int64 `json:"Executor Deserialize Time"`
	DeserializeCPU      int64 `json:"Executor Deserialize CPU Time"`
	ResultSize          int64 `json:"Result Size"`
	ResultSerialization int64 `json:"Result Serialization Time"`
	RunTime             int64 `json:"Executor Run Time"`
	CPUTime             int64 `json:"Executor CPU Time"`
	PeakExecutionMemory int64 `json:"Peak Execution Memory"`
	GCTime              int64 `json:"JVM GC Time"`
	MemorySpilled       int64 `json:"Memory Bytes Spilled"`
	DiskSpilled         int64 `json:"Disk Bytes Spilled"`
	ShuffleRead         struct {
		FetchWait   int64 `json:"Fetch Wait Time"`
		RemoteBytes int64 `json:"Remote Bytes Read"`
		LocalBytes  int64 `json:"Local Bytes Read"`
		RecordsRead int64 `json:"Total Records Read"`
		LocalBlocks int64 `json:"Local Blocks Fetched"`
		RemoteBlks  int64 `json:"Remote Blocks Fetched"`
		RemoteDisk  int64 `json:"Remote Bytes Read To Disk"`
		RemoteReqs  int64 `json:"Remote Requests Duration"`
		Push        struct {
			CorruptChunks  int64 `json:"Corrupt Merged Block Chunks"`
			Fallbacks      int64 `json:"Merged Fetch Fallback Count"`
			LocalBlocks    int64 `json:"Merged Local Blocks Fetched"`
			LocalBytes     int64 `json:"Merged Local Bytes Read"`
			LocalChunks    int64 `json:"Merged Local Chunks Fetched"`
			RemoteBlocks   int64 `json:"Merged Remote Blocks Fetched"`
			RemoteBytes    int64 `json:"Merged Remote Bytes Read"`
			RemoteChunks   int64 `json:"Merged Remote Chunks Fetched"`
			RemoteRequests int64 `json:"Merged Remote Requests Duration"`
		} `json:"Push Based Shuffle"`
	} `json:"Shuffle Read Metrics"`
	ShuffleWrite struct {
		Bytes   int64 `json:"Shuffle Bytes Written"`
		Records int64 `json:"Shuffle Records Written"`
		TimeNs  int64 `json:"Shuffle Write Time"`
	} `json:"Shuffle Write Metrics"`
	UpdatedBlocks []struct {
		Status struct {
			MemorySize int64 `json:"Memory Size"`
			DiskSize   int64 `json:"Disk Size"`
		} `json:"Status"`
	} `json:"Updated Blocks"`
	Input struct {
		Bytes   int64 `json:"Bytes Read"`
		Records int64 `json:"Records Read"`
	} `json:"Input Metrics"`
	Output struct {
		Bytes   int64 `json:"Bytes Written"`
		Records int64 `json:"Records Written"`
	} `json:"Output Metrics"`
}

type taskEndEvent struct {
	TaskType     string        `json:"Task Type"`
	StageID      int           `json:"Stage ID"`
	StageAttempt int           `json:"Stage Attempt ID"`
	Reason       taskEndReason `json:"Task End Reason"`
	Info         taskInfo      `json:"Task Info"`
	ExecMetrics  *execMetrics  `json:"Task Executor Metrics"`
	Metrics      *taskMetrics  `json:"Task Metrics"`
}

type storageLevel struct {
	UseDisk      bool `json:"Use Disk"`
	UseMemory    bool `json:"Use Memory"`
	UseOffHeap   bool `json:"Use Off Heap"`
	Deserialized bool `json:"Deserialized"`
	Replication  int  `json:"Replication"`
}

type rddInfo struct {
	ID            int          `json:"RDD ID"`
	Name          string       `json:"Name"`
	Callsite      string       `json:"Callsite"`
	StorageLevel  storageLevel `json:"Storage Level"`
	NumPartitions int          `json:"Number of Partitions"`
	MemorySize    int64        `json:"Memory Size"`
	DiskSize      int64        `json:"Disk Size"`
}

type stageInfo struct {
	ID             int           `json:"Stage ID"`
	Attempt        int           `json:"Stage Attempt ID"`
	Name           string        `json:"Stage Name"`
	NumTasks       int           `json:"Number of Tasks"`
	RDDs           []rddInfo     `json:"RDD Info"`
	ParentIDs      []int         `json:"Parent IDs"`
	SubmissionTime *int64        `json:"Submission Time"`
	CompletionTime *int64        `json:"Completion Time"`
	FailureReason  *string       `json:"Failure Reason"`
	Accumulables   []accumulable `json:"Accumulables"`
}

type stageEvent struct {
	Info stageInfo `json:"Stage Info"`
}

type jobStartEvent struct {
	JobID      int               `json:"Job ID"`
	Submitted  int64             `json:"Submission Time"`
	StageInfos []stageInfo       `json:"Stage Infos"`
	StageIDs   []int             `json:"Stage IDs"`
	Properties map[string]string `json:"Properties"`
}

type jobEndEvent struct {
	JobID     int   `json:"Job ID"`
	Completed int64 `json:"Completion Time"`
	Result    struct {
		Result    string `json:"Result"`
		Exception *struct {
			Message string       `json:"Message"`
			Stack   []stackFrame `json:"Stack Trace"`
		} `json:"Exception"`
	} `json:"Job Result"`
}

type envEvent struct {
	JVM       map[string]string `json:"JVM Information"`
	Spark     map[string]string `json:"Spark Properties"`
	Hadoop    map[string]string `json:"Hadoop Properties"`
	System    map[string]string `json:"System Properties"`
	Metrics   map[string]string `json:"Metrics Properties"`
	Classpath map[string]string `json:"Classpath Entries"`
}

type appStartEvent struct {
	DriverLogs       map[string]string `json:"Driver Logs"`
	DriverAttributes map[string]string `json:"Driver Attributes"`
	Name             string            `json:"App Name"`
	ID               string            `json:"App ID"`
	Timestamp        int64             `json:"Timestamp"`
	User             string            `json:"User"`
	AttemptID        string            `json:"App Attempt ID"`
}

type appEndEvent struct {
	Timestamp int64 `json:"Timestamp"`
	ExitCode  *int  `json:"ExitCode"` // written by Spark 4.0+, absent in 3.5
}

type logStartEvent struct {
	Version string `json:"Spark Version"`
}

type executorAddedEvent struct {
	Timestamp  int64  `json:"Timestamp"`
	ExecutorID string `json:"Executor ID"`
	Info       struct {
		Host              string            `json:"Host"`
		Cores             int               `json:"Total Cores"`
		ResourceProfileID int               `json:"Resource Profile Id"`
		LogURLs           map[string]string `json:"Log Urls"`
		Attributes        map[string]string `json:"Attributes"`
		Resources         map[string]struct {
			Addresses []string `json:"addresses"`
		} `json:"Resources"`
		RequestTime      *int64 `json:"Request Time"`
		RegistrationTime *int64 `json:"Registration Time"`
	} `json:"Executor Info"`
}

type executorRemovedEvent struct {
	Timestamp  int64  `json:"Timestamp"`
	ExecutorID string `json:"Executor ID"`
	Reason     string `json:"Removed Reason"`
}

type blockManagerID struct {
	ExecutorID string `json:"Executor ID"`
	Host       string `json:"Host"`
}

type blockManagerAddedEvent struct {
	ID         blockManagerID `json:"Block Manager ID"`
	Timestamp  int64          `json:"Timestamp"`
	MaxOnHeap  int64          `json:"Maximum Onheap Memory"`
	MaxOffHeap int64          `json:"Maximum Offheap Memory"`
}

type resourceProfileEvent struct {
	ID       int `json:"Resource Profile Id"`
	Executor map[string]struct {
		Amount int64 `json:"Amount"`
	} `json:"Executor Resource Requests"`
	Task map[string]struct {
		Amount float64 `json:"Amount"`
	} `json:"Task Resource Requests"`
}

type stageExecMetricsEvent struct {
	ExecutorID   string      `json:"Executor ID"`
	StageID      int         `json:"Stage ID"`
	StageAttempt int         `json:"Stage Attempt ID"`
	Metrics      execMetrics `json:"Executor Metrics"`
}

type metricsUpdateEvent struct {
	ExecutorID string `json:"Executor ID"`
	Updated    []struct {
		Metrics execMetrics `json:"Executor Metrics"`
	} `json:"Executor Metrics Updated"`
}

type blockUpdatedEvent struct {
	Info struct {
		BlockManager blockManagerID `json:"Block Manager ID"`
		BlockID      string         `json:"Block ID"`
		MemorySize   int64          `json:"Memory Size"`
		DiskSize     int64          `json:"Disk Size"`
		Level        storageLevel   `json:"Storage Level"`
	} `json:"Block Updated Info"`
}

type unpersistEvent struct {
	RDDID int `json:"RDD ID"`
}

type planNode struct {
	NodeName     string            `json:"nodeName"`
	SimpleString string            `json:"simpleString"`
	Children     []planNode        `json:"children"`
	Metadata     map[string]string `json:"metadata"`
	Metrics      []planMetric      `json:"metrics"`
}

type planMetric struct {
	Name          string `json:"name"`
	AccumulatorID int64  `json:"accumulatorId"`
	Type          string `json:"metricType"`
}

type driverAccumEvent struct {
	ExecutionID int64     `json:"executionId"`
	Updates     [][]int64 `json:"accumUpdates"`
}

type sqlStartEvent struct {
	ID          int64    `json:"executionId"`
	Description string   `json:"description"`
	Plan        string   `json:"physicalPlanDescription"`
	PlanInfo    planNode `json:"sparkPlanInfo"`
	Time        int64    `json:"time"`
}

type sqlEndEvent struct {
	ID    int64  `json:"executionId"`
	Time  int64  `json:"time"`
	Error string `json:"errorMessage"`
}

type catalogEvent struct {
	Database string `json:"database"`
	Name     string `json:"name"`
	NewName  string `json:"newName"`
}

type stackFrame struct {
	Class  string `json:"Declaring Class"`
	Method string `json:"Method Name"`
	File   string `json:"File Name"`
	Line   int    `json:"Line Number"`
}

type blockManagerRemovedEvent struct {
	ID        blockManagerID `json:"Block Manager ID"`
	Timestamp int64          `json:"Timestamp"`
}

type taskStartEvent struct {
	StageID      int      `json:"Stage ID"`
	StageAttempt int      `json:"Stage Attempt ID"`
	Info         taskInfo `json:"Task Info"`
}

// exclusionEvent is any of Spark's executor or node exclusion events (and
// their older "blacklist" names), which share these fields.
type exclusionEvent struct {
	Time             int64  `json:"time"`
	ExecutorID       string `json:"executorId"`
	HostID           string `json:"hostId"`
	TaskFailures     int    `json:"taskFailures"`
	ExecutorFailures int    `json:"executorFailures"`
	StageID          int    `json:"stageId"`
	StageAttempt     int    `json:"stageAttemptId"`
}
