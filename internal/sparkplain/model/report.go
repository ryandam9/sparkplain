package model

import (
	"strconv"
	"time"
)

// Report is the full output of one run: what the HTML shows and what the
// JSON export contains.
type Report struct {
	SchemaVersion string          `json:"schemaVersion"`
	Tool          string          `json:"tool"`
	GeneratedAt   time.Time       `json:"generatedAt"`
	TimeZone      string          `json:"timeZone"`
	ExitCode      int             `json:"exitCode"`
	Mode          string          `json:"mode"` // offline-eventlog, …
	Application   Application     `json:"application"`
	Summary       Summary         `json:"summary"`
	Coverage      []SectionStatus `json:"coverage"`
	Findings      []Finding       `json:"findings"`
	Timeline      Timeline        `json:"timeline"`
	Nodes         NodesSection    `json:"nodes"`
	Executors     ExecSection     `json:"executors"`
	Memory        MemorySection   `json:"memory"`
	CPU           CPUSection      `json:"cpu"`
	IO            IOSection       `json:"io"`
	Jobs          JobsSection     `json:"jobs"`
	Config        ConfigSection   `json:"config"`
	Identity      IdentitySection `json:"identity"`
	Sources       []SourceStatus  `json:"sources"`
	EventLog      *EventLogStats  `json:"eventLog,omitempty"`
	// Cluster, Steps and Logs come from the EMR API and the cluster's
	// container, step and node logs (online or -from runs).
	Cluster *Cluster     `json:"cluster,omitempty"`
	Steps   []Step       `json:"steps,omitempty"`
	Logs    *LogsSection `json:"logs,omitempty"`
	// Metrics come from CloudWatch (online runs).
	Metrics *MetricsSection `json:"metrics,omitempty"`
	// AWSCalls come from CloudTrail (online runs).
	AWSCalls *AWSCallsSection `json:"awsCalls,omitempty"`
}

// Summary is the "What happened" block.
type Summary struct {
	Sentences []string `json:"sentences"`
	KPIs      []KPI    `json:"kpis"`
}

// KPI is one header card. Value and Unit are display text; the numbers they
// come from are elsewhere in the report.
type KPI struct {
	Label   string `json:"label"`
	Value   string `json:"value"`
	Unit    string `json:"unit,omitempty"`
	Explain string `json:"explain"`
	Tone    string `json:"tone,omitempty"` // "", "crit" or "warn"
}

// SectionStatus says how complete one section is and what it is missing.
type SectionStatus struct {
	ID       string   `json:"id"`
	Title    string   `json:"title"`
	Coverage Coverage `json:"coverage"`
	Shown    string   `json:"shown"`
	Missing  []string `json:"missing,omitempty"`
}

// Evidence points a finding at the log line (or API call) behind it.
type Evidence struct {
	Source Source `json:"source,omitzero"`
	Text   string `json:"text"`
	// Ref names the stage, job, executor or query the evidence is about, as
	// "stage:27.0", "job:12", "executor:3" or "query:4", so the explorer
	// page can link to it. Empty when it is about the run as a whole.
	Ref string `json:"ref,omitempty"`
}

// StageRef, JobRef and ExecutorRef build Evidence.Ref values.
func StageRef(id, attempt int) string {
	return "stage:" + strconv.Itoa(id) + "." + strconv.Itoa(attempt)
}
func JobRef(id int) string         { return "job:" + strconv.Itoa(id) }
func ExecutorRef(id string) string { return "executor:" + id }

// Finding is one problem or notable fact, with evidence and a suggested fix.
type Finding struct {
	Rule        string     `json:"rule"`
	Severity    Severity   `json:"severity"`
	Title       string     `json:"title"`
	Explanation string     `json:"explanation"`
	Evidence    []Evidence `json:"evidence"`
	Fix         string     `json:"fix,omitempty"`
	Section     string     `json:"section"`
}

// Timeline holds the Gantt chart and the event list.
type Timeline struct {
	Coverage       Coverage        `json:"coverage"`
	Start          time.Time       `json:"start,omitzero"`
	End            time.Time       `json:"end,omitzero"`
	ExecutorSeries []CountPoint    `json:"executorSeries,omitempty"`
	Jobs           []Bar           `json:"jobs,omitempty"`
	Events         []TimelineEvent `json:"events,omitempty"`
}

// CountPoint is a step in a count-over-time series.
type CountPoint struct {
	Time  time.Time `json:"time"`
	Count int       `json:"count"`
}

// Bar is one bar of a Gantt chart.
type Bar struct {
	ID     int       `json:"id"`
	Label  string    `json:"label"`
	Start  time.Time `json:"start"`
	End    time.Time `json:"end"`
	Status string    `json:"status"`
	Stages []Bar     `json:"stages,omitempty"`
}

// TimelineEvent is one line in the timeline table.
type TimelineEvent struct {
	Time   time.Time `json:"time"`
	Kind   string    `json:"kind"` // start, end, executors, executor-lost, job-failed, …
	Text   string    `json:"text"`
	Source Source    `json:"source,omitzero"`
}

// Host is one machine that ran the driver or executors.
type Host struct {
	Name string `json:"name"`
	// Instance is the EC2 instance behind the host (EMR API runs only).
	Instance *Instance `json:"instance,omitempty"`
	// YARNMemoryBytes and YARNVCores are what the host's NodeManager
	// offered YARN, from the ResourceManager or NodeManager log.
	YARNMemoryBytes int64 `json:"yarnMemoryBytes,omitempty"`
	YARNVCores      int   `json:"yarnVCores,omitempty"`
	// HostCPU is the node's CPU while the application ran, from CloudWatch.
	HostCPU       *HostCPU   `json:"hostCpu,omitempty"`
	Driver        bool       `json:"driver"`
	Executors     []string   `json:"executors"`
	Cores         int        `json:"cores"`
	FirstSeen     time.Time  `json:"firstSeen,omitzero"`
	LastSeen      time.Time  `json:"lastSeen,omitzero"`
	Lost          int        `json:"executorsLost"`
	Tasks         TaskTotals `json:"tasks"`
	PeakHeap      int64      `json:"peakHeapBytes"`
	CPUShare      float64    `json:"cpuShare"`  // task CPU time / task run time
	AllocatedCore float64    `json:"busyShare"` // task run time / (cores × executor lifetime)
	Source        Source     `json:"source"`
}

// NodesSection is module 2.
type NodesSection struct {
	Coverage Coverage `json:"coverage"`
	Lede     string   `json:"lede"`
	Hosts    []Host   `json:"hosts"`
	Missing  []string `json:"missing,omitempty"`
}

// Count is a labelled number, used for small breakdowns.
type Count struct {
	Label string `json:"label"`
	N     int    `json:"n"`
}

// ExecSection is module 3.
type ExecSection struct {
	Coverage     Coverage    `json:"coverage"`
	Started      int         `json:"started"`
	Peak         int         `json:"peak"`
	PeakAt       time.Time   `json:"peakAt,omitzero"`
	EndedBy      []Count     `json:"endedBy"`
	Cores        int         `json:"coresEach"`
	DynamicAlloc string      `json:"dynamicAllocation"`
	Executors    []*Executor `json:"executors"`
	Driver       *Executor   `json:"driver,omitempty"`
	Exclusions   []Exclusion `json:"exclusions,omitempty"`
	Missing      []string    `json:"missing,omitempty"`
}

// MemoryConfig is the memory an executor asked for, and how Spark splits it.
type MemoryConfig struct {
	HeapBytes       int64   `json:"heapBytes"`
	HeapFrom        string  `json:"heapFrom"`
	OverheadBytes   int64   `json:"overheadBytes"`
	OverheadFrom    string  `json:"overheadFrom"`
	OffHeapBytes    int64   `json:"offHeapBytes"`
	PySparkBytes    int64   `json:"pysparkBytes"`
	ContainerBytes  int64   `json:"containerBytes"`
	Cores           int     `json:"cores"`
	MemoryFraction  float64 `json:"memoryFraction"`
	StorageFraction float64 `json:"storageFraction"`
	UnifiedBytes    int64   `json:"unifiedBytes"` // (heap − 300 MiB) × memoryFraction
	DriverHeapBytes int64   `json:"driverHeapBytes"`
}

// ExecMemory is one executor's memory use against what it was given.
type ExecMemory struct {
	ID            string  `json:"id"`
	Host          string  `json:"host"`
	HeapBytes     int64   `json:"configuredHeapBytes"`
	PeakHeap      int64   `json:"peakHeapBytes"`
	PeakOffHeap   int64   `json:"peakOffHeapBytes"`
	PeakRSS       int64   `json:"peakRssBytes"`
	PeakStorage   int64   `json:"peakStorageBytes"`
	PeakExecution int64   `json:"peakExecutionBytes"`
	GCShare       float64 `json:"gcShare"`
	Source        Source  `json:"source,omitzero"`
}

// StageSpill is one stage's spill against its shuffle write.
type StageSpill struct {
	StageID      int    `json:"stageId"`
	Attempt      int    `json:"attempt"`
	Name         string `json:"name"`
	MemoryBytes  int64  `json:"memorySpillBytes"`
	DiskBytes    int64  `json:"diskSpillBytes"`
	ShuffleWrite int64  `json:"shuffleWriteBytes"`
	Source       Source `json:"source"`
}

// MemorySection is module 4.
type MemorySection struct {
	Coverage       Coverage     `json:"coverage"`
	Lede           string       `json:"lede"`
	Config         MemoryConfig `json:"config"`
	Executors      []ExecMemory `json:"executors"`
	Driver         *ExecMemory  `json:"driver,omitempty"`
	HeapKnown      bool         `json:"heapKnown"`
	RSSKnown       bool         `json:"rssKnown"`
	Spill          []StageSpill `json:"spill"`
	TotalMemSpill  int64        `json:"totalMemorySpillBytes"`
	TotalDiskSpill int64        `json:"totalDiskSpillBytes"`
	GCShare        float64      `json:"gcShare"`
	Missing        []string     `json:"missing,omitempty"`
}

// Utilisation compares CPU time to run time for one executor or stage.
type Utilisation struct {
	ID     string  `json:"id"`
	Label  string  `json:"label"`
	CPUMs  int64   `json:"cpuMs"`
	RunMs  int64   `json:"runMs"`
	Share  float64 `json:"share"`
	Source Source  `json:"source,omitzero"`
}

// CPUSection is module 6.
type CPUSection struct {
	Coverage        Coverage      `json:"coverage"`
	CPUMs           int64         `json:"cpuMs"`
	RunMs           int64         `json:"runMs"`
	Share           float64       `json:"share"`
	AllocatedCoreMs int64         `json:"allocatedCoreMs"`
	BusyShare       float64       `json:"busyShare"`
	Executors       []Utilisation `json:"executors"`
	Stages          []Utilisation `json:"stages"`
	Missing         []string      `json:"missing,omitempty"`
}

// StageIO is one stage's data movement.
type StageIO struct {
	StageID int        `json:"stageId"`
	Attempt int        `json:"attempt"`
	Name    string     `json:"name"`
	Totals  TaskTotals `json:"totals"`
	Source  Source     `json:"source"`
}

// IOSection is module 5.
type IOSection struct {
	Coverage Coverage     `json:"coverage"`
	Totals   TaskTotals   `json:"totals"`
	Stages   []StageIO    `json:"stages"`
	Cached   []*CachedRDD `json:"cached"`
	// BlockKinds totals block-manager updates per kind of block, when
	// spark.eventLog.logBlockUpdates.enabled was on.
	BlockKinds []BlockKind `json:"blockKinds,omitempty"`
	Data       []DataRef   `json:"data"`
	Missing    []string    `json:"missing,omitempty"`
}

// JobsSection is module 7.
type JobsSection struct {
	Coverage     Coverage    `json:"coverage"`
	Jobs         []*Job      `json:"jobs"`
	Stages       []*Stage    `json:"stages"`
	SQL          []*SQLQuery `json:"sql"`
	CriticalPath []int       `json:"criticalPath,omitempty"` // stage IDs of the longest job's slowest chain
	CriticalJob  int         `json:"criticalJob"`
	// RunningTasks started but had not ended when the log ended.
	RunningTasks  []RunningTask `json:"runningTasks,omitempty"`
	RunningCapped bool          `json:"runningCapped,omitempty"`
	Failed        int           `json:"failedJobs"`
	Missing       []string      `json:"missing,omitempty"`
}

// ConfigGroup is one group of settings.
type ConfigGroup struct {
	Name    string       `json:"name"`
	Entries []ConfigView `json:"entries"`
}

// ConfigView is a setting as shown in the report.
type ConfigView struct {
	ConfigEntry
	Default    string `json:"default,omitempty"`
	NonDefault bool   `json:"nonDefault,omitempty"`
	Explain    string `json:"explain,omitempty"`
	Risk       string `json:"risk,omitempty"`
	// SetBy says where the value was set, when the EMR API or the step
	// shows it: "cluster configuration" or "spark-submit".
	SetBy string `json:"setBy,omitempty"`
}

// RuntimeRow is one line of the runtime environment table: a version, a
// runtime setting or a location, and where it was read.
type RuntimeRow struct {
	Group   string `json:"group"` // Versions, Runtime or Locations
	Label   string `json:"label"`
	Value   string `json:"value"`
	From    string `json:"from"` // the property or file it came from
	Explain string `json:"explain"`
	Missing bool   `json:"missing,omitempty"`
	Source  Source `json:"source,omitzero"`
}

// ConfigSection is module 8.
type ConfigSection struct {
	Coverage         Coverage          `json:"coverage"`
	Runtime          []RuntimeRow      `json:"runtime"`
	Key              []ConfigView      `json:"key"` // the settings that matter most, explained
	Groups           []ConfigGroup     `json:"groups"`
	Total            int               `json:"total"`
	NonDefault       int               `json:"nonDefault"`
	Redacted         int               `json:"redacted"`
	Missing          []string          `json:"missing,omitempty"`
	ResourceProfiles []ResourceProfile `json:"resourceProfiles,omitempty"`
}

// Fact is a labelled value with a one-line explanation.
type Fact struct {
	Label   string `json:"label"`
	Value   string `json:"value"`
	Explain string `json:"explain"`
	Source  Source `json:"source,omitzero"`
}

// IdentitySection is module 9. Phase 1 only fills what the event log knows.
type IdentitySection struct {
	Coverage Coverage `json:"coverage"`
	Facts    []Fact   `json:"facts"`
	Missing  []string `json:"missing,omitempty"`
}

// SourceStatus is one row in the Sources panel.
type SourceStatus struct {
	Name string `json:"name"`
	// Status is read, partial, not-supplied or error (these last three make
	// the run exit 3), none (looked, and it holds nothing for this
	// application), not-requested (the run was not asked to read it) or
	// not-yet (a later version reads it).
	Status   string `json:"status"`
	Class    string `json:"errorClass,omitempty"`
	Location string `json:"location,omitempty"`
	Detail   string `json:"detail"`
	// Files lists every object read or skipped for this source, and why.
	Files []SourceFile `json:"files,omitempty"`
}

// SourceFile is one object a source was read from, or skipped.
type SourceFile struct {
	Location string `json:"location"`
	Bytes    int64  `json:"bytes"`
	Status   string `json:"status"` // read, skipped, error
	Class    string `json:"errorClass,omitempty"`
	Detail   string `json:"detail,omitempty"`
}

// Cluster is the EMR cluster an application ran on, from the EMR API.
type Cluster struct {
	ID              string            `json:"id"`
	Name            string            `json:"name"`
	State           string            `json:"state"`
	StateCode       string            `json:"stateCode,omitempty"` // such as BOOTSTRAP_FAILURE or ALL_STEPS_COMPLETED
	StateReason     string            `json:"stateReason,omitempty"`
	PrimaryDNS      string            `json:"primaryDns,omitempty"` // the primary node's DNS name
	Release         string            `json:"release"`
	Applications    []string          `json:"applications"`
	LogURI          string            `json:"logUri,omitempty"`
	ServiceRole     string            `json:"serviceRole,omitempty"`
	InstanceProfile string            `json:"instanceProfile,omitempty"`
	SecurityConfig  string            `json:"securityConfiguration,omitempty"`
	Created         time.Time         `json:"created,omitzero"`
	Ended           time.Time         `json:"ended,omitzero"`
	Configurations  map[string]string `json:"configurations,omitempty"` // "classification/key" → value, redacted
	Source          string            `json:"source"`                   // the API call it came from
	Instances       []Instance        `json:"instances,omitempty"`      // from ListInstances
	Groups          []InstanceGroup   `json:"groups,omitempty"`         // instance groups or fleets
	Fleets          bool              `json:"fleets,omitempty"`         // the cluster uses instance fleets
	KerberosRealm   string            `json:"kerberosRealm,omitempty"`
	// Security is what the cluster's EMR security configuration turns on.
	Security *SecurityPosture `json:"security,omitempty"`
}

// InstanceGroup is one instance group or fleet: the primary, core or task
// nodes.
type InstanceGroup struct {
	ID            string   `json:"id"`
	Fleet         bool     `json:"fleet,omitempty"`
	Role          string   `json:"role"` // MASTER, CORE or TASK
	Name          string   `json:"name,omitempty"`
	InstanceTypes []string `json:"instanceTypes"`
	Market        string   `json:"market,omitempty"` // ON_DEMAND or SPOT (groups; fleets mix both)
	Requested     int      `json:"requested"`
	Running       int      `json:"running"`
}

// SecurityPosture is the parts of an EMR security configuration the report
// explains. Keys, passwords and certificates are never read.
type SecurityPosture struct {
	Name                string `json:"name"`
	AtRestEncryption    bool   `json:"atRestEncryption"`
	S3Encryption        string `json:"s3Encryption,omitempty"` // SSE-S3, SSE-KMS, CSE-KMS, CSE-Custom
	LocalDiskEncryption bool   `json:"localDiskEncryption"`
	EBSEncryption       bool   `json:"ebsEncryption"`
	InTransitEncryption bool   `json:"inTransitEncryption"`
	Kerberos            string `json:"kerberos,omitempty"` // ClusterDedicatedKdc or ExternalKdc
	LakeFormation       bool   `json:"lakeFormation"`
	RuntimeRoles        bool   `json:"runtimeRoles"` // EnableApplicationScopedIAMRole
	Source              string `json:"source"`
}

// Step is one EMR step: a spark-submit or other command the cluster ran.
type Step struct {
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	State          string    `json:"state"`
	Jar            string    `json:"jar,omitempty"`
	Args           []string  `json:"args,omitempty"` // redacted
	Started        time.Time `json:"started,omitzero"`
	Ended          time.Time `json:"ended,omitzero"`
	FailureReason  string    `json:"failureReason,omitempty"`
	FailureMessage string    `json:"failureMessage,omitempty"`
	FailureLog     string    `json:"failureLog,omitempty"`
	// AppID is the Spark application the step started, found in its stderr.
	AppID string `json:"appId,omitempty"`
	// ExecutionRole is the step's runtime role (DescribeStep), when it had one.
	ExecutionRole string `json:"executionRole,omitempty"`
	Source        string `json:"source"`
}

// Instance is one EC2 instance of a cluster, from the EMR API. Node logs
// are kept by instance ID and the event log names hosts, so this joins
// them.
type Instance struct {
	ID          string    `json:"id"`
	PrivateDNS  string    `json:"privateDns"`
	PrivateIP   string    `json:"privateIp,omitempty"`
	Type        string    `json:"type,omitempty"`   // m5.xlarge, …
	Market      string    `json:"market,omitempty"` // ON_DEMAND or SPOT
	State       string    `json:"state,omitempty"`
	StateReason string    `json:"stateReason,omitempty"`
	Primary     bool      `json:"primary,omitempty"`
	GroupID     string    `json:"groupId,omitempty"` // instance group or fleet
	Role        string    `json:"role,omitempty"`    // MASTER, CORE or TASK, from its group
	VCPU        int       `json:"vcpu,omitempty"`    // from EC2 DescribeInstanceTypes
	MemoryBytes int64     `json:"memoryBytes,omitempty"`
	Created     time.Time `json:"created,omitzero"`
	Ended       time.Time `json:"ended,omitzero"`
}

// LogsSection holds the container, step and node logs read and what the
// classifiers found in them.
type LogsSection struct {
	Coverage Coverage  `json:"coverage"`
	Files    []LogFile `json:"files"`
	Missing  []string  `json:"missing,omitempty"`
}

// LogFile is one log read, with the lines recognised in it.
type LogFile struct {
	Location  string `json:"location"` // s3://… or a local path; the Source.File of its lines
	Kind      string `json:"kind"`     // container-stderr, step-controller, nodemanager, …
	Container string `json:"container,omitempty"`
	Step      string `json:"step,omitempty"`
	Instance  string `json:"instance,omitempty"`
	// Executor is the executor the container ran ("driver" for the
	// application master in cluster mode), joined from the event log.
	Executor string    `json:"executor,omitempty"`
	Host     string    `json:"host,omitempty"`
	Bytes    int64     `json:"bytes"`
	Lines    int64     `json:"lines"`
	Dropped  int       `json:"dropped,omitempty"` // distinct lines past the per-file cap
	Found    []LogLine `json:"found,omitempty"`
}

// HostCPU is a node's CPU use over the run, as a percentage of all its
// vCPUs, from EC2's CloudWatch metrics.
type HostCPU struct {
	Average float64 `json:"average"`
	Peak    float64 `json:"peak"`
	Source  string  `json:"source"`
}
