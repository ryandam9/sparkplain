package eventlog

// Event names.
const (
	evLogStart        = "SparkListenerLogStart"
	evResourceProfile = "SparkListenerResourceProfileAdded"
	evBMAdded         = "SparkListenerBlockManagerAdded"
	evBMRemoved       = "SparkListenerBlockManagerRemoved"
	evEnv             = "SparkListenerEnvironmentUpdate"
	evAppStart        = "SparkListenerApplicationStart"
	evAppEnd          = "SparkListenerApplicationEnd"
	evExecAdded       = "SparkListenerExecutorAdded"
	evExecRemoved     = "SparkListenerExecutorRemoved"
	evJobStart        = "SparkListenerJobStart"
	evJobEnd          = "SparkListenerJobEnd"
	evStageSubmitted  = "SparkListenerStageSubmitted"
	evStageCompleted  = "SparkListenerStageCompleted"
	evTaskStart       = "SparkListenerTaskStart"
	evTaskGetting     = "SparkListenerTaskGettingResult"
	evTaskEnd         = "SparkListenerTaskEnd"
	evStageExecMetric = "SparkListenerStageExecutorMetrics"
	evMetricsUpdate   = "SparkListenerExecutorMetricsUpdate"
	evBlockUpdated    = "SparkListenerBlockUpdated"
	evUnpersist       = "SparkListenerUnpersistRDD"
	evSQLStart        = "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart"
	evSQLEnd          = "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd"
	evSQLAdaptive     = "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate"
	catalogPrefix     = "org.apache.spark.sql.catalyst.catalog."
)

// ignored are events Spark writes that sparkplain knows and does not need.
var ignored = map[string]bool{
	evTaskStart: true, evTaskGetting: true,
	"org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates":          true,
	"org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveSQLMetricUpdates": true,
	"SparkListenerSpeculativeTaskSubmitted":                                      true,
	"SparkListenerExecutorExcluded":                                              true,
	"SparkListenerExecutorExcludedForStage":                                      true,
	"SparkListenerExecutorUnexcluded":                                            true,
	"SparkListenerNodeExcluded":                                                  true,
	"SparkListenerNodeExcludedForStage":                                          true,
	"SparkListenerNodeUnexcluded":                                                true,
	"SparkListenerExecutorBlacklisted":                                           true,
	"SparkListenerExecutorBlacklistedForStage":                                   true,
	"SparkListenerExecutorUnblacklisted":                                         true,
	"SparkListenerNodeBlacklisted":                                               true,
	"SparkListenerNodeBlacklistedForStage":                                       true,
	"SparkListenerNodeUnblacklisted":                                             true,
	"SparkListenerUnschedulableTaskSetAdded":                                     true,
	"SparkListenerUnschedulableTaskSetRemoved":                                   true,
	"SparkListenerMiscellaneousProcessAdded":                                     true,
}

// knownKeys lists the top-level fields of each event in Spark 3.5. Others are
// counted as unknown fields, which flags schema drift in newer releases.
var knownKeys = map[string]map[string]bool{}

func init() {
	add := func(ev string, keys ...string) {
		m := map[string]bool{"Event": true}
		for _, k := range keys {
			m[k] = true
		}
		knownKeys[ev] = m
	}
	add(evLogStart, "Spark Version")
	add(evResourceProfile, "Resource Profile Id", "Executor Resource Requests", "Task Resource Requests")
	add(evBMAdded, "Block Manager ID", "Maximum Memory", "Timestamp", "Maximum Onheap Memory", "Maximum Offheap Memory")
	add(evBMRemoved, "Block Manager ID", "Timestamp")
	add(evEnv, "JVM Information", "Spark Properties", "Hadoop Properties", "System Properties", "Metrics Properties", "Classpath Entries")
	add(evAppStart, "App Name", "App ID", "Timestamp", "User", "App Attempt ID", "Driver Logs", "Driver Attributes")
	add(evAppEnd, "Timestamp", "ExitCode")
	add(evExecAdded, "Timestamp", "Executor ID", "Executor Info")
	add(evExecRemoved, "Timestamp", "Executor ID", "Removed Reason")
	add(evJobStart, "Job ID", "Submission Time", "Stage Infos", "Stage IDs", "Properties")
	add(evJobEnd, "Job ID", "Completion Time", "Job Result")
	add(evStageSubmitted, "Stage Info", "Properties")
	add(evStageCompleted, "Stage Info")
	add(evTaskStart, "Stage ID", "Stage Attempt ID", "Task Info")
	add(evTaskGetting, "Task Info")
	add(evTaskEnd, "Stage ID", "Stage Attempt ID", "Task Type", "Task End Reason", "Task Info", "Task Executor Metrics", "Task Metrics")
	add(evStageExecMetric, "Executor ID", "Stage ID", "Stage Attempt ID", "Executor Metrics")
	add(evMetricsUpdate, "Executor ID", "Metrics Updated", "Executor Metrics Updated")
	add(evBlockUpdated, "Block Updated Info")
	add(evUnpersist, "RDD ID")
	add(evSQLStart, "executionId", "rootExecutionId", "description", "details", "physicalPlanDescription", "sparkPlanInfo", "time", "modifiedConfigs", "jobTags")
	add(evSQLEnd, "executionId", "time", "errorMessage")
	add(evSQLAdaptive, "executionId", "physicalPlanDescription", "sparkPlanInfo")
}

// eventName pulls the "Event" value from a line without a full decode.
// Spark always writes it first; other orders fall back to "".
func eventName(line []byte) string {
	const prefix = `{"Event":"`
	if len(line) < len(prefix) || string(line[:len(prefix)]) != prefix {
		return ""
	}
	rest := line[len(prefix):]
	for i, c := range rest {
		if c == '"' {
			return string(rest[:i])
		}
		if c == '\\' {
			return ""
		}
	}
	return ""
}

// topLevelKeys calls fn with each key of the top-level JSON object in b.
func topLevelKeys(b []byte, fn func(key []byte)) {
	depth, expectKey := 0, false
	for i := 0; i < len(b); i++ {
		switch b[i] {
		case '{':
			depth++
			if depth == 1 {
				expectKey = true
			}
		case '[':
			depth++
		case '}', ']':
			depth--
		case ',':
			if depth == 1 {
				expectKey = true
			}
		case '"':
			j := i + 1
			for j < len(b) && b[j] != '"' {
				if b[j] == '\\' {
					j++
				}
				j++
			}
			if depth == 1 && expectKey && j <= len(b) {
				fn(b[i+1 : min(j, len(b))])
				expectKey = false
			}
			i = j
		}
	}
}
