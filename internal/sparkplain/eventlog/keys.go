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
	evDriverAccum     = "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates"
	catalogPrefix     = "org.apache.spark.sql.catalyst.catalog."
)

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
