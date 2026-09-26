package analyze

import (
	"strconv"
	"strings"
)

// setting describes one important Spark setting: its Spark 3.5 default and
// what it means in plain words. EMR ships its own spark-defaults on top of
// these; comparing against EMR's defaults needs the EMR API (phase 3).
type setting struct {
	key, def, explain string
	kind              string // "", "size-mb" (MiB when no unit), "size-b" (bytes when no unit), "bool", "num"
}

var keySettings = []setting{
	{"spark.submit.deployMode", "client", "Where the driver runs: on the machine that submitted the job (client) or inside the cluster (cluster).", ""},
	{"spark.executor.instances", "2", "How many executors to start when dynamic allocation is off.", "num"},
	{"spark.executor.cores", "1", "Tasks each executor can run at the same time (the YARN default is 1).", "num"},
	{"spark.executor.memory", "1g", "Java heap for each executor.", "size-mb"},
	{"spark.executor.memoryOverhead", "", "Extra container memory outside the heap: off-heap buffers, thread stacks, Python workers. Default is 10% of the heap, at least 384 MiB.", "size-mb"},
	{"spark.executor.pyspark.memory", "", "A separate memory cap for Python workers. Unset means Python shares the overhead.", "size-mb"},
	{"spark.driver.memory", "1g", "Java heap for the driver.", "size-mb"},
	{"spark.driver.cores", "1", "Cores for the driver (cluster mode only).", "num"},
	{"spark.driver.maxResultSize", "1g", "The most data actions like collect() may bring back to the driver. 0 means no limit.", "size-b"},
	{"spark.dynamicAllocation.enabled", "false", "Whether Spark adds and removes executors as the workload changes.", "bool"},
	{"spark.dynamicAllocation.minExecutors", "0", "The fewest executors dynamic allocation keeps.", "num"},
	{"spark.dynamicAllocation.maxExecutors", "infinity", "The most executors dynamic allocation may request.", "num"},
	{"spark.dynamicAllocation.executorIdleTimeout", "60s", "How long an executor may sit idle before dynamic allocation removes it.", ""},
	{"spark.shuffle.service.enabled", "false", "Whether shuffle files are served by the node's shuffle service, so removing an executor does not lose them.", "bool"},
	{"spark.memory.fraction", "0.6", "Share of (heap − 300 MiB) that Spark uses for execution and caching. The rest is for user objects.", "num"},
	{"spark.memory.storageFraction", "0.5", "Part of that memory protected for cached data.", "num"},
	{"spark.memory.offHeap.enabled", "false", "Whether Spark also uses memory outside the Java heap.", "bool"},
	{"spark.memory.offHeap.size", "0", "Off-heap memory per executor, when enabled.", "size-b"},
	{"spark.sql.shuffle.partitions", "200", "How many pieces shuffled data is split into for joins and aggregations.", "num"},
	{"spark.sql.adaptive.enabled", "true", "Adaptive query execution: Spark re-plans queries using real data sizes.", "bool"},
	{"spark.sql.adaptive.coalescePartitions.enabled", "true", "Whether adaptive execution merges small shuffle partitions.", "bool"},
	{"spark.sql.adaptive.skewJoin.enabled", "true", "Whether adaptive execution splits oversized partitions in joins.", "bool"},
	{"spark.sql.autoBroadcastJoinThreshold", "10MB", "Tables smaller than this are copied to every executor instead of shuffled. -1 turns this off.", "size-b"},
	{"spark.sql.files.maxPartitionBytes", "128MB", "The most file data one read task takes.", "size-b"},
	{"spark.default.parallelism", "", "Default partition count for RDD operations.", "num"},
	{"spark.serializer", "org.apache.spark.serializer.JavaSerializer", "How Spark serialises data it moves or caches. Kryo is usually faster.", ""},
	{"spark.speculation", "false", "Whether Spark starts backup copies of slow tasks.", "bool"},
	{"spark.task.maxFailures", "4", "How many times one task may fail before its job fails.", "num"},
	{"spark.sql.catalogImplementation", "in-memory", "Where table definitions live: hive means a Hive metastore or the AWS Glue Data Catalog.", ""},
	{"spark.yarn.queue", "default", "The YARN queue the application ran in.", ""},
	{"spark.eventLog.dir", "", "Where Spark wrote this event log.", ""},
	{"spark.eventLog.rolling.enabled", "false", "Whether the event log is split into parts.", "bool"},
	{"spark.eventLog.logBlockUpdates.enabled", "false", "Whether cache updates are logged. Needed to show cached data sizes.", "bool"},
	{"spark.executor.processTreeMetrics.enabled", "false", "Whether executors report whole-process memory (RSS), including Python workers.", "bool"},
	{"spark.executor.extraJavaOptions", "", "Extra JVM flags for executors, such as the garbage collector.", ""},
}

var settingByKey = func() map[string]setting {
	m := make(map[string]setting, len(keySettings))
	for _, s := range keySettings {
		m[s.key] = s
	}
	return m
}()

// parseSize reads Spark size strings ("4g", "512m", "10MB", "1073741824").
// Numbers without a unit use defUnit bytes. It returns -1 when s is not a size.
func parseSize(s string, defUnit int64) int64 {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return -1
	}
	i := 0
	for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.' || (i == 0 && s[i] == '-')) {
		i++
	}
	n, err := strconv.ParseFloat(s[:i], 64)
	if err != nil {
		return -1
	}
	mult := defUnit
	switch strings.TrimSpace(s[i:]) {
	case "":
	case "b":
		mult = 1
	case "k", "kb", "kib":
		mult = 1 << 10
	case "m", "mb", "mib":
		mult = 1 << 20
	case "g", "gb", "gib":
		mult = 1 << 30
	case "t", "tb", "tib":
		mult = 1 << 40
	case "p", "pb", "pib":
		mult = 1 << 50
	default:
		return -1
	}
	return int64(n * float64(mult))
}

// sameValue compares a value with a default the way Spark would read them.
func sameValue(s setting, v string) bool {
	if s.def == "" {
		return false
	}
	switch s.kind {
	case "size-mb", "size-b":
		unit := int64(1)
		if s.kind == "size-mb" {
			unit = 1 << 20
		}
		a, b := parseSize(v, unit), parseSize(s.def, unit)
		if a >= 0 && b >= 0 {
			return a == b
		}
	case "bool":
		return strings.EqualFold(strings.TrimSpace(v), s.def)
	case "num":
		a, errA := strconv.ParseFloat(strings.TrimSpace(v), 64)
		b, errB := strconv.ParseFloat(s.def, 64)
		if errA == nil && errB == nil {
			return a == b
		}
	}
	return strings.TrimSpace(v) == s.def
}
