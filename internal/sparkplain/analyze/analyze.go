// Package analyze turns parsed sources into report sections and findings.
// Each section has one analyzer; they run in a fixed order because later
// ones (memory, summary) read what earlier ones worked out.
package analyze

import (
	"sort"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// Thresholds tune the findings rules (SPEC §5). They can be set in the
// config file.
type Thresholds struct {
	SkewRatio        float64       `yaml:"skew-ratio"`         // slowest task over this × the stage median
	SkewMinTask      time.Duration `yaml:"skew-min-task"`      // ignore stages whose slowest task is shorter
	SkewMinTasks     int           `yaml:"skew-min-tasks"`     // ignore stages with fewer successful tasks
	SpillShare       float64       `yaml:"spill-share"`        // disk spill over this share of shuffle write
	GCShare          float64       `yaml:"gc-share"`           // GC time over this share of run time
	LowCPUShare      float64       `yaml:"low-cpu-share"`      // CPU time under this share of run time
	MemoryUsedShare  float64       `yaml:"memory-used-share"`  // peak heap under this share of the heap
	MinRunTime       time.Duration `yaml:"min-run-time"`       // skip CPU and GC rules for less task time than this
	SchedDelayShare  float64       `yaml:"sched-delay-share"`  // scheduler delay over this share of task time
	LocalityAnyShare float64       `yaml:"locality-any-share"` // input tasks off their data's host over this share
	ResultShare      float64       `yaml:"result-share"`       // a stage's results over this share of spark.driver.maxResultSize
	SlowStartup      time.Duration `yaml:"slow-startup"`       // executors taking longer than this to register
}

// DefaultThresholds are the values in SPEC §5.
func DefaultThresholds() Thresholds {
	return Thresholds{
		SkewRatio: 5, SkewMinTask: time.Second, SkewMinTasks: 5,
		SpillShare: 0.10, GCShare: 0.10, LowCPUShare: 0.30, MemoryUsedShare: 0.40,
		MinRunTime:      time.Minute,
		SchedDelayShare: 0.20, LocalityAnyShare: 0.30, ResultShare: 0.50, SlowStartup: time.Minute,
	}
}

// Input is everything one run collected.
type Input struct {
	AppID       string // the application asked for; the event log's own ID wins
	Tool        string
	Mode        string
	GeneratedAt time.Time
	TimeZone    string
	EventLog    *model.EventLog    // nil when not supplied or unreadable
	EventSource model.SourceStatus // the Sources row for the event log
	Thresholds  Thresholds

	// From the EMR API and the cluster's logs (online and -from runs).
	Cluster  *model.Cluster
	Steps    []model.Step
	Logs     []model.LogFile
	Metrics  *model.MetricsSection  // CloudWatch, when read
	AWSCalls *model.AWSCallsSection // CloudTrail, when read
	// LogsRead says the run was asked to read the cluster's logs;
	// LogSources are then its Sources rows (EMR API, container, step and
	// node logs).
	LogsRead   bool
	LogSources []model.SourceStatus
}

// ctx is shared state for the analyzers.
type ctx struct {
	in       Input
	log      *model.EventLog
	conf     map[string]string // Spark and Hadoop properties, already redacted
	sys      map[string]string // JVM and system properties
	confSrc  model.Source
	t        Thresholds
	end      time.Time // application end, or last event time
	findings []model.Finding
	pyspark  bool
	logs     *logView // nil when the run read no container, step or node logs
}

func (c *ctx) add(f model.Finding) { c.findings = append(c.findings, f) }

// Run builds the report.
func Run(in Input) *model.Report {
	r := &model.Report{
		SchemaVersion: model.SchemaVersion,
		Tool:          in.Tool,
		GeneratedAt:   in.GeneratedAt.UTC(),
		TimeZone:      in.TimeZone,
		Mode:          in.Mode,
	}
	c := &ctx{in: in, log: in.EventLog, t: in.Thresholds, conf: map[string]string{}, sys: map[string]string{}}
	if c.t == (Thresholds{}) {
		c.t = DefaultThresholds()
	}
	r.Cluster, r.Steps, r.Metrics, r.AWSCalls = in.Cluster, in.Steps, in.Metrics, in.AWSCalls
	if in.LogsRead {
		r.Logs = &model.LogsSection{Coverage: model.Complete, Files: in.Logs}
		if r.Logs.Files == nil {
			r.Logs.Files = []model.LogFile{}
		}
	}
	if l := c.log; l != nil {
		r.Application = l.Application
		r.EventLog = &l.Stats
		for _, e := range l.Config {
			switch e.Origin {
			case "Spark Properties", "Hadoop Properties":
				c.conf[e.Key] = e.Value
				c.confSrc = e.Source
			case "JVM Information", "System Properties":
				c.sys[e.Key] = e.Value
			}
		}
		_, c.pyspark = c.conf["spark.submit.pyFiles"]
		c.end = l.Application.End
		if c.end.IsZero() && !l.Application.Start.IsZero() {
			c.end = l.Application.Start.Add(time.Duration(l.Application.DurationMs) * time.Millisecond)
		}
	}
	if r.Application.ID == "" {
		r.Application.ID = in.AppID
	}
	for _, a := range []func(*ctx, *model.Report){
		analyzeConfig, analyzeExecutors, analyzeNodes, analyzeMemory, analyzeCPU,
		analyzeIO, analyzeJobs, analyzeTimeline, analyzeLogs, analyzeMetrics, analyzeIdentity, analyzeCalls,
	} {
		a(c, r)
	}
	investigateFindings(c)
	sort.SliceStable(c.findings, func(i, j int) bool {
		a, b := c.findings[i], c.findings[j]
		if a.Severity.Rank() != b.Severity.Rank() {
			return a.Severity.Rank() < b.Severity.Rank()
		}
		return rulePriority(a.Rule) < rulePriority(b.Rule)
	})
	r.Findings = c.findings
	if r.Findings == nil {
		r.Findings = []model.Finding{}
	}
	analyzeSources(c, r)
	analyzeCoverage(c, r)
	analyzeSummary(c, r)
	r.ExitCode = exitCode(c, r)
	return r
}

// exitCode follows SPEC §6: 3 when a source is missing or only partly read.
func exitCode(c *ctx, r *model.Report) int {
	for _, s := range r.Sources {
		if s.Status == "partial" || s.Status == "error" || s.Status == "not-supplied" {
			return 3
		}
	}
	return 0
}

func (c *ctx) has() bool { return c.log != nil }

func (c *ctx) confBool(key string, def bool) bool {
	v, ok := c.conf[key]
	if !ok {
		return def
	}
	return v == "true" || v == "True" || v == "TRUE"
}

func share(a, b int64) float64 {
	if b <= 0 {
		return 0
	}
	return float64(a) / float64(b)
}

// rulePriority orders findings of the same severity: what broke the run
// first, then what slowed it, then tuning notes.
func rulePriority(rule string) int {
	for i, r := range []string{
		"log-first-failure", "job-failed", "step-failed", "bootstrap-failed", "executor-memory-kill", "out-of-memory", "access-denied",
		"kerberos-failure", "metastore-failure", "hbase-failure", "executor-lost", "spot-interrupted", "app-retried", "waited-for-capacity", "executor-fit", "idle-nodes", "host-memory-pressure", "host-cpu-saturated", "executor-decommissioned", "stage-retried",
		"access-static-keys", "stage-skew", "memory-spill", "memory-gc-pressure", "memory-heap-near-limit",
		"config-unlimited-result", "config-dynalloc-no-shuffle",
	} {
		if r == rule {
			return i
		}
	}
	return 100
}
