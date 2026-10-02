package main

import (
	"bytes"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/ryandam9/sparkplain/internal/sparkplain/analyze"
	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// fileConfig is the YAML defaults file (SPEC §6), by default
// ~/.config/sparkplain/config.yaml. Flags override it.
type fileConfig struct {
	// The clusters, by name, and the AWS profile and region: with them
	// here, a run needs only -app-id. Flags override each.
	ClusterName      string `yaml:"cluster-name"`
	HBaseClusterName string `yaml:"hbase-cluster-name"`
	Profile          string `yaml:"profile"`
	Region           string `yaml:"region"`

	// Envs are named sets of the keys above that differ between
	// environments, such as prod and nonprod; -env picks one, whose keys
	// override the top-level ones.
	Envs map[string]envConfig `yaml:"environments"`

	EventLogPrefix string               `yaml:"eventlog-prefix"`
	TimeZone       string               `yaml:"timezone"`
	Out            string               `yaml:"out"`
	Format         string               `yaml:"format"`
	MaxSize        string               `yaml:"max-size"`
	MaxUnpacked    string               `yaml:"max-unpacked"`
	OverallTimeout time.Duration        `yaml:"overall-timeout"`
	Thresholds     thresholds           `yaml:"thresholds"`
	Explorer       model.ExplorerLimits `yaml:"explorer"`
	Read           readConfig           `yaml:"read"`
}

// readConfig turns sources off: every one is read unless set to no.
// Container logs, the EMR API and the event log are always read.
type readConfig struct {
	StepLogs   *yesNo `yaml:"step-logs"`
	NodeLogs   *yesNo `yaml:"node-logs"`
	HBaseLogs  *yesNo `yaml:"hbase-logs"`
	CloudWatch *yesNo `yaml:"cloudwatch"`
	CloudTrail *yesNo `yaml:"cloudtrail"`

	env map[string]string // key -> the environment whose read: block set it, for the reason shown
}

// yesNo is a switch written yes or no (also true, false, on or off): YAML
// 1.2, which the config reader follows, takes a bare yes as text.
type yesNo bool

func (v *yesNo) UnmarshalYAML(n *yaml.Node) error {
	switch strings.ToLower(n.Value) {
	case "yes", "true", "on":
		*v = true
	case "no", "false", "off":
		*v = false
	default:
		return fmt.Errorf("line %d: %q is not yes or no", n.Line, n.Value)
	}
	return nil
}

// over lays e's keys over r's.
func (r readConfig) over(e readConfig, env string) readConfig {
	keys := []string{"step-logs", "node-logs", "hbase-logs", "cloudwatch", "cloudtrail"}
	for i, p := range [][2]**yesNo{{&r.StepLogs, &e.StepLogs}, {&r.NodeLogs, &e.NodeLogs}, {&r.HBaseLogs, &e.HBaseLogs}, {&r.CloudWatch, &e.CloudWatch}, {&r.CloudTrail, &e.CloudTrail}} {
		if *p[1] != nil {
			*p[0] = *p[1]
			env2 := map[string]string{keys[i]: env}
			for k, v := range r.env {
				if k != keys[i] {
					env2[k] = v
				}
			}
			r.env = env2
		}
	}
	return r
}

// readSwitch is one source a run can be told not to read: its name in
// the Sources list, its key under read:, and its flag.
type readSwitch struct {
	source, key, flag string
	off               *bool
	cfg               *yesNo
}

// applyReads turns off what the flags or the config file's read: block
// say, and records why, by source, for the access check and the report.
func (o *options) applyReads(r readConfig) {
	o.off = map[string]string{}
	for _, s := range []readSwitch{
		{"Step logs", "step-logs", "no-step-logs", &o.noStepLogs, r.StepLogs},
		{"Node logs", "node-logs", "no-node-logs", &o.noNodeLogs, r.NodeLogs},
		{"HBase server logs", "hbase-logs", "no-hbase-logs", &o.noHBaseLogs, r.HBaseLogs},
		{"CloudWatch", "cloudwatch", "no-cloudwatch", &o.noCloudWatch, r.CloudWatch},
		{"CloudTrail", "cloudtrail", "no-cloudtrail", &o.noCloudTrail, r.CloudTrail},
	} {
		switch {
		case *s.off:
			o.off[s.source] = "turned off with -" + s.flag
		case s.cfg != nil && !bool(*s.cfg):
			*s.off = true
			where := "the config file"
			if env := r.env[s.key]; env != "" {
				where += "'s " + env + " environment"
			}
			o.off[s.source] = "turned off in " + where + " (read: " + s.key + ": no)"
		}
	}
}

// envConfig is one environment's keys: where its clusters and logs are,
// and how to reach them. Keys it leaves out keep the top-level values.
type envConfig struct {
	ClusterName      string     `yaml:"cluster-name"`
	HBaseClusterName string     `yaml:"hbase-cluster-name"`
	Profile          string     `yaml:"profile"`
	Region           string     `yaml:"region"`
	EventLogPrefix   string     `yaml:"eventlog-prefix"`
	TimeZone         string     `yaml:"timezone"`
	Out              string     `yaml:"out"`
	Read             readConfig `yaml:"read"`
}

// expandHome turns a leading ~/ (or a lone ~) into the home folder; any
// other path, and s3:// locations, stay as they are.
func expandHome(p string) string {
	if p != "~" && !strings.HasPrefix(p, "~/") {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return p
	}
	return filepath.Join(home, strings.TrimPrefix(p, "~"))
}

// withEnv is the config with environment name's keys laid over the
// top-level ones; "" is the config as it is.
func (c fileConfig) withEnv(name string) (fileConfig, error) {
	if name == "" {
		return c, nil
	}
	e, ok := c.Envs[name]
	if !ok {
		if len(c.Envs) == 0 {
			return c, fmt.Errorf("-env %s: the config file has no environments (add them under environments:)", name)
		}
		names := make([]string, 0, len(c.Envs))
		for n := range c.Envs {
			names = append(names, n)
		}
		sort.Strings(names)
		return c, fmt.Errorf("-env %s: the config file has no environment of that name; it has %s", name, strings.Join(names, ", "))
	}
	set := func(dst *string, v string) {
		if v != "" {
			*dst = v
		}
	}
	set(&c.ClusterName, e.ClusterName)
	set(&c.HBaseClusterName, e.HBaseClusterName)
	set(&c.Profile, e.Profile)
	set(&c.Region, e.Region)
	set(&c.EventLogPrefix, e.EventLogPrefix)
	set(&c.TimeZone, e.TimeZone)
	set(&c.Out, e.Out)
	c.Read = c.Read.over(e.Read, name)
	return c, nil
}

// thresholds mirrors analyze.Thresholds with optional fields, so a file
// can set only the values it wants to change.
type thresholds struct {
	SkewRatio        *float64       `yaml:"skew-ratio"`
	SkewMinTask      *time.Duration `yaml:"skew-min-task"`
	SkewMinTasks     *int           `yaml:"skew-min-tasks"`
	SpillShare       *float64       `yaml:"spill-share"`
	GCShare          *float64       `yaml:"gc-share"`
	LowCPUShare      *float64       `yaml:"low-cpu-share"`
	MemoryUsedShare  *float64       `yaml:"memory-used-share"`
	MinRunTime       *time.Duration `yaml:"min-run-time"`
	SchedDelayShare  *float64       `yaml:"sched-delay-share"`
	LocalityAnyShare *float64       `yaml:"locality-any-share"`
	ResultShare      *float64       `yaml:"result-share"`
	SlowStartup      *time.Duration `yaml:"slow-startup"`
	DriverGapShare   *float64       `yaml:"driver-gap-share"`
	DriverGapMin     *time.Duration `yaml:"driver-gap-min"`
	HBaseTimeShare   *float64       `yaml:"hbase-time-share"`
	HBaseConnections *int           `yaml:"hbase-connections"`
	HBaseHotspot     *float64       `yaml:"hbase-hotspot-share"`
}

func (t thresholds) apply(d analyze.Thresholds) analyze.Thresholds {
	setF := func(dst *float64, v *float64) {
		if v != nil {
			*dst = *v
		}
	}
	setD := func(dst *time.Duration, v *time.Duration) {
		if v != nil {
			*dst = *v
		}
	}
	setF(&d.SkewRatio, t.SkewRatio)
	setD(&d.SkewMinTask, t.SkewMinTask)
	if t.SkewMinTasks != nil {
		d.SkewMinTasks = *t.SkewMinTasks
	}
	setF(&d.SpillShare, t.SpillShare)
	setF(&d.GCShare, t.GCShare)
	setF(&d.LowCPUShare, t.LowCPUShare)
	setF(&d.MemoryUsedShare, t.MemoryUsedShare)
	setD(&d.MinRunTime, t.MinRunTime)
	setF(&d.SchedDelayShare, t.SchedDelayShare)
	setF(&d.LocalityAnyShare, t.LocalityAnyShare)
	setF(&d.ResultShare, t.ResultShare)
	setD(&d.SlowStartup, t.SlowStartup)
	setF(&d.DriverGapShare, t.DriverGapShare)
	setD(&d.DriverGapMin, t.DriverGapMin)
	setF(&d.HBaseTimeShare, t.HBaseTimeShare)
	if t.HBaseConnections != nil {
		d.HBaseConnections = *t.HBaseConnections
	}
	setF(&d.HBaseHotspot, t.HBaseHotspot)
	return d
}

// defaultConfigPath is ~/.config/sparkplain/config.yaml, or under
// $XDG_CONFIG_HOME when that is set, on macOS as on Linux: the place the
// docs name, not os.UserConfigDir's ~/Library/Application Support.
// Windows keeps %AppData%.
func defaultConfigPath() string {
	if runtime.GOOS == "windows" {
		if dir, err := os.UserConfigDir(); err == nil {
			return filepath.Join(dir, "sparkplain", "config.yaml")
		}
		return ""
	}
	if dir := os.Getenv("XDG_CONFIG_HOME"); filepath.IsAbs(dir) {
		return filepath.Join(dir, "sparkplain", "config.yaml")
	}
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".config", "sparkplain", "config.yaml")
	}
	return ""
}

// starterConfig is the config file -init-config writes: every key, with
// what it does, most of them commented out. A test reads it with the
// same strict reader as the real file, so it cannot drift from it.
//
//go:embed config.example.yaml
var starterConfig string

// writeStarterConfig writes the starter config to path, never over a file
// that is already there.
func writeStarterConfig(path string, stdout, stderr io.Writer) int {
	if path == "" {
		fmt.Fprintln(stderr, "sparkplain: no home folder for the config file; pass -config <path>")
		return exitFatal
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		fmt.Fprintf(stderr, "sparkplain: %v\n", err)
		return exitFatal
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, fs.ErrExist) {
		fmt.Fprintf(stderr, "sparkplain: %s already exists; it was left as it is. Move it aside to write a fresh one.\n", path)
		return exitFatal
	}
	if err == nil {
		_, err = io.WriteString(f, starterConfig)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}
	if err != nil {
		fmt.Fprintf(stderr, "sparkplain: writing %s: %v\n", path, err)
		return exitFatal
	}
	fmt.Fprintf(stdout, "Wrote %s.\nEdit the prod and nonprod blocks (cluster names, profile, region), then run:\n  sparkplain -app-id <application id> -env prod -check\n", path)
	return exitOK
}

// loadConfig reads path. A missing file is fine only when it is the default.
func loadConfig(path string, explicit bool) (fileConfig, error) {
	var c fileConfig
	if path == "" {
		return c, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) && !explicit {
			return c, nil
		}
		return c, fmt.Errorf("reading config %s: %w", path, err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil && !errors.Is(err, io.EOF) { // an empty file is fine
		return c, fmt.Errorf("config %s: %w", path, err)
	}
	return c, nil
}
