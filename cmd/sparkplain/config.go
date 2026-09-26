package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/ryandam9/sparkplain/internal/sparkplain/analyze"
	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// fileConfig is the YAML defaults file (SPEC §6), by default
// ~/.config/sparkplain/config.yaml. Flags override it.
type fileConfig struct {
	EventLogPrefix string               `yaml:"eventlog-prefix"`
	TimeZone       string               `yaml:"timezone"`
	Out            string               `yaml:"out"`
	Format         string               `yaml:"format"`
	MaxSize        string               `yaml:"max-size"`
	OverallTimeout time.Duration        `yaml:"overall-timeout"`
	Thresholds     thresholds           `yaml:"thresholds"`
	Explorer       model.ExplorerLimits `yaml:"explorer"`
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
	return d
}

func defaultConfigPath() string {
	if dir, err := os.UserConfigDir(); err == nil {
		return filepath.Join(dir, "sparkplain", "config.yaml")
	}
	return ""
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
