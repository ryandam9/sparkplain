// Package model holds sparkplain's canonical types. Parsers fill them,
// analyzers read them, and the JSON export is a direct encoding of Report.
//
// Every value that comes from a log records where it came from (Source), so
// findings and tables can point at the exact file and line.
package model

import (
	"fmt"
)

// SchemaVersion is bumped whenever the JSON shape changes incompatibly.
const SchemaVersion = "1.0"

// Source points to the file and line a value was read from. For values folded
// from many events (per-stage totals, for example) it points at the first
// contributing event and EndLine at the last one in the same file.
type Source struct {
	File    string `json:"file"`
	Line    int64  `json:"line,omitempty"`
	EndLine int64  `json:"endLine,omitempty"`
}

// IsZero reports whether s points nowhere.
func (s Source) IsZero() bool { return s.File == "" }

func (s Source) String() string {
	switch {
	case s.File == "":
		return ""
	case s.Line == 0:
		return s.File
	case s.EndLine > s.Line:
		return fmt.Sprintf("%s:%d-%d", s.File, s.Line, s.EndLine)
	default:
		return fmt.Sprintf("%s:%d", s.File, s.Line)
	}
}

// Extend widens s to also cover other, when both are in the same file.
func (s *Source) Extend(other Source) {
	if s.File == "" {
		*s = other
		return
	}
	if other.File != s.File || other.Line == 0 {
		return
	}
	if other.Line < s.Line {
		if s.EndLine == 0 {
			s.EndLine = s.Line
		}
		s.Line = other.Line
	}
	if other.Line > s.Line && other.Line > s.EndLine {
		s.EndLine = other.Line
	}
}

// Severity ranks findings.
type Severity string

const (
	Info     Severity = "info"
	Warning  Severity = "warning"
	Critical Severity = "critical"
)

// Rank orders severities, most severe first.
func (s Severity) Rank() int {
	switch s {
	case Critical:
		return 0
	case Warning:
		return 1
	default:
		return 2
	}
}

// Coverage says how much of a report section could be filled.
type Coverage string

const (
	Complete      Coverage = "complete"
	Partial       Coverage = "partial"
	NeedsEventLog Coverage = "needs-event-log"
	NoData        Coverage = "no-data" // its source was read but held nothing for this run
)

// Status values used by applications, jobs and stages.
const (
	StatusSucceeded  = "succeeded"
	StatusFailed     = "failed"
	StatusIncomplete = "incomplete"
	StatusSkipped    = "skipped"
	StatusRunning    = "running"
	StatusUnknown    = "unknown"
)
