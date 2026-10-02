package model

import (
	"strings"
	"time"
)

// HBaseScan is a TableInputFormat scan as the job defined it, decoded from
// its base64 string (hbase.mapreduce.scan). Row keys and values are shown
// as HBase's Bytes.toStringBinary does: printable ASCII as it is, any
// other byte as \xNN.
type HBaseScan struct {
	Table string `json:"table,omitempty"`
	// StartRow and StopRow are empty for the table's first and last row.
	StartRow       string       `json:"startRow"`
	StopRow        string       `json:"stopRow"`
	IncludeStart   bool         `json:"includeStart"`
	IncludeStop    bool         `json:"includeStop"`
	Reversed       bool         `json:"reversed,omitempty"`
	Columns        []string     `json:"columns,omitempty"` // family, or family:qualifier
	TimeRange      string       `json:"timeRange,omitempty"`
	FamilyTimes    []string     `json:"familyTimeRanges,omitempty"`
	MaxVersions    int          `json:"maxVersions"`
	Caching        int          `json:"caching,omitempty"`   // rows per call; 0: hbase.client.scanner.caching
	BatchSize      int          `json:"batchSize,omitempty"` // cells per Result; 0: whole rows
	MaxResultSize  int64        `json:"maxResultSize,omitempty"`
	CacheBlocks    bool         `json:"cacheBlocks"`
	ReadType       string       `json:"readType,omitempty"`
	Consistency    string       `json:"consistency,omitempty"`
	PartialResults bool         `json:"allowPartialResults,omitempty"`
	Attributes     []string     `json:"attributes,omitempty"`
	Filter         *HBaseFilter `json:"filter,omitempty"`
	// Source is where the scan string was found, and Time when (a
	// sparkplain-scan line's time, or the last logged before it).
	Source Source    `json:"source,omitzero"`
	Time   time.Time `json:"time,omitzero"`
}

// HBaseFilter is one filter of a scan: its class (without the package),
// what it tests in words, and the filters it holds (FilterList, SkipFilter,
// WhileMatchFilter). Decoded is false for a filter sparkplain does not
// know, which is named but not explained.
type HBaseFilter struct {
	Name     string        `json:"name"`
	Text     string        `json:"text,omitempty"`
	Decoded  bool          `json:"decoded"`
	Children []HBaseFilter `json:"children,omitempty"`
}

// Rows is the scan's key range in words, such as [2026-08-15, 2026-10-10).
func (s HBaseScan) Rows() string {
	start, stop := s.StartRow, s.StopRow
	if start == "" {
		start = "first row"
	}
	if stop == "" {
		stop = "last row"
	}
	open, close := "[", ")"
	if !s.IncludeStart && s.StartRow != "" {
		open = "("
	}
	if s.IncludeStop || s.StopRow == "" {
		close = "]"
	}
	return open + start + ", " + stop + close
}

// Lines is the filter tree, one line per filter, indented with box
// characters, for the console and the report.
func (f *HBaseFilter) Lines() []string {
	if f == nil {
		return nil
	}
	var out []string
	var walk func(f HBaseFilter, prefix, branch string)
	walk = func(f HBaseFilter, prefix, branch string) {
		line := f.Name
		if f.Text != "" {
			line += "  " + f.Text
		}
		if !f.Decoded {
			line += "  (not decoded)"
		}
		out = append(out, prefix+branch+line)
		next := prefix
		switch branch {
		case "├─ ":
			next += "│  "
		case "└─ ":
			next += "   "
		}
		for i, c := range f.Children {
			b := "├─ "
			if i == len(f.Children)-1 {
				b = "└─ "
			}
			walk(c, next, b)
		}
	}
	walk(*f, "", "")
	return out
}

// String is the whole filter tree on one line, for a table cell or a
// finding: FilterList MUST_PASS_ALL (A; B).
func (f *HBaseFilter) String() string {
	if f == nil {
		return ""
	}
	s := f.Name
	if f.Text != "" {
		s += " " + f.Text
	}
	if len(f.Children) > 0 {
		parts := make([]string, len(f.Children))
		for i := range f.Children {
			parts[i] = f.Children[i].String()
		}
		s += " (" + strings.Join(parts, "; ") + ")"
	}
	return s
}

// Facts are the scan's settings as label and value, leaving out what is
// HBase's default, for the console, the report and the explorer.
func (s HBaseScan) Facts() [][2]string {
	var out [][2]string
	add := func(label, value string) {
		if value != "" {
			out = append(out, [2]string{label, value})
		}
	}
	add("Table", s.Table)
	rows := s.Rows()
	if s.Reversed {
		rows += ", read in reverse"
	}
	add("Rows", rows)
	cols := "all"
	if len(s.Columns) > 0 {
		cols = strings.Join(s.Columns, ", ")
	}
	add("Columns", cols)
	add("Time range", s.TimeRange)
	add("Family time ranges", strings.Join(s.FamilyTimes, "; "))
	if s.MaxVersions != 1 {
		add("Versions", Num(int64(s.MaxVersions))+" per cell")
	}
	if s.Caching > 0 {
		add("Caching", Plural(s.Caching, "row", "rows")+" per call to the region server")
	}
	if s.BatchSize > 0 {
		add("Batch", Plural(s.BatchSize, "cell", "cells")+" per result")
	}
	if s.MaxResultSize > 0 {
		add("Max result size", Bytes(s.MaxResultSize)+" per call")
	}
	if !s.CacheBlocks {
		add("Block cache", "not filled by this scan")
	}
	add("Read type", s.ReadType)
	add("Consistency", s.Consistency)
	if s.PartialResults {
		add("Partial results", "allowed")
	}
	add("Attributes", strings.Join(s.Attributes, ", "))
	return out
}

// HBaseSplit is one region a TableInputFormat scan read: the line an
// executor logged as its task began (Input split: Split(tablename=…,
// startrow=…, endrow=…, regionLocation=…, regionname=…)), and the region's
// size as TableInputFormat estimated it (Input split length: 50 M bytes),
// when its line can be told apart from another task's. StartRow and
// EndRow are the scan's range cut to the region, as Bytes.toStringBinary
// prints them; empty means the table's first or last row.
type HBaseSplit struct {
	Table      string    `json:"table"`
	StartRow   string    `json:"startRow"`
	EndRow     string    `json:"endRow"`
	Server     string    `json:"server"`
	Region     string    `json:"region"`
	SizeBytes  int64     `json:"sizeBytes,omitempty"`
	Time       time.Time `json:"time,omitzero"`
	Source     Source    `json:"source"`
	SizeSource Source    `json:"sizeSource,omitzero"`
	// Task is the task that logged the split, when the log layout prints
	// the thread, which Spark names after it.
	Task *SplitTask `json:"task,omitempty"`
}

// SplitTask is a task as its executor thread names it: "… for task 41.0
// in stage 172.0 (TID 6429)". Start and End are the times of its "Running
// task" and "Finished task" lines (End from "Exception in task" when it
// Failed), when the log holds them; EndSource is that last line.
type SplitTask struct {
	TaskID       int64     `json:"taskId"`
	Partition    int       `json:"partition"`
	Attempt      int       `json:"attempt"`
	Stage        int       `json:"stage"`
	StageAttempt int       `json:"stageAttempt"`
	Start        time.Time `json:"start,omitzero"`
	End          time.Time `json:"end,omitzero"`
	Failed       bool      `json:"failed,omitempty"`
	EndSource    Source    `json:"endSource,omitzero"`
}

// HBaseScanRead is what one TableInputFormat scan stage read, from its
// executors' split lines and the event log: its key range, the regions in
// key order, and per region server how many regions, rows and bytes.
// Regions are tied to their tasks (rows, time, executor) when the stage's
// splits match its partitions one for one (Untied says why not).
// FromLogs marks a scan built without the event log, from split lines that
// name their tasks: its stage, tasks and times come from the executors'
// logs, and rows are not known. Rebuilt marks one whose stage and tasks
// come from a run rebuilt from the driver's log: rows are not known.
type HBaseScanRead struct {
	StageID   int               `json:"stageId"`
	Attempt   int               `json:"attempt"`
	Table     string            `json:"table"`
	Rows      string            `json:"rows"` // the key range its splits cover
	Scan      *HBaseScan        `json:"scan,omitempty"`
	Regions   []HBaseRegionRead `json:"regions"`
	Servers   []HBaseServerRead `json:"servers"`
	Tasks     int               `json:"tasks"`
	TotalRows int64             `json:"totalRows"`
	// SizeBytes adds up the sizes TableInputFormat estimated, of the
	// SizedRegions whose size line could be told apart.
	SizeBytes    int64  `json:"sizeBytes"`
	SizedRegions int    `json:"sizedRegions"`
	Tied         bool   `json:"tied"`
	TiedBy       string `json:"tiedBy,omitempty"` // "task" (each split line names its task) or "key order"
	Untied       string `json:"untied,omitempty"`
	FromLogs     bool   `json:"fromLogs,omitempty"`
	Rebuilt      bool   `json:"rebuilt,omitempty"`
	Source       Source `json:"source"`
}

// HBaseRegionRead is one region a scan read, and the task that read it
// when the region could be tied to one.
type HBaseRegionRead struct {
	Region     string    `json:"region"`
	StartRow   string    `json:"startRow"`
	EndRow     string    `json:"endRow"`
	Server     string    `json:"server"`
	SizeBytes  int64     `json:"sizeBytes,omitempty"`
	Task       *ScanTask `json:"task,omitempty"`
	Source     Source    `json:"source"`
	SizeSource Source    `json:"sizeSource,omitzero"`
}

// HBaseServerRead is what a scan read from one region server. Rows and
// TaskMs count only the regions tied to their tasks.
type HBaseServerRead struct {
	Server    string `json:"server"`
	Regions   int    `json:"regions"`
	Rows      int64  `json:"rows"`
	SizeBytes int64  `json:"sizeBytes"`
	TaskMs    int64  `json:"taskMs"`
}

// HBaseTaskRead is one task attempt that read an HBase region with
// TableInputFormat: the region from its split line, and the task from the
// executor thread that logged it or, with the event log, from the scan's
// tie. Stage, Partition and TaskID are -1 when not known (a split line in
// a layout with no thread that could not be tied). With the event log,
// Rows, DurationMs and the executor come from it (TimeFrom "event log");
// without, the time runs from the task's Running to its Finished line
// (TimeFrom "executor log").
type HBaseTaskRead struct {
	Stage        int       `json:"stage"`
	StageAttempt int       `json:"stageAttempt"`
	Partition    int       `json:"partition"`
	Attempt      int       `json:"attempt"`
	TaskID       int64     `json:"taskId"`
	Table        string    `json:"table"`
	StartRow     string    `json:"startRow"`
	EndRow       string    `json:"endRow"`
	Region       string    `json:"region"`
	Server       string    `json:"server"`
	SizeBytes    int64     `json:"sizeBytes,omitempty"`
	ExecutorID   string    `json:"executorId,omitempty"`
	Host         string    `json:"host,omitempty"`
	Start        time.Time `json:"start,omitzero"`
	End          time.Time `json:"end,omitzero"`
	DurationMs   int64     `json:"durationMs"`
	Timed        bool      `json:"timed"`
	TimeFrom     string    `json:"timeFrom,omitempty"`
	// Outcome is "finished", "failed" or "no end logged", or empty when the
	// split line names no task to follow.
	Outcome    string `json:"outcome,omitempty"`
	Rows       int64  `json:"rows"`
	RowsKnown  bool   `json:"rowsKnown"`
	Source     Source `json:"source"`
	SizeSource Source `json:"sizeSource,omitzero"`
	EndSource  Source `json:"endSource,omitzero"`  // the Finished or Exception line
	TaskSource Source `json:"taskSource,omitzero"` // the event log's task end
	// Slow marks a task that took at least twice its stage's median (and
	// skew-min-task more); Events are what its region's server logged
	// while it read it, as indexes into the section's RegionEvents.
	Slow   bool  `json:"slow,omitempty"`
	Events []int `json:"events,omitempty"`
}

// HBaseTaskStage sums one stage's rows of the tasks table: its tables, its
// task attempts and how many failed, the regions and region servers they
// read, and its first start and last end.
type HBaseTaskStage struct {
	Stage        int       `json:"stage"`
	StageAttempt int       `json:"stageAttempt"`
	Tables       []string  `json:"tables"`
	Tasks        int       `json:"tasks"`
	Failed       int       `json:"failed"`
	Regions      int       `json:"regions"`
	Servers      int       `json:"servers"`
	Start        time.Time `json:"start,omitzero"`
	End          time.Time `json:"end,omitzero"`
}

// HBaseRegionEvent is something an HBase server logged about one region
// while the run went on, named by the region's encoded name: a flush, a
// compaction, the region closed or opened, a move or split the Master
// ran, refused writes (busy) or a slow call. DurationMs is how long it
// took, when the line says, ending at Time. Tasks are the run's tasks
// that were reading the region then, and Slow the ones that took at least
// twice their stage's median.
type HBaseRegionEvent struct {
	Time       time.Time `json:"time"`
	Event      string    `json:"event"`
	Region     string    `json:"region"`
	Table      string    `json:"table,omitempty"`
	Host       string    `json:"host,omitempty"`
	Detail     string    `json:"detail,omitempty"`
	DurationMs int64     `json:"durationMs,omitempty"`
	Source     Source    `json:"source"`
	// Count is how many such lines were folded into one (the same event on
	// the same region and server, a minute apart at most), from First.
	Count int       `json:"count,omitempty"`
	First time.Time `json:"first,omitzero"`
	Tasks []string  `json:"tasks,omitempty"`
	Slow  []string  `json:"slow,omitempty"`
}
