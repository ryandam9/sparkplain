package model

import "strings"

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
	Source         Source       `json:"source,omitzero"`
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
