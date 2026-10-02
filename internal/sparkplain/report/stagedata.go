package report

import (
	"strings"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// maxDataCell caps the folders and tables a Stages table cell lists.
const maxDataCell = 4

// dataLabel says in words what a stage read or wrote: "read folder
// s3://b/in: 20 file splits, 2.4 GiB".
func dataLabel(d model.StageData) string {
	verb := "read"
	if d.Access == model.DataWrite {
		verb = "wrote"
	}
	return verb + " " + dataWhat(d) + dataFacts(d)
}

// dataWhat names the folder or table.
func dataWhat(d model.StageData) string {
	switch d.Kind {
	case model.DataHBase:
		return "HBase table " + d.Name
	case model.DataTable:
		return "table " + d.Name
	}
	return d.Name
}

// dataFacts is ": " and what the logs say of its size, or "".
func dataFacts(d model.StageData) string {
	var f []string
	if d.Parts > 0 {
		unit := "file"
		switch {
		case d.Kind == model.DataHBase:
			unit = "region"
		case d.Access == model.DataRead:
			unit = "file split"
		}
		if d.Parts != 1 {
			unit += "s"
		}
		f = append(f, model.Num(int64(d.Parts))+" "+unit)
	}
	if d.Bytes > 0 {
		b := model.Bytes(d.Bytes)
		if d.Sized < d.Parts {
			b = "at least " + b
		}
		f = append(f, b)
	}
	if len(f) == 0 {
		return ""
	}
	return ": " + strings.Join(f, ", ")
}

// dataRows is a stage's StageData for the explorer: access, kind, name,
// format, parts, sized, bytes, sources, line, files.
func dataRows(ds []model.StageData, src func(model.Source) any) [][]any {
	if len(ds) == 0 {
		return nil
	}
	out := make([][]any, 0, len(ds))
	for _, d := range ds {
		out = append(out, []any{d.Access, d.Kind, d.Name, d.Format, d.Parts, d.Sized, d.Bytes, d.From, src(d.Source), orEmpty(d.Paths)})
	}
	return out
}
