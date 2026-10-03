package report

import (
	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

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
