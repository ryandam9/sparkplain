package report

import (
	"html/template"
)

func esc(s string) string { return template.HTMLEscapeString(s) }
