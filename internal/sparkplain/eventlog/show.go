package eventlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/redact"
)

// errFound stops the line loop once the event is found.
var errFound = errors.New("found")

// ParseLocation reads a "file:line" as the pages cite it.
func ParseLocation(s string) (file string, line int64, err error) {
	i := strings.LastIndex(s, ":")
	if i <= 0 {
		return "", 0, fmt.Errorf("%q is not file:line", s)
	}
	line, err = strconv.ParseInt(s[i+1:], 10, 64)
	if err != nil || line <= 0 {
		return "", 0, fmt.Errorf("%q is not file:line", s)
	}
	return s[:i], line, nil
}

// ShowEvent returns the event at file:line as indented JSON, with every
// value redacted by its key and free text redacted, so the event behind any
// value on the pages can be read without exposing secrets.
func ShowEvent(ctx context.Context, in *Input, file string, line int64) ([]byte, error) {
	var out []byte
	var parseErr error
	var names []string
	in.eachLine(ctx, func(b []byte, src model.Source, partial bool) error {
		if len(names) == 0 || names[len(names)-1] != src.File {
			names = append(names, src.File)
		}
		if src.File != file || src.Line != line {
			return nil
		}
		var v any
		if err := json.Unmarshal(b, &v); err != nil {
			parseErr = fmt.Errorf("the line at %s is not a whole event: %v", src, err)
			return errFound
		}
		out, parseErr = json.MarshalIndent(redactJSON("", v), "", "  ")
		return errFound
	}, func(model.Source) {})
	if parseErr != nil {
		return nil, parseErr
	}
	if out == nil {
		return nil, fmt.Errorf("no line %d in %s; this log's files are named %s", line, file, strings.Join(names, ", "))
	}
	return out, nil
}

func redactJSON(key string, v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = redactJSON(k, e)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = redactJSON(key, e)
		}
		return out
	case string:
		v, _ := redact.Value(key, x)
		return v
	}
	return v
}
