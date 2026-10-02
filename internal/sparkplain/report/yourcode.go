package report

import (
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// DefaultSourceContext is how many lines of the application's code are
// shown before and after the line a stage or job ran (source-context).
const DefaultSourceContext = 20

// YourCode is where in the application's own code something ran. Via says
// how it was found: "" for its own call site; "the action of job 3" when
// its call site is Spark's (a PySpark stage named after PythonRDD.scala)
// and its job's is the application's; or, when Spark recorded neither,
// the nearest line it recorded before the stage, said to be so.
type YourCode struct {
	model.CodeLocation
	Via string
}

// sparkFiles are the files of Spark's own that PySpark stages and jobs are
// named after; their call sites are not the application's.
var sparkFiles = map[string]bool{"PythonRDD.scala": true, "SerDeUtil.scala": true, "PythonSQLUtils.scala": true, "PythonUtils.scala": true,
	"NativeMethodAccessorImpl.java": true, "DelegatingMethodAccessorImpl.java": true, "DirectMethodHandleAccessor.java": true, "Method.java": true}

// mine says which code locations are the application's: with its source
// files at hand, those they match; without, any that is not Spark's own.
func mine(srcs []SourceFile) func(model.CodeLocation) bool {
	if len(srcs) > 0 {
		names := map[string]bool{}
		for _, s := range srcs {
			for _, n := range s.Logged {
				names[n] = true
			}
		}
		return func(c model.CodeLocation) bool { return names[c.File] }
	}
	return func(c model.CodeLocation) bool {
		return c.File != "" && c.Line > 0 && !sparkFiles[path.Base(c.File)]
	}
}

// yourCode finds, for each stage attempt ("id.attempt"), the line of the
// application's code it ran: the first of its call site's frames that is
// the application's, else the first of its earliest job's.
func yourCode(r *model.Report, srcs []SourceFile) map[string]*YourCode {
	isMine := mine(srcs)
	first := func(cs []model.CodeLocation) (model.CodeLocation, bool) {
		for _, c := range cs {
			if isMine(c) {
				return c, true
			}
		}
		return model.CodeLocation{}, false
	}
	jobs := map[int]*model.Job{}
	for _, j := range r.Jobs.Jobs {
		jobs[j.ID] = j
	}
	out := map[string]*YourCode{}
	for _, st := range r.Jobs.Stages {
		key := strconv.Itoa(st.ID) + "." + strconv.Itoa(st.Attempt)
		if c, ok := first(st.Code); ok {
			out[key] = &YourCode{CodeLocation: c}
			continue
		}
		best := -1
		for _, id := range st.JobIDs {
			if j := jobs[id]; j != nil && (best < 0 || id < best) {
				if _, ok := first(j.Code); ok {
					best = id
				}
			}
		}
		if best >= 0 {
			c, _ := first(jobs[best].Code)
			out[key] = &YourCode{CodeLocation: c, Via: fmt.Sprintf("the action of job %d", best)}
		}
	}
	// Spark records no line for some actions (PySpark's write, saveAsTable
	// and sql name only NativeMethodAccessorImpl.java:0, for the stage and
	// its job alike): point at the last line it recorded before the stage
	// started, said to be a pointer and not a certainty.
	type at struct {
		t time.Time
		y *YourCode
	}
	var known []at
	for _, st := range r.Jobs.Stages {
		if y := out[strconv.Itoa(st.ID)+"."+strconv.Itoa(st.Attempt)]; y != nil && !st.Submitted.IsZero() {
			known = append(known, at{st.Submitted, y})
		}
	}
	sort.Slice(known, func(i, j int) bool { return known[i].t.Before(known[j].t) })
	for _, st := range r.Jobs.Stages {
		key := strconv.Itoa(st.ID) + "." + strconv.Itoa(st.Attempt)
		if out[key] != nil || st.Submitted.IsZero() {
			continue
		}
		var near *YourCode
		for _, k := range known {
			if k.t.After(st.Submitted) {
				break
			}
			near = k.y
		}
		if near != nil {
			out[key] = &YourCode{CodeLocation: near.CodeLocation, Via: "nearest line recorded before it; Spark recorded none for this stage"}
		}
	}
	return out
}

// Label is "etl.py:32", the file's base name and the line.
func (y YourCode) Label() string {
	return path.Base(strings.ReplaceAll(y.File, `\`, "/")) + ":" + strconv.Itoa(y.Line)
}
