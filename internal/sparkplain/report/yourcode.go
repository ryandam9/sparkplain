package report

import (
	"fmt"
	"path"
	"regexp"
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
// and its job's is the application's; the line that set its job's
// description, when Spark recorded no line of the application's but the
// code at hand has one; or, failing those, the nearest line recorded
// before the stage, said to be so.
type YourCode struct {
	model.CodeLocation
	Via string
}

// sparkFiles are the files of Spark's own that PySpark stages and jobs are
// named after, and the JVM's thread pool that runs Spark's background work
// (a broadcast exchange's stage is named "… at FutureTask.java:264"); their
// call sites are not the application's.
var sparkFiles = map[string]bool{"PythonRDD.scala": true, "SerDeUtil.scala": true, "PythonSQLUtils.scala": true, "PythonUtils.scala": true,
	"NativeMethodAccessorImpl.java": true, "DelegatingMethodAccessorImpl.java": true, "DirectMethodHandleAccessor.java": true, "Method.java": true,
	"FutureTask.java": true, "ThreadPoolExecutor.java": true, "Thread.java": true}

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
	// PySpark's DataFrame actions record no line of the application's
	// (count, show and write name NativeMethodAccessorImpl.java:0), but a
	// job description the code set names the line that set it.
	descs := descriptionLines(srcs)
	for _, st := range r.Jobs.Stages {
		key := strconv.Itoa(st.ID) + "." + strconv.Itoa(st.Attempt)
		if out[key] != nil {
			continue
		}
		ids := append([]int(nil), st.JobIDs...)
		sort.Ints(ids)
		for _, id := range ids {
			if j := jobs[id]; j != nil {
				if c, ok := descs.find(j.Description); ok {
					out[key] = &YourCode{CodeLocation: c, Via: fmt.Sprintf("the line that set job %d's description", id)}
					break
				}
			}
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

// descLine is a line of the application's code that sets a job description,
// with the description as a pattern.
type descLine struct {
	re  *regexp.Regexp
	loc model.CodeLocation
}

type descLines []descLine

// setDesc finds a call that sets a job description and the start of its
// string: sc.setJobDescription(f"…") in Python, Scala or Java, or
// setLocalProperty("spark.job.description", "…"); the prefix letters say
// whether the string interpolates.
var setDesc = regexp.MustCompile(`(?:setJobDescription\(\s*|setLocalProperty\(\s*["']spark\.job\.description["']\s*,\s*)([A-Za-z]{0,2})("|')`)

// interp is what a description string fills in at run time: Python's {x}
// and %s, Scala's ${x} and $x.
var interp = regexp.MustCompile(`\$\{[^}]*\}|\{[^}]*\}|%[-+ #0-9.]*[sdifr]|\$[A-Za-z_][A-Za-z0-9_]*`)

// minDescFixed is the least literal text a description pattern needs, so a
// string that is all placeholders does not match every job.
const minDescFixed = 8

// descriptionLines lists the lines in the application's files that set a
// job description to a string literal written on that line.
func descriptionLines(srcs []SourceFile) descLines {
	var out descLines
	for _, sf := range srcs {
		if len(sf.Logged) == 0 {
			continue
		}
		for i, line := range sf.Lines {
			for _, m := range setDesc.FindAllStringSubmatchIndex(line, -1) {
				prefix, quote := strings.ToLower(line[m[2]:m[3]]), line[m[4]]
				body, ok := stringBody(line[m[5]:], quote)
				if !ok {
					continue
				}
				// An f-string or Scala s"…" fills its placeholders in; so may
				// % or .format() after a plain string, but not a raw or
				// bytes one.
				pat, fixed, last := "", 0, 0
				if strings.ContainsAny(prefix, "fs") || !strings.ContainsAny(prefix, "rb") {
					for _, ph := range interp.FindAllStringIndex(body, -1) {
						pat += regexp.QuoteMeta(body[last:ph[0]]) + ".*?"
						fixed += ph[0] - last
						last = ph[1]
					}
				}
				pat += regexp.QuoteMeta(body[last:])
				fixed += len(body) - last
				if fixed < minDescFixed {
					continue
				}
				out = append(out, descLine{re: regexp.MustCompile(`^` + pat + `$`), loc: model.CodeLocation{File: sf.Logged[0], Line: i + 1, Action: "setJobDescription"}})
			}
		}
	}
	return out
}

// stringBody is a string literal's text up to its closing quote, with
// backslash escapes undone; false when it does not close on the line.
func stringBody(s string, quote byte) (string, bool) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '\\' && i+1 < len(s):
			i++
			b.WriteByte(s[i])
		case c == quote:
			return b.String(), true
		default:
			b.WriteByte(c)
		}
	}
	return "", false
}

// find is the one line whose description pattern matches desc; none when
// no line does or several different lines do.
func (d descLines) find(desc string) (model.CodeLocation, bool) {
	if desc == "" {
		return model.CodeLocation{}, false
	}
	var hit *model.CodeLocation
	for i := range d {
		if !d[i].re.MatchString(desc) {
			continue
		}
		if hit != nil && *hit != d[i].loc {
			return model.CodeLocation{}, false
		}
		hit = &d[i].loc
	}
	if hit == nil {
		return model.CodeLocation{}, false
	}
	return *hit, true
}

// Label is "etl.py:32", the file's base name and the line.
func (y YourCode) Label() string {
	return path.Base(strings.ReplaceAll(y.File, `\`, "/")) + ":" + strconv.Itoa(y.Line)
}
