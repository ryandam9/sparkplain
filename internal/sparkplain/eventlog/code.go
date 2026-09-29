package eventlog

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/redact"
)

// Code locations (HISTORY.md, phase 1c step 7). Spark records where in the code
// work ran: a short call site ("collect at /jobs/etl.py:32") and, for stages
// and queries, the call stack. For JVM applications the stack holds the
// user's own frames; for PySpark it holds only Spark and py4j frames, and
// the short call site names a Python line for some actions only.

// frameRE matches a JVM stack frame, with or without "at " and a module
// prefix: "com.acme.Job$.main(Job.scala:42)".
var frameRE = regexp.MustCompile(`^\s*(?:at\s+)?(?:[\w.$-]+/)?([\w.$<>-]+)\.([\w$<>-]+)\(([^:()]+):(\d+)\)\s*$`)

// systemPrefixes are the packages whose frames are Spark's or the JVM's, not
// the application's.
var systemPrefixes = []string{"org.apache.spark.", "scala.", "java.", "javax.", "jdk.", "sun.", "com.sun.", "py4j.",
	"org.apache.hadoop.", "org.apache.hive.", "org.apache.parquet.", "com.amazon.", "com.amazonaws.", "software.amazon."}

// userFrames returns the application's frames from a long call site,
// innermost first, capped.
func userFrames(details string) []model.CodeLocation {
	var out []model.CodeLocation
	for _, line := range strings.Split(details, "\n") {
		m := frameRE.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		class := m[1]
		system := false
		for _, p := range systemPrefixes {
			if strings.HasPrefix(class, p) {
				system = true
				break
			}
		}
		n, _ := strconv.Atoi(m[4])
		if system || n <= 0 {
			continue
		}
		out = append(out, model.CodeLocation{File: redact.Text(m[3]), Line: n, Function: redact.Text(class + "." + m[2])})
		if len(out) == maxCodeFrames {
			break
		}
	}
	return out
}

const maxCodeFrames = 8

// shortSite reads a short call site, "collect at /jobs/etl.py:32". It
// reports false for sites that name no user code, such as Spark's own
// NativeMethodAccessorImpl.java:0 for many PySpark actions.
func shortSite(site string) (model.CodeLocation, bool) {
	i := strings.LastIndex(site, " at ")
	if i < 0 {
		return model.CodeLocation{}, false
	}
	loc := site[i+len(" at "):]
	colon := strings.LastIndexByte(loc, ':')
	if colon <= 0 {
		return model.CodeLocation{}, false
	}
	n, err := strconv.Atoi(loc[colon+1:])
	file := loc[:colon]
	if err != nil || n <= 0 || strings.ContainsAny(file, " \t") || file == "<stdin>" || strings.HasPrefix(file, "<") {
		return model.CodeLocation{}, false
	}
	return model.CodeLocation{File: redact.Text(file), Line: n, Action: redact.Text(site[:i])}, true
}

// codeOf gives the code locations for a short call site and a call stack:
// the user's frames when the stack has them, else the short site.
func codeOf(site, details string) []model.CodeLocation {
	frames := userFrames(details)
	if s, ok := shortSite(site); ok {
		if len(frames) == 0 {
			return []model.CodeLocation{s}
		}
		if frames[0].Action == "" && baseName(frames[0].File) == baseName(s.File) && frames[0].Line == s.Line {
			frames[0].Action = s.Action
		}
	}
	return frames
}

func baseName(p string) string {
	if i := strings.LastIndexAny(p, `/\`); i >= 0 {
		return p[i+1:]
	}
	return p
}
