package eventlog

import (
	"regexp"
	"strings"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

var exitCodeRE = regexp.MustCompile(`(?i)(?:exit (?:status|code)|exited with code)[:\s]+(-?\d+)`)

// removalKind classifies an executor removal reason. Spark and YARN phrase
// these as free text, so this matches on the phrases they use.
func removalKind(reason string) string {
	r := strings.ToLower(reason)
	code := ""
	if m := exitCodeRE.FindStringSubmatch(reason); m != nil {
		code = m[1]
	}
	switch {
	case reason == "":
		return model.RemovalNone
	case strings.Contains(r, "exceeding physical memory"), strings.Contains(r, "exceeding memory limits"),
		strings.Contains(r, "exceeding virtual memory"), strings.Contains(r, "outofmemory"),
		code == "137" || code == "-104":
		return model.RemovalMemoryKill
	case strings.Contains(r, "decommission"):
		return model.RemovalDecommissioned
	case strings.Contains(r, "killed by driver"), strings.Contains(r, "requested by driver"):
		return model.RemovalKilledByDriver
	case strings.Contains(r, "idle"):
		return model.RemovalIdle
	case strings.Contains(r, "heartbeat"), strings.Contains(r, "lost node"), strings.Contains(r, "*lost*"),
		strings.Contains(r, "disconnected"), strings.Contains(r, "bad node"), code != "":
		return model.RemovalLost
	}
	return model.RemovalOther
}
