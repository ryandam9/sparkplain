package eventlog

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// The field inventory (HISTORY.md, phase 1c) lists every event and field Spark
// 3.5 and EMR write, and what sparkplain does with each. TestFieldInventory
// checks every fixture against it, and parsing counts anything not listed
// as unknown, so a newer Spark's additions are visible instead of lost.
//
//go:embed inventory.txt
var inventoryText string

// Field statuses in inventory.txt.
const (
	fieldUsed    = "used"
	fieldAside   = "aside"
	fieldPlanned = "planned"
)

type fieldEntry struct {
	Status string // used, aside or planned
	Step   int    // for planned: the phase 1c step that reads it
	Note   string
}

// inventory maps event name → field path → entry. Paths use / for nesting,
// [] for array elements and /* for maps whose keys are data.
var inventory = mustParseInventory(inventoryText)

func mustParseInventory(text string) map[string]map[string]fieldEntry {
	inv, err := parseInventory(text)
	if err != nil {
		panic(err)
	}
	return inv
}

func parseInventory(text string) (map[string]map[string]fieldEntry, error) {
	inv := map[string]map[string]fieldEntry{}
	var cur map[string]fieldEntry
	for n, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, " ") {
			cur = map[string]fieldEntry{}
			inv[strings.TrimSpace(line)] = cur
			continue
		}
		parts := strings.Split(strings.TrimSpace(line), " | ")
		if cur == nil || len(parts) != 3 {
			return nil, fmt.Errorf("inventory.txt:%d: want \"path | status | note\" under an event", n+1)
		}
		e := fieldEntry{Status: parts[1], Note: parts[2]}
		if s, ok := strings.CutPrefix(parts[1], fieldPlanned+":"); ok {
			if _, err := fmt.Sscan(s, &e.Step); err != nil {
				return nil, fmt.Errorf("inventory.txt:%d: bad step %q", n+1, s)
			}
			e.Status = fieldPlanned
		} else if e.Status != fieldUsed && e.Status != fieldAside {
			return nil, fmt.Errorf("inventory.txt:%d: unknown status %q", n+1, parts[1])
		}
		cur[parts[0]] = e
	}
	return inv, nil
}

// knownEvent reports whether the inventory lists the event.
func knownEvent(name string) bool { _, ok := inventory[name]; return ok }

// topLevelKnown reports whether a top-level field of the event is listed.
func topLevelKnown(event, key string) bool {
	_, ok := inventory[event][key]
	return ok
}

// eventFields calls fn with the path of every field in one decoded event,
// as the inventory writes them. Maps whose keys are data (listed with /*)
// are reported once, not per key, and a plan node's children are walked as
// the node itself.
func eventFields(event string, v any, fn func(path string)) {
	fields := inventory[event]
	var walk func(v any, prefix string)
	walk = func(v any, prefix string) {
		switch x := v.(type) {
		case map[string]any:
			keys := make([]string, 0, len(x))
			for k := range x {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				p := k
				if prefix != "" {
					p = prefix + "/" + k
				}
				fn(p)
				switch {
				case fields != nil && hasKey(fields, p+"/*"):
					if m, ok := x[k].(map[string]any); ok && len(m) > 0 {
						fn(p + "/*")
					}
				case k == "children" && strings.HasSuffix(prefix, "sparkPlanInfo"):
					if kids, ok := x[k].([]any); ok {
						for _, kid := range kids {
							walk(kid, prefix)
						}
					}
				default:
					walk(x[k], p)
				}
			}
		case []any:
			for _, e := range x {
				if _, ok := e.(map[string]any); ok {
					walk(e, prefix+"[]")
				}
			}
		}
	}
	walk(v, "")
}

func hasKey(m map[string]fieldEntry, k string) bool { _, ok := m[k]; return ok }

// nestedSampleEvents is how many events of each type have their nested
// fields checked against the inventory while parsing. Spark writes every
// event of a type with the same shape, so a sample finds new fields without
// decoding every task event twice.
const nestedSampleEvents = 100

// checkNested records fields of the event that the inventory does not list.
func (p *parser) checkNested(name string, line []byte) {
	if p.nestedSeen == nil {
		p.nestedSeen = map[string]int{}
	}
	if p.nestedSeen[name] >= nestedSampleEvents {
		return
	}
	p.nestedSeen[name]++
	var v any
	if json.Unmarshal(line, &v) != nil {
		return
	}
	fields := inventory[name]
	eventFields(name, v, func(path string) {
		if !strings.Contains(path, "/") {
			return // top-level fields are checked on every event
		}
		if !hasKey(fields, path) {
			p.log.Stats.UnknownFields[name+"."+path]++
		}
	})
}
