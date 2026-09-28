package eventlog

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

var fixtureApp = regexp.MustCompile(`application_\d+_\d{4}`)

// fixtureInputs lists each distinct fixture log in testdata/eventlog with its
// app ID. Compressed and zipped copies of a log hold the same events, so only
// one form of each is read; TestAllVariantsParseTheSame covers the codecs.
func fixtureInputs(t *testing.T) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(fixtures)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, e := range entries {
		n := e.Name()
		if strings.HasSuffix(n, ".lz4") || strings.HasSuffix(n, ".zstd") || strings.HasSuffix(n, ".snappy") || strings.HasSuffix(n, ".zip") {
			continue
		}
		if app := fixtureApp.FindString(n); app != "" {
			out[n] = app
		}
	}
	if len(out) < 8 {
		t.Fatalf("only %d fixtures found", len(out))
	}
	return out
}

// TestFieldInventory is phase 1c's guarantee: every event and field in every
// fixture is listed in inventory.txt with what sparkplain does with it, and
// every listed field was seen in a fixture or says it was not.
func TestFieldInventory(t *testing.T) {
	t.Parallel()
	seen := map[string]map[string]bool{}
	var missing []string
	for name, app := range fixtureInputs(t) {
		in, err := Resolve(filepath.Join(fixtures, name), app, Limits{})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		in.eachLine(context.Background(), func(line []byte, src model.Source, partial bool) error {
			var v map[string]any
			if json.Unmarshal(line, &v) != nil {
				return nil // torn last line of an in-progress log
			}
			ev, _ := v["Event"].(string)
			if !knownEvent(ev) {
				missing = append(missing, name+": event "+ev)
				return nil
			}
			if seen[ev] == nil {
				seen[ev] = map[string]bool{}
			}
			eventFields(ev, v, func(path string) {
				seen[ev][path] = true
				if !hasKey(inventory[ev], path) {
					missing = append(missing, ev+" :: "+path)
				}
			})
			return nil
		}, func(model.Source) {})
		in.Close()
	}
	sort.Strings(missing)
	missing = dedupeStrings(missing)
	for i, m := range missing {
		if i == 30 {
			t.Errorf("… and %d more", len(missing)-30)
			break
		}
		t.Errorf("not in inventory.txt: %s", m)
	}

	planned := map[int]int{}
	for ev, fields := range inventory {
		for path, f := range fields {
			if f.Status == fieldPlanned {
				planned[f.Step]++
			}
			unseen := strings.Contains(f.Note, "not seen in a fixture")
			if !seen[ev][path] && !unseen {
				t.Errorf("inventory.txt lists %s :: %s, which no fixture holds; mark it \"not seen in a fixture\" or drop it", ev, path)
			}
			if seen[ev][path] && unseen {
				t.Errorf("%s :: %s is marked not seen, but a fixture holds it", ev, path)
			}
		}
	}
	if len(planned) > 0 {
		t.Errorf("fields still planned, by phase 1c step: %v; every field must be used or set aside with a reason", planned)
	}
}

// Parsing every fixture must find no unknown events or fields: the runtime
// check and the inventory agree.
func TestFixturesHaveNoUnknownFields(t *testing.T) {
	t.Parallel()
	for name, app := range fixtureInputs(t) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			l := parseFixture(t, name, app)
			if len(l.Stats.UnknownEvents) > 0 || len(l.Stats.UnknownFields) > 0 {
				t.Errorf("unknown events %v, fields %v", l.Stats.UnknownEvents, l.Stats.UnknownFields)
			}
		})
	}
}

// A field Spark adds later is counted, even when it is nested.
func TestNestedUnknownFieldCounted(t *testing.T) {
	t.Parallel()
	p := newParser(Options{})
	line := `{"Event":"SparkListenerTaskEnd","Stage ID":1,"Stage Attempt ID":0,"Task Info":{"Task ID":1,"Brand New":7},"Task End Reason":{"Reason":"Success"}}`
	if err := p.line([]byte(line), model.Source{File: "f", Line: 1}); err != nil {
		t.Fatal(err)
	}
	if p.log.Stats.UnknownFields["SparkListenerTaskEnd.Task Info/Brand New"] != 1 {
		t.Errorf("unknown fields %v", p.log.Stats.UnknownFields)
	}
}

func TestInventoryRejectsBadLines(t *testing.T) {
	t.Parallel()
	for _, bad := range []string{"Ev\n    a | used\n", "Ev\n    a | maybe | x\n", "Ev\n    a | planned:x | y\n", "    a | used | x\n"} {
		if _, err := parseInventory(bad); err == nil {
			t.Errorf("parseInventory(%q) accepted a bad line", bad)
		}
	}
}

func dedupeStrings(s []string) []string {
	out := s[:0]
	for i, v := range s {
		if i == 0 || v != s[i-1] {
			out = append(out, v)
		}
	}
	return out
}
