package eventlog

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

func countLines(t *testing.T, in *Input) (int64, []model.FileRead, bool) {
	t.Helper()
	var n int64
	files, trunc, _ := in.eachLine(context.Background(), func(line []byte, src model.Source, partial bool) error {
		n++
		return nil
	}, func(model.Source) { t.Error("unexpected long line") })
	return n, files, trunc
}

func TestResolveLayouts(t *testing.T) {
	cases := []struct {
		name, path, app, layout string
		parts                   int
		inProgress              bool
	}{
		{"plain file", mainApp, mainApp, "single", 1, false},
		{"lz4 file", mainApp + ".lz4", mainApp, "single", 1, false},
		{"history server zip", mainApp + ".zip", mainApp, "zip", 1, false},
		{"rolling folder", "eventlog_v2_application_1790380000000_0043", "application_1790380000000_0043", "rolling", 6, false},
		{"rolling zip", "application_1790380000000_0043_rolling.zip", "application_1790380000000_0043", "zip-rolling", 6, false},
		{"folder of logs, rolling match", ".", "application_1790380000000_0043", "rolling", 6, false},
		{"folder of logs, in progress", ".", "application_1790380000000_0045", "single", 1, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			in, err := Resolve(filepath.Join(fixtures, c.path), c.app, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			defer in.Close()
			if in.Layout != c.layout || len(in.parts) != c.parts || in.InProgress != c.inProgress {
				t.Fatalf("layout %q parts %v inProgress %v", in.Layout, in.PartNames(), in.InProgress)
			}
			n, files, trunc := countLines(t, in)
			if n == 0 || len(files) != c.parts {
				t.Fatalf("read %d lines from %d files", n, len(files))
			}
			// Plain logs have no stream framing, so a cut shows up as a
			// partial last line in the parser, not as truncation here.
			if trunc {
				t.Fatalf("truncated = %v", trunc)
			}
		})
	}
}

func TestRollingPartsInNumericOrder(t *testing.T) {
	in, err := Resolve(filepath.Join(fixtures, "eventlog_v2_application_1790380000000_0043"), "application_1790380000000_0043", Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for i, n := range in.PartNames() {
		want := "events_" + string(rune('1'+i)) + "_"
		if !strings.Contains(n, want) {
			t.Fatalf("part %d is %s", i, n)
		}
	}
}

func TestRollingCompactionStartsAtCompactFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "eventlog_v2_app_1")
	os.MkdirAll(dir, 0o755)
	for _, n := range []string{"events_1_app_1", "events_2_app_1", "events_2_app_1.compact", "events_3_app_1", "events_10_app_1", "appstatus_app_1.inprogress"} {
		os.WriteFile(filepath.Join(dir, n), []byte(`{"Event":"x"}`+"\n"), 0o644)
	}
	in, err := Resolve(dir, "app_1", Limits{})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"eventlog_v2_app_1/events_2_app_1.compact", "eventlog_v2_app_1/events_3_app_1", "eventlog_v2_app_1/events_10_app_1"}
	if !reflect.DeepEqual(in.PartNames(), want) || !in.InProgress || len(in.Notes) == 0 {
		t.Fatalf("parts %v inProgress %v notes %v", in.PartNames(), in.InProgress, in.Notes)
	}
}

func TestResolvePicksLatestAttempt(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []string{"application_1_2_1.lz4", "application_1_2_2.inprogress", "application_1_2_2", "application_1_20"} {
		os.WriteFile(filepath.Join(dir, n), nil, 0o644)
	}
	in, err := Resolve(dir, "application_1_2", Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if got := in.PartNames(); len(got) != 1 || got[0] != "application_1_2_2" {
		t.Fatalf("picked %v", got)
	}
}

func TestResolveErrors(t *testing.T) {
	if _, err := Resolve(filepath.Join(fixtures, "nope"), mainApp, Limits{}); ErrorClass(err) != ClassNotFound {
		t.Errorf("missing path: %v (%s)", err, ErrorClass(err))
	}
	if _, err := Resolve(fixtures, "application_9_9", Limits{}); ErrorClass(err) != ClassNotFound {
		t.Errorf("no match in folder: %v", err)
	}
	if _, err := Resolve(filepath.Join(fixtures, mainApp), mainApp, Limits{MaxObjectBytes: 1000}); ErrorClass(err) != ClassTooLarge {
		t.Errorf("size cap: %v", err)
	}
	in, err := Resolve(filepath.Join(fixtures, mainApp+".zip"), mainApp, Limits{MaxObjectBytes: 300000})
	if err == nil {
		// Stored zip entry is under the cap but the zip itself fits too; the
		// entry check happens at resolve time.
		in.Close()
	}
}

func TestReadLinesLongAndPartial(t *testing.T) {
	long := strings.Repeat("x", 3<<20)
	data := "{\"a\":1}\r\n" + long + "\n{\"b\":2}\n{\"c\":"
	var got []string
	var partial []bool
	var longAt []int64
	var n int64
	err := readLines(context.Background(), strings.NewReader(data), "f", 2<<20, &n,
		func(line []byte, src model.Source, p bool) error {
			got = append(got, string(line))
			partial = append(partial, p)
			return nil
		}, func(src model.Source) { longAt = append(longAt, src.Line) })
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []string{`{"a":1}`, `{"b":2}`, `{"c":`}) || !reflect.DeepEqual(partial, []bool{false, false, true}) {
		t.Fatalf("lines %q partial %v", got, partial)
	}
	if !reflect.DeepEqual(longAt, []int64{2}) || n != 4 {
		t.Fatalf("long lines at %v, count %d", longAt, n)
	}
}
