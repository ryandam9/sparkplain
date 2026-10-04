package yarnlog

import (
	"bufio"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// sparkHeadRE is the pattern sparkHead replaced; the two must read every
// line the same way.
var sparkHeadRE = regexp.MustCompile(`^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) (TRACE|DEBUG|INFO|WARN|ERROR|FATAL) ([^\s:]+): ?(.*)$`)

func TestSparkHeadMatchesItsPattern(t *testing.T) {
	t.Parallel()
	lines := []string{
		"26/09/26 07:43:14 INFO Executor: Running task 1.0 in stage 10.0 (TID 100)",
		"26/09/26 07:43:14 INFO Executor:Running",
		"26/09/26 07:43:14 INFO Executor:  two spaces",
		"26/09/26 07:43:14 WARN org.apache.spark.Foo: x: y",
		"26/09/26 07:43:14 INFO : empty logger",
		"26/09/26 07:43:14 INFO Exec utor: a space in the logger",
		"26/09/26 07:43:14 NOTICE Executor: an unknown level",
		"26/09/26 07:43:14 INFO Executor",
		"26/09/26 07:43:14  INFO Executor: two spaces before the level",
		"2026-09-26 07:43:14,000 INFO Executor: another layout",
		"26/09/2a 07:43:14 INFO Executor: a letter in the date",
		"26/09/26 07:43:14 INFO Exec\tutor: a tab in the logger",
		"26/09/26 07:43:14 INFO Executor: ",
		"",
	}
	_ = filepath.WalkDir(emrlogs, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".gz") {
			return nil
		}
		f, err := os.Open(p)
		if err != nil {
			return nil
		}
		defer f.Close()
		var r io.Reader = f
		if zr, err := gzip.NewReader(f); err == nil {
			r = zr
		}
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			lines = append(lines, sc.Text())
		}
		return nil
	})
	if len(lines) < 1000 {
		t.Fatalf("only %d lines to compare", len(lines))
	}
	for _, l := range lines {
		h, ok := sparkHead(l)
		m := sparkHeadRE.FindStringSubmatch(l)
		if ok != (m != nil) {
			t.Fatalf("%q: sparkHead %v, the pattern %v", l, ok, m != nil)
		}
		if !ok {
			continue
		}
		want, _ := time.Parse("06/01/02 15:04:05", m[1])
		if !h.time.Equal(want) || h.level != m[2] || h.logger != m[3] || h.msg != m[4] {
			t.Fatalf("%q: sparkHead %+v, the pattern %q", l, h, m[1:])
		}
	}
}
