package main

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"
)

// field encodes a length-delimited protobuf field (all a scan's fields
// used here are); lengths stay under 128, so one byte each.
func field(num int, body ...[]byte) []byte {
	b := bytes.Join(body, nil)
	return append([]byte{byte(num<<3 | 2), byte(len(b))}, b...)
}

// -decode-scan prints the scan's key range and filters, from the flag or
// from stdin (which keeps the string out of the shell's history), and
// needs no -app-id: it reads nothing else.
func TestDecodeScanFlag(t *testing.T) {
	scan := bytes.Join([][]byte{
		field(3, []byte("2026-08-15")), field(4, []byte("2026-10-10")),
		field(5, field(1, []byte("org.apache.hadoop.hbase.filter.PrefixFilter")), field(2, field(1, []byte("2026-09")))),
	}, nil)
	s := base64.StdEncoding.EncodeToString(scan) + "\n"
	want := []string{"HBase scan", "Rows               [2026-08-15, 2026-10-10)", "Columns            all", "Filters", `PrefixFilter  "2026-09"`}
	code, stdout, stderr := runCLI(t, "-decode-scan", s)
	for _, w := range want {
		if code != exitOK || !strings.Contains(stdout, w) {
			t.Errorf("flag: exit %d, lacks %q:\n%s%s", code, w, stdout, stderr)
		}
	}
	var out, errs bytes.Buffer
	if code := decodeScan("-", strings.NewReader(s), &out, &errs); code != exitOK || !strings.Contains(out.String(), `PrefixFilter  "2026-09"`) {
		t.Errorf("stdin: exit %d\n%s%s", code, out.String(), errs.String())
	}
	if code, _, stderr := runCLI(t, "-decode-scan", "not a scan"); code != exitFatal || !strings.Contains(stderr, "not a serialized HBase Scan") {
		t.Errorf("bad scan: exit %d, %s", code, stderr)
	}
}
