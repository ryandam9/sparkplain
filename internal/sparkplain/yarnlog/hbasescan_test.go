package yarnlog

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// The scans below are encoded by hand from HBase 2.4.17's .proto field
// numbers (Client.proto Scan, Filter.proto, Comparator.proto), the way
// HBase's ProtobufUtil.toScan writes them.

func pbTag(num, wire int) []byte { return binary.AppendUvarint(nil, uint64(num<<3|wire)) }

func pbV(num int, v uint64) []byte { return append(pbTag(num, 0), binary.AppendUvarint(nil, v)...) }

func pbB(num int, parts ...[]byte) []byte {
	var body []byte
	for _, p := range parts {
		body = append(body, p...)
	}
	out := append(pbTag(num, 2), binary.AppendUvarint(nil, uint64(len(body)))...)
	return append(out, body...)
}

func pbS(num int, s string) []byte { return pbB(num, []byte(s)) }

const hf = "org.apache.hadoop.hbase.filter."

// filter is a Filter message: the class name and its own message.
func filter(num int, class string, body ...[]byte) []byte {
	return pbB(num, pbS(1, hf+class), pbB(2, body...))
}

// cmp is a Comparator message holding a ByteArrayComparable value.
func cmp(num int, class string, value []byte) []byte {
	return pbB(num, pbS(1, hf+class), pbB(2, pbB(1, pbB(1, value))))
}

func TestDecodeScan(t *testing.T) {
	scan := concat(
		pbB(1, pbS(1, "d"), pbS(2, "status"), pbS(2, "amount")), // columns d:status, d:amount
		pbB(1, pbS(1, "m")), // family m
		pbB(2, pbS(1, "scan.attributes.table.name"), pbS(2, "orders")),
		pbS(3, "2026-08-15"), pbS(4, "2026-10-10\x00"),
		pbB(6, pbV(1, 1790000000000), pbV(2, 1790086400000)),
		pbV(7, 3), pbV(8, 0), pbV(17, 500), pbV(9, 100), pbV(23, 2))
	// FilterList ALL ( SCVF d:status = "SHIPPED" (filterIfMissing);
	//                  FilterList ONE ( PrefixFilter "2026-10-"; RowFilter >= Binary "C1000" );
	//                  ValueFilter != Substring "test";
	//                  SCVF d:api_token = "s3cr3t" )
	scvf := filter(2, "SingleColumnValueFilter", pbS(1, "d"), pbS(2, "status"), pbV(3, 2), cmp(4, "BinaryComparator", []byte("SHIPPED")), pbV(5, 1))
	inner := filter(2, "FilterList", pbV(1, 2),
		filter(2, "PrefixFilter", pbS(1, "2026-10-")),
		filter(2, "RowFilter", pbB(1, pbV(1, 4), cmp(2, "BinaryComparator", []byte("C1000")))))
	sub := filter(2, "ValueFilter", pbB(1, pbV(1, 3), pbB(2, pbS(1, hf+"SubstringComparator"), pbB(2, pbS(1, "test")))))
	secret := filter(2, "SingleColumnValueFilter", pbS(1, "d"), pbS(2, "api_token"), pbV(3, 2), cmp(4, "BinaryComparator", []byte("s3cr3t")))
	custom := pbB(2, pbS(1, "com.example.MyFilter"), pbB(2))
	scan = append(scan, pbB(5, pbS(1, hf+"FilterList"), pbB(2, pbV(1, 1), scvf, inner, sub, secret, custom))...)

	// b2a_base64 adds a newline; printing the bytes adds b'…'.
	for _, in := range []string{
		base64.StdEncoding.EncodeToString(scan) + "\n",
		"b'" + base64.StdEncoding.EncodeToString(scan) + `\n'`,
	} {
		sc, err := DecodeScan(in)
		if err != nil {
			t.Fatal(err)
		}
		if sc.Table != "orders" || sc.Rows() != `[2026-08-15, 2026-10-10\x00)` || strings.Join(sc.Columns, ",") != "d:status,d:amount,m" ||
			sc.MaxVersions != 3 || sc.CacheBlocks || sc.Caching != 500 || sc.BatchSize != 100 || sc.ReadType != "PREAD" ||
			sc.TimeRange != "from 2026-09-21 14:13:20.000 UTC to before 2026-09-22 14:13:20.000 UTC" {
			t.Errorf("scan = %+v", sc)
		}
		got := strings.Join(sc.Filter.Lines(), "\n")
		want := strings.Join([]string{
			"FilterList  MUST_PASS_ALL",
			`├─ SingleColumnValueFilter  d:status EQUAL Binary "SHIPPED" (rows without the column are left out)`,
			"├─ FilterList  MUST_PASS_ONE",
			`│  ├─ PrefixFilter  "2026-10-"`,
			`│  └─ RowFilter  GREATER_OR_EQUAL Binary "C1000"`,
			`├─ ValueFilter  NOT_EQUAL Substring "test" (any case)`,
			"├─ SingleColumnValueFilter  d:api_token EQUAL Binary [redacted]",
			"└─ MyFilter  (not decoded)",
		}, "\n")
		if got != want {
			t.Errorf("filters:\n%s\nwant:\n%s", got, want)
		}
		if strings.Contains(got, "s3cr3t") {
			t.Error("a value of a column named like a token must be hidden")
		}
	}
}

// Every comparator HBase 2.4 ships, and the filters with their own fields.
func TestDecodeFilters(t *testing.T) {
	long := make([]byte, 8)
	binary.BigEndian.PutUint64(long, uint64(1<<40))
	dec := []byte{0, 0, 0, 2, 0x04, 0xE2} // 12.50 as BigDecimal: scale 2, unscaled 1250
	neg := []byte{0, 0, 0, 1, 0xFF, 0x85} // -12.3
	for _, c := range []struct {
		class string
		body  [][]byte
		want  string
	}{
		{"QualifierFilter", [][]byte{pbB(1, pbV(1, 0), cmp(2, "BinaryPrefixComparator", []byte("amt")))}, `LESS BinaryPrefix "amt"`},
		{"ValueFilter", [][]byte{pbB(1, pbV(1, 5), cmp(2, "LongComparator", long))}, "GREATER Long 1099511627776"},
		{"ValueFilter", [][]byte{pbB(1, pbV(1, 1), cmp(2, "BigDecimalComparator", dec))}, "LESS_OR_EQUAL BigDecimal 12.50"},
		{"ValueFilter", [][]byte{pbB(1, pbV(1, 1), cmp(2, "BigDecimalComparator", neg))}, "LESS_OR_EQUAL BigDecimal -12.3"},
		{"ValueFilter", [][]byte{pbB(1, pbV(1, 2), pbB(2, pbS(1, hf+"RegexStringComparator"), pbB(2, pbS(1, "^C[0-9]+$"), pbV(2, 2), pbS(3, "UTF-8"))))}, "EQUAL Regex /^C[0-9]+$/ (case-insensitive)"},
		{"FamilyFilter", [][]byte{pbB(1, pbV(1, 3), pbB(2, pbS(1, hf+"NullComparator"), pbB(2)))}, "NOT_EQUAL Null"},
		{"ValueFilter", [][]byte{pbB(1, pbV(1, 2), pbB(2, pbS(1, hf+"BitComparator"), pbB(2, pbB(1, pbB(1, []byte{1})), pbV(2, 1))))}, `EQUAL Bit AND "\x01"`},
		{"ColumnPrefixFilter", [][]byte{pbS(1, "amt_")}, `"amt_"`},
		{"MultipleColumnPrefixFilter", [][]byte{pbS(1, "a"), pbS(1, "b")}, `"a", "b"`},
		{"InclusiveStopFilter", [][]byte{pbS(1, "z")}, `up to and including "z"`},
		{"ColumnRangeFilter", [][]byte{pbS(1, "a"), pbV(2, 1), pbS(3, "m"), pbV(4, 0)}, "columns [a, m)"},
		{"PageFilter", [][]byte{pbV(1, 1000)}, "1000 rows per region"},
		{"KeyOnlyFilter", [][]byte{pbV(1, 0)}, ""},
		{"TimestampsFilter", [][]byte{pbB(1, binary.AppendUvarint(binary.AppendUvarint(nil, 1790000000000), 1790000001000))}, "2026-09-21 14:13:20.000 UTC, 2026-09-21 14:13:21.000 UTC"},
		{"MultiRowRangeFilter", [][]byte{pbB(1, pbS(1, "a"), pbV(2, 1), pbS(3, "c"), pbV(4, 0)), pbB(1, pbS(1, "x"), pbV(2, 0), pbS(3, "z"), pbV(4, 1))}, "[a, c), (x, z]"},
		{"ColumnValueFilter", [][]byte{pbS(1, "d"), pbS(2, "qty"), pbV(3, 4), cmp(4, "BinaryComparator", []byte{0, 0, 0, 5})}, `d:qty GREATER_OR_EQUAL Binary "\x00\x00\x00\x05"`},
		{"SingleColumnValueExcludeFilter", [][]byte{pbB(1, pbS(1, "d"), pbS(2, "flag"), pbV(3, 2), cmp(4, "BinaryComparator", []byte("Y")))}, `d:flag EQUAL Binary "Y" (the column itself left out of the results)`},
		{"DependentColumnFilter", [][]byte{pbB(1, pbV(1, 6), pbB(2, pbS(1, hf+"NullComparator"), pbB(2))), pbS(2, "d"), pbS(3, "ts"), pbV(4, 1)}, "d:ts NO_OP Null (the column dropped from the results)"},
		{"RowFilter", [][]byte{pbB(1, pbV(1, 2))}, "EQUAL (no comparator)"},
	} {
		f := decodeFilter(hf+c.class, concat(c.body...), 0)
		if f.Text != c.want || !f.Decoded {
			t.Errorf("%s: %q (decoded %v), want %q", c.class, f.Text, f.Decoded, c.want)
		}
	}
	// A wrapper holds its filter.
	w := decodeFilter(hf+"WhileMatchFilter", filter(1, "PrefixFilter", pbS(1, "a")), 0)
	if len(w.Children) != 1 || w.Children[0].Name != "PrefixFilter" {
		t.Errorf("wrapper = %+v", w)
	}
}

func concat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// A scan with nothing set reads the whole table with HBase's defaults; a
// string that is not one says so.
func TestDecodeScanDefaultsAndErrors(t *testing.T) {
	sc, err := DecodeScan("")
	if err != nil || sc.Rows() != "[first row, last row]" || sc.MaxVersions != 1 || !sc.CacheBlocks || sc.Filter != nil {
		t.Errorf("empty scan = %+v, %v", sc, err)
	}
	for _, bad := range []string{"not base64!", base64.StdEncoding.EncodeToString([]byte{0x0a, 0xff})} {
		if _, err := DecodeScan(bad); err == nil {
			t.Errorf("%q decoded", bad)
		}
	}
	// Filters nested past the limit stop there, rather than recurse
	// without end; the scan still shows.
	deep := pbS(1, hf+"PrefixFilter")
	for range maxFilterDepth + 5 {
		deep = concat(pbS(1, hf+"SkipFilter"), pbB(2, pbB(1, deep)))
	}
	sc, err = DecodeScan(base64.StdEncoding.EncodeToString(pbB(5, deep)))
	if err != nil || len(sc.Filter.Lines()) > maxFilterDepth+2 || !strings.Contains(strings.Join(sc.Filter.Lines(), "\n"), "could not be read") {
		t.Errorf("deep filters: %v, %d lines", err, len(sc.Filter.Lines()))
	}
}

func FuzzDecodeScan(f *testing.F) {
	f.Add(base64.StdEncoding.EncodeToString(concat(pbS(3, "a"), pbS(4, "b"), filter(5, "FilterList", pbV(1, 1), filter(2, "PrefixFilter", pbS(1, "x"))))))
	f.Add("")
	f.Add("CgQKAmNm")
	f.Fuzz(func(t *testing.T, s string) {
		sc, err := DecodeScan(s)
		if err == nil && sc != nil {
			_ = sc.Rows()
			_ = sc.Filter.Lines()
			_ = sc.Filter.String()
		}
	})
}

// A job may print its scan as a sparkplain-scan line (not in production);
// only the decoded, redacted scan is kept, never the string with its
// values.
func TestScanLine(t *testing.T) {
	scan := concat(pbS(3, "2026-08-15"), pbS(4, "2026-10-10"),
		filter(5, "SingleColumnValueFilter", pbS(1, "d"), pbS(2, "api_token"), pbV(3, 2), cmp(4, "BinaryComparator", []byte("PLANTED-SECRET-0042"))))
	b64 := base64.StdEncoding.EncodeToString(scan) + `\n` // as print(json.dumps(...)) shows b2a_base64's newline
	log := "Starting job\n" +
		`sparkplain-scan {"table": "orders", "scan": "` + b64 + `"}` + "\n" +
		"26/10/02 05:00:01 INFO Demo: sparkplain-scan {\"table\": \"orders\", \"scan\": \"!!\"}\n" +
		"sparkplain-scan not json\n"
	res, err := Classify(strings.NewReader(log), "stdout", File{Kind: ContainerStdout, Container: "container_1_0001_01_000001"}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Scans) != 1 || res.Scans[0].Table != "orders" || res.Scans[0].Rows() != "[2026-08-15, 2026-10-10)" || res.Scans[0].Source.Line != 2 ||
		res.Scans[0].Filter.String() != "SingleColumnValueFilter d:api_token EQUAL Binary [redacted]" {
		t.Fatalf("scans = %+v", res.Scans)
	}
	var warnings int
	for _, l := range res.Lines {
		if l.Kind == model.LogHBaseScan && l.Severity == model.Warning {
			warnings++
		}
	}
	all, _ := json.Marshal(res)
	if warnings != 2 || strings.Contains(string(all), "PLANTED-SECRET") || strings.Contains(string(all), base64.StdEncoding.EncodeToString(scan)[:20]) {
		t.Errorf("%d warnings; result: %s", warnings, all)
	}
}
