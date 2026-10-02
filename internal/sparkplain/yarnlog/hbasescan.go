package yarnlog

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/redact"
)

// A TableInputFormat scan is passed to the job as hbase.mapreduce.scan: a
// serialized HBase Scan message (protobuf), base64-encoded. The field
// numbers below are HBase 2.4.17's (hbase-protocol-shaded Client.proto,
// Filter.proto, Comparator.proto and HBase.proto); a filter or comparator
// is its Java class name plus its own message.

var errScanFormat = errors.New("not a serialized HBase Scan")

// DecodeScan decodes a base64 scan string. It accepts what Python's
// binascii.b2a_base64 prints, trailing newline and b'…' quoting included.
// A filter or comparator it does not know is named, not guessed.
func DecodeScan(s string) (*model.HBaseScan, error) {
	raw, err := scanBytes(s)
	if err != nil {
		return nil, err
	}
	m, err := pbParse(raw)
	if err != nil {
		return nil, err
	}
	sc := &model.HBaseScan{MaxVersions: 1, CacheBlocks: true, IncludeStart: true}
	for _, f := range m {
		switch f.num {
		case 1: // Column
			c, err := pbParse(f.b)
			if err != nil {
				return nil, err
			}
			fam := binaryString(c.bytes(1))
			quals := c.all(2)
			if len(quals) == 0 {
				sc.Columns = append(sc.Columns, fam)
			}
			for _, q := range quals {
				sc.Columns = append(sc.Columns, fam+":"+binaryString(q))
			}
		case 2: // NameBytesPair
			a, err := pbParse(f.b)
			if err != nil {
				return nil, err
			}
			name := string(a.bytes(1))
			if name == "scan.attributes.table.name" { // Scan.SCAN_ATTRIBUTES_TABLE_NAME
				sc.Table = binaryString(a.bytes(2))
				continue
			}
			val, _ := redact.Value(name, binaryString(a.bytes(2)))
			sc.Attributes = append(sc.Attributes, redact.Clean(name)+"="+val)
		case 3:
			sc.StartRow = binaryString(f.b)
		case 4:
			sc.StopRow = binaryString(f.b)
		case 5:
			flt, err := decodeFilterMsg(f.b, 0)
			if err != nil {
				return nil, err
			}
			sc.Filter = &flt
		case 6:
			t, err := pbParse(f.b)
			if err != nil {
				return nil, err
			}
			sc.TimeRange = timeRange(t)
		case 7:
			sc.MaxVersions = int(f.v)
		case 8:
			sc.CacheBlocks = f.v != 0
		case 9:
			sc.BatchSize = int(f.v)
		case 10:
			sc.MaxResultSize = int64(f.v)
		case 15:
			sc.Reversed = f.v != 0
		case 16:
			if f.v == 1 {
				sc.Consistency = "TIMELINE"
			}
		case 17:
			sc.Caching = int(f.v)
		case 18:
			sc.PartialResults = f.v != 0
		case 19:
			t, err := pbParse(f.b)
			if err != nil {
				return nil, err
			}
			tr, err := pbParse(t.bytes(2))
			if err != nil {
				return nil, err
			}
			if r := timeRange(tr); r != "" {
				sc.FamilyTimes = append(sc.FamilyTimes, binaryString(t.bytes(1))+": "+r)
			}
		case 21:
			sc.IncludeStart = f.v != 0
		case 22:
			sc.IncludeStop = f.v != 0
		case 23:
			sc.ReadType = map[uint64]string{1: "STREAM", 2: "PREAD"}[f.v]
		}
	}
	return sc, nil
}

// scanBytes undoes the base64, tolerating Python's printing of it.
func scanBytes(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "b'") || strings.HasPrefix(s, `b"`) {
		s = strings.Trim(s[1:], `'"`)
	}
	s = strings.NewReplacer(`\n`, "", "\n", "", "\r", "", " ", "").Replace(s)
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if b, err := enc.DecodeString(s); err == nil {
			return b, nil
		}
	}
	return nil, fmt.Errorf("%w: it is not base64", errScanFormat)
}

const maxFilterDepth = 32

func decodeFilterMsg(b []byte, depth int) (model.HBaseFilter, error) {
	if depth > maxFilterDepth {
		return model.HBaseFilter{}, fmt.Errorf("%w: filters nested over %d deep", errScanFormat, maxFilterDepth)
	}
	m, err := pbParse(b)
	if err != nil {
		return model.HBaseFilter{}, err
	}
	return decodeFilter(string(m.bytes(1)), m.bytes(2), depth), nil
}

// decodeFilter explains one filter. A filter whose own message does not
// parse keeps its name and says so, and the rest of the scan still shows.
func decodeFilter(class string, ser []byte, depth int) model.HBaseFilter {
	f := model.HBaseFilter{Name: scanClass(class)}
	m, err := pbParse(ser)
	if err != nil {
		f.Text = "could not be read"
		return f
	}
	f.Decoded = true
	child := func(b []byte) {
		c, err := decodeFilterMsg(b, depth+1)
		if err != nil {
			c = model.HBaseFilter{Name: "filter", Text: "could not be read"}
		}
		f.Children = append(f.Children, c)
	}
	switch f.Name {
	case "FilterList":
		f.Text = map[uint64]string{1: "MUST_PASS_ALL", 2: "MUST_PASS_ONE"}[m.uint(1)]
		for _, c := range m.all(2) {
			child(c)
		}
	case "FilterWrapper", "SkipFilter", "WhileMatchFilter":
		child(m.bytes(1))
	case "SingleColumnValueFilter":
		f.Text = columnValue(m)
	case "SingleColumnValueExcludeFilter":
		inner, err := pbParse(m.bytes(1))
		if err != nil {
			f.Decoded, f.Text = false, "could not be read"
			break
		}
		f.Text = columnValue(inner) + " (the column itself left out of the results)"
	case "ColumnValueFilter":
		col := column(m.bytes(1), m.bytes(2))
		f.Text = col + " " + compareOp(m.uint(3)) + " " + comparator(m.bytes(4), redact.IsSensitiveKey(col))
	case "RowFilter", "FamilyFilter", "QualifierFilter", "ValueFilter":
		f.Text = compareFilter(m.bytes(1), false)
	case "DependentColumnFilter":
		col := column(m.bytes(2), m.bytes(3))
		f.Text = col + " " + compareFilter(m.bytes(1), redact.IsSensitiveKey(col))
		if m.uint(4) != 0 {
			f.Text += " (the column dropped from the results)"
		}
	case "PrefixFilter", "ColumnPrefixFilter":
		f.Text = quote(m.bytes(1))
	case "MultipleColumnPrefixFilter", "FirstKeyValueMatchingQualifiersFilter":
		var ps []string
		for _, p := range m.all(1) {
			ps = append(ps, quote(p))
		}
		f.Text = strings.Join(ps, ", ")
	case "InclusiveStopFilter":
		f.Text = "up to and including " + quote(m.bytes(1))
	case "ColumnRangeFilter":
		lo, hi := "[", ")"
		if !m.has(2) || m.uint(2) == 0 {
			lo = "("
		}
		if m.uint(4) != 0 {
			hi = "]"
		}
		f.Text = "columns " + lo + binaryString(m.bytes(1)) + ", " + binaryString(m.bytes(3)) + hi
	case "ColumnCountGetFilter":
		f.Text = fmt.Sprintf("first %d columns", int32(m.uint(1)))
	case "ColumnPaginationFilter":
		f.Text = fmt.Sprintf("%d columns", int32(m.uint(1)))
		if m.has(3) {
			f.Text += " from column " + quote(m.bytes(3))
		} else {
			f.Text += fmt.Sprintf(" from offset %d", int32(m.uint(2)))
		}
	case "PageFilter":
		f.Text = fmt.Sprintf("%d rows per region", int64(m.uint(1)))
	case "KeyOnlyFilter":
		if m.uint(1) != 0 {
			f.Text = "values replaced by their length"
		}
	case "FirstKeyOnlyFilter", "FilterAllFilter":
	case "RandomRowFilter":
		f.Text = fmt.Sprintf("keeps each row with chance %g", math.Float32frombits(uint32(m.uint(1))))
	case "TimestampsFilter":
		var ts []string
		for _, v := range m.varints(1) {
			ts = append(ts, millis(int64(v)))
		}
		f.Text = strings.Join(ts, ", ")
	case "MultiRowRangeFilter":
		var rs []string
		for _, b := range m.all(1) {
			r, err := pbParse(b)
			if err != nil {
				continue
			}
			sc := model.HBaseScan{StartRow: binaryString(r.bytes(1)), StopRow: binaryString(r.bytes(3)),
				IncludeStart: !r.has(2) || r.uint(2) != 0, IncludeStop: r.uint(4) != 0}
			rs = append(rs, sc.Rows())
		}
		f.Text = strings.Join(rs, ", ")
	case "FuzzyRowFilter":
		var ks []string
		for _, b := range m.all(1) {
			p, err := pbParse(b)
			if err != nil {
				continue
			}
			ks = append(ks, quote(p.bytes(1))+" mask "+quote(p.bytes(2)))
		}
		f.Text = strings.Join(ks, ", ")
	default:
		f.Decoded = false
	}
	f.Text = redact.Text(f.Text)
	return f
}

func columnValue(m pbMsg) string {
	col := column(m.bytes(1), m.bytes(2))
	s := col + " " + compareOp(m.uint(3)) + " " + comparator(m.bytes(4), redact.IsSensitiveKey(col))
	if m.uint(5) != 0 {
		s += " (rows without the column are left out)"
	}
	if m.has(6) && m.uint(6) == 0 {
		s += " (every version tested)"
	}
	return s
}

func column(fam, qual []byte) string {
	if len(qual) == 0 {
		return binaryString(fam)
	}
	return binaryString(fam) + ":" + binaryString(qual)
}

func compareFilter(b []byte, secret bool) string {
	m, err := pbParse(b)
	if err != nil {
		return "could not be read"
	}
	return compareOp(m.uint(1)) + " " + comparator(m.bytes(2), secret)
}

func compareOp(v uint64) string {
	ops := []string{"LESS", "LESS_OR_EQUAL", "EQUAL", "NOT_EQUAL", "GREATER_OR_EQUAL", "GREATER", "NO_OP"}
	if v < uint64(len(ops)) {
		return ops[v]
	}
	return fmt.Sprintf("operator %d", v)
}

// comparator explains a filter's comparator. secret hides the value, for
// a column named like a password or token.
func comparator(b []byte, secret bool) string {
	if len(b) == 0 {
		return "(no comparator)"
	}
	m, err := pbParse(b)
	if err != nil {
		return "comparator could not be read"
	}
	name := scanClass(string(m.bytes(1)))
	c, err := pbParse(m.bytes(2))
	if err != nil {
		return name + " (not decoded)"
	}
	value := func() []byte {
		v, _ := pbParse(c.bytes(1))
		return v.bytes(1)
	}
	show := func(s string) string {
		if secret {
			return redact.Mask
		}
		return s
	}
	switch name {
	case "BinaryComparator", "BinaryPrefixComparator":
		return strings.TrimSuffix(name, "Comparator") + " " + show(quote(value()))
	case "LongComparator":
		v := value()
		if len(v) != 8 {
			return "Long " + show(quote(v))
		}
		return "Long " + show(fmt.Sprint(int64(binary.BigEndian.Uint64(v))))
	case "BigDecimalComparator":
		return "BigDecimal " + show(bigDecimal(value()))
	case "BitComparator":
		op := map[uint64]string{1: "AND", 2: "OR", 3: "XOR"}[c.uint(2)]
		return "Bit " + op + " " + show(quote(value()))
	case "NullComparator":
		return "Null"
	case "SubstringComparator":
		return "Substring " + show(quote(c.bytes(1))) + " (any case)"
	case "RegexStringComparator":
		s := "Regex " + show("/"+redact.Clean(string(c.bytes(1)))+"/")
		if fl := regexFlags(int32(c.uint(2))); fl != "" {
			s += " (" + fl + ")"
		}
		return s
	case "BinaryComponentComparator":
		return fmt.Sprintf("BinaryComponent %s at byte %d", show(quote(c.bytes(1))), c.uint(2))
	}
	return name + " (not decoded)"
}

// regexFlags names java.util.regex.Pattern's flags.
func regexFlags(f int32) string {
	names := []string{"unix lines", "case-insensitive", "comments", "multiline", "literal", "dot matches newline", "unicode case", "canonical equivalence", "unicode classes"}
	var on []string
	for i, n := range names {
		if f&(1<<i) != 0 {
			on = append(on, n)
		}
	}
	return strings.Join(on, ", ")
}

// bigDecimal reads HBase's Bytes.toBytes(BigDecimal): the scale as four
// bytes, then the unscaled value in two's complement.
func bigDecimal(b []byte) string {
	if len(b) < 5 {
		return quote(b)
	}
	scale := int32(binary.BigEndian.Uint32(b))
	u := new(big.Int).SetBytes(b[4:])
	if b[4]&0x80 != 0 {
		u.Sub(u, new(big.Int).Lsh(big.NewInt(1), uint(8*(len(b)-4))))
	}
	if scale <= 0 || scale > 1000 {
		return u.String()
	}
	r := new(big.Rat).SetFrac(u, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil))
	return r.FloatString(int(scale))
}

func timeRange(t pbMsg) string {
	from, to := int64(t.uint(1)), int64(math.MaxInt64)
	if t.has(2) {
		to = int64(t.uint(2))
	}
	switch {
	case from <= 0 && to == math.MaxInt64:
		return ""
	case to == math.MaxInt64:
		return "from " + millis(from)
	case from <= 0:
		return "before " + millis(to)
	}
	return "from " + millis(from) + " to before " + millis(to)
}

func millis(ms int64) string {
	return time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05.000 UTC")
}

func scanClass(c string) string {
	c = redact.Clean(c)
	if i := strings.LastIndexByte(c, '.'); i >= 0 {
		c = c[i+1:]
	}
	if c == "" {
		return "filter"
	}
	return c
}

func quote(b []byte) string { return `"` + binaryString(b) + `"` }

// binaryString is HBase's Bytes.toStringBinary: printable ASCII as it is,
// every other byte as \xNN, so row keys read as they do in HBase's logs.
func binaryString(b []byte) string {
	var sb strings.Builder
	for _, c := range b {
		if c >= ' ' && c <= '~' && c != '\\' {
			sb.WriteByte(c)
		} else {
			fmt.Fprintf(&sb, `\x%02X`, c)
		}
	}
	return sb.String()
}

// pbField is one field of a protobuf message: a varint or fixed value in
// v, or a length-delimited value in b.
type pbField struct {
	num, wire int
	v         uint64
	b         []byte
}

type pbMsg []pbField

const maxPBFields = 1 << 16

// pbParse reads one protobuf message's fields. It never reads past b, and
// fails on anything malformed, which a fuzz test checks.
func pbParse(b []byte) (pbMsg, error) {
	var out pbMsg
	for len(b) > 0 {
		tag, n := binary.Uvarint(b)
		if n <= 0 || tag>>3 == 0 || tag>>3 > math.MaxInt32 {
			return nil, errScanFormat
		}
		b = b[n:]
		f := pbField{num: int(tag >> 3), wire: int(tag & 7)}
		switch f.wire {
		case 0:
			f.v, n = binary.Uvarint(b)
			if n <= 0 {
				return nil, errScanFormat
			}
			b = b[n:]
		case 1:
			if len(b) < 8 {
				return nil, errScanFormat
			}
			f.v, b = binary.LittleEndian.Uint64(b), b[8:]
		case 2:
			l, n := binary.Uvarint(b)
			if n <= 0 || l > uint64(len(b)-n) {
				return nil, errScanFormat
			}
			f.b, b = b[n:n+int(l)], b[n+int(l):]
		case 5:
			if len(b) < 4 {
				return nil, errScanFormat
			}
			f.v, b = uint64(binary.LittleEndian.Uint32(b)), b[4:]
		default:
			return nil, errScanFormat
		}
		if len(out) == maxPBFields {
			return nil, errScanFormat
		}
		out = append(out, f)
	}
	return out, nil
}

func (m pbMsg) has(num int) bool {
	for _, f := range m {
		if f.num == num {
			return true
		}
	}
	return false
}

// bytes is a length-delimited field's last value, as protobuf keeps the
// last of a repeated optional field.
func (m pbMsg) bytes(num int) []byte {
	var b []byte
	for _, f := range m {
		if f.num == num && f.wire == 2 {
			b = f.b
		}
	}
	return b
}

func (m pbMsg) uint(num int) uint64 {
	var v uint64
	for _, f := range m {
		if f.num == num && f.wire != 2 {
			v = f.v
		}
	}
	return v
}

func (m pbMsg) all(num int) [][]byte {
	var out [][]byte
	for _, f := range m {
		if f.num == num && f.wire == 2 {
			out = append(out, f.b)
		}
	}
	return out
}

// varints is a repeated integer field, packed or not.
func (m pbMsg) varints(num int) []uint64 {
	var out []uint64
	for _, f := range m {
		switch {
		case f.num != num:
		case f.wire == 0:
			out = append(out, f.v)
		case f.wire == 2:
			for b := f.b; len(b) > 0; {
				v, n := binary.Uvarint(b)
				if n <= 0 {
					break
				}
				out, b = append(out, v), b[n:]
			}
		}
	}
	return out
}
