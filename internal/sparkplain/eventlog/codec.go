package eventlog

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// Codec names, matching Spark's short codec names and file extensions.
const (
	CodecPlain  = "plain"
	CodecLZ4    = "lz4"
	CodecZstd   = "zstd"
	CodecSnappy = "snappy"
	CodecLZF    = "lzf"
)

var zstdMagic = []byte{0x28, 0xb5, 0x2f, 0xfd}

// codecFromName reads the codec from a file name such as
// application_1_2.lz4.inprogress or events_3_app.zstd.
func codecFromName(name string) string {
	name = strings.TrimSuffix(name, ".inprogress")
	name = strings.TrimSuffix(name, ".compact")
	i := strings.LastIndexByte(name, '.')
	if i < 0 {
		return CodecPlain
	}
	switch name[i+1:] {
	case "lz4":
		return CodecLZ4
	case "zstd", "zst":
		return CodecZstd
	case "snappy":
		return CodecSnappy
	case "lzf":
		return CodecLZF
	}
	return CodecPlain
}

// codecFromMagic identifies a codec from the first bytes of a file. It
// returns "" when the bytes match nothing it knows.
func codecFromMagic(b []byte) string {
	switch {
	case bytes.HasPrefix(b, lz4Magic):
		return CodecLZ4
	case bytes.HasPrefix(b, zstdMagic):
		return CodecZstd
	case bytes.HasPrefix(b, snappyMagic):
		return CodecSnappy
	}
	t := bytes.TrimPrefix(bytes.TrimLeft(b, " \t\r\n"), []byte("\xef\xbb\xbf"))
	if len(t) == 0 || t[0] == '{' {
		return CodecPlain
	}
	return ""
}

// decompressor wraps r, detecting the codec from the name and confirming it
// with the magic bytes. The returned note is non-empty when the two disagree;
// the magic bytes win.
func decompressor(r io.Reader, name string) (io.Reader, string, string, func(), error) {
	br := bufio.NewReaderSize(r, 1<<20)
	head, _ := br.Peek(8)
	byName := codecFromName(name)
	byMagic := codecFromMagic(head)
	codec, note := byName, ""
	switch {
	case byName == CodecLZF:
		return nil, CodecLZF, "", nil, errors.New("the lzf codec is not supported; re-run Spark with lz4, zstd or snappy")
	case byMagic == "" && byName == CodecPlain:
		codec = CodecPlain // let the line parser count malformed lines
	case byMagic == "":
		return nil, byName, "", nil, fmt.Errorf("file name says %s but the content does not start like a %s stream", byName, byName)
	case byMagic != byName:
		codec = byMagic
		note = fmt.Sprintf("%s: file name says %s but the content is %s; read as %s", name, byName, byMagic, byMagic)
	}
	nop := func() {}
	switch codec {
	case CodecLZ4:
		return newLZ4BlockReader(br), codec, note, nop, nil
	case CodecSnappy:
		s, err := newSnappyStreamReader(br)
		if err != nil {
			return nil, codec, note, nil, err
		}
		return s, codec, note, nop, nil
	case CodecZstd:
		d, err := zstd.NewReader(br,
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderLowmem(true),
			zstd.WithDecoderMaxWindow(1<<27))
		if err != nil {
			return nil, codec, note, nil, err
		}
		return zstdReader{d}, codec, note, d.Close, nil
	}
	return br, CodecPlain, note, nop, nil
}

// zstdReader maps a stream cut part-way through a frame to errTruncated.
type zstdReader struct{ d *zstd.Decoder }

func (z zstdReader) Read(p []byte) (int, error) {
	n, err := z.d.Read(p)
	if err != nil && err != io.EOF && errors.Is(err, io.ErrUnexpectedEOF) {
		err = fmt.Errorf("zstd: %w", errTruncated)
	}
	return n, err
}
