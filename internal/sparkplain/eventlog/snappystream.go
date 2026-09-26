package eventlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/snappy"
)

// xerial snappy-java's SnappyOutputStream format, which Spark uses for
// .snappy event logs. It is not the Snappy framing format:
//
//	header: magic "\x82SNAPPY\x00" | version (4, BE) | compatible version (4, BE)
//	chunks: compressed length (4, BE) | Snappy block
//
// Concatenated streams repeat the header, so a chunk length position may hold
// the magic instead.
var snappyMagic = []byte("\x82SNAPPY\x00")

const (
	snappyHeaderLen = 16
	snappyMaxChunk  = 1 << 28
)

type snappyStreamReader struct {
	r    io.Reader
	comp []byte
	out  []byte
	pos  int
	err  error
	lenb [8]byte
}

func newSnappyStreamReader(r io.Reader) (*snappyStreamReader, error) {
	s := &snappyStreamReader{r: r}
	var h [snappyHeaderLen]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return nil, fmt.Errorf("snappy header: %w", errTruncated)
	}
	if !bytes.Equal(h[:8], snappyMagic) {
		return nil, errors.New("snappy: missing SnappyOutputStream header")
	}
	return s, nil
}

func (s *snappyStreamReader) Read(p []byte) (int, error) {
	for s.pos >= len(s.out) {
		if s.err != nil {
			return 0, s.err
		}
		s.err = s.next()
	}
	n := copy(p, s.out[s.pos:])
	s.pos += n
	return n, nil
}

func (s *snappyStreamReader) next() error {
	s.out, s.pos = s.out[:0], 0
	n, err := io.ReadFull(s.r, s.lenb[:4])
	if err == io.EOF {
		return io.EOF
	}
	if err != nil {
		return fmt.Errorf("snappy chunk length (%d of 4 bytes): %w", n, errTruncated)
	}
	if s.lenb[0] == snappyMagic[0] {
		// Possibly another stream's header.
		if _, err := io.ReadFull(s.r, s.lenb[4:8]); err != nil {
			return fmt.Errorf("snappy header: %w", errTruncated)
		}
		if !bytes.Equal(s.lenb[:8], snappyMagic) {
			return errors.New("snappy: bad chunk length")
		}
		var rest [8]byte
		if _, err := io.ReadFull(s.r, rest[:]); err != nil {
			return fmt.Errorf("snappy header: %w", errTruncated)
		}
		return nil
	}
	clen := int(binary.BigEndian.Uint32(s.lenb[:4]))
	if clen <= 0 || clen > snappyMaxChunk {
		return fmt.Errorf("snappy: bad chunk length %d", clen)
	}
	if cap(s.comp) < clen {
		s.comp = make([]byte, clen)
	}
	s.comp = s.comp[:clen]
	if _, err := io.ReadFull(s.r, s.comp); err != nil {
		return fmt.Errorf("snappy chunk: %w", errTruncated)
	}
	dlen, err := snappy.DecodedLen(s.comp)
	if err != nil || dlen > snappyMaxChunk {
		return fmt.Errorf("snappy: corrupt chunk: %v", err)
	}
	if cap(s.out) < dlen {
		s.out = make([]byte, dlen)
	}
	out, err := snappy.Decode(s.out[:cap(s.out)], s.comp)
	if err != nil {
		return fmt.Errorf("snappy: corrupt chunk: %v", err)
	}
	s.out = out
	return nil
}
