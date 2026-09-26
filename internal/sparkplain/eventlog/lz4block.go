package eventlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/pierrec/lz4/v4"
)

// lz4-java's LZ4BlockOutputStream format, which Spark uses for .lz4 event logs.
// It is not the LZ4 frame format. Each block is:
//
//	magic "LZ4Block" | token (1) | compressed length (4, LE) |
//	decompressed length (4, LE) | checksum (4, LE) | data
//
// The token's high nibble is the method (0x10 raw, 0x20 LZ4) and its low
// nibble the block size exponent minus 10. The checksum is XXH32 of the
// decompressed block with seed 0x9747b28c, masked to 28 bits. A block with
// both lengths zero ends a stream; several streams may follow each other.
var lz4Magic = []byte("LZ4Block")

const (
	lz4HeaderLen    = 8 + 1 + 4 + 4 + 4
	lz4MethodRaw    = 0x10
	lz4MethodLZ4    = 0x20
	lz4Seed         = 0x9747b28c
	lz4MaxBlockSize = 1 << 25 // lz4-java allows up to 32 MiB
)

// errTruncated marks a stream that ends part-way through a block or frame,
// which is normal for a log that is still being written.
var errTruncated = errors.New("compressed stream ends part-way (log may still be in progress)")

type lz4BlockReader struct {
	r     io.Reader
	hdr   [lz4HeaderLen]byte
	comp  []byte
	out   []byte
	pos   int
	ended bool // saw an end-of-stream block
	err   error
}

func newLZ4BlockReader(r io.Reader) *lz4BlockReader { return &lz4BlockReader{r: r} }

func (z *lz4BlockReader) Read(p []byte) (int, error) {
	for z.pos >= len(z.out) {
		if z.err != nil {
			return 0, z.err
		}
		z.err = z.next()
	}
	n := copy(p, z.out[z.pos:])
	z.pos += n
	return n, nil
}

func (z *lz4BlockReader) next() error {
	z.out, z.pos = z.out[:0], 0
	n, err := io.ReadFull(z.r, z.hdr[:])
	if err == io.EOF {
		if !z.ended {
			// Clean end at a block boundary but without the end marker:
			// the writer has not closed the stream yet.
			return errUnterminated
		}
		return io.EOF
	}
	if err != nil {
		return fmt.Errorf("lz4 block header (%d of %d bytes): %w", n, lz4HeaderLen, errTruncated)
	}
	if !bytes.Equal(z.hdr[:8], lz4Magic) {
		return fmt.Errorf("lz4 block: bad magic %q", z.hdr[:8])
	}
	token := z.hdr[8]
	method := int(token & 0xf0)
	maxSize := 1 << (10 + int(token&0x0f))
	compLen := int(int32(binary.LittleEndian.Uint32(z.hdr[9:])))
	origLen := int(int32(binary.LittleEndian.Uint32(z.hdr[13:])))
	sum := binary.LittleEndian.Uint32(z.hdr[17:])
	if origLen < 0 || compLen < 0 || origLen > maxSize || origLen > lz4MaxBlockSize || compLen > lz4MaxBlockSize+lz4MaxBlockSize/255+16 {
		return fmt.Errorf("lz4 block: bad lengths %d/%d (max %d)", compLen, origLen, maxSize)
	}
	if origLen == 0 {
		if compLen != 0 || sum != 0 {
			return errors.New("lz4 block: malformed end-of-stream block")
		}
		z.ended = true
		return nil // next call reads the next stream, or EOF
	}
	z.ended = false
	if cap(z.comp) < compLen {
		z.comp = make([]byte, compLen)
	}
	z.comp = z.comp[:compLen]
	if _, err := io.ReadFull(z.r, z.comp); err != nil {
		return fmt.Errorf("lz4 block data: %w", errTruncated)
	}
	if cap(z.out) < origLen {
		z.out = make([]byte, origLen)
	}
	z.out = z.out[:origLen]
	switch method {
	case lz4MethodRaw:
		if compLen != origLen {
			return fmt.Errorf("lz4 raw block: length %d != %d", compLen, origLen)
		}
		copy(z.out, z.comp)
	case lz4MethodLZ4:
		got, err := lz4.UncompressBlock(z.comp, z.out)
		if err != nil || got != origLen {
			return fmt.Errorf("lz4 block: corrupt data (%d of %d bytes): %v", got, origLen, err)
		}
	default:
		return fmt.Errorf("lz4 block: unknown method 0x%x", method)
	}
	if xxh32(z.out, lz4Seed)&0x0fffffff != sum {
		return errors.New("lz4 block: checksum mismatch")
	}
	return nil
}

// errUnterminated marks a compressed stream that stops cleanly between blocks
// without its end marker. Everything before it was read correctly.
var errUnterminated = errors.New("compressed stream has no end marker (log may still be in progress)")
