package yarnlog

import (
	"bufio"
	"errors"
	"io"
)

// maxLine is the longest line kept; the rest of a longer line is skipped,
// so one huge line (a dumped buffer, say) cannot hold the whole file in
// memory.
const maxLine = 64 << 10

// lineReader streams lines, dropping "\r\n" endings and cutting long lines.
type lineReader struct {
	r         *bufio.Reader
	buf       []byte
	truncated int
}

func newLineReader(r io.Reader) *lineReader {
	return &lineReader{r: bufio.NewReaderSize(r, 64<<10)}
}

// next returns the next line, or io.EOF after the last one. The slice is
// only valid until the next call.
func (lr *lineReader) next() ([]byte, error) {
	lr.buf = lr.buf[:0]
	cut := false
	for {
		frag, err := lr.r.ReadSlice('\n')
		if !cut {
			room := maxLine - len(lr.buf)
			if len(frag) > room {
				lr.buf = append(lr.buf, frag[:room]...)
				cut = true
				lr.truncated++
			} else {
				lr.buf = append(lr.buf, frag...)
			}
		}
		switch {
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case err == io.EOF:
			if len(lr.buf) == 0 && !cut {
				return nil, io.EOF
			}
			return trimEOL(lr.buf), nil
		case err != nil:
			return nil, err
		}
		return trimEOL(lr.buf), nil
	}
}

func trimEOL(b []byte) []byte {
	if n := len(b); n > 0 && b[n-1] == '\n' {
		b = b[:n-1]
	}
	if n := len(b); n > 0 && b[n-1] == '\r' {
		b = b[:n-1]
	}
	return b
}
