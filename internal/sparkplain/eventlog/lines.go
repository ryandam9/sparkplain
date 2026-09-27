package eventlog

import (
	"bufio"
	"context"
	"errors"
	"io"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/source"
)

// lineFunc receives one line (without the newline; the slice is reused after
// the call returns) and where it came from. partial is true for a final line
// that had no newline because the file ended.
type lineFunc func(line []byte, src model.Source, partial bool) error

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// eachLine streams every line of every part, in order. A part that cannot be
// read, or ends early, is recorded in its FileRead and reading moves on to
// the next part. It returns an error only for cancellation or a failing fn.
func (in *Input) eachLine(ctx context.Context, fn lineFunc, onLong func(model.Source)) ([]model.FileRead, bool, []string) {
	var (
		files     []model.FileRead
		truncated bool
		notes     []string
	)
	for _, p := range in.parts {
		fr := model.FileRead{Name: p.name, Bytes: p.size}
		err := func() error {
			rc, err := p.open()
			if err != nil {
				return classify(err)
			}
			defer rc.Close()
			raw := &countingReader{r: rc}
			dr, codec, note, done, err := decompressor(raw, p.name)
			fr.Codec = codec
			if note != "" {
				notes = append(notes, note)
			}
			if err != nil {
				return err
			}
			defer done()
			// Codecs bound their own block and window sizes, not the total:
			// stop a log that unpacks past the limit and mark it partial.
			cr := &countingReader{r: source.Bounded(dr, in.limits.MaxUnpackedBytes, p.name, "-max-unpacked")}
			err = readLines(ctx, cr, p.name, in.limits.MaxLineBytes, &fr.Lines, fn, onLong)
			fr.Decompressed = cr.n
			return err
		}()
		if err != nil {
			if ctx.Err() != nil {
				fr.Error = ctx.Err().Error()
				files = append(files, fr)
				return files, true, notes
			}
			if errors.Is(err, errTruncated) || errors.Is(err, errUnterminated) {
				truncated = true
				if !in.InProgress {
					fr.Error = err.Error()
				}
			} else {
				fr.Error = err.Error()
				truncated = true
			}
		}
		files = append(files, fr)
	}
	return files, truncated, notes
}

func readLines(ctx context.Context, r io.Reader, name string, maxLine int, count *int64, fn lineFunc, onLong func(model.Source)) error {
	br := bufio.NewReaderSize(r, 1<<20)
	var (
		acc     []byte
		long    bool
		tooLong bool
		lineNo  int64
	)
	for {
		chunk, err := br.ReadSlice('\n')
		if err == bufio.ErrBufferFull {
			if !tooLong {
				acc = append(acc, chunk...)
				if len(acc) > maxLine {
					tooLong, acc = true, acc[:0]
				}
			}
			long = true
			continue
		}
		if len(chunk) > 0 || long {
			lineNo++
			*count = lineNo
			line := chunk
			if long {
				if !tooLong {
					acc = append(acc, chunk...)
				}
				line = acc
			}
			partial := err != nil
			if n := len(line); !partial && n > 0 && line[n-1] == '\n' {
				line = line[:n-1]
			}
			if n := len(line); n > 0 && line[n-1] == '\r' {
				line = line[:n-1]
			}
			src := model.Source{File: name, Line: lineNo}
			if tooLong {
				onLong(src)
			} else if len(line) > 0 {
				if ferr := fn(line, src, partial); ferr != nil {
					return ferr
				}
			}
			acc, long, tooLong = acc[:0], false, false
			if lineNo%4096 == 0 && ctx.Err() != nil {
				return ctx.Err()
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
