package source

import (
	"archive/zip"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"
)

// Limits bound one fetch (SPEC §2).
type Limits struct {
	Workers    int           // objects read at once; default 16
	MaxObject  int64         // largest object read, compressed; default 10 GiB
	MaxZip     int64         // largest zip read into memory; default 256 MiB
	PerObject  time.Duration // timeout per object; default 5 minutes
	MaxEntries int           // zip entries read per archive; default 1000
}

func (l Limits) withDefaults() Limits {
	if l.Workers <= 0 {
		l.Workers = 16
	}
	if l.MaxObject <= 0 {
		l.MaxObject = 10 << 30
	}
	if l.MaxZip <= 0 {
		l.MaxZip = 256 << 20
	}
	if l.PerObject <= 0 {
		l.PerObject = 5 * time.Minute
	}
	if l.MaxEntries <= 0 {
		l.MaxEntries = 1000
	}
	return l
}

// Read is one object read: its content streamed through fn, or why not.
type Read struct {
	Object Object
	Where  string // s3://bucket/key or local path
	Err    error  // nil when read; classify with ClassOf
}

// Fetch reads objs with bounded concurrency, decompressing each by its name
// (.gz, .bz2, .zip; others as is) and streaming the content to fn with the
// name of the file inside (the key, or key!entry for zips). fn may be
// called from several goroutines at once. Results come back in objs' order.
func Fetch(ctx context.Context, st Store, objs []Object, lim Limits, fn func(obj Object, name string, r io.Reader) error) []Read {
	lim = lim.withDefaults()
	out := make([]Read, len(objs))
	sem := make(chan struct{}, lim.Workers)
	var wg sync.WaitGroup
	for i, o := range objs {
		out[i] = Read{Object: o, Where: st.Location(o.Key)}
		if o.Archived {
			out[i].Err = &Error{Class: ClassArchived, Key: out[i].Where, Err: errors.New("archived in " + o.StorageClass + " and not restored")}
			continue
		}
		if o.Size > lim.MaxObject {
			out[i].Err = &Error{Class: ClassTooLarge, Key: out[i].Where, Err: fmt.Errorf("%d bytes is over the %d-byte limit", o.Size, lim.MaxObject)}
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, o Object) {
			defer wg.Done()
			defer func() { <-sem }()
			octx, cancel := context.WithTimeout(ctx, lim.PerObject)
			defer cancel()
			out[i].Err = readOne(octx, st, o, lim, fn)
		}(i, o)
	}
	wg.Wait()
	return out
}

func readOne(ctx context.Context, st Store, o Object, lim Limits, fn func(Object, string, io.Reader) error) error {
	rc, err := st.Open(ctx, o)
	if err != nil {
		return err
	}
	defer rc.Close()
	where := st.Location(o.Key)
	body := io.LimitReader(rc, lim.MaxObject+1)
	switch strings.ToLower(path.Ext(o.Key)) {
	case ".gz":
		zr, err := gzip.NewReader(body)
		if err != nil {
			return &Error{Class: ClassCorrupt, Key: where, Err: err}
		}
		defer zr.Close()
		return wrapRead(where, fn(o, o.Key, zr))
	case ".bz2":
		return wrapRead(where, fn(o, o.Key, bzip2.NewReader(body)))
	case ".zip":
		if o.Size > lim.MaxZip {
			return &Error{Class: ClassTooLarge, Key: where, Err: fmt.Errorf("zip of %d bytes is over the %d-byte limit", o.Size, lim.MaxZip)}
		}
		data, err := io.ReadAll(io.LimitReader(body, lim.MaxZip+1))
		if err != nil {
			return wrapRead(where, err)
		}
		zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return &Error{Class: ClassCorrupt, Key: where, Err: err}
		}
		for n, f := range zr.File {
			if n == lim.MaxEntries {
				break
			}
			if f.FileInfo().IsDir() {
				continue
			}
			er, err := f.Open()
			if err != nil {
				return &Error{Class: ClassCorrupt, Key: where + "!" + f.Name, Err: err}
			}
			err = fn(o, o.Key+"!"+f.Name, io.LimitReader(er, lim.MaxZip))
			er.Close()
			if err != nil {
				return wrapRead(where, err)
			}
		}
		return nil
	}
	return wrapRead(where, fn(o, o.Key, body))
}

// wrapRead classifies an error raised while streaming an object.
func wrapRead(where string, err error) error {
	if err == nil {
		return nil
	}
	var e *Error
	if errors.As(err, &e) {
		return err
	}
	class := ClassCorrupt
	if errors.Is(err, context.DeadlineExceeded) {
		class = ClassTimeout
	}
	return &Error{Class: class, Key: where, Err: err}
}
