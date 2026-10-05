package source

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// TraceEvery is how often a file still being read says how far it is;
// tests shorten it.
var TraceEvery = 30 * time.Second

// Trace wraps st so that note hears of each listing and each file read
// (-verbose): the prefix or file, how much, and how long it took, and,
// while a long file is read, how far it is every TraceEvery. note is
// called from the goroutines that read, so it must be safe for that. A
// nil note returns st as it is.
func Trace(st Store, note func(string)) Store {
	if note == nil {
		return st
	}
	return &traced{st: st, note: note}
}

type traced struct {
	st   Store
	note func(string)
}

func (t *traced) Location(key string) string { return t.st.Location(key) }

func (t *traced) List(ctx context.Context, prefix string) ([]Object, error) {
	where := t.st.Location(prefix)
	t.note("listing " + where)
	start := time.Now()
	objs, err := t.st.List(ctx, prefix)
	took := since(start)
	if err != nil {
		t.note(fmt.Sprintf("could not list %s (%s): %v", where, took, err))
		return objs, err
	}
	var size int64
	for _, o := range objs {
		size += o.Size
	}
	t.note(fmt.Sprintf("listed %s: %s, %s (%s)", where, model.Plural(len(objs), "object", "objects"), model.Bytes(size), took))
	return objs, nil
}

func (t *traced) Head(ctx context.Context, key string) (Object, bool, error) {
	o, ok, err := t.st.Head(ctx, key)
	switch {
	case err != nil:
		t.note(fmt.Sprintf("could not look up %s: %v", t.st.Location(key), err))
	case !ok:
		t.note("no file at " + t.st.Location(key))
	default:
		t.note(fmt.Sprintf("found %s (%s)", t.st.Location(key), model.Bytes(o.Size)))
	}
	return o, ok, err
}

func (t *traced) Sample(ctx context.Context, prefix string, n int) ([]Object, error) {
	objs, err := Sample(ctx, t.st, prefix, n)
	if err != nil {
		t.note(fmt.Sprintf("could not list %s: %v", t.st.Location(prefix), err))
	} else {
		t.note(fmt.Sprintf("checked %s: %s found", t.st.Location(prefix), model.Plural(len(objs), "object", "objects")))
	}
	return objs, err
}

func (t *traced) Peek(ctx context.Context, obj Object) error {
	return Peek(ctx, t.st, obj)
}

func (t *traced) Open(ctx context.Context, obj Object) (io.ReadCloser, error) {
	where := t.st.Location(obj.Key)
	t.note(fmt.Sprintf("reading %s (%s)", where, model.Bytes(obj.Size)))
	r, err := t.st.Open(ctx, obj)
	if err != nil {
		t.note(fmt.Sprintf("could not read %s: %v", where, err))
		return nil, err
	}
	tr := &traceReader{r: r, t: t, where: where, size: obj.Size, start: time.Now(), stop: make(chan struct{})}
	go tr.watch()
	return tr, nil
}

// traceReader counts what is read from one file and says how far it is
// until it is closed.
type traceReader struct {
	r         io.ReadCloser
	t         *traced
	where     string
	size      int64
	start     time.Time
	n         atomic.Int64
	stop      chan struct{}
	closeOnce sync.Once
}

func (r *traceReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n.Add(int64(n))
	return n, err
}

func (r *traceReader) watch() {
	tick := time.NewTicker(TraceEvery)
	defer tick.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-tick.C:
			msg := fmt.Sprintf("still reading %s: %s", r.where, model.Bytes(r.n.Load()))
			if r.size > 0 {
				msg += " of " + model.Bytes(r.size)
			}
			r.t.note(msg + " so far (" + since(r.start) + ")")
		}
	}
}

func (r *traceReader) Close() error {
	err := r.r.Close()
	r.closeOnce.Do(func() {
		close(r.stop)
		r.t.note(fmt.Sprintf("done %s: %s in %s", r.where, model.Bytes(r.n.Load()), since(r.start)))
	})
	return err
}

func since(t time.Time) string { return time.Since(t).Round(100 * time.Millisecond).String() }
