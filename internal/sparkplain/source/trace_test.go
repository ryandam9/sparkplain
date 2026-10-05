package source

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// A traced store says what it lists and reads, with sizes, and says how
// far a file is while it is still being read.
func TestTrace(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "logs"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "logs", "stderr"), []byte(strings.Repeat("x", 2048)), 0o600); err != nil {
		t.Fatal(err)
	}
	local := NewLocalStore(dir)
	if Trace(local, nil) != Store(local) {
		t.Fatal("Trace with no note should return the store as it is")
	}
	var mu sync.Mutex
	var lines []string
	st := Trace(local, func(s string) { mu.Lock(); lines = append(lines, s); mu.Unlock() })
	old := TraceEvery
	TraceEvery = 10 * time.Millisecond
	defer func() { TraceEvery = old }()

	ctx := context.Background()
	objs, err := st.List(ctx, "logs/")
	if err != nil || len(objs) != 1 {
		t.Fatalf("List: %v, %d objects", err, len(objs))
	}
	r, err := st.Open(ctx, objs[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(r, make([]byte, 1024)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // a slow reader: the trace says how far it is
	if _, err := io.Copy(io.Discard, r); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	_ = r.Close() // closing twice says done once

	mu.Lock()
	got := strings.Join(lines, "\n")
	mu.Unlock()
	for _, want := range []string{"listing ", "listed ", "1 object, 2.0 KiB", "reading ", "(2.0 KiB)", "still reading ", "1.0 KiB of 2.0 KiB so far", "done "} {
		if !strings.Contains(got, want) {
			t.Errorf("trace lacks %q:\n%s", want, got)
		}
	}
	if n := strings.Count(got, "done "); n != 1 {
		t.Errorf("done said %d times, want 1:\n%s", n, got)
	}
	// The trace's lines stop once the file is closed.
	mu.Lock()
	n := len(lines)
	mu.Unlock()
	time.Sleep(40 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(lines) != n {
		t.Errorf("still tracing after Close:\n%s", strings.Join(lines[n:], "\n"))
	}
}
