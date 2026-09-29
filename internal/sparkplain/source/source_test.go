package source

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// stubS3 is an in-memory bucket. Tests never call real AWS.
type stubS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	class   map[string]types.ObjectStorageClass
	etag    map[string]string
	errs    map[string]error // GetObject errors by key
	pageLen int
	block   bool // GetObject waits for the context to end
	active  int32
	peak    int32
	gotIf   []string
}

func (s *stubS3) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	var keys []string
	for k := range s.objects {
		if strings.HasPrefix(k, aws.ToString(in.Prefix)) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys) // S3 lists keys in order, so pages never overlap
	start := 0
	if in.ContinuationToken != nil {
		start = len(aws.ToString(in.ContinuationToken))
	}
	page := s.pageLen
	if in.MaxKeys != nil && int(*in.MaxKeys) < page {
		page = int(*in.MaxKeys)
	}
	end := min(len(keys), start+page)
	out := &s3.ListObjectsV2Output{}
	for _, k := range keys[start:end] {
		o := types.Object{Key: aws.String(k), Size: aws.Int64(int64(len(s.objects[k]))), ETag: aws.String(s.etag[k]), StorageClass: s.class[k]}
		if s.class[k] == "restored" {
			o.StorageClass = types.ObjectStorageClassGlacier
			o.RestoreStatus = &types.RestoreStatus{IsRestoreInProgress: aws.Bool(false), RestoreExpiryDate: aws.Time(time.Now().Add(time.Hour))}
		}
		out.Contents = append(out.Contents, o)
	}
	if end < len(keys) {
		out.IsTruncated, out.NextContinuationToken = aws.Bool(true), aws.String(strings.Repeat("x", end))
	}
	return out, nil
}

func (s *stubS3) GetObject(ctx context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	n := atomic.AddInt32(&s.active, 1)
	defer atomic.AddInt32(&s.active, -1)
	for {
		p := atomic.LoadInt32(&s.peak)
		if n <= p || atomic.CompareAndSwapInt32(&s.peak, p, n) {
			break
		}
	}
	s.mu.Lock()
	s.gotIf = append(s.gotIf, aws.ToString(in.IfMatch))
	s.mu.Unlock()
	if s.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	time.Sleep(2 * time.Millisecond)
	key := aws.ToString(in.Key)
	if err := s.errs[key]; err != nil {
		return nil, err
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(s.objects[key]))}, nil
}

func gz(s string) []byte {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write([]byte(s))
	w.Close()
	return b.Bytes()
}

func zipOf(files map[string]string) []byte {
	var b bytes.Buffer
	w := zip.NewWriter(&b)
	for n, c := range files {
		f, _ := w.Create(n)
		f.Write([]byte(c))
	}
	w.Close()
	return b.Bytes()
}

func TestS3ListAndArchive(t *testing.T) {
	st := &stubS3{pageLen: 2, objects: map[string][]byte{
		"logs/c/a.gz": gz("a"), "logs/c/b.gz": gz("b"), "logs/c/c.gz": gz("c"), "logs/c/old.gz": gz("old"), "logs/c/back.gz": gz("back"), "other/x": []byte("x"),
	}, class: map[string]types.ObjectStorageClass{"logs/c/old.gz": types.ObjectStorageClassDeepArchive, "logs/c/back.gz": "restored"}, etag: map[string]string{}}
	objs, err := NewS3Store(st, "bkt").List(context.Background(), "logs/")
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	archived := map[string]bool{}
	for _, o := range objs {
		keys = append(keys, o.Key)
		archived[o.Key] = o.Archived
	}
	if strings.Join(keys, ",") != "logs/c/a.gz,logs/c/b.gz,logs/c/back.gz,logs/c/c.gz,logs/c/old.gz" {
		t.Errorf("listed %v", keys)
	}
	if !archived["logs/c/old.gz"] || archived["logs/c/back.gz"] {
		t.Errorf("archived %v: deep archive should be unreadable, a restored Glacier object readable", archived)
	}
}

func TestFetchReadsDecompressesAndClassifies(t *testing.T) {
	st := &stubS3{pageLen: 100, etag: map[string]string{}, objects: map[string][]byte{
		"a/stderr.gz":   gz("line one\nline two\n"),
		"a/plain.txt":   []byte("plain\n"),
		"a/bundle.zip":  zipOf(map[string]string{"x.txt": "from zip"}),
		"a/denied.gz":   gz("no"),
		"a/throttle.gz": gz("no"),
		"a/changed.gz":  gz("no"),
		"a/bad.gz":      []byte("not gzip"),
		"a/huge.log":    bytes.Repeat([]byte("x"), 2048),
	}, errs: map[string]error{
		"a/denied.gz":   &smithy.GenericAPIError{Code: "AccessDenied", Message: "no"},
		"a/throttle.gz": &smithy.GenericAPIError{Code: "SlowDown", Message: "slow"},
		"a/changed.gz":  &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "etag"},
	}}
	store := NewS3Store(st, "bkt")
	objs, _ := store.List(context.Background(), "a/")
	for i := range objs {
		objs[i].ETag = `"etag-` + objs[i].Key + `"`
	}
	var mu sync.Mutex
	got := map[string]string{}
	reads := Fetch(context.Background(), store, objs, Limits{Workers: 3, MaxObject: 1024}, func(o Object, name string, r io.Reader) error {
		b, err := io.ReadAll(r)
		mu.Lock()
		got[name] = string(b)
		mu.Unlock()
		return err
	})
	class := map[string]string{}
	for _, r := range reads {
		if r.Err != nil {
			class[r.Object.Key] = ClassOf(r.Err)
		}
	}
	want := map[string]string{"a/denied.gz": ClassAccessDenied, "a/throttle.gz": ClassThrottled, "a/changed.gz": ClassChanged, "a/bad.gz": ClassCorrupt, "a/huge.log": ClassTooLarge}
	for k, c := range want {
		if class[k] != c {
			t.Errorf("%s: class %q, want %q", k, class[k], c)
		}
	}
	if got["a/stderr.gz"] != "line one\nline two\n" || got["a/plain.txt"] != "plain\n" || got["a/bundle.zip!x.txt"] != "from zip" {
		t.Errorf("read %v", got)
	}
	if st.peak > 3 {
		t.Errorf("%d reads at once, limit 3", st.peak)
	}
	for _, e := range st.gotIf {
		if !strings.HasPrefix(e, `"etag-`) {
			t.Errorf("GetObject without If-Match: %q", e)
		}
	}
}

func TestFetchTimesOut(t *testing.T) {
	st := &stubS3{pageLen: 10, block: true, objects: map[string][]byte{"k": []byte("x")}, etag: map[string]string{}}
	reads := Fetch(context.Background(), NewS3Store(st, "b"), []Object{{Key: "k", Size: 1}}, Limits{PerObject: 20 * time.Millisecond}, func(Object, string, io.Reader) error { return nil })
	if c := ClassOf(reads[0].Err); c != ClassTimeout {
		t.Errorf("class %q, want timeout (%v)", c, reads[0].Err)
	}
}

func TestLocalStore(t *testing.T) {
	dir := t.TempDir()
	for _, p := range []string{"containers/app_1/c_01/stderr.gz", "containers/app_1/c_02/stdout.gz", "steps/s-1/stderr.gz"} {
		os.MkdirAll(filepath.Join(dir, filepath.Dir(p)), 0o755)
		os.WriteFile(filepath.Join(dir, p), gz(p), 0o644)
	}
	st := NewLocalStore(dir)
	objs, err := st.List(context.Background(), "containers/app_1/")
	if err != nil || len(objs) != 2 || objs[0].Key != "containers/app_1/c_01/stderr.gz" {
		t.Fatalf("%v %v", objs, err)
	}
	reads := Fetch(context.Background(), st, objs, Limits{}, func(o Object, _ string, r io.Reader) error {
		b, _ := io.ReadAll(r)
		if string(b) != o.Key {
			t.Errorf("%s read %q", o.Key, b)
		}
		return nil
	})
	if reads[0].Err != nil || reads[1].Err != nil {
		t.Errorf("errors %v", reads)
	}
	if _, err := st.Open(context.Background(), Object{Key: "missing"}); ClassOf(err) != ClassNotFound {
		t.Errorf("missing file: %v", err)
	}
	if b, k, ok := ParseS3("s3://bucket/a/b.lz4"); !ok || b != "bucket" || k != "a/b.lz4" {
		t.Errorf("ParseS3 = %q %q %v", b, k, ok)
	}
}

// bz1MiB is bzip2 of 1 MiB of "x" (made with Python's bz2 module; Go has
// no bzip2 writer).
const bz1MiB = "QlpoOTFBWSZTWe0tAGYACApAgIAEAEAACCAAMMwFSepxBgFAYB4u5IpwoSHaWgDM"

// SP-003: streams that unpack past a limit fail with tooLarge instead of
// stopping quietly, zip entries over the entry limit are refused, and an
// entry exactly at a limit reads cleanly.
func TestFetchSizeLimits(t *testing.T) {
	bz, _ := base64.StdEncoding.DecodeString(bz1MiB)
	big := strings.Repeat("y", 64<<10)
	st := &stubS3{pageLen: 100, etag: map[string]string{}, objects: map[string][]byte{
		"a/bomb.gz":    gz(strings.Repeat("z", 1<<20)), // ~1 KiB unpacking to 1 MiB
		"a/exact.gz":   gz(strings.Repeat("e", 4096)),
		"a/bomb.bz2":   bz,
		"a/bundle.zip": zipOf(map[string]string{"small.txt": "fine", "large.txt": big}),
	}}
	store := NewS3Store(st, "bkt")
	objs, _ := store.List(context.Background(), "a/")
	var mu sync.Mutex
	got := map[string]int{}
	reads := Fetch(context.Background(), store, objs, Limits{MaxUnpacked: 4096, MaxZipEntry: 1024}, func(o Object, name string, r io.Reader) error {
		b, err := io.ReadAll(r)
		mu.Lock()
		got[name] = len(b)
		mu.Unlock()
		return err
	})
	class := map[string]string{}
	for _, r := range reads {
		class[r.Object.Key] = ClassOf(r.Err)
		if r.Err == nil {
			class[r.Object.Key] = "ok"
		}
	}
	want := map[string]string{"a/bomb.gz": ClassTooLarge, "a/bomb.bz2": ClassTooLarge, "a/exact.gz": "ok", "a/bundle.zip": ClassTooLarge}
	for k, c := range want {
		if class[k] != c {
			t.Errorf("%s: %q, want %q", k, class[k], c)
		}
	}
	if got["a/exact.gz"] != 4096 || got["a/bomb.gz"] > 4096 || got["a/bundle.zip!small.txt"] != 4 {
		t.Errorf("bytes read %v", got)
	}
	if _, ok := got["a/bundle.zip!large.txt"]; ok {
		t.Error("an entry over the entry limit must not be read")
	}
}

// A zip whose header understates an entry's size must not read as a
// complete, successful entry.
func TestFetchForgedZipEntrySize(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, _ := zw.CreateRaw(&zip.FileHeader{Name: "lie.txt", Method: zip.Store, CompressedSize64: 4096, UncompressedSize64: 10})
	w.Write(bytes.Repeat([]byte("q"), 4096))
	zw.Close()
	st := &stubS3{pageLen: 10, etag: map[string]string{}, objects: map[string][]byte{"a/lie.zip": buf.Bytes()}}
	store := NewS3Store(st, "b")
	objs, _ := store.List(context.Background(), "a/")
	reads := Fetch(context.Background(), store, objs, Limits{MaxZipEntry: 1024}, func(o Object, name string, r io.Reader) error {
		_, err := io.ReadAll(r)
		return err
	})
	if reads[0].Err == nil {
		t.Fatal("a zip entry larger than its header says read as complete")
	}
}

// Zips are held in memory while read; the memory budget stops several
// workers from each holding a large one at once.
func TestFetchZipMemoryBudget(t *testing.T) {
	objects := map[string][]byte{}
	for i := 0; i < 4; i++ {
		objects[fmt.Sprintf("a/z%d.zip", i)] = zipOf(map[string]string{"e.txt": strings.Repeat("w", 100)})
	}
	st := &stubS3{pageLen: 10, etag: map[string]string{}, objects: objects}
	store := NewS3Store(st, "b")
	objs, _ := store.List(context.Background(), "a/")
	size := objs[0].Size
	var inside, peak atomic.Int32
	Fetch(context.Background(), store, objs, Limits{Workers: 4, MaxZip: size, MaxZipMemory: size}, func(o Object, name string, r io.Reader) error {
		n := inside.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		inside.Add(-1)
		_, err := io.ReadAll(r)
		return err
	})
	if peak.Load() != 1 {
		t.Errorf("%d zips held at once, budget allows 1", peak.Load())
	}
}

func TestBoundedExactAndOver(t *testing.T) {
	b, err := io.ReadAll(Bounded(strings.NewReader("12345"), 5, "k", "x"))
	if err != nil || string(b) != "12345" {
		t.Errorf("exact: %q %v", b, err)
	}
	b, err = io.ReadAll(Bounded(strings.NewReader("123456"), 5, "k", "x"))
	if ClassOf(err) != ClassTooLarge || len(b) != 5 {
		t.Errorf("over: %q %v", b, err)
	}
}

// SP-009: a zip with more entries than the limit reads the first ones and
// is marked tooLarge (partial), saying how many were left out.
func TestFetchZipEntryLimitIsPartial(t *testing.T) {
	entries := map[string]string{}
	for i := 0; i < 5; i++ {
		entries[fmt.Sprintf("e%d.txt", i)] = "x"
	}
	st := &stubS3{pageLen: 10, etag: map[string]string{}, objects: map[string][]byte{"a/many.zip": zipOf(entries)}}
	store := NewS3Store(st, "b")
	objs, _ := store.List(context.Background(), "a/")
	var n atomic.Int32
	reads := Fetch(context.Background(), store, objs, Limits{MaxEntries: 4}, func(o Object, name string, r io.Reader) error {
		n.Add(1)
		_, err := io.ReadAll(r)
		return err
	})
	if n.Load() != 4 || ClassOf(reads[0].Err) != ClassTooLarge || !strings.Contains(reads[0].Err.Error(), "4 of 5 entries") {
		t.Fatalf("read %d entries, err %v", n.Load(), reads[0].Err)
	}
	// Exactly at the limit: complete, no error.
	delete(entries, "e4.txt")
	st.objects["a/many.zip"] = zipOf(entries)
	objs, _ = store.List(context.Background(), "a/")
	reads = Fetch(context.Background(), store, objs, Limits{MaxEntries: 4}, func(o Object, name string, r io.Reader) error { _, err := io.ReadAll(r); return err })
	if reads[0].Err != nil {
		t.Fatalf("a zip at the limit must read cleanly: %v", reads[0].Err)
	}
}

func TestS3HeadFindsOnlyTheExactKey(t *testing.T) {
	st := &stubS3{pageLen: 1000, etag: map[string]string{"logs/app.lz4": `"e1"`}, objects: map[string][]byte{
		"logs/app.lz4": []byte("abc"), "logs/app.lz4.inprogress": []byte("x"), "logs/other": nil,
	}}
	store := NewS3Store(st, "b")
	o, ok, err := store.Head(context.Background(), "logs/app.lz4")
	if err != nil || !ok || o.Size != 3 || o.ETag != `"e1"` {
		t.Fatalf("exact key: %+v %v %v", o, ok, err)
	}
	if _, ok, err := store.Head(context.Background(), "logs/ap"); ok || err != nil {
		t.Fatalf("a prefix of a key is not an object: %v %v", ok, err)
	}
	if _, ok, _ := store.Head(context.Background(), "logs/missing"); ok {
		t.Fatal("missing key found")
	}
}

// slowStore serves one object whose reads each take a while and which, like
// LocalStore, ignores the context in Open.
type slowStore struct{}

func (slowStore) List(context.Context, string) ([]Object, error) { return nil, nil }
func (slowStore) Head(context.Context, string) (Object, bool, error) {
	return Object{}, false, nil
}
func (slowStore) Location(k string) string { return k }
func (slowStore) Open(context.Context, Object) (io.ReadCloser, error) {
	return io.NopCloser(slowReader{}), nil
}

type slowReader struct{}

func (slowReader) Read(p []byte) (int, error) {
	time.Sleep(5 * time.Millisecond)
	p[0] = 'x'
	return 1, nil // endless, one byte at a time
}

// SP-007: the per-object timeout ends the read even when the store ignores
// the context, rather than letting the callback run on.
func TestFetchTimeoutIsADeadline(t *testing.T) {
	start := time.Now()
	reads := Fetch(context.Background(), slowStore{}, []Object{{Key: "slow.log", Size: 1}}, Limits{PerObject: 100 * time.Millisecond},
		func(o Object, name string, r io.Reader) error {
			_, err := io.Copy(io.Discard, r)
			return err
		})
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("Fetch took %v with a 100ms per-object timeout", took)
	}
	if c := ClassOf(reads[0].Err); c != ClassTimeout {
		t.Fatalf("class %q, want timeout (%v)", c, reads[0].Err)
	}
}

// badEntry is a directory entry whose metadata cannot be read.
type badEntry struct{ err error }

func (b badEntry) Name() string               { return "stderr.gz" }
func (b badEntry) IsDir() bool                { return false }
func (b badEntry) Type() fs.FileMode          { return 0 }
func (b badEntry) Info() (fs.FileInfo, error) { return nil, b.err }

// SP-012: a file whose metadata cannot be read stays in the listing so its
// read fails visibly; one that vanished is left out.
func TestLocalObjectKeepsUnreadableMetadata(t *testing.T) {
	if o, ok := localObject("c/stderr.gz", badEntry{fs.ErrPermission}); !ok || o.Key != "c/stderr.gz" {
		t.Errorf("permission error: %+v %v, want kept", o, ok)
	}
	if _, ok := localObject("c/stderr.gz", badEntry{fs.ErrNotExist}); ok {
		t.Error("a vanished file should be left out")
	}
}

// Sample lists at most n objects under a prefix with one small call, and
// a store without its own Sample lists and cuts.
func TestSample(t *testing.T) {
	t.Parallel()
	api := &stubS3{objects: map[string][]byte{"logs/a": nil, "logs/b": nil, "logs/c": nil}, pageLen: 1000}
	st := NewS3Store(api, "bucket")
	for prefix, want := range map[string]int{"logs/": 1, "none/": 0} {
		objs, err := Sample(context.Background(), st, prefix, 1)
		if err != nil || len(objs) != want {
			t.Errorf("S3 %s: %d objects, %v", prefix, len(objs), err)
		}
	}
	dir := t.TempDir()
	for _, n := range []string{"a", "b"} {
		if err := os.WriteFile(filepath.Join(dir, n), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if objs, err := Sample(context.Background(), NewLocalStore(dir), "", 1); err != nil || len(objs) != 1 || objs[0].Key != "a" {
		t.Errorf("local: %v, %v", objs, err)
	}
}
