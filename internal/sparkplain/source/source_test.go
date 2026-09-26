package source

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"io"
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
	end := min(len(keys), start+s.pageLen)
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
