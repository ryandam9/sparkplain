package eventlog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/ryandam9/sparkplain/internal/sparkplain/source"
)

// countingStore is an in-memory store that records every List prefix and
// how many keys each call returned.
type countingStore struct {
	objs     map[string][]byte
	keys     []string
	lists    []string
	returned int
}

func newCountingStore(objs map[string][]byte) *countingStore {
	s := &countingStore{objs: objs}
	for k := range objs {
		s.keys = append(s.keys, k)
	}
	sort.Strings(s.keys)
	return s
}

func (s *countingStore) List(_ context.Context, prefix string) ([]source.Object, error) {
	s.lists = append(s.lists, prefix)
	var out []source.Object
	for _, k := range s.keys {
		if strings.HasPrefix(k, prefix) {
			out = append(out, source.Object{Key: k, Size: int64(len(s.objs[k]))})
		}
	}
	s.returned += len(out)
	return out, nil
}

func (s *countingStore) Head(_ context.Context, key string) (source.Object, bool, error) {
	b, ok := s.objs[key]
	return source.Object{Key: key, Size: int64(len(b))}, ok, nil
}

func (s *countingStore) Open(_ context.Context, o source.Object) (io.ReadCloser, error) {
	b, ok := s.objs[o.Key]
	if !ok {
		return nil, &source.Error{Class: source.ClassNotFound, Key: o.Key, Err: errors.New("no such key")}
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (s *countingStore) Location(key string) string { return "s3://bucket/" + key }

// SP-002: resolving one application under a busy history prefix lists
// only that application's names, never the whole prefix.
func TestResolveStoreNeverListsWholePrefix(t *testing.T) {
	t.Parallel()
	objs := map[string][]byte{}
	for i := 0; i < 100000; i++ {
		objs[fmt.Sprintf("spark-events/application_1790000000000_%06d", i)] = nil
	}
	objs["spark-events/application_1790380000000_0042.lz4"] = []byte("x")
	objs["spark-events/eventlog_v2_application_1790380000000_0043/events_1_application_1790380000000_0043.zstd"] = []byte("x")
	objs["spark-events/eventlog_v2_application_1790380000000_0043/appstatus_application_1790380000000_0043"] = nil

	for _, c := range []struct{ loc, app, want string }{
		{"spark-events/", "application_1790380000000_0042", "application_1790380000000_0042.lz4"},
		{"spark-events", "application_1790380000000_0042", "application_1790380000000_0042.lz4"},
		{"spark-events/", "application_1790380000000_0043", "events_1_application_1790380000000_0043.zstd"},
	} {
		st := newCountingStore(objs)
		in, err := ResolveStore(context.Background(), st, c.loc, c.app, Limits{})
		if err != nil {
			t.Fatalf("%s %s: %v", c.loc, c.app, err)
		}
		if names := in.PartNames(); len(names) != 1 || !strings.HasSuffix(names[0], c.want) {
			t.Errorf("%s %s: parts %v", c.loc, c.app, names)
		}
		for _, p := range st.lists {
			if !strings.Contains(p, c.app) {
				t.Errorf("%s %s: listed the broad prefix %q", c.loc, c.app, p)
			}
		}
		if st.returned > 10 {
			t.Errorf("%s %s: listings returned %d keys; want only the application's", c.loc, c.app, st.returned)
		}
	}

	// An exact object is found with Head and nothing is listed.
	st := newCountingStore(objs)
	in, err := ResolveStore(context.Background(), st, "spark-events/application_1790380000000_0042.lz4", "application_1790380000000_0042", Limits{})
	if err != nil || in.Layout != "single" || len(st.lists) != 0 || !in.NameMatches {
		t.Fatalf("exact object: %v layout=%v lists=%v", err, in, st.lists)
	}
}
