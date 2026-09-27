package eventlog

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/ryandam9/sparkplain/internal/sparkplain/source"
)

// ResolveStore finds the event log for appID in a store (S3, SPEC §3): loc is
// either one event log object or a prefix holding event logs, such as the
// cluster's spark.eventLog.dir. It picks the application's single-file log or
// rolling eventlog_v2_* folder, the highest attempt first and finished logs
// before in-progress ones, as Resolve does for local folders. Parts are read
// lazily, each checked against the ETag it was listed with.
func ResolveStore(ctx context.Context, st source.Store, loc, appID string, lim Limits) (*Input, error) {
	lim = lim.withDefaults()
	in := &Input{Location: st.Location(loc), limits: lim}
	// One object named exactly: read it. A location ending in / is a
	// prefix, and a prefix is never listed whole (it may hold every
	// application's logs): only the application's own names are listed.
	if loc != "" && !strings.HasSuffix(loc, "/") {
		o, ok, err := st.Head(ctx, loc)
		if err != nil {
			return nil, storeErr(err)
		}
		if ok {
			if strings.HasSuffix(strings.ToLower(o.Key), ".zip") {
				return nil, &SourceError{ClassUnsupported, fmt.Errorf("%s: download History Server zips and pass the local file", in.Location)}
			}
			in.Layout, in.InProgress = "single", strings.HasSuffix(o.Key, ".inprogress")
			in.NameMatches = matchesApp(path.Base(o.Key), appID)
			return in, in.addObject(ctx, st, o, path.Base(o.Key))
		}
	}
	prefix := loc
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	var cands []string
	files := map[string]source.Object{}
	folders := map[string][]source.Object{}
	for _, look := range []string{prefix + appID, prefix + "eventlog_v2_" + appID} {
		found, err := st.List(ctx, look)
		if err != nil {
			return nil, storeErr(err)
		}
		for _, o := range found {
			rel := strings.TrimPrefix(o.Key, prefix)
			top, _, nested := strings.Cut(rel, "/")
			if !matchesApp(top, appID) {
				continue
			}
			if nested {
				if folders[top] == nil {
					cands = append(cands, top)
				}
				folders[top] = append(folders[top], o)
				continue
			}
			files[top] = o
			cands = append(cands, top)
		}
	}
	if len(cands) == 0 {
		return nil, &SourceError{ClassNotFound, fmt.Errorf("no event log for %s under %s (looked for %s[.codec] and eventlog_v2_%s/)", appID, st.Location(prefix), appID, appID)}
	}
	pick := pickAttempt(cands)
	in.NameMatches = true // every candidate was listed under the application's name
	if len(cands) > 1 {
		in.Notes = append(in.Notes, fmt.Sprintf("Found %d logs for this application (%s); read %s (the highest attempt, preferring a finished log).", len(cands), strings.Join(cands, ", "), pick))
	}
	if parts, ok := folders[pick]; ok {
		in.Location = st.Location(prefix + pick + "/")
		byName := map[string]source.Object{}
		var names []string
		for _, o := range parts {
			n := path.Base(o.Key)
			byName[n] = o
			names = append(names, n)
		}
		err := in.addRolling(pick, names, func(n string) (int64, func() (io.ReadCloser, error), error) {
			o := byName[n]
			return o.Size, opener(ctx, st, o), nil
		}, "rolling")
		if err != nil {
			return nil, err
		}
		return in, nil
	}
	o := files[pick]
	in.Location = st.Location(o.Key)
	in.Layout, in.InProgress = "single", strings.HasSuffix(pick, ".inprogress")
	return in, in.addObject(ctx, st, o, pick)
}

func (in *Input) addObject(ctx context.Context, st source.Store, o source.Object, name string) error {
	if o.Size > in.limits.MaxObjectBytes {
		return &SourceError{ClassTooLarge, fmt.Errorf("%s is %d bytes, over the %d byte limit (-max-size)", name, o.Size, in.limits.MaxObjectBytes)}
	}
	in.parts = append(in.parts, part{name: name, size: o.Size, open: opener(ctx, st, o)})
	return nil
}

func opener(ctx context.Context, st source.Store, o source.Object) func() (io.ReadCloser, error) {
	return func() (io.ReadCloser, error) {
		rc, err := st.Open(ctx, o)
		if err != nil {
			return nil, storeErr(err)
		}
		return rc, nil
	}
}

// storeErr carries a store's error class into the Sources panel.
func storeErr(err error) error {
	switch source.ClassOf(err) {
	case source.ClassAccessDenied:
		return &SourceError{ClassAccessDenied, err}
	case source.ClassNotFound:
		return &SourceError{ClassNotFound, err}
	case source.ClassTimeout:
		return &SourceError{ClassTimeout, err}
	case source.ClassTooLarge:
		return &SourceError{ClassTooLarge, err}
	case source.ClassArchived, source.ClassThrottled, source.ClassChanged:
		return &SourceError{source.ClassOf(err), err}
	}
	return &SourceError{ClassCorrupt, err}
}
