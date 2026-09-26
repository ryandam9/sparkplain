package eventlog

import (
	"archive/zip"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Error classes shown in the report's Sources panel (SPEC §2).
const (
	ClassAccessDenied = "accessDenied"
	ClassNotFound     = "notFound"
	ClassCorrupt      = "corrupt"
	ClassTooLarge     = "tooLarge"
	ClassUnsupported  = "unsupported"
	ClassTimeout      = "timeout"
)

// SourceError is an error with a class for the Sources panel.
type SourceError struct {
	Class string
	Err   error
}

func (e *SourceError) Error() string { return e.Err.Error() }
func (e *SourceError) Unwrap() error { return e.Err }

func classify(err error) error {
	var se *SourceError
	switch {
	case err == nil || errors.As(err, &se):
		return err
	case errors.Is(err, fs.ErrNotExist):
		return &SourceError{ClassNotFound, err}
	case errors.Is(err, fs.ErrPermission):
		return &SourceError{ClassAccessDenied, err}
	}
	return &SourceError{ClassCorrupt, err}
}

// ErrorClass returns the Sources-panel class of err.
func ErrorClass(err error) string {
	var se *SourceError
	if errors.As(err, &se) {
		return se.Class
	}
	return ClassCorrupt
}

// Limits bound what is read.
type Limits struct {
	MaxObjectBytes int64 // largest file or zip entry accepted (stored size); 0 means 10 GiB
	MaxLineBytes   int   // longer lines are skipped and counted; 0 means 256 MiB
}

func (l Limits) withDefaults() Limits {
	if l.MaxObjectBytes <= 0 {
		l.MaxObjectBytes = 10 << 30
	}
	if l.MaxLineBytes <= 0 {
		l.MaxLineBytes = 256 << 20
	}
	return l
}

type part struct {
	name string // shown in provenance
	size int64
	open func() (io.ReadCloser, error)
}

// Input is a resolved event log: the ordered parts to read.
type Input struct {
	Location   string
	Layout     string // single, rolling, zip, zip-rolling
	InProgress bool
	Notes      []string
	parts      []part
	closer     io.Closer
	limits     Limits
}

// Close releases the zip file, if any.
func (in *Input) Close() error {
	if in.closer != nil {
		return in.closer.Close()
	}
	return nil
}

// PartNames lists the files that will be read, in order.
func (in *Input) PartNames() []string {
	out := make([]string, len(in.parts))
	for i, p := range in.parts {
		out[i] = p.name
	}
	return out
}

// Resolve finds the event log for appID at p, which may be a single log
// file, a rolling eventlog_v2_* folder, a folder holding event logs, or a
// History Server download zip.
func Resolve(p, appID string, lim Limits) (*Input, error) {
	lim = lim.withDefaults()
	st, err := os.Stat(p)
	if err != nil {
		return nil, classify(err)
	}
	in := &Input{Location: p, limits: lim}
	if st.IsDir() {
		err = in.resolveDir(p, appID)
	} else if isZip(p) {
		err = in.resolveZip(p, st.Size(), appID)
	} else {
		err = in.addFile(p, filepath.Base(p), st.Size())
		in.Layout = "single"
		in.InProgress = strings.HasSuffix(p, ".inprogress")
	}
	if err != nil {
		in.Close()
		return nil, classify(err)
	}
	return in, nil
}

func isZip(p string) bool {
	f, err := os.Open(p)
	if err != nil {
		return strings.HasSuffix(strings.ToLower(p), ".zip")
	}
	defer f.Close()
	var b [4]byte
	_, _ = io.ReadFull(f, b[:])
	return string(b[:]) == "PK\x03\x04"
}

func (in *Input) addFile(full, name string, size int64) error {
	if size > in.limits.MaxObjectBytes {
		return &SourceError{ClassTooLarge, fmt.Errorf("%s is %d bytes, over the %d byte limit (-max-size)", name, size, in.limits.MaxObjectBytes)}
	}
	in.parts = append(in.parts, part{name: name, size: size, open: func() (io.ReadCloser, error) { return os.Open(full) }})
	return nil
}

func (in *Input) resolveDir(dir, appID string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if hasRollingParts(names) {
		return in.addRolling(filepath.Base(dir), names, func(n string) (int64, func() (io.ReadCloser, error), error) {
			full := filepath.Join(dir, n)
			st, err := os.Stat(full)
			if err != nil {
				return 0, nil, err
			}
			return st.Size(), func() (io.ReadCloser, error) { return os.Open(full) }, nil
		}, "rolling")
	}
	var cands []string
	for _, e := range entries {
		if matchesApp(e.Name(), appID) {
			cands = append(cands, e.Name())
		}
	}
	if len(cands) == 0 {
		return &SourceError{ClassNotFound, fmt.Errorf("no event log for %s in %s (looked for %s[.codec] or eventlog_v2_%s/)", appID, dir, appID, appID)}
	}
	pick := pickAttempt(cands)
	if len(cands) > 1 {
		in.Notes = append(in.Notes, fmt.Sprintf("Found %d logs for this application (%s); read %s (the highest attempt, preferring a finished log).", len(cands), strings.Join(cands, ", "), pick))
	}
	full := filepath.Join(dir, pick)
	st, err := os.Stat(full)
	if err != nil {
		return err
	}
	if st.IsDir() {
		return in.resolveDir(full, appID)
	}
	in.Layout = "single"
	in.InProgress = strings.HasSuffix(pick, ".inprogress")
	return in.addFile(full, pick, st.Size())
}

func (in *Input) resolveZip(p string, size int64, appID string) error {
	if size > in.limits.MaxObjectBytes {
		return &SourceError{ClassTooLarge, fmt.Errorf("%s is over the %d byte limit (-max-size)", p, in.limits.MaxObjectBytes)}
	}
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	zr, err := zip.NewReader(f, size)
	if err != nil {
		f.Close()
		return &SourceError{ClassCorrupt, fmt.Errorf("reading zip %s: %w", p, err)}
	}
	in.closer = f
	if len(zr.File) > 100000 {
		return &SourceError{ClassTooLarge, fmt.Errorf("zip %s has %d entries", p, len(zr.File))}
	}
	byName := map[string]*zip.File{}
	dirs := map[string][]string{} // rolling folder -> file names
	var singles []string
	for _, zf := range zr.File {
		name := path.Clean(strings.ReplaceAll(zf.Name, `\`, "/"))
		if zf.FileInfo().IsDir() || strings.HasPrefix(name, "../") || path.IsAbs(name) {
			continue
		}
		byName[name] = zf
		d, base := path.Split(name)
		d = strings.TrimSuffix(d, "/")
		if strings.HasPrefix(path.Base(d), "eventlog_v2_") {
			dirs[d] = append(dirs[d], base)
		} else if !strings.HasPrefix(base, "appstatus_") && !strings.HasPrefix(base, ".") {
			singles = append(singles, name)
		}
	}
	open := func(name string) (int64, func() (io.ReadCloser, error), error) {
		zf := byName[name]
		if zf == nil {
			return 0, nil, fmt.Errorf("zip entry %s missing", name)
		}
		if zf.UncompressedSize64 > uint64(in.limits.MaxObjectBytes) {
			return 0, nil, &SourceError{ClassTooLarge, fmt.Errorf("zip entry %s unpacks to %d bytes, over the limit (-max-size)", name, zf.UncompressedSize64)}
		}
		limit := in.limits.MaxObjectBytes
		return int64(zf.UncompressedSize64), func() (io.ReadCloser, error) {
			rc, err := zf.Open()
			if err != nil {
				return nil, err
			}
			// Guard against entries whose header understates their size.
			return struct {
				io.Reader
				io.Closer
			}{&capReader{r: rc, left: limit, name: name}, rc}, nil
		}, nil
	}
	if len(dirs) > 0 {
		var keys []string
		for d := range dirs {
			if matchesApp(path.Base(d), appID) {
				keys = append(keys, d)
			}
		}
		if len(keys) == 0 {
			for d := range dirs {
				keys = append(keys, d)
			}
		}
		sort.Strings(keys)
		d := keys[len(keys)-1]
		if len(keys) > 1 {
			d = path.Join(path.Dir(d), pickAttempt(baseNames(keys)))
		}
		return in.addRolling(d, dirs[d], func(n string) (int64, func() (io.ReadCloser, error), error) {
			return open(path.Join(d, n))
		}, "zip-rolling")
	}
	var cands []string
	for _, s := range singles {
		if matchesApp(path.Base(s), appID) {
			cands = append(cands, s)
		}
	}
	if len(cands) == 0 {
		cands = singles
	}
	if len(cands) == 0 {
		return &SourceError{ClassNotFound, fmt.Errorf("zip %s holds no event log", p)}
	}
	pick := cands[0]
	if len(cands) > 1 {
		pick = pickAttempt(cands)
		in.Notes = append(in.Notes, fmt.Sprintf("The zip holds %d event logs; read %s.", len(cands), pick))
	}
	sz, opener, err := open(pick)
	if err != nil {
		return err
	}
	in.Layout = "zip"
	in.InProgress = strings.HasSuffix(pick, ".inprogress")
	in.parts = append(in.parts, part{name: path.Base(p) + "!" + pick, size: sz, open: opener})
	return nil
}

func baseNames(ps []string) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = path.Base(p)
	}
	return out
}

type capReader struct {
	r    io.Reader
	left int64
	name string
}

func (c *capReader) Read(p []byte) (int, error) {
	if c.left <= 0 {
		return 0, &SourceError{ClassTooLarge, fmt.Errorf("zip entry %s unpacks past the size limit (-max-size)", c.name)}
	}
	if int64(len(p)) > c.left {
		p = p[:c.left]
	}
	n, err := c.r.Read(p)
	c.left -= int64(n)
	return n, err
}

func hasRollingParts(names []string) bool {
	for _, n := range names {
		if _, ok := rollingIndex(n); ok {
			return true
		}
	}
	return false
}

// rollingIndex parses the n in events_<n>_<appId>[...].
func rollingIndex(name string) (int, bool) {
	if !strings.HasPrefix(name, "events_") {
		return 0, false
	}
	rest := name[len("events_"):]
	i := strings.IndexByte(rest, '_')
	if i <= 0 {
		return 0, false
	}
	n, err := strconv.Atoi(rest[:i])
	return n, err == nil
}

// addRolling orders the events_<n>_* parts. When Spark has compacted older
// parts, reading starts at the newest .compact file.
func (in *Input) addRolling(dir string, names []string, open func(string) (int64, func() (io.ReadCloser, error), error), layout string) error {
	in.Layout = layout
	type rp struct {
		n    int
		name string
	}
	var ps []rp
	compactFrom := -1
	for _, n := range names {
		if strings.HasPrefix(n, "appstatus_") && strings.HasSuffix(n, ".inprogress") {
			in.InProgress = true
		}
		idx, ok := rollingIndex(n)
		if !ok {
			continue
		}
		ps = append(ps, rp{idx, n})
		if strings.HasSuffix(n, ".compact") && idx > compactFrom {
			compactFrom = idx
		}
	}
	sort.Slice(ps, func(i, j int) bool { return ps[i].n < ps[j].n })
	if compactFrom >= 0 {
		in.Notes = append(in.Notes, fmt.Sprintf("Spark compacted the parts up to %d; reading from the compacted file. Compaction drops some finished-job events, so totals may be lower.", compactFrom))
	}
	for i, p := range ps {
		if p.n < compactFrom || (p.n == compactFrom && !strings.HasSuffix(p.name, ".compact")) {
			continue
		}
		if i > 0 && ps[i-1].n == p.n && !strings.HasSuffix(p.name, ".compact") {
			continue
		}
		sz, opener, err := open(p.name)
		if err != nil {
			return err
		}
		if sz > in.limits.MaxObjectBytes {
			return &SourceError{ClassTooLarge, fmt.Errorf("%s is over the size limit (-max-size)", p.name)}
		}
		in.parts = append(in.parts, part{name: path.Base(dir) + "/" + p.name, size: sz, open: opener})
	}
	if len(in.parts) == 0 {
		return &SourceError{ClassNotFound, fmt.Errorf("rolling event log %s has no events_* parts", dir)}
	}
	return nil
}

// stripLogName removes codec, .inprogress and eventlog_v2_ decoration.
func stripLogName(n string) string {
	n = strings.TrimPrefix(n, "eventlog_v2_")
	n = strings.TrimSuffix(n, ".inprogress")
	if c := codecFromName(n); c != CodecPlain {
		n = n[:strings.LastIndexByte(n, '.')]
	}
	return n
}

// matchesApp reports whether a file or folder name is a log of appID
// (<appId> or <appId>_<attempt>).
func matchesApp(name, appID string) bool {
	s := stripLogName(name)
	if s == appID {
		return true
	}
	if !strings.HasPrefix(s, appID+"_") {
		return false
	}
	_, err := strconv.Atoi(s[len(appID)+1:])
	return err == nil
}

// pickAttempt chooses the highest attempt, preferring finished logs.
func pickAttempt(names []string) string {
	attempt := func(n string) int {
		s := stripLogName(n)
		i := strings.LastIndexByte(s, '_')
		if i < 0 {
			return 0
		}
		a, err := strconv.Atoi(s[i+1:])
		if err != nil || strings.Count(s, "_") < 3 {
			return 0
		}
		return a
	}
	sorted := append([]string(nil), names...)
	sort.SliceStable(sorted, func(i, j int) bool {
		ai, aj := attempt(sorted[i]), attempt(sorted[j])
		if ai != aj {
			return ai < aj
		}
		return strings.HasSuffix(sorted[i], ".inprogress") && !strings.HasSuffix(sorted[j], ".inprogress")
	})
	return sorted[len(sorted)-1]
}
