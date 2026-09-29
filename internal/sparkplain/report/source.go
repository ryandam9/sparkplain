package report

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/redact"
)

// SourceFile is one of the application's source files, embedded in the
// explorer beside the jobs, stages and queries that ran its lines.
type SourceFile struct {
	Path   string   // local path it was read from
	Logged []string // the file names the event log uses for it
	Lines  []string // redacted
	Cut    bool     // longer than maxSourceLines
}

// Limits on embedded source (HISTORY.md, phase 1c step 7).
const (
	maxSourceLines  = 5000
	maxSourceBytes  = 4 << 20
	maxSourceLine   = 1 << 20 // longest source line embedded; a longer one cuts the file there
	maxSourceWalked = 20000
)

var sourceExts = map[string]bool{".py": true, ".scala": true, ".java": true, ".kt": true, ".sql": true, ".r": true, ".R": true}

// codeFiles lists every file name the report's code locations use.
func codeFiles(r *model.Report) []string {
	seen := map[string]bool{}
	add := func(cs []model.CodeLocation) {
		for _, c := range cs {
			seen[c.File] = true
		}
	}
	for _, j := range r.Jobs.Jobs {
		add(j.Code)
	}
	for _, s := range r.Jobs.Stages {
		add(s.Code)
	}
	for _, q := range r.Jobs.SQL {
		add(q.Code)
	}
	if r.Logs != nil {
		// Tracebacks name the script even when there is no event log.
		for _, f := range r.Logs.Files {
			for _, l := range f.Found {
				if p := l.Fields["pyFile"]; p != "" {
					seen[p] = true
				}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for f := range seen {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

// LoadSources finds the files the event log names among the given files
// and folders (the -source flag), and reads the ones it matches, redacted.
// A logged name matches the local file sharing the longest tail of path
// components with it, so /mnt/yarn/.../etl.py matches jobs/etl.py and a JVM
// frame's bare ClaimsJob.java matches src/main/java/ClaimsJob.java. Notes
// say what could not be matched or was cut.
func LoadSources(r *model.Report, roots []string) ([]SourceFile, []string, error) {
	return LoadSourcesFrom(r, roots, nil)
}

// FetchedSource is a source file read from elsewhere, such as the
// application's script on S3, named by where it came from.
type FetchedSource struct {
	Path string // s3://bucket/key
	Data []byte
}

// LoadSourcesFrom is LoadSources with files already fetched (the step's
// script on S3) matched alongside the local ones.
func LoadSourcesFrom(r *model.Report, roots []string, fetched []FetchedSource) ([]SourceFile, []string, error) {
	var local []string
	var notes []string
	data := map[string][]byte{}
	for _, f := range fetched {
		local = append(local, f.Path)
		data[f.Path] = f.Data
	}
	for _, root := range roots {
		info, err := os.Stat(root)
		if errors.Is(err, fs.ErrPermission) {
			notes = append(notes, fmt.Sprintf("No permission to read %s, so its code is not shown.", root))
			continue
		}
		if err != nil {
			return nil, nil, fmt.Errorf("-source %s: %w", root, err)
		}
		if !info.IsDir() {
			local = append(local, root)
			continue
		}
		err = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			name := d.Name()
			if d.IsDir() {
				if p != root && (strings.HasPrefix(name, ".") || name == "node_modules" || name == "vendor" || name == "target" || name == "__pycache__" || name == "venv") {
					return filepath.SkipDir
				}
				return nil
			}
			if sourceExts[filepath.Ext(name)] {
				local = append(local, p)
				if len(local) >= maxSourceWalked {
					return filepath.SkipAll
				}
			}
			return nil
		})
		if err != nil {
			return nil, nil, err
		}
	}
	byLocal := map[string]*SourceFile{}
	var order []string
	for _, logged := range codeFiles(r) {
		best, score, tie := "", 0, false
		for _, l := range local {
			s := tailMatch(logged, l)
			switch {
			case s > score:
				best, score, tie = l, s, false
			case s == score && s > 0:
				tie = true
			}
		}
		if score == 0 {
			notes = append(notes, fmt.Sprintf("No source file given for %s.", logged))
			continue
		}
		if tie {
			notes = append(notes, fmt.Sprintf("Several files could be %s; showing %s.", logged, best))
		}
		sf := byLocal[best]
		if sf == nil {
			sf = &SourceFile{Path: best}
			byLocal[best] = sf
			order = append(order, best)
		}
		sf.Logged = append(sf.Logged, logged)
	}
	var out []SourceFile
	total := 0
	for _, p := range order {
		sf := byLocal[p]
		var f io.ReadCloser
		var err error
		if b, ok := data[p]; ok {
			f = io.NopCloser(bytes.NewReader(b))
		} else if f, err = os.Open(p); err != nil {
			notes = append(notes, fmt.Sprintf("Could not read %s: %v.", p, err))
			continue
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 64<<10), maxSourceLine)
		for sc.Scan() {
			if len(sf.Lines) == maxSourceLines || total+len(sc.Text()) > maxSourceBytes {
				sf.Cut = true
				break
			}
			line := redact.Code(sc.Text())
			total += len(line)
			sf.Lines = append(sf.Lines, line)
		}
		f.Close()
		// A scanner that stops on an error ends its loop just as it does at
		// the end of the file: without this check a file cut by an
		// over-long line or a read error would look complete.
		switch err := sc.Err(); {
		case errors.Is(err, bufio.ErrTooLong):
			sf.Cut = true
			notes = append(notes, fmt.Sprintf("%s is shown only up to line %d: line %d is longer than %d bytes.", p, len(sf.Lines), len(sf.Lines)+1, maxSourceLine))
		case err != nil:
			sf.Cut = true
			notes = append(notes, fmt.Sprintf("%s is shown only up to line %d: reading it failed (%v).", p, len(sf.Lines), err))
		case sf.Cut:
			notes = append(notes, fmt.Sprintf("%s is shown only up to line %d.", p, len(sf.Lines)))
		}
		out = append(out, *sf)
	}
	return out, notes, nil
}

// tailMatch counts the path components two paths share from the end; 0 when
// even the file names differ.
func tailMatch(a, b string) int {
	pa := strings.FieldsFunc(a, func(r rune) bool { return r == '/' || r == '\\' })
	pb := strings.FieldsFunc(b, func(r rune) bool { return r == '/' || r == '\\' })
	n := 0
	for i, j := len(pa)-1, len(pb)-1; i >= 0 && j >= 0 && pa[i] == pb[j]; i, j = i-1, j-1 {
		n++
	}
	return n
}
