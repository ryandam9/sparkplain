package report

import (
	"bufio"
	"fmt"
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

// Limits on embedded source (SPEC §8, phase 1c step 7).
const (
	maxSourceLines  = 5000
	maxSourceBytes  = 4 << 20
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
	var local []string
	for _, root := range roots {
		info, err := os.Stat(root)
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
	var notes []string
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
		f, err := os.Open(p)
		if err != nil {
			notes = append(notes, fmt.Sprintf("Could not read %s: %v.", p, err))
			continue
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 64<<10), 1<<20)
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
		if sf.Cut {
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
