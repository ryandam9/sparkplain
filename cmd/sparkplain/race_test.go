package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// Only the packages in the Makefile's RACE_PKG run under the race detector,
// which is ten times slower than the plain tests. That is safe only while
// they are the only packages that start goroutines, so a go statement
// anywhere else fails here until its package is added.
func TestRaceDetectorCoversGoroutines(t *testing.T) {
	root := filepath.Join("..", "..")
	mk, err := os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^RACE_PKG\s*:=\s*(.+)$`).FindSubmatch(mk)
	if m == nil {
		t.Fatal("Makefile has no RACE_PKG")
	}
	race := strings.Fields(string(m[1]))
	var starts []string
	for _, dir := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			ast.Inspect(f, func(n ast.Node) bool {
				if _, ok := n.(*ast.GoStmt); ok {
					rel, _ := filepath.Rel(root, filepath.Dir(path))
					starts = append(starts, "./"+filepath.ToSlash(rel))
					return false
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(starts) == 0 {
		t.Fatal("found no go statement at all; the scan is broken")
	}
	for _, p := range slices.Compact(slices.Sorted(slices.Values(starts))) {
		if !slices.Contains(race, p) {
			t.Errorf("%s starts goroutines but is not in the Makefile's RACE_PKG, so the race detector never checks it", p)
		}
	}
}
