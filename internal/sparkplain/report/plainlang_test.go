package report

import (
	"bufio"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// The pages' text follows ASD-STE100 Simplified Technical English as
// docs/STYLE.md applies it. TestPlainLanguage checks the rules a program
// can check, on every string that can reach the report or the explorer:
// the analysis, model and report packages' Go strings, the report
// template's text and explorer.js's strings. Text that broke a rule
// before the check came in is listed in testdata/ste-baseline.txt, which
// only shrinks: a new break fails, and so does a listed one that is gone.
// After rewriting text, refresh the list with
//
//	go test ./internal/sparkplain/report -run TestPlainLanguage -ste-update
//
// A string that is not prose (a log pattern, a code fragment) can be
// skipped with a "ste:ignore" comment on its line.
var steUpdate = flag.Bool("ste-update", false, "rewrite testdata/ste-baseline.txt from the current text")

const (
	steMaxWords     = 25 // a descriptive sentence
	steMaxWordsStep = 20 // an instruction: a finding's "Try"
	steBaseline     = "testdata/ste-baseline.txt"
)

// steWords are words STE does not approve, with what to write instead.
// Only words whose status is certain are here; docs/STYLE.md lists the
// ones to confirm against the dictionary.
var steWords = map[string]string{
	"accomplish": "do", "additional": "more", "adjacent": "next to",
	"allow": "let", "allows": "lets", "allowed": "let", "allowing": "letting",
	"approximately": "about", "assist": "help", "commence": "start",
	"consequently": "as a result", "demonstrate": "show", "eliminate": "remove", "employ": "use",
	"enable": "let, or set to true", "enables": "lets", "enabling": "setting to true",
	"ensure": "make sure", "ensures": "makes sure",
	"exceed": "be more than", "exceeds": "is more than", "exceeded": "was more than", "exceeding": "more than",
	"facilitate": "help", "happen": "occur", "happens": "occurs", "happened": "occurred",
	"however": "but", "indicate": "show", "indicates": "shows", "indicated": "showed",
	"initiate": "start", "insufficient": "not sufficient", "numerous": "many",
	"obtain": "get", "perform": "do", "performs": "does", "performed": "did",
	"prior": "before", "provide": "give", "provides": "gives", "provided": "gave",
	"require": "is necessary", "requires": "is necessary", "required": "necessary",
	"utilize": "use", "utilise": "use", "whether": "if",
	"may": "can", "might": "can", "via": "through", "upon": "on",
	"lot": "many, or much", "lots": "many, or much", "e.g.": "for example", "i.e.": "that is",
}

var (
	steContraction = regexp.MustCompile(`(?i)\b(\w+n't|it's|that's|there's|what's|here's|let's|you're|we're|they're|i'm|you've|we've|they've|you'll|we'll|it'll|you'd|we'd)\b`)
	steSentenceEnd = regexp.MustCompile(`([.!?])\s+`)
	steJSString    = regexp.MustCompile(`"((?:[^"\\\n]|\\.)*)"`)
	steTmplAction  = regexp.MustCompile(`\{\{.*?\}\}`)
	steTag         = regexp.MustCompile(`<[^>]*>`)
	steMarkup      = regexp.MustCompile(`<[a-zA-Z/!]`)
)

// steText is one string that can reach the pages, with where it is.
type steText struct {
	file, text string
	step       bool // an instruction
}

func TestPlainLanguage(t *testing.T) {
	var texts []steText
	for _, dir := range []string{"../analyze", "../model", "."} {
		texts = append(texts, goTexts(t, dir)...)
	}
	texts = append(texts, tmplTexts(t, "templates/report.html.tmpl")...)
	texts = append(texts, jsTexts(t, "assets/explorer.js")...)
	t.Logf("checked %d strings", len(texts))
	if len(texts) < 500 {
		t.Fatalf("found only %d strings to check", len(texts))
	}
	var got []string
	for _, x := range texts {
		got = append(got, steBreaks(x)...)
	}
	slices.Sort(got)
	got = slices.Compact(got)
	if *steUpdate {
		body := "# Text that broke an ASD-STE100 rule before TestPlainLanguage came in.\n# Rewrite it, then run with -ste-update; this list only shrinks.\n" + strings.Join(got, "\n") + "\n"
		if err := os.WriteFile(steBaseline, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %d entries to %s", len(got), steBaseline)
		return
	}
	known := map[string]bool{}
	f, err := os.Open(steBaseline)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		if l := sc.Text(); l != "" && !strings.HasPrefix(l, "#") {
			known[l] = true
		}
	}
	for _, b := range got {
		if !known[b] {
			t.Errorf("new text breaks an ASD-STE100 rule (docs/STYLE.md):\n  %s", strings.ReplaceAll(b, "\t", " | "))
		}
		delete(known, b)
	}
	if len(known) > 0 {
		t.Errorf("%d entries in %s are gone from the text; run with -ste-update to drop them", len(known), steBaseline)
	}
}

// steBreaks lists the rules x breaks, one line each: file, rule, text.
func steBreaks(x steText) []string {
	var out []string
	add := func(rule string) { out = append(out, x.file+"\t"+rule+"\t"+x.text) }
	limit, kind := steMaxWords, "sentence"
	if x.step {
		limit, kind = steMaxWordsStep, "instruction"
	}
	for _, s := range sentences(x.text) {
		if n := len(words(s)); n > limit {
			add(fmt.Sprintf("%s of %d words (at most %d)", kind, n, limit))
			break
		}
	}
	if m := steContraction.FindString(x.text); m != "" {
		add("contraction " + strconv.Quote(m))
	}
	var bad []string
	for _, w := range words(x.text) {
		lw := strings.ToLower(strings.Trim(w, `"'()[],;:!?…`))
		lw = strings.TrimSuffix(lw, ".")
		if lw == "e.g" || lw == "i.e" {
			lw += "."
		}
		if use, ok := steWords[lw]; ok && !strings.ContainsAny(w, "_/=#-") {
			bad = append(bad, fmt.Sprintf("%q (use %s)", lw, use))
		}
	}
	if len(bad) > 0 {
		slices.Sort(bad)
		add("not approved: " + strings.Join(slices.Compact(bad), ", "))
	}
	return out
}

// sentences splits text at sentence ends and line breaks.
func sentences(text string) []string {
	var out []string
	for _, line := range strings.Split(text, "\n") {
		for _, s := range strings.Split(steSentenceEnd.ReplaceAllString(line, "$1\x00"), "\x00") {
			if s = strings.TrimSpace(s); s != "" {
				out = append(out, s)
			}
		}
	}
	return out
}

// words are the tokens with a letter or digit in them; a format verb
// such as %s counts as one.
func words(s string) []string {
	var out []string
	for _, w := range strings.Fields(s) {
		if strings.IndexFunc(w, func(r rune) bool {
			return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
		}) >= 0 {
			out = append(out, w)
		}
	}
	return out
}

// prose says s reads as text for people: four words or more, and not
// markup, a pattern or a code fragment.
func prose(s string) bool {
	if len(words(s)) < 4 || steMarkup.MatchString(s) {
		return false
	}
	letters := 0
	for _, r := range s {
		if r >= 'a' && r <= 'z' {
			letters++
		}
	}
	return letters*2 > len(s) // mostly lower-case letters, as sentences are
}

// steSkipCalls are calls whose string arguments match or build things
// rather than say something: log patterns, keys, formats for machines.
var steSkipCalls = []string{"regexp.", "strings.", "bytes.", "filepath.", "path.", "errors.New", "fmt.Errorf", "strconv.", "time.", "json."}

// goTexts are the string literals in a package's non-test Go files that
// read as prose; a Fix field's are instructions.
func goTexts(t *testing.T, dir string) []steText {
	t.Helper()
	fset := token.NewFileSet()
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	var out []steText
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		ignored := map[int]bool{}
		for _, cg := range f.Comments {
			for _, c := range cg.List {
				if strings.Contains(c.Text, "ste:ignore") {
					ignored[fset.Position(c.Pos()).Line] = true
				}
			}
		}
		skip := map[*ast.BasicLit]bool{}
		step := map[*ast.BasicLit]bool{}
		mark := func(n ast.Node, m map[*ast.BasicLit]bool) {
			ast.Inspect(n, func(n ast.Node) bool {
				if l, ok := n.(*ast.BasicLit); ok {
					m[l] = true
				}
				return true
			})
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.ImportSpec, *ast.Field:
				mark(n, skip)
			case *ast.CaseClause:
				for _, e := range n.List {
					mark(e, skip)
				}
			case *ast.CallExpr:
				if sel, ok := n.Fun.(*ast.SelectorExpr); ok {
					if x, ok := sel.X.(*ast.Ident); ok {
						call := x.Name + "." + sel.Sel.Name
						for _, p := range steSkipCalls {
							if strings.HasPrefix(call, p) && call != "strings.Join" {
								mark(n, skip)
							}
						}
					}
				}
			case *ast.KeyValueExpr:
				if k, ok := n.Key.(*ast.Ident); ok && k.Name == "Fix" {
					mark(n.Value, step)
				}
				if _, ok := n.Key.(*ast.BasicLit); ok {
					mark(n.Key, skip) // a map key
				}
			case *ast.AssignStmt:
				for _, l := range n.Lhs {
					if sel, ok := l.(*ast.SelectorExpr); ok && sel.Sel.Name == "Fix" {
						for _, r := range n.Rhs {
							mark(r, step)
						}
					}
					if id, ok := l.(*ast.Ident); ok && id.Name == "fix" {
						for _, r := range n.Rhs {
							mark(r, step)
						}
					}
				}
			}
			return true
		})
		ast.Inspect(f, func(n ast.Node) bool {
			l, ok := n.(*ast.BasicLit)
			if !ok || l.Kind != token.STRING || skip[l] || ignored[fset.Position(l.Pos()).Line] {
				return true
			}
			s, err := strconv.Unquote(l.Value)
			if err != nil || !prose(s) {
				return true
			}
			pkg := filepath.Base(dir)
			if dir == "." {
				pkg = "report"
			}
			out = append(out, steText{file: pkg + "/" + filepath.Base(name), text: s, step: step[l]})
			return true
		})
	}
	return out
}

// tmplTexts are the template's text runs between tags and actions.
func tmplTexts(t *testing.T, name string) []steText {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var out []steText
	for _, line := range strings.Split(string(b), "\n") {
		if strings.Contains(line, "ste:ignore") {
			continue
		}
		for _, run := range strings.Split(steTag.ReplaceAllString(steTmplAction.ReplaceAllString(line, "\x00"), "\x00"), "\x00") {
			if run = strings.TrimSpace(run); prose(run) {
				out = append(out, steText{file: "report/" + filepath.ToSlash(name), text: run})
			}
		}
	}
	return out
}

// jsTexts are explorer.js's double-quoted strings that read as prose.
func jsTexts(t *testing.T, name string) []steText {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var out []steText
	for _, line := range strings.Split(string(b), "\n") {
		if strings.Contains(line, "ste:ignore") || strings.HasPrefix(strings.TrimSpace(line), "//") {
			continue
		}
		for _, ix := range steJSString.FindAllStringSubmatchIndex(line, -1) {
			if strings.HasSuffix(line[:ix[0]], "words(") {
				continue // a highlighter's keyword list
			}
			raw := line[ix[2]:ix[3]]
			s, err := strconv.Unquote(`"` + raw + `"`)
			if err != nil {
				s = raw
			}
			if prose(s) {
				out = append(out, steText{file: "report/" + filepath.ToSlash(name), text: s})
			}
		}
	}
	return out
}
