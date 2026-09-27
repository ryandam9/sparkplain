package report

import (
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// The Code tab's syntax highlighter (explorer.js) run under Node, which is
// on developer machines and CI runners; skipped where it is not.
func TestSyntaxHighlighter(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not installed")
	}
	js, err := os.ReadFile("assets/explorer.js")
	if err != nil {
		t.Fatal(err)
	}
	src := string(js)
	start := strings.Index(src, "// ---------- syntax highlighting ----------")
	end := strings.Index(src, "// ---------- end syntax highlighting ----------")
	if start < 0 || end < start {
		t.Fatal("highlighter block not found in explorer.js")
	}
	files := map[string][]string{
		"job.py": {
			`@udf("string")`,
			`def clean(s):  # trim it`,
			`    """Strip it, # not a comment`,
			`    across lines"""`,
			`    return s.strip() + 'x' + f"{n}" if s else None`,
			`classify = 0x1F + 3.5e2 + x2`,
			`print("<script>alert(1)</script>")`,
			`spark.conf.set("k", str(len(x)))`,
		},
		"Job.scala": {
			`/* spans`,
			`   lines */ val n = 1 // done`,
			`object Job { def main(args: Array[String]) = println("a\"b") }`,
		},
		"notes.txt": {`def not_code(): pass`},
	}
	in, _ := json.Marshal(files)
	script := src[start:end] + `
var files = ` + string(in) + `, out = {};
Object.keys(files).forEach(function (f) { var l = HL.langOf(f); out[f] = l ? HL.tokens(files[f], l) : null; });
process.stdout.write(JSON.stringify(out));`
	b, err := exec.Command(node, "-e", script).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, b)
	}
	var got map[string][][][2]string
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("%v: %s", err, b)
	}
	if got["notes.txt"] != nil {
		t.Error("an unknown file type should stay plain")
	}
	// kinds lists "class:text" for the highlighted tokens of one line.
	kinds := func(file string, line int) string {
		var s []string
		for _, tk := range got[file][line] {
			if tk[0] != "" {
				s = append(s, tk[0]+":"+tk[1])
			}
		}
		return strings.Join(s, " ")
	}
	for _, c := range []struct {
		file string
		line int
		want string
	}{
		{"job.py", 0, `d:@udf s:"string"`},
		{"job.py", 1, `k:def f:clean c:# trim it`},
		{"job.py", 2, `s:"""Strip it, # not a comment`},
		{"job.py", 3, `s:    across lines"""`},
		{"job.py", 4, `k:return s:'x' s:f"{n}" k:if k:else k:None`},
		{"job.py", 5, `n:0x1F n:3.5e2`},
		{"job.py", 6, `b:print s:"<script>alert(1)</script>"`},
		{"job.py", 7, `s:"k" b:str b:len`},
		{"Job.scala", 0, `c:/* spans`},
		{"Job.scala", 1, `c:   lines */ k:val n:1 c:// done`},
		{"Job.scala", 2, `k:object f:Job k:def f:main b:Array b:String b:println s:"a\"b"`},
	} {
		if k := kinds(c.file, c.line); k != c.want {
			t.Errorf("%s line %d: %s, want %s", c.file, c.line+1, k, c.want)
		}
	}
	// Highlighting only splits lines: every character is kept, in order.
	for f, lines := range files {
		for i, toks := range got[f] {
			var b strings.Builder
			for _, tk := range toks {
				b.WriteString(tk[1])
			}
			if b.String() != lines[i] {
				t.Errorf("%s line %d rebuilt as %q, want %q", f, i+1, b.String(), lines[i])
			}
		}
	}
}
