// Package redact removes secrets and unsafe characters from text before it
// reaches the model, the report or the logs.
//
// Keys that look like password, secret, token, key or credential have their
// values hidden entirely. Free text (log lines, exception messages, SQL plans)
// has key=value pairs with such keys, URL passwords and AWS access key IDs
// hidden. Control characters and characters that can disguise text (bidi
// overrides, zero-width marks) are stripped.
package redact

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

// Mask replaces every hidden value.
const Mask = "[redacted]"

var (
	sensitiveKey = regexp.MustCompile(`(?i)passw(or)?d|secret|token|key|credential`)

	// key=value, key: value, "key":"value" and -Dkey=value inside free text.
	// Values starting with [ or ( are left alone: in Spark plans they are
	// column lists such as keys=[region#12], not secrets.
	pairRE = regexp.MustCompile(`(?i)([A-Za-z0-9_.\-]*(?:passw(?:or)?d|secret|token|credential|key)[A-Za-z0-9_.\-]*)("?\s*[=:]\s*"?)([^\s"',;&\[\(\]\)][^\s"',;&]*)`)

	// scheme://user:password@host
	urlPassRE = regexp.MustCompile(`([A-Za-z][A-Za-z0-9+.\-]*://[^\s/:@]+:)([^\s/@]+)(@)`)

	// AWS access key IDs.
	awsKeyRE = regexp.MustCompile(`\b(?:AKIA|ASIA|AROA|AIDA)[A-Z0-9]{16}\b`)
)

// IsSensitiveKey reports whether a setting's value must never be shown.
func IsSensitiveKey(key string) bool { return sensitiveKey.MatchString(key) }

// Value returns the value to show for a setting, and whether it was hidden.
// Values of sensitive keys are hidden entirely; others are cleaned and have
// embedded secrets hidden.
func Value(key, value string) (string, bool) {
	if IsSensitiveKey(key) && value != "" {
		return Mask, true
	}
	out := Text(value)
	return out, out != Clean(value)
}

// Text cleans s and hides any secrets embedded in it.
func Text(s string) string {
	s = Clean(s)
	if s == "" {
		return s
	}
	s = urlPassRE.ReplaceAllString(s, "${1}"+Mask+"${3}")
	s = awsKeyRE.ReplaceAllString(s, Mask)
	s = pairRE.ReplaceAllStringFunc(s, func(m string) string {
		sub := pairRE.FindStringSubmatch(m)
		if sub[3] == Mask || strings.HasPrefix(sub[3], "*") { // already hidden (by us or by Spark)
			return m
		}
		return sub[1] + sub[2] + Mask
	})
	return s
}

// Clean makes s valid UTF-8 and strips control characters (except tab and
// newline) and characters that can make text look like something else.
func Clean(s string) string {
	if isClean(s) {
		return s
	}
	s = strings.ToValidUTF8(s, "�")
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if !dropRune(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func isClean(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 0x80 || (c < 0x20 && c != '\n' && c != '\t') || c == 0x7f {
			return false
		}
	}
	return true
}

func dropRune(r rune) bool {
	switch {
	case r == '\n' || r == '\t':
		return false
	case r == '\r':
		return true
	case r < 0x20 || (r >= 0x7f && r <= 0x9f):
		return true
	case r >= 0x200b && r <= 0x200f, // zero-width and direction marks
		r >= 0x202a && r <= 0x202e, // bidi embedding and overrides
		r >= 0x2060 && r <= 0x2069, // word joiner, invisible operators, bidi isolates
		r == 0x061c, r == 0xfeff, r == 0x00ad:
		return true
	case r == utf8.RuneError:
		return false
	}
	return false
}

// stringLit matches a quoted string literal in code: "…" or '…', with
// backslash escapes.
var stringLit = regexp.MustCompile(`"(?:[^"\\\n]|\\.)*"|'(?:[^'\\\n]|\\.)*'`)

// Code redacts one line of source code. On top of Text, a line that names
// a sensitive key has every string literal hidden except the first one that
// is itself a key name, so .config("spark.db.password", "hunter2") keeps the
// key and loses the value, even when the value also contains a word like
// PASSWORD. It errs towards hiding: a line mentioning "key" loses its other
// literals too.
func Code(line string) string {
	line = Text(line)
	if !sensitiveKey.MatchString(line) {
		return line
	}
	keptKey := false
	return stringLit.ReplaceAllStringFunc(line, func(lit string) string {
		body := lit[1 : len(lit)-1]
		if body == "" || body == Mask {
			return lit
		}
		if !keptKey && sensitiveKey.MatchString(body) && !strings.ContainsAny(body, " \t") && len(body) < 100 {
			keptKey = true
			return lit
		}
		return lit[:1] + Mask + lit[len(lit)-1:]
	})
}

// Args hides values in command-line arguments, such as spark-submit's:
// key=value pairs by key (so --conf spark.db.password=… loses its value),
// --option=value by option name, the value after a sensitive option given
// on its own (--db-password value, --token value), and anything else that
// looks like a secret. Like the rest of this package it errs towards
// hiding: an option such as --keytab also loses the path after it.
func Args(args []string) []string {
	out := make([]string, len(args))
	hideNext := false
	for i, a := range args {
		switch {
		case hideNext && a != "" && !strings.HasPrefix(a, "--"):
			out[i] = Mask
			hideNext = false
			continue
		case hideNext:
			hideNext = false
		}
		if k, v, ok := strings.Cut(a, "="); ok && !strings.HasPrefix(a, "-") {
			rv, _ := Value(k, v)
			out[i] = k + "=" + rv
			continue
		}
		out[i] = Text(a)
		hideNext = sensitiveOption(a)
	}
	return out
}

// sensitiveOption reports whether a is an option such as --db-password or
// -token whose value follows as the next argument.
func sensitiveOption(a string) bool {
	if !strings.HasPrefix(a, "-") || strings.Contains(a, "=") {
		return false
	}
	name := strings.TrimLeft(a, "-")
	return name != "" && sensitiveKey.MatchString(name)
}

// Command redacts a command line logged as one string. It splits the line
// as a shell would for quotes and backslashes (without expanding anything),
// redacts the words as Args does, and joins them again, re-quoting words
// that hold spaces. Runs of spaces collapse to one.
func Command(cmd string) string {
	words, quoted := splitCommand(cmd)
	red := Args(words)
	for i, w := range red {
		red[i] = quoteWord(w, quoted[i])
	}
	return strings.Join(red, " ")
}

// quoteWord writes w back so splitCommand reads it as one word again: as it
// was when it needs no quoting, in its original single quotes when it holds
// none, and otherwise in double quotes with " and \ escaped.
func quoteWord(w string, orig rune) string {
	plain := w != "" && !strings.ContainsAny(w, " \t\n'\"\\")
	switch {
	case orig == 0 && plain:
		return w
	case orig == '\'' && !strings.Contains(w, "'"):
		return "'" + w + "'"
	}
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`)
	return `"` + r.Replace(w) + `"`
}

// splitCommand splits a command line into words, honouring single quotes,
// double quotes and backslash escapes. quoted records the quote character a
// word was written with (0 when unquoted), so Command can keep it.
func splitCommand(cmd string) (words []string, quoted []rune) {
	var (
		b      strings.Builder
		in     rune // open quote
		first  rune // quote the current word started with
		inWord bool
		esc    bool
	)
	flush := func() {
		if inWord {
			words = append(words, b.String())
			quoted = append(quoted, first)
		}
		b.Reset()
		inWord, first = false, 0
	}
	for _, r := range cmd {
		switch {
		case esc:
			b.WriteRune(r)
			esc, inWord = false, true
		case r == '\\' && in != '\'':
			esc, inWord = true, true
		case in != 0 && r == in:
			in = 0
		case in != 0:
			b.WriteRune(r)
		case r == '\'' || r == '"':
			in, inWord = r, true
			if b.Len() == 0 {
				first = r
			}
		case r == ' ' || r == '\t' || r == '\n':
			flush()
		default:
			b.WriteRune(r)
			inWord = true
		}
	}
	flush()
	return words, quoted
}
