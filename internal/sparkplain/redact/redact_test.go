package redact

import (
	"strings"
	"testing"
)

func TestValueHidesSensitiveKeys(t *testing.T) {
	for _, k := range []string{
		"spark.hadoop.fs.s3a.secret.key", "spark.hadoop.fs.s3a.access.key", "spark.myapp.db.password",
		"spark.executorEnv.AWS_SECRET_ACCESS_KEY", "spark.authenticate.secret", "hbase.client.token", "some.credentials.file",
		"javax.net.ssl.keyStorePassword", "db.passwd",
	} {
		got, hidden := Value(k, "FAKE-VALUE")
		if got != Mask || !hidden {
			t.Errorf("Value(%q) = %q, %v; want hidden", k, got, hidden)
		}
	}
	got, hidden := Value("spark.executor.memory", "4g")
	if got != "4g" || hidden {
		t.Errorf("plain key changed: %q %v", got, hidden)
	}
}

func TestTextHidesEmbeddedSecrets(t *testing.T) {
	cases := map[string]string{
		"bad row, password=FAKE-PW-0006":                                  "bad row, password=[redacted]",
		"-Dapi.token=FAKE-TOKEN-0003 -XX:+UseG1GC":                        "-Dapi.token=[redacted] -XX:+UseG1GC",
		"jdbc:postgresql://svc:FAKE-URL-PASSWORD@db.example.internal/":    "jdbc:postgresql://svc:[redacted]@db.example.internal/",
		"key id AKIAIOSFODNN7EXAMPLE used":                                "key id [redacted] used",
		`{"secret": "FAKE-S1"}`:                                           `{"secret": "[redacted]"}`,
		"AWS_SECRET_ACCESS_KEY=FAKE-AWS":                                  "AWS_SECRET_ACCESS_KEY=[redacted]",
		"spark.hadoop.fs.s3a.secret.key: FAKE-S3":                         "spark.hadoop.fs.s3a.secret.key: [redacted]",
		"already *********(redacted) by spark, token=*********(redacted)": "already *********(redacted) by spark, token=*********(redacted)",
	}
	for in, want := range cases {
		if got := Text(in); got != want {
			t.Errorf("Text(%q)\n got %q\nwant %q", in, got, want)
		}
	}
}

func TestTextKeepsSparkPlans(t *testing.T) {
	plans := []string{
		"HashAggregate(keys=[region#12], functions=[sum(amount#3)])",
		"SortMergeJoin [provider_id#2L], [provider_id#40L], Inner",
		"Exchange hashpartitioning(key#1L, 16), ENSURE_REQUIREMENTS, [plan_id=12]",
		"count at NativeMethodAccessorImpl.java:0",
	}
	for _, p := range plans {
		if got := Text(p); got != p {
			t.Errorf("plan text changed:\n got %q\nwant %q", got, p)
		}
	}
}

func TestCleanStripsDeceptiveCharacters(t *testing.T) {
	in := "ok‮evil​\x00\x1b[31mred\r\n\ttab\xff"
	got := Clean(in)
	for _, bad := range []string{"‮", "​", "\x00", "\x1b", "\r"} {
		if strings.Contains(got, bad) {
			t.Errorf("Clean kept %q in %q", bad, got)
		}
	}
	if !strings.Contains(got, "\n\ttab") || !strings.HasSuffix(got, "�") {
		t.Errorf("Clean dropped allowed text: %q", got)
	}
}

func FuzzText(f *testing.F) {
	f.Add("password=abc")
	f.Add("s3://u:p@bucket/x")
	f.Add("‮\xff")
	f.Fuzz(func(t *testing.T, s string) {
		out := Text(s)
		if Text(out) != out {
			t.Fatalf("Text not idempotent for %q: %q -> %q", s, out, Text(out))
		}
		if strings.ContainsRune(out, '‮') || strings.ContainsRune(out, 0) {
			t.Fatalf("unsafe rune left in %q", out)
		}
	})
}

func TestCodeRedaction(t *testing.T) {
	for in, want := range map[string]string{
		`     .config("spark.myapp.db.password", "FAKE-DB-PASSWORD-0002")`:       `     .config("spark.myapp.db.password", "[redacted]")`,
		`spark.conf.set('spark.myapp.session.token', 'FAKE-SESSION-TOKEN-0009')`: `spark.conf.set('spark.myapp.session.token', '[redacted]')`,
		`password = "hunter2"`:           `password = "[redacted]"`,
		`df.groupBy("provider").count()`: `df.groupBy("provider").count()`,
		`url = "jdbc:postgresql://svc:FAKE-URL-PASSWORD-0008@db.example.internal:5432/claims"`: `url = "jdbc:postgresql://svc:[redacted]@db.example.internal:5432/claims"`,
		`# the secret sauce is "caching"`:                                     `# the secret sauce is "[redacted]"`,
		`raise ValueError("bad row 13 while loading, password=FAKE-PW-0006")`: `raise ValueError("[redacted]")`,
	} {
		if got := Code(in); got != want {
			t.Errorf("Code(%q)\n got %q\nwant %q", in, got, want)
		}
	}
}

// SP-004: an option's value in the next argument must be hidden when the
// option name is sensitive, and kept when it is not.
func TestArgsHidesSplitSecretValues(t *testing.T) {
	cases := []struct{ in, want []string }{
		{[]string{"--password", "FAKE-PW-1"}, []string{"--password", Mask}},
		{[]string{"--db-password", "FAKE-PW-2", "--name", "nightly"}, []string{"--db-password", Mask, "--name", "nightly"}},
		{[]string{"--token", "FAKE-TOK-3"}, []string{"--token", Mask}},
		{[]string{"--secret-key", "FAKE-SK-4"}, []string{"--secret-key", Mask}},
		{[]string{"-token", "FAKE-TOK-5"}, []string{"-token", Mask}},
		{[]string{"--password=FAKE-PW-6"}, []string{"--password=" + Mask}},
		{[]string{"--conf", "spark.db.password=FAKE-PW-7", "--class", "com.example.Main"}, []string{"--conf", "spark.db.password=" + Mask, "--class", "com.example.Main"}},
		{[]string{"--token", "--verbose"}, []string{"--token", "--verbose"}}, // a flag with no value
		{[]string{"spark-submit", "--deploy-mode", "cluster", "s3://code/etl.py", "2026-09-26"}, []string{"spark-submit", "--deploy-mode", "cluster", "s3://code/etl.py", "2026-09-26"}},
	}
	for _, c := range cases {
		if got := Args(c.in); strings.Join(got, "|") != strings.Join(c.want, "|") {
			t.Errorf("Args(%q)\n got %q\nwant %q", c.in, got, c.want)
		}
	}
}

func TestCommandIsQuoteAware(t *testing.T) {
	cases := map[string]string{
		`spark-submit --password FAKE-PW-1 --name nightly s3://b/job.py`:                  `spark-submit --password [redacted] --name nightly s3://b/job.py`,
		`spark-submit --password 'FAKE secret with spaces' --name "daily load" job.py`:    `spark-submit --password '[redacted]' --name "daily load" job.py`,
		`spark-submit --driver-java-options "-Dapi.token=FAKE-TOK-2 -Xmx2g" job.py`:       `spark-submit --driver-java-options "-Dapi.token=[redacted] -Xmx2g" job.py`,
		`spark-submit --conf spark.hadoop.fs.s3a.secret.key=FAKE-SK-3 --class com.x.Main`: `spark-submit --conf spark.hadoop.fs.s3a.secret.key=[redacted] --class com.x.Main`,
		`run.sh --token=FAKE-TOK-4   --verbose`:                                           `run.sh --token=[redacted] --verbose`,
	}
	for in, want := range cases {
		if got := Command(in); got != want {
			t.Errorf("Command(%q)\n got %q\nwant %q", in, got, want)
		}
		if strings.Contains(Command(in), "FAKE") {
			t.Errorf("secret left in %q", Command(in))
		}
	}
}

func FuzzCommand(f *testing.F) {
	f.Add(`spark-submit --password 'a b' --name "x y" job.py`)
	f.Add(`a\ b "c\"d" 'e'`)
	f.Add(`--token`)
	f.Fuzz(func(t *testing.T, s string) {
		out := Command(s)
		if Command(out) != out {
			t.Fatalf("Command not idempotent for %q: %q -> %q", s, out, Command(out))
		}
		words, _ := splitCommand(s)
		for i, w := range words {
			if sensitiveOption(w) && i+1 < len(words) && words[i+1] != "" && !strings.HasPrefix(words[i+1], "--") && strings.Contains(out, " "+words[i+1]+" ") && words[i+1] != Mask && len(words[i+1]) > 3 {
				if !strings.Contains(strings.Join(Args(words), " "), Mask) {
					t.Fatalf("value after %q kept in %q", w, out)
				}
			}
		}
	})
}
