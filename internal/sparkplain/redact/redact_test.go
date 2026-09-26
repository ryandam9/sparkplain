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
