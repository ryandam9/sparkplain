package report

import (
	"encoding/json"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"testing"
)

// The pattern props.js marks configuration keys with, run under Node,
// which is on developer machines and CI runners; skipped where it is not.
func TestPropKeys(t *testing.T) {
	t.Parallel()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not installed")
	}
	js, err := os.ReadFile("assets/props.js")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^\s*var KEY = (/.*/g);$`).FindSubmatch(js)
	if m == nil {
		t.Fatal("KEY pattern not found in props.js")
	}
	cases := map[string][]string{
		"Increase spark.executor.memoryOverhead.":                               {"spark.executor.memoryOverhead"},
		"Set spark.sql.adaptive.enabled=true and spark.memory.fraction to 0.6.": {"spark.sql.adaptive.enabled=true", "spark.memory.fraction"},
		"fails 3 times (spark.task.maxFailures). Set yarn.maxAppAttempts=1.":    {"spark.task.maxFailures", "yarn.maxAppAttempts=1"},
		"keys: spark.hadoop.fs.s3a.access.key, fs.s3a.secret.key":               {"spark.hadoop.fs.s3a.access.key", "fs.s3a.secret.key"},
		"org.apache.spark.api.python.PythonException: bad row":                  nil,
		"read s3://bucket/spark.events/part-0 and /var/log/hadoop.yarn.log":     nil,
		"spark ran on hadoop; the spark. at the end is not a key":               nil,
		"hbase.client.scanner.timeout.period=120000 (ms)":                       {"hbase.client.scanner.timeout.period=120000"},
	}
	in, _ := json.Marshal(cases)
	script := `var KEY = ` + string(m[1]) + `, cases = ` + string(in) + `, out = {};
Object.keys(cases).forEach(function (s) { var r = [], x; KEY.lastIndex = 0; while ((x = KEY.exec(s))) r.push(x[2]); out[s] = r; });
process.stdout.write(JSON.stringify(out));`
	b, err := exec.Command(node, "-e", script).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, b)
	}
	var got map[string][]string
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("%v: %s", err, b)
	}
	for s, want := range cases {
		if len(want) == 0 && len(got[s]) == 0 {
			continue
		}
		if !reflect.DeepEqual(got[s], want) {
			t.Errorf("%q: got %q, want %q", s, got[s], want)
		}
	}
}
