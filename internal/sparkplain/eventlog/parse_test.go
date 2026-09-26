package eventlog

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

func parseFixture(t *testing.T, name, app string) *model.EventLog {
	t.Helper()
	in, err := Resolve(filepath.Join(fixtures, name), app, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	l, err := Parse(context.Background(), in, Options{})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// Expected values were computed independently with Python's json module
// from the plain fixture (see the commit that added this test).
func TestParseMainFixture(t *testing.T) {
	l := parseFixture(t, mainApp, mainApp)
	a := l.Application
	if a.ID != mainApp || a.Name != "claims_enrich_fixture" || a.User != "hadoop" || a.SparkVersion != "3.5.1" {
		t.Errorf("application %+v", a)
	}
	if a.Status != model.StatusSucceeded || a.DurationMs <= 0 || a.Source.Line == 0 {
		t.Errorf("status %q (%s) duration %d", a.Status, a.StatusReason, a.DurationMs)
	}
	var tot model.TaskTotals
	for _, s := range l.Stages {
		tot.Add(s.Totals)
	}
	want := model.TaskTotals{Tasks: 141, Succeeded: 136, Failed: 5, CPUTimeNs: 51219122901, RunTimeMs: 92702,
		DiskSpillBytes: 353639229, InputBytes: 2713968999, ShuffleWriteBytes: 583207658}
	if tot.Tasks != want.Tasks || tot.Succeeded != want.Succeeded || tot.Failed != want.Failed || tot.CPUTimeNs != want.CPUTimeNs ||
		tot.RunTimeMs != want.RunTimeMs || tot.DiskSpillBytes != want.DiskSpillBytes || tot.InputBytes != want.InputBytes ||
		tot.ShuffleWriteBytes != want.ShuffleWriteBytes {
		t.Errorf("stage totals %+v", tot)
	}
	var execTot model.TaskTotals
	for _, x := range l.Executors {
		execTot.Add(x.Tasks)
	}
	if execTot.Tasks != 141 || execTot.CPUTimeNs != want.CPUTimeNs {
		t.Errorf("executor totals disagree with stage totals: %+v", execTot)
	}
	if len(l.Jobs) != 24 {
		t.Fatalf("%d jobs", len(l.Jobs))
	}
	if j := l.Jobs[20]; j.Status != model.StatusFailed || !strings.Contains(j.Failure, "stage failure") || j.Description != "validate claims" {
		t.Errorf("job 20: %+v", j)
	}
	if len(l.Executors) != 2 || l.Driver == nil {
		t.Fatalf("executors %d driver %v", len(l.Executors), l.Driver)
	}
	peaks := map[string]int64{"0": 793504304, "1": 777212008}
	lost := 0
	for _, x := range l.Executors {
		if x.Peak.JVMHeap != peaks[x.ID] || x.Peak.HeapSource.Line == 0 {
			t.Errorf("executor %s peak heap %d at %s", x.ID, x.Peak.JVMHeap, x.Peak.HeapSource)
		}
		if !strings.HasPrefix(x.Host, "ip-10-0-1-") || x.Cores != 2 {
			t.Errorf("executor %s host %q cores %d", x.ID, x.Host, x.Cores)
		}
		if x.RemovedReason != "" {
			lost++
			if x.RemovalKind != model.RemovalMemoryKill || x.RemovedSource.Line == 0 {
				t.Errorf("executor %s removal %q -> %q", x.ID, x.RemovedReason, x.RemovalKind)
			}
		}
	}
	if lost != 1 {
		t.Errorf("%d executors removed, want 1", lost)
	}
	if l.Driver.Peak.JVMHeap != 151233760 {
		t.Errorf("driver peak %d", l.Driver.Peak.JVMHeap)
	}
	var skewed *model.Stage
	for _, s := range l.Stages {
		if s.ID == 18 {
			skewed = s
		}
	}
	if skewed == nil || skewed.TaskDuration.Count != 16 || skewed.TaskDuration.Max < 5*skewed.TaskDuration.P50 || skewed.Slowest == nil {
		t.Errorf("stage 18 should be skewed: %+v", skewed)
	}
	if len(l.RDDs) == 0 || !l.RDDs[0].Unpersisted || l.RDDs[0].SizeKnown {
		t.Errorf("cached RDDs %+v", l.RDDs)
	}
	if l.Stats.Malformed != 0 || l.Stats.Truncated || len(l.Stats.UnknownEvents) != 0 || len(l.Stats.UnknownFields) != 0 {
		t.Errorf("stats %+v", l.Stats)
	}
	if len(l.Config) < 1000 || l.Stats.Redacted < 4 {
		t.Errorf("config %d entries, %d redacted", len(l.Config), l.Stats.Redacted)
	}
}

func TestSQLDataRefs(t *testing.T) {
	l := parseFixture(t, mainApp, mainApp)
	got := map[string]bool{}
	for _, q := range l.SQL {
		for _, r := range append(q.Reads, q.Writes...) {
			got[r.Access+" "+r.Kind+" "+r.Name] = true
		}
	}
	for _, want := range []string{
		"write path file:/mnt/fixture/main/lake/claims_raw",
		"read path file:/mnt/fixture/main/lake/claims_raw",
		"read table spark_catalog.claims.provider_dim",
		"write table spark_catalog.claims.region_totals",
		"read table spark_catalog.claims.region_totals",
	} {
		if !got[want] {
			t.Errorf("missing %q in %v", want, got)
		}
	}
	created := 0
	for _, c := range l.CatalogEvents {
		if c.Access == "create" {
			created++
		}
	}
	if created != 2 {
		t.Errorf("catalog events %+v", l.CatalogEvents)
	}
}

// Every codec and layout of the same log must parse to the same model.
func TestAllVariantsParseTheSame(t *testing.T) {
	base := summary(parseFixture(t, mainApp, mainApp))
	for _, v := range []string{mainApp + ".lz4", mainApp + ".zstd", mainApp + ".snappy", mainApp + ".zip"} {
		if got := summary(parseFixture(t, v, mainApp)); got != base {
			t.Errorf("%s differs:\n got %s\nwant %s", v, got, base)
		}
	}
}

func summary(l *model.EventLog) string {
	var tot model.TaskTotals
	for _, s := range l.Stages {
		tot.Add(s.Totals)
	}
	b, _ := json.Marshal(struct {
		A      model.Application
		T      model.TaskTotals
		J, S   int
		Events int64
	}{l.Application, tot, len(l.Jobs), len(l.Stages), l.Stats.Events})
	return regexp.MustCompile(`"file":"[^"]*"`).ReplaceAllString(string(b), "")
}

func TestParseRollingLog(t *testing.T) {
	for _, v := range []string{"eventlog_v2_application_1790380000000_0043", "application_1790380000000_0043_rolling.zip"} {
		l := parseFixture(t, v, "application_1790380000000_0043")
		if l.Application.Status != model.StatusSucceeded || len(l.Stats.Files) != 6 || len(l.Jobs) != 51 { // AQE runs each aggregation as two jobs; counted from the raw Spark output
			t.Errorf("%s: status %s, %d files, %d jobs", v, l.Application.Status, len(l.Stats.Files), len(l.Jobs))
		}
		if len(l.RDDs) != 1 || !l.RDDs[0].SizeKnown || l.RDDs[0].MemoryBytes == 0 {
			t.Errorf("%s: block updates should give cached sizes: %+v", v, l.RDDs)
		}
	}
}

func TestParseFailedAndInProgress(t *testing.T) {
	f := parseFixture(t, "application_1790380000000_0044", "application_1790380000000_0044")
	if f.Application.Status != model.StatusFailed || !strings.Contains(f.Application.StatusReason, "job 2") {
		t.Errorf("failed app: %s %s", f.Application.Status, f.Application.StatusReason)
	}
	p := parseFixture(t, "application_1790380000000_0045.inprogress", "application_1790380000000_0045")
	if p.Application.Status != model.StatusIncomplete || !p.Stats.Truncated || !p.Stats.InProgress || p.Stats.Malformed != 0 {
		t.Errorf("in-progress app: %s truncated=%v inprogress=%v malformed=%d", p.Application.Status, p.Stats.Truncated, p.Stats.InProgress, p.Stats.Malformed)
	}
	if p.Application.DurationMs <= 0 {
		t.Error("in-progress app should report duration so far")
	}
}

func TestUnknownAndMalformedCounted(t *testing.T) {
	dir := t.TempDir()
	log := strings.Join([]string{
		`{"Event":"SparkListenerLogStart","Spark Version":"3.5.1","New Field":1}`,
		`{"Event":"SparkListenerApplicationStart","App Name":"x","App ID":"app_1","Timestamp":1000,"User":"u"}`,
		`{"Event":"SparkListenerSomethingNew","a":1}`,
		`not json`,
		`{"Event":"SparkListenerApplicationEnd","Timestamp":5000}`,
	}, "\n") + "\n"
	os.WriteFile(filepath.Join(dir, "app_1"), []byte(log), 0o644)
	in, err := Resolve(filepath.Join(dir, "app_1"), "app_1", Limits{})
	if err != nil {
		t.Fatal(err)
	}
	l, _ := Parse(context.Background(), in, Options{})
	st := l.Stats
	if st.UnknownEvents["SparkListenerSomethingNew"] != 1 || st.UnknownFields["SparkListenerLogStart.New Field"] != 1 {
		t.Errorf("unknown %v %v", st.UnknownEvents, st.UnknownFields)
	}
	if st.Malformed != 1 || st.FirstMalformed.Line != 4 {
		t.Errorf("malformed %d at %v", st.Malformed, st.FirstMalformed)
	}
	if l.Application.DurationMs != 4000 || l.Application.Status != model.StatusSucceeded {
		t.Errorf("app %+v", l.Application)
	}
}

// The fixtures carry planted fake secrets; none may survive parsing.
func TestPlantedSecretsNeverInModel(t *testing.T) {
	for _, name := range []string{mainApp, "application_1790380000000_0044"} {
		l := parseFixture(t, name, strings.TrimSuffix(name, ""))
		b, err := json.Marshal(l)
		if err != nil {
			t.Fatal(err)
		}
		assertNoSecrets(t, string(b))
	}
}

var planted = regexp.MustCompile(`FAKE-[A-Z0-9-]+|AKIAIOSFODNN7EXAMPLE`)

func assertNoSecrets(t *testing.T, s string) {
	t.Helper()
	if m := planted.FindAllString(s, 5); m != nil {
		t.Errorf("planted secrets leaked: %v", m)
	}
}

func TestRemovalKind(t *testing.T) {
	cases := map[string]string{
		"Command exited with code 137": model.RemovalMemoryKill,
		"Container killed by YARN for exceeding physical memory limits. 12.1 GB of 12 GB physical memory used.":       model.RemovalMemoryKill,
		"Container marked as failed: container_1_01_000003 on host: ip-10-0-3-41. Exit status: 143. Diagnostics: ...": model.RemovalLost,
		"Executor heartbeat timed out after 130023 ms":                                                                model.RemovalLost,
		"Container released on a *lost* node":                                                                         model.RemovalLost,
		"Executor decommission: Spot instance interruption":                                                           model.RemovalDecommissioned,
		"Executor killed by driver.":                                                                                  model.RemovalKilledByDriver,
		"":                                                                                                            model.RemovalNone,
		"something new":                                                                                               model.RemovalOther,
	}
	for in, want := range cases {
		if got := removalKind(in); got != want {
			t.Errorf("removalKind(%q) = %q, want %q", in, got, want)
		}
	}
}

func FuzzParseLine(f *testing.F) {
	data, err := os.ReadFile(filepath.Join(fixtures, "application_1790380000000_0044"))
	if err != nil {
		f.Fatal(err)
	}
	for _, l := range strings.Split(string(data), "\n")[:40] {
		f.Add([]byte(l))
	}
	f.Fuzz(func(t *testing.T, line []byte) {
		p := &parser{opt: Options{MaxPlanBytes: 1024, MaxPlans: 2}, log: &model.EventLog{Stats: model.EventLogStats{ByType: map[string]int64{}, UnknownEvents: map[string]int64{}, UnknownFields: map[string]int64{}}},
			execs: map[string]*model.Executor{}, jobs: map[int]*model.Job{}, stages: map[stageKey]*stageAcc{}, stageJobs: map[int][]int{},
			sql: map[int64]*model.SQLQuery{}, sqlJobs: map[int64][]int{}, rdds: map[int]*model.CachedRDD{}, blocks: map[string]blockSize{}}
		_ = p.line(line, model.Source{File: "f", Line: 1})
		p.finish()
	})
}

func TestComponentsFromClasspath(t *testing.T) {
	l := parseFixture(t, mainApp, mainApp)
	got := map[string]string{}
	for _, c := range l.Components {
		got[c.Name] = c.Version
		if c.Source.Line == 0 || c.Path == "" {
			t.Errorf("%s lacks provenance: %+v", c.Name, c)
		}
	}
	for name, want := range map[string]string{"Spark (jars)": "3.5.1", "Scala library": "2.12.18", "Hadoop": "3.3.4"} {
		if got[name] != want {
			t.Errorf("%s = %q, want %q (all: %v)", name, got[name], want, got)
		}
	}
	if l.Application.VersionSrc.Line != 1 {
		t.Errorf("Spark version should cite the log start event: %v", l.Application.VersionSrc)
	}
}

func TestComponentsEMRNames(t *testing.T) {
	cp := map[string]string{
		"/usr/lib/spark/jars/spark-core_2.12-3.5.1-amzn-0.jar":                       "System Classpath",
		"/usr/lib/hadoop/hadoop-common-3.3.6-amzn-3.jar":                             "System Classpath",
		"/usr/share/aws/emr/emrfs/lib/emrfs-hadoop-assembly-2.62.0.jar":              "System Classpath",
		"/usr/share/aws/aws-java-sdk-v2/aws-sdk-java-bundle-2.25.53.jar":             "System Classpath",
		"/usr/share/aws/aws-java-sdk-v2/bundle-2.25.53.jar":                          "System Classpath",
		"/usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.5.0-amzn-0.jar": "System Classpath",
		"/etc/hadoop/conf/": "System Classpath",
		"/usr/lib/spark/python/lib/py4j-0.10.9.7-src.zip": "System Classpath",
	}
	got := map[string]string{}
	for _, c := range components(cp, model.Source{File: "f", Line: 4}) {
		got[c.Name] = c.Version
	}
	want := map[string]string{"Spark (jars)": "3.5.1-amzn-0", "Hadoop": "3.3.6-amzn-3", "EMRFS": "2.62.0", "AWS SDK for Java v2": "2.25.53", "Apache Iceberg": "1.5.0-amzn-0", "Py4J (PySpark bridge)": "0.10.9.7"}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q (all %v)", k, got[k], v, got)
		}
	}
}
