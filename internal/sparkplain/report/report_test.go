package report

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/analyze"
	"github.com/ryandam9/sparkplain/internal/sparkplain/eventlog"
	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

func build(t *testing.T, name string) *model.Report {
	t.Helper()
	in, err := eventlog.Resolve(filepath.Join("../../../testdata/eventlog", name), name, eventlog.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	l, err := eventlog.Parse(context.Background(), in, eventlog.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return analyze.Run(analyze.Input{Tool: "sparkplain test", GeneratedAt: time.Date(2026, 9, 26, 1, 0, 0, 0, time.UTC), TimeZone: "UTC",
		EventLog: l, EventSource: model.SourceStatus{Name: "Spark event log", Status: "read"}, Thresholds: analyze.DefaultThresholds()})
}

func render(t *testing.T, r *model.Report, loc *time.Location) string {
	t.Helper()
	var b bytes.Buffer
	if err := WriteHTML(&b, r, Options{Location: loc}); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func TestHTMLIsSelfContained(t *testing.T) {
	html := render(t, build(t, "application_1790380000000_0042"), nil)
	for _, re := range []string{`<link\b`, `<script[^>]+src=`, `<img[^>]+src="?https?:`, `@import`, `url\(\s*['"]?https?:`, `<iframe`, `fonts\.googleapis`} {
		if m := regexp.MustCompile(re).FindString(html); m != "" {
			t.Errorf("report loads something external: %q", m)
		}
	}
}

func TestHTMLHasEverySection(t *testing.T) {
	html := render(t, build(t, "application_1790380000000_0042"), nil)
	for _, id := range []string{"summary", "coverage", "findings", "timeline", "nodes", "executors", "memory", "cpu", "io", "stages", "sql", "config", "access", "sources"} {
		if !strings.Contains(html, `<section id="`+id+`"`) {
			t.Errorf("missing section %s", id)
		}
	}
	for _, want := range []string{"claims_enrich_fixture", "Stage 18 is skewed", "<svg", "ip-10-0-1-23.ec2.internal", "spark_catalog.claims.region_totals", "Set · value hidden"} {
		if !strings.Contains(html, want) {
			t.Errorf("report lacks %q", want)
		}
	}
}

func TestNoPlantedSecretsInOutputs(t *testing.T) {
	for _, n := range []string{"application_1790380000000_0042", "application_1790380000000_0044"} {
		r := build(t, n)
		html := render(t, r, nil)
		var js bytes.Buffer
		if err := WriteJSON(&js, r); err != nil {
			t.Fatal(err)
		}
		for _, out := range []string{html, js.String()} {
			if m := regexp.MustCompile(`FAKE-[A-Z0-9-]+|AKIAIOSFODNN7EXAMPLE`).FindAllString(out, 3); m != nil {
				t.Errorf("%s: secrets leaked: %v", n, m)
			}
		}
	}
}

func TestTimesRenderInConfiguredZoneWithLabel(t *testing.T) {
	r := build(t, "application_1790380000000_0042")
	syd, err := time.LoadLocation("Australia/Sydney")
	if err != nil {
		t.Skip("no tzdata")
	}
	html := render(t, r, syd)
	want := r.Application.Start.In(syd).Format("15:04:05")
	if !strings.Contains(html, want) || !strings.Contains(html, "Australia/Sydney") {
		t.Errorf("start time %s or zone label missing", want)
	}
	if !strings.Contains(html, `data-time="`+r.Application.Start.UTC().Format(time.RFC3339Nano)+`"`) {
		t.Error("times must carry data-time for the viewer's zone")
	}
}

func TestJSONRoundTrips(t *testing.T) {
	r := build(t, "application_1790380000000_0042")
	var b bytes.Buffer
	if err := WriteJSON(&b, r); err != nil {
		t.Fatal(err)
	}
	var back model.Report
	if err := json.Unmarshal(b.Bytes(), &back); err != nil {
		t.Fatal(err)
	}
	if back.SchemaVersion != model.SchemaVersion || back.Application.ID != r.Application.ID || len(back.Findings) != len(r.Findings) {
		t.Errorf("round trip lost data")
	}
}

func TestRendersWithoutEventLog(t *testing.T) {
	r := analyze.Run(analyze.Input{Tool: "t", EventSource: model.SourceStatus{Name: "Spark event log", Status: "error", Class: "corrupt", Detail: "bad magic"}})
	html := render(t, r, nil)
	if !strings.Contains(html, "Event log: not read") || !strings.Contains(html, "bad magic") {
		t.Error("degraded report should say the event log was not read and why")
	}
}

func TestHostileTextIsEscaped(t *testing.T) {
	r := build(t, "application_1790380000000_0044")
	r.Application.Name = `<script>alert(1)</script>`
	r.Findings = append(r.Findings, model.Finding{Title: `<img src=x onerror=alert(1)>`, Severity: model.Info})
	html := render(t, r, nil)
	if strings.Contains(html, "<script>alert(1)") || strings.Contains(html, "<img src=x") {
		t.Error("log text must be escaped")
	}
}

func TestRuntimeEnvironmentTable(t *testing.T) {
	html := render(t, build(t, "application_1790380000000_0042"), nil)
	for _, want := range []string{`id="runtime"`, `href="#runtime"`, "Runtime environment", "Spark 3.5.1</b> · Java 21.0.10 · Hadoop 3.3.4",
		"/usr/lib/jvm/java-21-openjdk-amd64", `<tr class="grp"><th colspan="4">Locations</th></tr>`, `class="missingrow"`} {
		if !strings.Contains(html, want) {
			t.Errorf("report lacks %q", want)
		}
	}
}
