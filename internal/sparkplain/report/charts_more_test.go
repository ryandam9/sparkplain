package report

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

func html(t *testing.T, r *model.Report, opt Options) string {
	t.Helper()
	var b bytes.Buffer
	if err := WriteHTML(&b, r, opt); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

// Each chart appears when its data does, and not otherwise.
func TestReportCharts(t *testing.T) {
	r, _ := buildWithExplorer(t, "application_1790380000000_0042")
	page := html(t, r, Options{ExplorerHref: "app-explorer.html"})
	for _, title := range []string{"The longest stages", "Task time spread", "Data each stage moved", "Data over time", "Spill by stage", "Where executor time went", "The longest queries"} {
		if !strings.Contains(page, "<h4>"+title+"</h4>") {
			t.Errorf("chart %q missing", title)
		}
	}
	for _, title := range []string{"What YARN placed on each node", "Node CPU"} {
		if strings.Contains(page, "<h4>"+title+"</h4>") {
			t.Errorf("chart %q drawn without the cluster's data", title)
		}
	}
	if !strings.Contains(page, `href="app-explorer.html#stage/`) {
		t.Error("stage rows should link to the explorer")
	}

	// With the EMR API's and CloudWatch's data, the node charts appear.
	at := r.Application.Start
	r.Nodes.Hosts = append(r.Nodes.Hosts, model.Host{Name: "ip-10-0-0-9.ec2.internal", Executors: []string{"1"}, PeakExecutors: 1,
		YARNMemoryBytes: 12 << 30, DriverContainerBytes: 2 << 30, ExecutorContainerBytes: 11 << 30, Instance: &model.Instance{ID: "i-9", Role: "CORE"}})
	r.Metrics = &model.MetricsSection{From: at, To: at.Add(time.Hour), Hosts: []model.Series{{Name: "CPUUtilization", Stat: "Average", Scope: "i-9",
		Points: []model.Point{{T: at, V: 20}, {T: at.Add(5 * time.Minute), V: 80}}}}}
	page = html(t, r, Options{})
	for _, title := range []string{"What YARN placed on each node", "Node CPU"} {
		if !strings.Contains(page, "<h4>"+title+"</h4>") {
			t.Errorf("chart %q missing", title)
		}
	}
	if !strings.Contains(page, "i-9 (1 executors)") {
		t.Error("the CPU chart should name each node by what it ran")
	}

	// A stage name cannot break out of the SVG.
	r.Jobs.Stages[0].Name = `</text><script>alert(4)</script>`
	if strings.Contains(html(t, r, Options{}), "<script>alert(4)") {
		t.Fatal("a stage name reached the chart unescaped")
	}
}

// Without the event log there is nothing to chart.
func TestNoChartsWithoutData(t *testing.T) {
	page := html(t, &model.Report{Application: model.Application{ID: "application_1_1"}}, Options{})
	if strings.Contains(page, "<h4>") {
		t.Error("charts drawn for an empty report")
	}
}
