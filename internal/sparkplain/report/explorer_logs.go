package report

import (
	"net/url"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/source"
)

// Caps on log lines embedded in the explorer, so a run with thousands of
// failing containers still gives a page that opens and stays inside the
// page's size budget with the event log's worst case (about 2.5 MB of
// text and 2 MB of detail at most; report.json keeps everything).
const (
	maxLogLinesPerFile = 200
	maxLogLines        = 5000
	maxLogText         = 500
	maxLogDetailBytes  = 2 << 20
	maxSkippedObjects  = 2000
)

// xLogFile is one container, step or node log and what was found in it.
type xLogFile struct {
	Loc       string  `json:"loc"`
	Href      string  `json:"href,omitempty"` // the object in the S3 console
	Kind      string  `json:"kind"`
	Container string  `json:"container,omitempty"`
	Step      string  `json:"step,omitempty"`
	Instance  string  `json:"instance,omitempty"`
	Exec      string  `json:"exec,omitempty"`
	Host      string  `json:"host,omitempty"`
	Bytes     int64   `json:"bytes"`
	Lines     int64   `json:"lines"`
	Dropped   int     `json:"dropped,omitempty"`
	Cut       int     `json:"cut,omitempty"` // lines not embedded here (see report.json)
	Found     [][]any `json:"found"`         // logLineCols
}

// logLineCols are the columns of xLogFile.Found rows.
var logLineCols = []string{"kind", "sev", "line", "end", "time", "text", "detail", "count", "last", "fields"}

// xLogSource is a Sources row for the EMR API or the logs, with the
// objects skipped or unreadable.
type xLogSource struct {
	Name    string             `json:"name"`
	Status  string             `json:"status"`
	Class   string             `json:"class,omitempty"`
	Loc     string             `json:"loc,omitempty"`
	Detail  string             `json:"detail"`
	Skipped []model.SourceFile `json:"skipped,omitempty"`
	More    int                `json:"more,omitempty"` // skipped objects not listed
}

// xCluster is the EMR cluster, from the EMR API.
type xCluster struct {
	ID        string           `json:"id"`
	Name      string           `json:"name"`
	Release   string           `json:"release"`
	State     string           `json:"state"`
	Reason    string           `json:"reason,omitempty"`
	LogURI    string           `json:"logUri,omitempty"`
	Profile   string           `json:"profile,omitempty"`
	Role      string           `json:"role,omitempty"`
	Security  string           `json:"security,omitempty"`
	Instances []model.Instance `json:"instances"`
}

// logData encodes the logs for the explorer.
func logData(r *model.Report) ([]xLogFile, []xLogSource, *xCluster) {
	var files []xLogFile
	var srcs []xLogSource
	var cl *xCluster
	if c := r.Cluster; c != nil {
		cl = &xCluster{ID: c.ID, Name: c.Name, Release: c.Release, State: c.State, Reason: c.StateReason, LogURI: c.LogURI,
			Profile: c.InstanceProfile, Role: c.ServiceRole, Security: c.SecurityConfig, Instances: orEmpty(c.Instances)}
	}
	for _, s := range r.Sources {
		switch s.Name {
		case "EMR API", "EC2 API", "Container logs", "Step logs", "Node logs", "Application code", "CloudWatch", "CloudTrail":
		default:
			continue
		}
		xs := xLogSource{Name: s.Name, Status: s.Status, Class: s.Class, Loc: s.Location, Detail: s.Detail}
		for _, f := range s.Files {
			if f.Status == "read" {
				continue
			}
			if len(xs.Skipped) == maxSkippedObjects {
				xs.More++
				continue
			}
			xs.Skipped = append(xs.Skipped, f)
		}
		srcs = append(srcs, xs)
	}
	if r.Logs == nil {
		return files, srcs, cl
	}
	total, detail := 0, 0
	for _, f := range r.Logs.Files {
		xf := xLogFile{Loc: f.Location, Href: consoleURL(f.Location), Kind: f.Kind, Container: f.Container, Step: f.Step, Instance: f.Instance,
			Exec: f.Executor, Host: f.Host, Bytes: f.Bytes, Lines: f.Lines, Dropped: f.Dropped, Found: [][]any{}}
		for _, l := range f.Found {
			if len(xf.Found) == maxLogLinesPerFile || total == maxLogLines {
				xf.Cut++
				continue
			}
			total++
			var d []string
			if detail < maxLogDetailBytes {
				d = l.Detail
				for _, s := range d {
					detail += len(s)
				}
			}
			xf.Found = append(xf.Found, []any{string(l.Kind), string(l.Severity), l.Source.Line, l.Source.EndLine, unixMs(l.Time), clipText(l.Text, maxLogText), orEmpty(d), l.Count, l.LastLine, orMap(l.Fields)})
		}
		files = append(files, xf)
	}
	return files, srcs, cl
}

// consoleURL links an S3 object to the S3 console, where people with
// access can open it; the page itself never fetches it.
func consoleURL(loc string) string {
	bucket, key, ok := source.ParseS3(loc)
	if !ok || key == "" {
		return ""
	}
	return "https://s3.console.aws.amazon.com/s3/object/" + url.PathEscape(bucket) + "?prefix=" + url.QueryEscape(key)
}

// clipText cuts s to at most n bytes on a rune boundary.
func clipText(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && s[n]&0xC0 == 0x80 {
		n--
	}
	return s[:n] + "…"
}

// xNodeRow is one row of the explorer's Cluster tab.
type xNodeRow struct {
	Host      string          `json:"host"`
	Instance  *model.Instance `json:"instance,omitempty"`
	Driver    bool            `json:"driver,omitempty"`
	Executors []string        `json:"executors"`
	YARNMem   int64           `json:"yarnMem,omitempty"`
	YARNCores int             `json:"yarnCores,omitempty"`
	CPUAvg    *float64        `json:"cpuAvg,omitempty"`
	CPUPeak   *float64        `json:"cpuPeak,omitempty"`
}

// xSeries is a metric's points as [Unix ms, value] pairs.
type xSeries struct {
	Name   string       `json:"name"`
	Stat   string       `json:"stat"`
	Scope  string       `json:"scope"`
	Unit   string       `json:"unit"`
	Points [][2]float64 `json:"points"`
}

// xAWS is what the explorer's Cluster tab shows from CloudWatch and
// CloudTrail.
type xAWS struct {
	Nodes       []xNodeRow             `json:"nodes"`
	From        int64                  `json:"from,omitempty"`
	To          int64                  `json:"to,omitempty"`
	Metrics     []xSeries              `json:"metrics"`
	MetricFacts []model.Fact           `json:"metricFacts,omitempty"`
	MetricsGaps []string               `json:"metricsGaps,omitempty"`
	Calls       []model.AWSCall        `json:"calls"`
	Denied      []model.AWSEvent       `json:"denied"`
	CallsGaps   []string               `json:"callsGaps,omitempty"`
	CallUsers   []string               `json:"callUsers,omitempty"`
	CallEvents  int                    `json:"callEvents,omitempty"`
	Security    *model.SecurityPosture `json:"security,omitempty"`
	Groups      []model.InstanceGroup  `json:"groups,omitempty"`
}

// awsData encodes the cluster's nodes, metrics and AWS calls.
func awsData(r *model.Report) *xAWS {
	if r.Cluster == nil && r.Metrics == nil && r.AWSCalls == nil {
		return nil
	}
	a := &xAWS{Nodes: []xNodeRow{}, Metrics: []xSeries{}, Calls: []model.AWSCall{}, Denied: []model.AWSEvent{}}
	if r.Cluster != nil {
		a.Security, a.Groups = r.Cluster.Security, r.Cluster.Groups
	}
	for _, h := range r.Nodes.Hosts {
		row := xNodeRow{Host: h.Name, Instance: h.Instance, Driver: h.Driver, Executors: orEmpty(h.Executors), YARNMem: h.YARNMemoryBytes, YARNCores: h.YARNVCores}
		if h.HostCPU != nil {
			row.CPUAvg, row.CPUPeak = &h.HostCPU.Average, &h.HostCPU.Peak
		}
		a.Nodes = append(a.Nodes, row)
	}
	if m := r.Metrics; m != nil {
		a.From, a.To, a.MetricFacts, a.MetricsGaps = unixMs(m.From), unixMs(m.To), m.Summary, m.Missing
		for _, s := range append(append([]model.Series{}, m.Cluster...), m.Hosts...) {
			if len(s.Points) == 0 {
				continue
			}
			x := xSeries{Name: s.Name, Stat: s.Stat, Scope: s.Scope, Unit: s.Unit}
			for _, p := range s.Points {
				x.Points = append(x.Points, [2]float64{float64(unixMs(p.T)), p.V})
			}
			a.Metrics = append(a.Metrics, x)
		}
	}
	if c := r.AWSCalls; c != nil {
		a.Calls, a.Denied, a.CallsGaps, a.CallUsers, a.CallEvents = orEmpty(c.Calls), orEmpty(c.Denied), c.Missing, c.Users, c.Events
	}
	return a
}
