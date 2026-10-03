package main

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"

	"github.com/ryandam9/sparkplain/internal/sparkplain/awsmeta"
	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/yarnlog"
)

// hbaseCluster holds the phase 5 test cluster's runs: Spark jobs that read
// and write HBase on the same EMR 7.3.0 cluster (testdata/emrscripts/hbase).
const hbaseCluster = "j-FIXTURE0083CLUSTER"

// hbaseRuns lists each run with its event log (the last attempt's, as the
// History Server keeps it), what it did, the ZooKeeper port its client
// dialled ("" when it failed before reaching ZooKeeper), and its HBase and
// missing-class findings with a phrase each title holds.
var hbaseRuns = []struct {
	app, eventlog, name, status, zkPort, what string
	findings                                  map[string]string
}{
	{"0081", "", "", model.StatusFailed, "", "connector jar missing slf4j 1's StaticLoggerBinder",
		map[string]string{"classpath-clash": "org.slf4j.impl.StaticLoggerBinder"}},
	{"0082", "application_1790380000000_0082_2", "sparkplain_hbase_connector", model.StatusFailed, "2181", "HBase client missing protobuf 2.5's RpcChannel",
		map[string]string{"classpath-clash": "com.google.protobuf.RpcChannel"}},
	{"0083", "application_1790380000000_0083.zstd", "sparkplain_hbase_connector", model.StatusSucceeded, "2181", "connector writes and reads, one ZooKeeper session per task",
		map[string]string{"hbase-zk-connections": "Executor 1 opened 215 ZooKeeper connections"}},
	{"0084", "application_1790380000000_0084", "sparkplain_hbase_rdd", model.StatusSucceeded, "2181", "TableInputFormat scan, TableOutputFormat write",
		map[string]string{"hbase-remote-regions": "2 of 5 HBase regions were read from another node"}},
	{"0085", "application_1790380000000_0085_2", "sparkplain_hbase_missing", model.StatusFailed, "2181", "write to a table that does not exist",
		map[string]string{"hbase-table-missing": "HBase table sp_missing does not exist"}},
	{"0086", "application_1790380000000_0086_2", "sparkplain_hbase_badquorum", model.StatusFailed, "2182", "ZooKeeper port nothing listens on",
		map[string]string{"hbase-zookeeper": ":2182"}},
	{"0088", "application_1790380000000_0088", "sparkplain_hbase_slow", model.StatusSucceeded, "2181", "scanner leases expired; the client reopened them",
		map[string]string{"hbase-scanner-expired": "HBase scanner leases expired (6 times", "hbase-time": "took 9 min 0 s of the 9 min 3 s run (99%)"}},
	{"0090", "application_1790380000000_0090", "sparkplain_hbase_hot", model.StatusSucceeded, "2181", "one small region pushed back (RegionTooBusyException)",
		map[string]string{"hbase-busy": "HBase pushed back on writes to sp_hot", "hbase-time": "took 1 min 19 s of the 1 min 25 s run (93%)",
			"hbase-regions-changed": "HBase split 1 region of sp_hot while the run used it"}},
	{"0092", "application_1790380000000_0092.zstd", "sparkplain_hbase_connector", model.StatusSucceeded, "2181", "a region server stopped mid-job; only HBase's own logs show it",
		map[string]string{"hbase-zk-connections": "Executor 1 opened 215 ZooKeeper connections", "hbase-time": "took 54 s of the 1 min 2 s run (86%)",
			"hbase-server-lost": "Region server ip-10-0-2-10.us-east-1.compute.internal stopped while the run was using HBase"}},
}

// Every HBase run renders from its logs, with its event log where it has
// one, names the ZooKeeper quorum its client dialled, and gets the HBase
// finding that says what went wrong, and no other.
func TestHBaseFixtures(t *testing.T) {
	for _, tc := range hbaseRuns {
		t.Run(tc.app, func(t *testing.T) {
			dir := t.TempDir()
			app := "application_1790380000000_" + tc.app
			args := []string{"-app-id", app, "-from", filepath.Join(emrlogs, hbaseCluster), "-out", dir, "-format", "json,html,explorer"}
			if tc.eventlog != "" {
				args = append(args, "-eventlog", filepath.Join(fx, tc.eventlog))
			}
			// Partial: -from has no EMR API, and EMR had not yet copied the
			// node logs when the fixture was taken.
			if code, _, errs := runCLI(t, args...); code != exitPartial {
				t.Fatalf("%s: exit %d, want %d: %s", tc.what, code, exitPartial, errs)
			}
			r := readReport(t, dir)
			if r.Application.Status != tc.status || r.Application.Name != tc.name {
				t.Errorf("%s: status %q name %q, want %q %q", tc.what, r.Application.Status, r.Application.Name, tc.status, tc.name)
			}
			var zk string
			for _, f := range r.Identity.Facts {
				if f.Label == "HBase connection" {
					zk = f.Value
				}
			}
			if tc.zkPort == "" && zk != "" || tc.zkPort != "" && !strings.HasSuffix(zk, ".compute.internal:"+tc.zkPort) {
				t.Errorf("%s: HBase connection %q, want port %q", tc.what, zk, tc.zkPort)
			}
			got := map[string]string{}
			for _, f := range r.Findings {
				if strings.HasPrefix(f.Rule, "hbase-") || f.Rule == "classpath-clash" {
					got[f.Rule] = f.Title
				}
			}
			for rule, phrase := range tc.findings {
				if !strings.Contains(got[rule], phrase) {
					t.Errorf("%s: %s = %q, want it to hold %q", tc.what, rule, got[rule], phrase)
				}
			}
			for rule, title := range got {
				if _, ok := tc.findings[rule]; !ok {
					t.Errorf("%s: unexpected %s: %s", tc.what, rule, title)
				}
			}
			if want := hbaseTables[tc.app]; hbaseSummary(r) != want {
				t.Errorf("%s: HBase tables\n%s\nwant\n%s", tc.what, hbaseSummary(r), want)
			}
		})
	}
}

// hbaseTables is what each run did with HBase, as hbaseSummary prints it.
// The connector's tables need the event log; TableInputFormat's regions
// come from the executors' split lines.
var hbaseTables = map[string]string{
	"0081": "",
	"0082": "sp_orders w hbase-spark connector",
	"0083": "sp_events w hbase-spark connector; sp_orders rw hbase-spark connector; sp_totals rw hbase-spark connector",
	"0084": "sp_orders r TableInputFormat [ip-10-0-2-12.us-east-1.compute.internal=3 ip-10-0-2-10.us-east-1.compute.internal=2]; sp_totals w TableOutputFormat",
	"0085": "sp_missing w hbase-spark connector",
	"0086": "", // it failed on ZooKeeper before Spark planned the read
	"0088": "sp_events r TableInputFormat [ip-10-0-2-10.us-east-1.compute.internal=2 ip-10-0-2-12.us-east-1.compute.internal=1]",
	"0090": "sp_hot w hbase-spark connector",
	"0092": "sp_events w hbase-spark connector; sp_orders rw hbase-spark connector; sp_totals rw hbase-spark connector",
}

func hbaseSummary(r model.Report) string {
	if r.HBase == nil {
		return ""
	}
	var out []string
	for _, x := range r.HBase.Tables {
		rw := map[[2]bool]string{{true, false}: "r", {false, true}: "w", {true, true}: "rw"}[[2]bool{x.Read, x.Written}]
		s := x.Name + " " + rw + " " + strings.Join(x.APIs, "+")
		if len(x.Regions) > 0 {
			var reg []string
			for _, g := range x.Regions {
				reg = append(reg, g.Server+"="+strconv.Itoa(g.Regions))
			}
			s += " [" + strings.Join(reg, " ") + "]"
		}
		out = append(out, s)
	}
	return strings.Join(out, "; ")
}

func TestHBaseOnSeparateEMRCluster(t *testing.T) {
	bucket, stub := hbaseCluster0083(t, true)
	const sparkCluster = "j-FIXTURE0083SPARK"
	copyTree(t, filepath.Join(emrlogs, hbaseCluster), filepath.Join(bucket, "emr", sparkCluster))
	stub.clusters[sparkCluster] = cluster(sparkCluster, "")
	fakeAWS(t, map[string]string{"logs": bucket}, stub.clusters)
	// The HBase cluster by name too: the one of that name running now,
	// since the application's ID says nothing about it and the one up when
	// its YARN started has ended.
	yarnStart := appClusterStart("application_1790380000000_0092")
	stub.summaries = []types.ClusterSummary{
		{Id: aws.String("j-HBASEOLD"), Name: aws.String("hbase-prod"), Status: &types.ClusterStatus{State: types.ClusterStateTerminated,
			Timeline: &types.ClusterTimeline{CreationDateTime: aws.Time(yarnStart.Add(-48 * time.Hour)), EndDateTime: aws.Time(yarnStart.Add(-24 * time.Hour))}}},
		{Id: aws.String(hbaseCluster), Name: aws.String("hbase-prod"), Status: &types.ClusterStatus{State: types.ClusterStateWaiting,
			Timeline: &types.ClusterTimeline{CreationDateTime: aws.Time(yarnStart.Add(time.Hour))}}},
	}
	awsDeps.emr = func(aws.Config) awsmeta.EMRAPI { return stub }

	for _, by := range [][]string{{"-hbase-cluster-id", hbaseCluster}, {"-hbase-cluster-name", "hbase-prod"}} {
		out := t.TempDir()
		code, stdout, errs := runCLI(t, append([]string{
			"-app-id", "application_1790380000000_0092",
			"-profile", "test",
			"-cluster-id", sparkCluster,
			"-eventlog", filepath.Join(fx, "application_1790380000000_0092.zstd"),
			"-no-cloudwatch", "-no-cloudtrail",
			"-format", "json",
			"-out", out,
		}, by...)...)
		separateHBaseChecks(t, by[0], code, stdout, errs, out)
	}
}

func separateHBaseChecks(t *testing.T, by string, code int, stdout, errs, out string) {
	t.Helper()
	if by == "-hbase-cluster-name" && !strings.Contains(flat(stdout), flat("found by name hbase-prod: the one running now")) {
		t.Errorf("the HBase cluster found by name does not say how:\n%s", stdout)
	}
	if code == exitFatal {
		t.Fatalf("separate HBase cluster: exit %d: %s\n%s", code, errs, stdout)
	}
	if strings.Contains(stdout, "HBase is not installed on this Spark cluster") {
		t.Fatalf("separate HBase cluster was ignored:\n%s", stdout)
	}
	r := readReport(t, out)
	var found bool
	for _, f := range r.Findings {
		if f.Rule == "hbase-server-lost" && strings.Contains(f.Title, "Region server") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("report did not use HBase server logs from %s", hbaseCluster)
	}
	var checked bool
	for _, row := range r.AccessCheck {
		if row.Name == "HBase server logs" {
			checked = row.Status == "ok" && strings.Contains(row.Location, hbaseCluster)
			break
		}
	}
	if !checked {
		t.Errorf("access check did not verify HBase logs on %s: %+v", hbaseCluster, r.AccessCheck)
	}
}

func TestHBaseClusterRequiresSparkCluster(t *testing.T) {
	code, _, errs := runCLI(t,
		"-app-id", "application_1790380000000_0092",
		"-profile", "test",
		"-hbase-cluster-id", hbaseCluster,
		"-eventlog", filepath.Join(fx, "application_1790380000000_0092.zstd"),
	)
	if code != exitFatal || !strings.Contains(errs, "-hbase-cluster-id and -hbase-cluster-name need -cluster-id or -cluster-name") {
		t.Errorf("exit %d, stderr %q", code, errs)
	}
}

// With no HBase server log on the HBase cluster, the run says which
// cluster it read and how it was chosen, never naming a flag that was not
// used, and what it found in place of the logs.
func TestHBaseNoneFoundSaysWhy(t *testing.T) {
	cl := model.Cluster{ID: "j-HBASE", Name: "hbase-prod"}
	for _, c := range []struct {
		byID string
		col  yarnlog.Collection
		want []string
		not  string
	}{
		{"", yarnlog.Collection{HBaseNodes: 4, HBaseOther: []string{"a.out.gz", "b.out.gz", "c.out.gz", "d.out.gz"}},
			[]string{`j-HBASE (picked by its name, "hbase-prod")`, "on 4 nodes up during the run", "found 4 other files there, such as a.out.gz, b.out.gz, c.out.gz, but none named"}, "-hbase-cluster-id"},
		{"j-HBASE", yarnlog.Collection{HBaseNodes: 1},
			[]string{`j-HBASE ("hbase-prod", given by -hbase-cluster-id)`, "on 1 node up", "found nothing there: check that this is the cluster HBase ran on"}, "picked by"},
	} {
		got := hbaseNoneFound(cl, c.byID, c.col)
		for _, w := range c.want {
			if !strings.Contains(got, w) {
				t.Errorf("lacks %q:\n%s", w, got)
			}
		}
		if strings.Contains(got, c.not) {
			t.Errorf("says %q:\n%s", c.not, got)
		}
	}
}

// Without the event log (it is on HDFS by default), the separate HBase
// cluster's logs are still kept to the application's time, taken from its
// container logs on the Spark cluster, rather than read from the oldest
// hour up to the file cap.
func TestSeparateHBaseClusterWithoutEventLog(t *testing.T) {
	bucket, stub := hbaseCluster0083(t, true)
	const sparkCluster = "j-FIXTURE0083SPARK"
	copyTree(t, filepath.Join(emrlogs, hbaseCluster), filepath.Join(bucket, "emr", sparkCluster))
	stub.clusters[sparkCluster] = cluster(sparkCluster, "")
	fakeAWS(t, map[string]string{"logs": bucket}, stub.clusters)
	awsDeps.emr = func(aws.Config) awsmeta.EMRAPI { return stub }
	out := t.TempDir()
	code, stdout, errs := runCLI(t, "-app-id", "application_1790380000000_0092", "-profile", "test", "-cluster-id", sparkCluster,
		"-hbase-cluster-id", hbaseCluster, "-no-cloudwatch", "-no-cloudtrail", "-format", "json", "-out", out)
	if code == exitFatal {
		t.Fatalf("exit %d: %s\n%s", code, errs, stdout)
	}
	r := readReport(t, out)
	for _, s := range r.Sources {
		if s.Name != "HBase server logs" {
			continue
		}
		if s.Status != "read" || !strings.Contains(s.Detail, "kept to the application's time (2026-09-29 06:09–06:10 UTC, from the times its container logs cover)") || !strings.Contains(s.Detail, "hourly logs outside it skipped") {
			t.Errorf("HBase row = %s: %s", s.Status, s.Detail)
		}
		return
	}
	t.Fatalf("no HBase server logs row: %+v", r.Sources)
}

// The 0084 run's TableInputFormat scan of sp_orders, region by region:
// its five splits in key order tie to partitions 0–4 (one executor ran
// them all), each with its task's rows; sizes only where the size line
// could be told apart.
func TestHBaseScanRegions(t *testing.T) {
	dir := t.TempDir()
	app := "application_1790380000000_0084"
	runCLI(t, "-app-id", app, "-from", filepath.Join(emrlogs, hbaseCluster), "-eventlog", filepath.Join(fx, app), "-out", dir, "-format", "json")
	r := readReport(t, dir)
	if r.HBase == nil || len(r.HBase.Scans) != 1 {
		t.Fatalf("scans: %+v", r.HBase)
	}
	sc := r.HBase.Scans[0]
	var got []string
	for _, g := range sc.Regions {
		row := fmt.Sprintf("[%s,%s) %s %dM", g.StartRow, g.EndRow, strings.SplitN(g.Server, ".", 2)[0], g.SizeBytes>>20)
		if g.Task != nil {
			row += fmt.Sprintf(" p%d %d rows", g.Task.Index, g.Task.Rows)
		}
		got = append(got, row)
	}
	want := []string{
		"[,2) ip-10-0-2-10 0M p0 400000 rows",
		"[2,4) ip-10-0-2-12 50M p1 400000 rows",
		"[4,6) ip-10-0-2-12 50M p2 400000 rows",
		"[6,8) ip-10-0-2-12 0M p3 400000 rows",
		"[8,) ip-10-0-2-10 50M p4 400000 rows",
	}
	if sc.StageID != 0 || sc.Table != "sp_orders" || sc.Rows != "[first row, last row]" || !sc.Tied || strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("stage %d table %q rows %q tied %v (%s):\n%s", sc.StageID, sc.Table, sc.Rows, sc.Tied, sc.Untied, strings.Join(got, "\n"))
	}
	if sc.TotalRows != 2000000 || sc.SizedRegions != 3 || sc.SizeBytes != 150<<20 || len(sc.Servers) != 2 ||
		sc.Servers[0].Server != "ip-10-0-2-12.us-east-1.compute.internal" || sc.Servers[0].Regions != 3 || sc.Servers[0].Rows != 1200000 {
		t.Errorf("totals %d rows, %d sized (%d bytes), servers %+v", sc.TotalRows, sc.SizedRegions, sc.SizeBytes, sc.Servers)
	}
}

// A scan the driver printed (a sparkplain-scan line in its stdout) joins
// the stage that read its table, decoded; the string itself never reaches
// the report.
func TestHBaseScanPrinted(t *testing.T) {
	logs := filepath.Join(t.TempDir(), "logs")
	copyTree(t, filepath.Join(emrlogs, hbaseCluster), logs)
	scan := bytes.Join([][]byte{field(3, []byte("2")), field(4, []byte("8")),
		field(5, field(1, []byte("org.apache.hadoop.hbase.filter.PrefixFilter")), field(2, field(1, []byte("PLANTED-VALUE"))))}, nil)
	b64 := base64.StdEncoding.EncodeToString(scan)
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	fmt.Fprintf(zw, "sparkplain-scan {\"table\": \"sp_orders\", \"scan\": \"%s\\n\"}\n", b64)
	zw.Close()
	if err := os.WriteFile(filepath.Join(logs, "containers/application_1790380000000_0084/container_1790380000000_0084_01_000001/stdout.gz"), gz.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	app := "application_1790380000000_0084"
	runCLI(t, "-app-id", app, "-from", logs, "-eventlog", filepath.Join(fx, app), "-out", dir, "-format", "json,html,explorer")
	r := readReport(t, dir)
	if r.HBase == nil || len(r.HBase.Scans) != 1 || r.HBase.Scans[0].Scan == nil {
		t.Fatalf("scans: %+v", r.HBase)
	}
	sc := r.HBase.Scans[0].Scan
	if sc.Table != "sp_orders" || sc.Rows() != "[2, 8)" || sc.Filter.String() != `PrefixFilter "PLANTED-VALUE"` || !strings.HasSuffix(sc.Source.File, "stdout.gz") {
		t.Errorf("scan = %+v", sc)
	}
	for _, name := range []string{app + "-report.json", app + "-report.html", app + "-explorer.html"} {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), b64[:16]) {
			t.Errorf("%s holds the scan string", name)
		}
		// The explorer shows the scan stage, region by region, with the
		// decoded filters.
		if strings.HasSuffix(name, "explorer.html") && (!strings.Contains(string(b), "PrefixFilter") || !strings.Contains(string(b), "554f89fb3a319d50d2ea3299dc1270c1")) {
			t.Errorf("%s lacks the scan's filters or regions", name)
		}
	}
}
