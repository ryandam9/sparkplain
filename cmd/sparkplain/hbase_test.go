package main

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
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
	{"0083", "application_1790380000000_0083.zstd", "sparkplain_hbase_connector", model.StatusSucceeded, "2181", "connector writes and reads, one ZooKeeper session per task", nil},
	{"0084", "application_1790380000000_0084", "sparkplain_hbase_rdd", model.StatusSucceeded, "2181", "TableInputFormat scan, TableOutputFormat write", nil},
	{"0085", "application_1790380000000_0085_2", "sparkplain_hbase_missing", model.StatusFailed, "2181", "write to a table that does not exist",
		map[string]string{"hbase-table-missing": "HBase table sp_missing does not exist"}},
	{"0086", "application_1790380000000_0086_2", "sparkplain_hbase_badquorum", model.StatusFailed, "2182", "ZooKeeper port nothing listens on",
		map[string]string{"hbase-zookeeper": ":2182"}},
	{"0088", "application_1790380000000_0088", "sparkplain_hbase_slow", model.StatusSucceeded, "2181", "scanner leases expired; the client reopened them",
		map[string]string{"hbase-scanner-expired": "HBase scanner leases expired (6 times"}},
	{"0090", "application_1790380000000_0090", "sparkplain_hbase_hot", model.StatusSucceeded, "2181", "one small region pushed back (RegionTooBusyException)",
		map[string]string{"hbase-busy": "HBase pushed back on writes to sp_hot"}},
	{"0092", "", "", model.StatusSucceeded, "2181", "a region server stopped mid-job; the client logged nothing", nil},
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
		})
	}
}
