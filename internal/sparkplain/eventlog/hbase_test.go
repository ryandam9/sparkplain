package eventlog

import (
	"slices"
	"strings"
	"testing"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// The hbase-spark connector's tables come from the plan text: its write
// command prints no arguments in the plan's nodes, and a read under a write
// appears only there. The phase 5 connector run wrote sp_orders and
// sp_events, read sp_orders into sp_totals, and read sp_totals back.
func TestHBaseConnectorTables(t *testing.T) {
	t.Parallel()
	l := parseFixture(t, "application_1790380000000_0083.zstd", "application_1790380000000_0083")
	var got []string
	for _, q := range l.SQL {
		for _, d := range append(q.Reads, q.Writes...) {
			if strings.ContainsAny(d.Name, " >(),") {
				t.Errorf("query %d: a table named %q", q.ID, d.Name)
			}
			got = append(got, d.Access+" "+d.Name+" "+d.Format)
		}
	}
	want := []string{"write sp_orders hbase", "write sp_events hbase", "read sp_orders hbase", "write sp_totals hbase", "read sp_totals hbase"}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("tables = %v, want %v", got, want)
	}
}

// A relation printed with its options is not a table name: before, the
// connector's scan gave "-> sp_totals, hbase.columns.mapping -> …".
func TestScanWithOptionsIsNotATable(t *testing.T) {
	t.Parallel()
	n := planNode{NodeName: "Scan HBaseRelation(Map(hbase.table -> sp_totals, hbase.columns.mapping -> key STRING :key),None) "}
	if d, ok := scanRef(n, model.Source{}); ok {
		t.Errorf("scanRef = %+v", d)
	}
	reads, _ := planData(n, "== Physical Plan ==\n+- Scan HBaseRelation(Map(hbase.table -> ns:sp_totals, hbase.columns.mapping -> key STRING :key),None) []\n", model.Source{})
	if len(reads) != 1 || reads[0].Name != "ns:sp_totals" || reads[0].Format != "hbase" {
		t.Errorf("reads = %+v", reads)
	}
}

// Jars shipped with --jars are on the classpath in cluster mode, but the
// environment lists them only in the jar settings. The phase 5 runs
// shipped HBase's client and the hbase-spark connector.
func TestComponentsFromShippedJars(t *testing.T) {
	t.Parallel()
	l := parseFixture(t, "application_1790380000000_0084", "application_1790380000000_0084")
	got := map[string]string{}
	for _, c := range l.Components {
		got[c.Name] = c.Version + " " + c.Path
	}
	if got["HBase client"] != "2.4.17-amzn-7 file:///usr/lib/hbase/lib/hbase-client-2.4.17-amzn-7.jar" ||
		!strings.HasPrefix(got["HBase Spark connector"], "1.0.1 ") {
		t.Errorf("components = %v", got)
	}
}
