package report

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

const gib = int64(1) << 30

// anatReport is a small cluster like the NOAA test run: the driver alone on
// one node, one 11 GiB executor on the other, 12 GiB of YARN on each.
func anatReport() *model.Report {
	r := &model.Report{Cluster: &model.Cluster{ID: "j-1", Name: "test", Release: "emr-7.3.0"}}
	r.Nodes.Hosts = []model.Host{
		{Name: "ip-10-0-0-1.internal", Instance: &model.Instance{ID: "i-1", Role: "MASTER", Type: "m5.xlarge"}},
		{Name: "ip-10-0-0-2.internal", Instance: &model.Instance{ID: "i-2", Role: "CORE", Type: "m5.xlarge"}, YARNMemoryBytes: 12 * gib, YARNVCores: 4, DriverContainerBytes: 2432 << 20},
		{Name: "ip-10-0-0-3.internal", Instance: &model.Instance{ID: "i-3", Role: "CORE", Type: "m5.xlarge"}, YARNMemoryBytes: 12 * gib, YARNVCores: 4, ExecutorContainerBytes: 11 * gib, PeakExecutors: 1, Executors: []string{"1"}},
	}
	r.Executors.Executors = []*model.Executor{{ID: "1", Host: "ip-10-0-0-3.internal", Cores: 4}}
	r.Executors.Peak, r.Executors.Started = 1, 1
	r.Memory.Config = model.MemoryConfig{HeapBytes: 9486 << 20, OverheadBytes: 1778 << 20, ContainerBytes: 11 * gib, Cores: 4, MemoryFraction: 0.6, StorageFraction: 0.5, DriverHeapBytes: 2 * gib}
	r.Memory.Executors = []model.ExecMemory{{ID: "1", Host: "ip-10-0-0-3.internal", HeapBytes: 9486 << 20, PeakHeap: 4 * gib}}
	return r
}

func TestAnatomyShowsWhatDidNotFit(t *testing.T) {
	t.Parallel()
	a := buildAnatomy(anatReport())
	if a.Primary == nil || a.Primary.Short != "ip-10-0-0-1" {
		t.Fatalf("primary = %+v", a.Primary)
	}
	if len(a.Nodes) != 2 || a.Nodes[0].DriverBytes == 0 {
		t.Fatalf("the driver's node should come first: %+v", a.Nodes)
	}
	// The driver's node ran no executor, but is judged against the
	// application's executor size: 9.6 GiB free cannot hold 11 GiB.
	if free, fits := a.Nodes[0].free(); free != 12*gib-2432<<20 || fits {
		t.Errorf("driver's node: free %d, fits %v", free, fits)
	}
	if free, fits := a.Nodes[1].free(); free != gib || fits {
		t.Errorf("executor's node: free %d, fits %v", free, fits)
	}
	if a.RM.OfferedBytes != 24*gib || a.RM.HeldBytes != 2432<<20+11*gib {
		t.Errorf("RM = %+v", a.RM)
	}
	svg := anatomySVG(a, anatLinks{Finding: func(int) string { return "" }, Ref: func(string) string { return "" }})
	for _, want := range []string{"too small for another 11.0 GiB executor", "Inside executor 1", "Java heap 9.3 GiB", "The driver"} {
		if !strings.Contains(svg, want) {
			t.Errorf("diagram lacks %q", want)
		}
	}
}

func TestAnatomyPinsFindings(t *testing.T) {
	t.Parallel()
	r := anatReport()
	var exeHost string
	for _, n := range buildAnatomy(r).Nodes {
		if n.DriverBytes == 0 {
			exeHost = n.Short + ".another-domain.internal" // matched by its short name
		}
	}
	r.Findings = []model.Finding{
		{Rule: "waited-for-capacity", Severity: model.Warning, Title: "waited"},
		{Rule: "executor-memory-kill", Severity: model.Critical, Title: "killed", Evidence: []model.Evidence{{Ref: "executor:1"}}},
		{Rule: "idle-nodes", Severity: model.Warning, Title: "idle", Evidence: []model.Evidence{{Text: "ip-10-0-0-2.internal ran no executors"}}},
		{Rule: "stage-skew", Severity: model.Warning, Title: "skew"},
		{Rule: "hbase-server-lost", Severity: model.Warning, Title: "region server lost", Evidence: []model.Evidence{{Ref: model.NodeRef(exeHost)}}},
	}
	a := buildAnatomy(r)
	a.pinDetail()
	if !hasInt(a.RM.Badges, 1) {
		t.Errorf("RM badges %v", a.RM.Badges)
	}
	var drv, exe *anatNode
	for _, n := range a.Nodes {
		if n.DriverBytes > 0 {
			drv = n
		} else {
			exe = n
		}
	}
	if !hasInt(exe.Execs[0].Badges, 2) || !hasInt(a.Detail.Badges, 2) {
		t.Errorf("memory kill should pin to the executor and the detail panel: %v, %v", exe.Execs[0].Badges, a.Detail.Badges)
	}
	if !hasInt(drv.Badges, 3) {
		t.Errorf("idle-nodes should pin to the node its evidence names: %v", drv.Badges)
	}
	if !hasInt(exe.Badges, 5) {
		t.Errorf("a finding whose evidence refers to a node should pin to it: %v", exe.Badges)
	}
	if len(a.Unpinned) != 1 || a.Unpinned[0] != 4 {
		t.Errorf("unpinned = %v", a.Unpinned)
	}
}

// A big uniform cluster collapses into one card per kind of node.
func TestAnatomyFoldsLookAlikes(t *testing.T) {
	t.Parallel()
	r := anatReport()
	base := r.Nodes.Hosts[2]
	for i := 4; i < 30; i++ {
		h := base
		h.Name = "ip-10-0-1-" + string(rune('a'+i-4)) + ".internal"
		h.Executors = []string{h.Name}
		r.Nodes.Hosts = append(r.Nodes.Hosts, h)
		r.Executors.Executors = append(r.Executors.Executors, &model.Executor{ID: h.Name, Host: h.Name, Cores: 4})
	}
	a := buildAnatomy(r)
	if len(a.Nodes) != 2 {
		t.Fatalf("got %d node cards, want the driver's and one for the look-alikes", len(a.Nodes))
	}
	if a.Nodes[1].Alike != 26 {
		t.Errorf("look-alikes = %d, want 26", a.Nodes[1].Alike)
	}
}

func TestAnatomyEscapesAndDegrades(t *testing.T) {
	t.Parallel()
	if buildAnatomy(&model.Report{}) != nil {
		t.Error("an empty report should have no diagram")
	}
	r := anatReport()
	r.Cluster = nil
	r.Nodes.Hosts[2].Name = `<script>alert(1)</script>`
	r.Executors.Executors[0].Host = r.Nodes.Hosts[2].Name
	svg := anatomySVG(buildAnatomy(r), anatLinks{Finding: func(int) string { return "" }, Ref: func(string) string { return "" }})
	if strings.Contains(svg, "<script>") {
		t.Error("a host name reached the diagram unescaped")
	}
	if !strings.Contains(svg, "pass -cluster-id") {
		t.Error("without the EMR API the diagram should say what it needs")
	}
}

// The explorer carries the same diagram, linked to its own pages.
func TestExplorerCarriesAnatomy(t *testing.T) {
	t.Parallel()
	r, x := buildWithExplorer(t, "application_1790380000000_0042")
	a, _ := embedded(t, renderExplorer(t, r, x))["anatomy"].(string)
	if !strings.Contains(a, `<svg class="anat"`) || !strings.Contains(a, `href="#executor/`) {
		t.Fatalf("explorer anatomy = %.200s", a)
	}
	if len(r.Findings) > 0 && !strings.Contains(a, `href="#finding/1"`) {
		t.Error("finding badges should open the explorer's finding")
	}
}

// On a shared cluster the diagram cannot see other applications'
// containers, so it must not claim room for more executors.
func TestAnatomySharedClusterMakesNoRoomClaim(t *testing.T) {
	t.Parallel()
	r := anatReport()
	r.Nodes.Hosts[1].DriverContainerBytes = 0 // a node with 12 GiB unused
	r.Metrics = &model.MetricsSection{Summary: []model.Fact{{Label: "Applications at once", Value: "2 at most"}}}
	svg := anatomySVG(buildAnatomy(r), anatLinks{Finding: func(int) string { return "" }, Ref: func(string) string { return "" }})
	if strings.Contains(svg, "room for") || !strings.Contains(svg, "not used by this application") {
		t.Errorf("shared cluster diagram claims free room: %v", strings.Contains(svg, "room for"))
	}
}

var noLinks = anatLinks{Finding: func(int) string { return "" }, Ref: func(string) string { return "" }}

// An executor's name and status share the chip's top line: however long
// the ID and whatever the status, the two must fit side by side.
func TestAnatomyChipLabelsFit(t *testing.T) {
	t.Parallel()
	nameRE := regexp.MustCompile(`class="b"[^>]*>([^<]*)<`)
	statusRE := regexp.MustCompile(`class="m st"[^>]*>([^<]*)<`)
	for _, id := range []string{"5", "25", "1234", "12345678"} {
		for _, kind := range []string{model.RemovalMemoryKill, model.RemovalLost, model.RemovalDecommissioned, model.RemovalIdle, model.RemovalKilledByDriver} {
			var b svgw
			drawChip(&b, &anatomy{}, anatExec{ID: id, Cores: 2, Heap: 10 * gib, PeakHeap: 6 * gib, Kind: kind}, 0, 0, noLinks)
			name, status := nameRE.FindStringSubmatch(b.String()), statusRE.FindStringSubmatch(b.String())
			if name == nil || status == nil {
				t.Fatalf("%s %s: no name or status in %s", id, kind, b.String())
			}
			if w := textW(name[1], 11, true) + textW(status[1], 10, true); w > anChipW-24 {
				t.Errorf("%s %s: %q + %q is %.0f units, room %.0f", id, kind, name[1], status[1], w, anChipW-24)
			}
		}
	}
}

// The peak heap label is never drawn with the line's class (which stroked
// it doubled), and stays inside the heap strip when the peak is at its edge.
func TestAnatomyPeakLabel(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		peak   int64
		anchor string
	}{{1 << 20, "start"}, {500 << 20, "middle"}, {gib - 1<<20, "end"}} {
		var b svgw
		drawJVM(&b, &anatomy{}, &anatJVM{Title: "The driver", Container: gib, Heap: gib, PeakHeap: c.peak, MemoryFraction: 0.6, StorageFraction: 0.5}, 0, noLinks)
		m := regexp.MustCompile(`<text class="([^"]*)"[^>]*text-anchor="([a-z]+)">peak heap`).FindStringSubmatch(b.String())
		if m == nil {
			t.Fatalf("peak %d: no peak label in %s", c.peak, b.String())
		}
		if strings.Contains(" "+m[1]+" ", " peak ") {
			t.Errorf("peak %d: label has the line's class %q", c.peak, m[1])
		}
		if m[2] != c.anchor {
			t.Errorf("peak %d: anchor %q, want %q", c.peak, m[2], c.anchor)
		}
	}
}

// With the cluster read, missing YARN capacity is not for want of
// -cluster-id: on a running cluster its node logs may not be in S3 yet.
func TestAnatomySaysWhyCapacityIsMissing(t *testing.T) {
	t.Parallel()
	links := anatLinks{Finding: func(int) string { return "" }, Ref: func(string) string { return "" }}
	for state, want := range map[string]string{
		"WAITING":    "YARN capacity not in S3 yet (the cluster is still running)",
		"TERMINATED": "YARN capacity not in its logs",
	} {
		r := anatReport()
		r.Cluster.State = state
		for i := range r.Nodes.Hosts {
			r.Nodes.Hosts[i].YARNMemoryBytes, r.Nodes.Hosts[i].YARNVCores = 0, 0
		}
		svg := anatomySVG(buildAnatomy(r), links)
		if !strings.Contains(svg, want) || strings.Contains(svg, "-cluster-id") {
			t.Errorf("%s: diagram lacks %q or still asks for -cluster-id", state, want)
		}
	}
}

// A node with no CPU points says CloudWatch recorded none, not that it
// needs CloudWatch, when CloudWatch was read.
func TestAnatomySaysWhyCPUIsMissing(t *testing.T) {
	t.Parallel()
	links := anatLinks{Finding: func(int) string { return "" }, Ref: func(string) string { return "" }}
	r := anatReport()
	if svg := anatomySVG(buildAnatomy(r), links); !strings.Contains(svg, "CPU not known (needs CloudWatch)") {
		t.Error("without CloudWatch the diagram should say it needs it")
	}
	r.Metrics = &model.MetricsSection{Coverage: model.Partial}
	if svg := anatomySVG(buildAnatomy(r), links); !strings.Contains(svg, "CPU not recorded while the application ran") || strings.Contains(svg, "needs CloudWatch") {
		t.Error("with CloudWatch read, a node without points should say none were recorded")
	}
}

// A spot node that went away early, or joined near the end, offered its
// memory for only part of the run: the diagram says so and makes no room
// claim for it, and YARN's total leaves it out.
func TestAnatomyNodesNotThereForTheRun(t *testing.T) {
	t.Parallel()
	r := anatReport()
	start := time.Date(2026, 9, 27, 8, 2, 19, 0, time.UTC)
	r.Application.Start, r.Application.End = start, start.Add(257*time.Second)
	for i := range r.Nodes.Hosts {
		r.Nodes.Hosts[i].Instance.Ready = start.Add(-time.Hour)
	}
	spot := func(name, id string, ready, ended time.Time) model.Host {
		return model.Host{Name: name, Instance: &model.Instance{ID: id, Role: "TASK", Type: "m5.xlarge", Market: "SPOT", Ready: ready, Ended: ended}, YARNMemoryBytes: 12 * gib, YARNVCores: 4}
	}
	r.Nodes.Hosts = append(r.Nodes.Hosts,
		spot("ip-10-0-0-4.internal", "i-4", start.Add(-time.Hour), start.Add(79*time.Second)),
		spot("ip-10-0-0-5.internal", "i-5", start.Add(221*time.Second), time.Time{}))
	a := buildAnatomy(r)
	if a.RM.OfferedBytes != 24*gib {
		t.Errorf("offered %d GiB, want the two nodes there throughout", a.RM.OfferedBytes/gib)
	}
	svg := anatomySVG(a, anatLinks{Finding: func(int) string { return "" }, Ref: func(string) string { return "" }})
	for _, want := range []string{"12.0 GiB free, but the node went away 1 min 19 s into the run", "12.0 GiB free, but the node joined 3 min 41 s into the run"} {
		if !strings.Contains(svg, want) {
			t.Errorf("diagram lacks %q", want)
		}
	}
	if strings.Count(svg, "room for") != 0 { // the other nodes are full
		t.Errorf("room claimed on a node that was not there: %d claims", strings.Count(svg, "room for"))
	}
}
