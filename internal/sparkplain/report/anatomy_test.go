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

// The explorer carries the same diagram, linked to its own pages: an
// executor opens At a glance with that executor drawn in full.
func TestExplorerCarriesAnatomy(t *testing.T) {
	t.Parallel()
	r, x := buildWithExplorer(t, "application_1790380000000_0042")
	a, _ := embedded(t, renderExplorer(t, r, x))["anatomy"].(string)
	if !strings.Contains(a, `<svg class="anat"`) || !strings.Contains(a, `href="#anatomy/`) {
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
			drawChip(&b, &anatomy{}, anatExec{ID: id, Cores: 2, Heap: 10 * gib, PeakHeap: 6 * gib, Kind: kind}, 3*gib, 0, 0, noLinks)
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
	// They ran nothing of the application, so they add up in the box of
	// unused nodes, which says they were not there throughout.
	svg := anatomySVG(a, anatLinks{Finding: func(int) string { return "" }, Ref: func(string) string { return "" }})
	for _, want := range []string{"Not used by this application: 2 worker nodes", "2 task nodes (m5.xlarge)", "1 went away and 1 joined while the application ran"} {
		if !strings.Contains(svg, want) {
			t.Errorf("diagram lacks %q", want)
		}
	}
	if strings.Count(svg, "room for") != 0 { // the other nodes are full
		t.Errorf("room claimed on a node that was not there: %d claims", strings.Count(svg, "room for"))
	}
}

// Each executor says what it was given, and the diagram has a key for every
// colour and mark it uses, and none for what it does not.
func TestAnatomyShowsExecutorSizeAndKey(t *testing.T) {
	gib := int64(1 << 30)
	n := &anatNode{Name: "ip-10-0-0-2", YARNBytes: 12 * gib, YARNCores: 4, DriverBytes: 2 * gib, ExecBytes: 3 * gib, AtOnce: 2,
		Execs: []anatExec{{ID: "1", Cores: 2, Heap: 2 * gib, PeakHeap: gib}, {ID: "2", Cores: 2, Heap: 2 * gib, PeakHeap: gib}}}
	svg := anatomySVG(&anatomy{Cluster: "c", Nodes: []*anatNode{n}}, noLinks)
	for _, want := range []string{"2 cores · 3.0 GiB container", ">Executor 3.0 GiB<", "Driver&#39;s container", "Executor container", "Free YARN memory",
		"Executor heap: fill is its peak", "One core each; darker is busier"} {
		if !strings.Contains(svg, want) {
			t.Errorf("diagram lacks %q", want)
		}
	}
	if strings.Contains(svg, "killed or lost") {
		t.Error("key explains red outlines, but no executor was killed or lost")
	}
	n.Execs[1].Kind = model.RemovalLost
	if svg := anatomySVG(&anatomy{Cluster: "c", Nodes: []*anatNode{n}}, noLinks); !strings.Contains(svg, "Red outline: executor killed or lost") {
		t.Error("a lost executor, but no key for its red outline")
	}
}

// On a shared cluster most worker nodes run nothing of the application:
// those it used get a card each (not folded into "more like it"), and the
// rest one box counting them by role and type, with what they offered
// YARN and the findings about them.
func TestAnatomyFoldsUnusedNodes(t *testing.T) {
	t.Parallel()
	r := anatReport()
	for i := 4; i <= 13; i++ {
		role, typ := "CORE", "r5.4xlarge"
		if i > 10 {
			role, typ = "TASK", "m5.2xlarge"
		}
		r.Nodes.Hosts = append(r.Nodes.Hosts, model.Host{Name: "ip-10-0-0-" + string(rune('0'+i%10)) + string(rune('a'+i)) + ".internal",
			Instance: &model.Instance{ID: "i-x", Role: role, Type: typ}, YARNMemoryBytes: 100 * gib, YARNVCores: 16})
	}
	r.Findings = []model.Finding{{Rule: "idle-nodes", Severity: model.Info, Title: "idle", Evidence: []model.Evidence{{Text: r.Nodes.Hosts[5].Name + " ran no executors"}}}}
	a := buildAnatomy(r)
	if len(a.Nodes) != 2 || a.Nodes[0].Alike != 0 || a.Nodes[1].Alike != 0 {
		t.Fatalf("nodes the application used: %+v", a.Nodes)
	}
	u := a.Unused
	if u == nil || u.Count != 10 || strings.Join(u.Groups, " · ") != "7 core nodes (r5.4xlarge) · 3 task nodes (m5.2xlarge)" ||
		u.YARNBytes != 1000*gib || u.YARNCores != 160 || len(u.Badges) != 1 {
		t.Fatalf("unused = %+v", u)
	}
	svg := anatomySVG(a, anatLinks{Finding: func(int) string { return "" }, Ref: func(string) string { return "" }})
	for _, want := range []string{"Not used by this application: 10 worker nodes", "7 core nodes (r5.4xlarge) · 3 task nodes (m5.2xlarge)", "Their NodeManagers offered 1000 GiB and 160 vcores."} {
		if !strings.Contains(svg, want) {
			t.Errorf("diagram lacks %q", want)
		}
	}
	// With nothing saying which nodes ran the application, every node is
	// drawn as before.
	r = anatReport()
	r.Executors.Executors, r.Nodes.Hosts[1].DriverContainerBytes, r.Nodes.Hosts[2].Executors = nil, 0, nil
	if a = buildAnatomy(r); a.Unused != nil || len(a.Nodes) != 2 {
		t.Errorf("no executors known: unused %+v, %d nodes", a.Unused, len(a.Nodes))
	}
}

// The explorer can draw any executor in full, not only the one the
// diagram picks: a panel per executor, with its own peaks, cores and the
// findings about it; none when the executors' size is not known.
func TestAnatomyPanelPerExecutor(t *testing.T) {
	t.Parallel()
	r := anatReport()
	r.Executors.Executors = append(r.Executors.Executors, &model.Executor{ID: "2", Host: "ip-10-0-0-3.internal", Cores: 2})
	r.Memory.Executors = append(r.Memory.Executors, model.ExecMemory{ID: "2", Host: "ip-10-0-0-3.internal", HeapBytes: 9486 << 20, PeakHeap: 8 * gib})
	r.Findings = []model.Finding{{Rule: "out-of-memory", Severity: model.Critical, Title: "oom", Evidence: []model.Evidence{{Ref: "executor:2"}}}}
	a := buildAnatomy(r)
	ps := execPanels(r, a, anatLinks{Finding: func(int) string { return "#f" }, Ref: func(string) string { return "" }})
	if len(ps) != 2 {
		t.Fatalf("%d panels, want one per executor", len(ps))
	}
	p := ps["2"]
	for _, want := range []string{`<svg class="anat"`, "Inside executor 2", "peak heap 8.0 GiB (86%)", "2 cores", `data-finding="1"`} {
		if !strings.Contains(p, want) {
			t.Errorf("executor 2's panel lacks %q", want)
		}
	}
	if strings.Contains(ps["1"], `data-finding="1"`) || !strings.Contains(ps["1"], "peak heap 4.0 GiB") {
		t.Error("executor 1's panel should show its own peak and no badge of executor 2's finding")
	}
	r.Memory.Config.HeapBytes = 0
	if ps := execPanels(r, buildAnatomy(r), anatLinks{Finding: func(int) string { return "" }, Ref: func(string) string { return "" }}); ps != nil {
		t.Errorf("no executor size, but %d panels", len(ps))
	}
}
