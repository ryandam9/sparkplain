package report

import (
	"fmt"
	"html/template"
	"math"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// The anatomy diagram draws the run the way its parts nest: the cluster,
// its nodes, what each NodeManager offered YARN, the containers placed in
// it, and inside an executor the JVM's memory regions and cores. Container
// widths are to scale within a node, so space that cannot hold another
// executor looks too small for one. Findings are pinned as numbered badges
// on the part they are about. It is built from values the report already
// has; nothing is estimated.

// anatomy is the diagram's content, independent of drawing.
type anatomy struct {
	Cluster     string // "EMR cluster name (j-…) · emr-7.3.0", or "" when not read
	ClusterNote string // why cluster detail is missing
	RM          anatRM
	Primary     *anatNode
	Nodes       []*anatNode // worker nodes shown
	Folded      string      // "and 22 more nodes: …" when there were too many
	// Unused adds up the worker nodes that ran nothing of this
	// application, when the run says which did: on a shared cluster they
	// are most of it, and one box says more than a card each.
	Unused   *anatUnused
	Detail   *anatJVM // one executor drawn in full
	Driver   *anatJVM
	Badges   []anatBadge
	Unpinned []int // findings not about placement (1-based)
	PeakNote string
	Shared   bool // other applications ran on the cluster too
	// NoCapacity says why YARN's capacity is missing, on up to two lines,
	// and NoNodeCapacity in short on each node.
	NoCapacity     []string
	NoNodeCapacity string
	NoCPU          string // why a node's CPU is missing

	detailBadges []int // findings for the executor drawn in full
}

// anatUnused is the worker nodes this application did not use.
type anatUnused struct {
	Count     int
	Groups    []string // "10 core nodes (r5.4xlarge)", largest first
	YARNBytes int64    // what their NodeManagers offered, when known
	YARNCores int
	Known     int // nodes whose offer is known
	Badges    []int
	// Left and Joined count the nodes that went away or joined while the
	// application ran, so their offer was not there throughout.
	Left, Joined int
}

// used reports whether the node ran any of the application's containers.
func (n *anatNode) used() bool { return len(n.Execs) > 0 || n.DriverBytes > 0 || n.ClientDriver }

type anatRM struct {
	Known        bool
	OfferedBytes int64
	OfferedCores int
	HeldBytes    int64 // this application's containers at its busiest
	HeldCores    int   // its executors' vCPUs then (the driver's are not known)
	// Sizes are the worker nodes behind the totals, grouped by what each
	// offered YARN: "2 worker nodes × 6.0 GiB and 4 vCPUs each".
	Sizes   []string
	Waiting string
	Apps    string
	Badges  []int
}

// rmWaitRE and rmAppsRE read the CloudWatch summary's facts: "5 at most,
// for 1 min 0 s" (containers waiting) and "2 at most" (applications).
var (
	rmWaitRE = regexp.MustCompile(`^(\d+) at most, for (.+)$`)
	rmAppsRE = regexp.MustCompile(`^(\d+) at most$`)
)

// rmLines says in plain words what the cluster had and what this
// application took of it, one fact per line.
func rmLines(rm anatRM) []string {
	var out []string
	if rm.Known {
		have := fmt.Sprintf("The cluster had %s of memory and %d vCPUs", model.Bytes(rm.OfferedBytes), rm.OfferedCores)
		if len(rm.Sizes) > 0 {
			have += ": " + strings.Join(rm.Sizes, ", and ") + "."
		} else {
			have += "."
		}
		out = append(out, have,
			"What each node gives YARN is set by yarn.nodemanager.resource.memory-mb and yarn.nodemanager.resource.cpu-vcores.")
		used := fmt.Sprintf("At its busiest, this application used %s (%.0f%%)", model.Bytes(rm.HeldBytes), 100*float64(rm.HeldBytes)/float64(rm.OfferedBytes))
		if rm.HeldCores > 0 && rm.OfferedCores > 0 {
			used += fmt.Sprintf(" and %d of the %d vCPUs", rm.HeldCores, rm.OfferedCores)
		}
		if left := rm.OfferedBytes - rm.HeldBytes; left > 0 {
			used += "; " + model.Bytes(left) + " was left."
		} else {
			used += "; no memory was left."
		}
		out = append(out, used)
	}
	shared := false
	if m := rmAppsRE.FindStringSubmatch(rm.Apps); m != nil {
		n, _ := strconv.Atoi(m[1])
		shared = n > 1
	}
	if m := rmWaitRE.FindStringSubmatch(rm.Waiting); m != nil {
		switch n, _ := strconv.Atoi(m[1]); {
		case n == 0:
			out = append(out, "Nothing had to wait for room.")
		case shared:
			out = append(out, fmt.Sprintf("Up to %s had to wait for room, for %s in all; some may have been other applications'.", model.Plural(n, "container", "containers"), m[2]))
		default:
			out = append(out, fmt.Sprintf("Up to %s had to wait for room, for %s in all.", model.Plural(n, "container", "containers"), m[2]))
		}
	}
	if m := rmAppsRE.FindStringSubmatch(rm.Apps); m != nil {
		if n, _ := strconv.Atoi(m[1]); n > 1 {
			out = append(out, fmt.Sprintf("Up to %s ran at the same time, so this one shared the cluster.", model.Plural(n-1, "other application", "other applications")))
		} else {
			out = append(out, "No other application ran at the same time, so this one had the cluster to itself.")
		}
	}
	return out
}

type anatNode struct {
	Name, Short     string
	Role, Type      string
	Market          string
	VCPU            int
	MemBytes        int64
	YARNBytes       int64
	YARNCores       int
	DriverBytes     int64 // the driver's (application master's) container here
	ClientDriver    bool  // the driver ran here outside YARN
	ExecBytes       int64 // each executor container
	AtOnce          int   // most executors alive here at once
	Execs           []anatExec
	CPUAvg, CPUPeak float64
	CPU             []float64 // CloudWatch average points while it ran
	HasCPU          bool
	Badges          []int
	Alike           int // more nodes like this one, drawn as this card
	// Away says the node was not there for most of the run ("went away
	// 1 min 19 s into the run"), so its free space was never on offer.
	Away string
}

// shape is what makes two nodes look alike in the diagram; nodes that
// stand out (the driver's, those with findings or lost executors) have none.
func (n *anatNode) shape() string {
	if nodeRank(n) < 2 {
		return ""
	}
	return fmt.Sprintf("%s|%s|%d|%d|%d|%d|%s", n.Role, n.Type, n.YARNBytes, n.ExecBytes, len(n.Execs), n.AtOnce, n.Away)
}

// free is the YARN memory this application left unused on the node at its
// busiest, and fits whether another executor would have fitted in it.
func (n *anatNode) free() (int64, bool) {
	if n.YARNBytes == 0 {
		return 0, false
	}
	f := n.YARNBytes - n.DriverBytes - int64(n.AtOnce)*n.ExecBytes
	return max(f, 0), n.ExecBytes > 0 && f >= n.ExecBytes
}

type anatExec struct {
	ID       string
	Cores    int
	Heap     int64
	PeakHeap int64
	CPUShare float64
	Kind     string // model.Removal* kinds, "" when it ran to the end
	Reason   string
	Tasks    int64
	Badges   []int
}

func (x anatExec) bad() bool {
	return x.Kind == model.RemovalMemoryKill || x.Kind == model.RemovalLost || x.Kind == model.RemovalDecommissioned
}

// anatJVM is one JVM drawn region by region.
type anatJVM struct {
	Title, Why        string
	Container         int64
	Overhead          int64
	Heap              int64
	PySpark, OffHeap  int64
	MemoryFraction    float64
	StorageFraction   float64
	PeakHeap          int64
	PeakExecution     int64
	PeakStorage       int64
	PeakPython        int64
	Cores             int
	CPUShare, GCShare float64
	Badges            []int
	Href              string
}

type anatBadge struct {
	N     int
	Sev   model.Severity
	Title string
}

// Limits that keep the diagram readable on big clusters.
const (
	anatMaxNodes    = 24
	anatReservedMiB = 300 // Spark's reserved heap
)

// noCapacity says why the logs did not give a node's YARN capacity: no
// cluster to read them from, or a running cluster whose NodeManager and
// ResourceManager logs had not reached S3 (EMR copies them from time to
// time while it runs; on the phase 4 test cluster a node's current
// NodeManager log was still missing half an hour after it started).
func noCapacity(r *model.Report) (full []string, node string) {
	if r.Cluster == nil {
		return []string{"YARN's capacity is not known: it needs the EMR API and the node logs (-cluster-id)."}, "YARN capacity not known (needs -cluster-id)"
	}
	if st := r.Cluster.State; st != "" && !strings.HasPrefix(st, "TERMINAT") {
		return []string{"YARN's capacity is not in the node logs yet: EMR copies them to S3 from time to",
			"time while the cluster runs. Run sparkplain again later, or once the cluster has ended."}, "YARN capacity not in S3 yet (the cluster is still running)"
	}
	return []string{"YARN's capacity is not known: the node logs read did not record it."}, "YARN capacity not in its logs"
}

// away says when a node that was not up for most of the run left or
// joined it, as time into the run, or "" when it was there throughout.
// Spot nodes come and go: on the phase 4 test cluster one went away 80 s
// into a run and its replacement joined 36 s before the end.
func away(in model.Instance, start, end time.Time) string {
	if start.IsZero() || end.IsZero() || !end.After(start) {
		return ""
	}
	if !in.Ended.IsZero() && in.Ended.After(start) && in.Ended.Before(end) {
		return "went away " + model.Duration(in.Ended.Sub(start).Milliseconds()) + " into the run"
	}
	from := in.Ready
	if from.IsZero() {
		from = in.Created
	}
	if from.After(start.Add(end.Sub(start) / 2)) {
		if from.After(end) {
			return "joined after the run"
		}
		return "joined " + model.Duration(from.Sub(start).Milliseconds()) + " into the run"
	}
	return ""
}

func buildAnatomy(r *model.Report) *anatomy {
	a := &anatomy{}
	if len(r.Nodes.Hosts) == 0 && r.Cluster == nil {
		return nil
	}
	if c := r.Cluster; c != nil {
		name := c.Name
		if name == "" {
			name = c.ID
		} else if c.ID != "" {
			name += " (" + c.ID + ")"
		}
		a.Cluster = "EMR cluster " + name
		if c.Release != "" {
			a.Cluster += " · " + c.Release
		}
	} else {
		a.ClusterNote = "Instance types and YARN's capacity need the EMR API: pass -cluster-id with -profile."
	}
	a.NoCapacity, a.NoNodeCapacity = noCapacity(r)
	a.NoCPU = "CPU not known (needs CloudWatch)"
	if r.Metrics != nil && r.Metrics.Coverage != model.NoData {
		a.NoCPU = "CPU not recorded while the application ran"
	}

	// Executors, with their memory and CPU.
	mem := map[string]model.ExecMemory{}
	for _, m := range r.Memory.Executors {
		mem[m.ID] = m
	}
	cpu := map[string]float64{}
	for _, u := range r.CPU.Executors {
		cpu[u.ID] = u.Share
	}
	byHost := map[string][]anatExec{}
	for _, x := range r.Executors.Executors {
		if x == nil || x.ID == "driver" {
			continue
		}
		m := mem[x.ID]
		heap := m.HeapBytes
		if heap == 0 {
			heap = r.Memory.Config.HeapBytes
		}
		byHost[x.Host] = append(byHost[x.Host], anatExec{ID: x.ID, Cores: x.Cores, Heap: heap, PeakHeap: max(m.PeakHeap, x.Peak.JVMHeap),
			CPUShare: cpu[x.ID], Kind: x.RemovalKind, Reason: x.RemovedReason, Tasks: x.Tasks.Tasks})
	}
	driverHost := ""
	if d := r.Executors.Driver; d != nil {
		driverHost = d.Host
	}

	for _, h := range r.Nodes.Hosts {
		n := &anatNode{Name: h.Name, Short: shortHost(h.Name), YARNBytes: h.YARNMemoryBytes, YARNCores: h.YARNVCores,
			DriverBytes: h.DriverContainerBytes, ExecBytes: h.ExecutorContainerBytes, AtOnce: h.PeakExecutors, Execs: byHost[h.Name]}
		if n.ExecBytes == 0 && len(n.Execs) > 0 {
			n.ExecBytes = r.Memory.Config.ContainerBytes
		}
		if n.AtOnce == 0 {
			n.AtOnce = len(n.Execs)
		}
		sort.Slice(n.Execs, func(i, j int) bool { return idLess(n.Execs[i].ID, n.Execs[j].ID) })
		if in := h.Instance; in != nil {
			n.Role, n.Type, n.VCPU, n.MemBytes = in.Role, in.Type, in.VCPU, in.MemoryBytes
			if in.Market == "SPOT" {
				n.Market = "spot"
			}
			n.CPU = cpuPoints(r, in.ID)
			n.Away = away(*in, r.Application.Start, r.Application.End)
		}
		if h.HostCPU != nil {
			n.CPUAvg, n.CPUPeak, n.HasCPU = h.HostCPU.Average, h.HostCPU.Peak, true
		}
		if h.Name == driverHost && n.DriverBytes == 0 {
			n.ClientDriver = true
		}
		if n.Role == "MASTER" && a.Primary == nil {
			a.Primary = n
			continue
		}
		a.Nodes = append(a.Nodes, n)
	}

	// Every node is judged against the application's executor size, so a
	// node that ran none still says whether one would have fitted.
	execSize := r.Memory.Config.ContainerBytes
	for _, n := range a.Nodes {
		execSize = max(execSize, n.ExecBytes)
	}
	for _, n := range a.Nodes {
		if n.ExecBytes == 0 {
			n.ExecBytes = execSize
		}
	}

	// The ResourceManager: YARN's capacity across the workers, and what
	// this application held at its busiest.
	type size struct {
		b int64
		c int
	}
	sizes, order := map[size]int{}, []size{}
	for _, n := range append([]*anatNode{a.Primary}, a.Nodes...) {
		if n == nil {
			continue
		}
		if n.Away == "" {
			a.RM.OfferedBytes += n.YARNBytes
			a.RM.OfferedCores += n.YARNCores
			if n.YARNBytes > 0 {
				k := size{n.YARNBytes, n.YARNCores}
				if sizes[k] == 0 {
					order = append(order, k)
				}
				sizes[k]++
			}
		}
		a.RM.HeldBytes += n.DriverBytes + int64(n.AtOnce)*n.ExecBytes
		if len(n.Execs) > 0 {
			a.RM.HeldCores += n.AtOnce * n.Execs[0].Cores
		}
	}
	a.RM.Known = a.RM.OfferedBytes > 0
	for i, k := range order {
		what := model.Plural(sizes[k], "node", "nodes")
		if i == 0 {
			what = model.Plural(sizes[k], "worker node", "worker nodes")
		}
		each := ""
		if sizes[k] > 1 {
			each = " each"
		}
		a.RM.Sizes = append(a.RM.Sizes, fmt.Sprintf("%s × %s and %d vCPUs%s", what, model.Bytes(k.b), k.c, each))
	}
	if m := r.Metrics; m != nil {
		for _, f := range m.Summary {
			switch f.Label {
			case "Containers waiting":
				a.RM.Waiting = f.Value
			case "Applications at once":
				a.RM.Apps = f.Value
				a.Shared = !strings.HasPrefix(f.Value, "1 ")
			}
		}
	}
	if r.Executors.Peak > 0 {
		a.PeakNote = fmt.Sprintf("Drawn at the application's busiest moment: %s alive at once, of %d started.", model.Plural(r.Executors.Peak, "executor", "executors"), r.Executors.Started)
	}

	// Nodes in a useful order: the driver's, those with findings or lost
	// executors, then by name; past the limit the rest fold into one line.
	pinFindings(a, r)
	// When the run says which nodes it used, those are drawn each in
	// full, and the rest add up to one box.
	if slices.ContainsFunc(a.Nodes, (*anatNode).used) {
		var kept, unused []*anatNode
		for _, n := range a.Nodes {
			if n.used() {
				kept = append(kept, n)
			} else {
				unused = append(unused, n)
			}
		}
		a.Nodes, a.Unused = kept, unusedNodes(unused)
	}
	// In executor order, as a reader counts them: the node that ran
	// executor 1 first, then the node of the next executor not yet shown,
	// and so on; a node that ran only the driver before them, and nodes
	// that ran nothing after.
	sort.SliceStable(a.Nodes, func(i, j int) bool {
		gi, fi := nodeOrder(a.Nodes[i])
		gj, fj := nodeOrder(a.Nodes[j])
		switch {
		case gi != gj:
			return gi < gj
		case fi != fj:
			return idLess(fi, fj)
		}
		return a.Nodes[i].Name < a.Nodes[j].Name
	})
	// Nodes that look alike collapse into one card, so a big uniform
	// cluster reads as "25 nodes like this" instead of a wall of copies;
	// not the nodes the application used, which are each worth a card.
	if len(a.Nodes) > 4 && a.Unused == nil {
		first := map[string]*anatNode{}
		var kept []*anatNode
		for _, n := range a.Nodes {
			k := n.shape()
			if f := first[k]; k != "" && f != nil {
				f.Alike++
				continue
			}
			first[k] = n
			kept = append(kept, n)
		}
		a.Nodes = kept
	}
	if len(a.Nodes) > anatMaxNodes {
		rest := a.Nodes[anatMaxNodes:]
		a.Nodes = a.Nodes[:anatMaxNodes]
		kinds := map[string]int{}
		execs := 0
		for _, n := range rest {
			k := strings.TrimSpace(strings.ToLower(n.Role) + " " + n.Type)
			if k == "" {
				k = "node"
			}
			kinds[k]++
			execs += len(n.Execs)
		}
		var parts []string
		for k, c := range kinds {
			parts = append(parts, fmt.Sprintf("%d × %s", c, k))
		}
		sort.Strings(parts)
		a.Folded = fmt.Sprintf("and %d more nodes (%s) with %s, like those above", len(rest), strings.Join(parts, ", "), model.Plural(execs, "executor", "executors"))
	}

	a.Detail = detailJVM(r, a)
	if c := r.Memory.Config; c.DriverHeapBytes > 0 {
		d := &anatJVM{Title: "The driver", Heap: c.DriverHeapBytes, MemoryFraction: c.MemoryFraction, StorageFraction: c.StorageFraction}
		for _, n := range append([]*anatNode{a.Primary}, a.Nodes...) {
			if n != nil && n.DriverBytes > 0 {
				d.Container = n.DriverBytes
			}
		}
		if d.Container > d.Heap {
			d.Overhead = d.Container - d.Heap
		}
		if m := r.Memory.Driver; m != nil {
			d.PeakHeap, d.PeakExecution, d.PeakStorage = m.PeakHeap, m.PeakExecution, m.PeakStorage
		}
		d.Why = "It plans the work and collects results; it runs no tasks."
		a.Driver = d
	}
	return a
}

// unusedNodes adds up the worker nodes the application did not use, by
// role and instance type.
func unusedNodes(ns []*anatNode) *anatUnused {
	if len(ns) == 0 {
		return nil
	}
	u := &anatUnused{Count: len(ns)}
	type group struct {
		role, typ string
		n         int
	}
	by := map[[2]string]*group{}
	for _, n := range ns {
		k := [2]string{strings.ToLower(n.Role), n.Type}
		if by[k] == nil {
			by[k] = &group{role: k[0], typ: k[1]}
		}
		by[k].n++
		switch {
		case strings.HasPrefix(n.Away, "went away"):
			u.Left++
		case strings.HasPrefix(n.Away, "joined"):
			u.Joined++
		}
		if n.YARNBytes > 0 && n.Away == "" {
			u.YARNBytes += n.YARNBytes
			u.YARNCores += n.YARNCores
			u.Known++
		}
		for _, b := range n.Badges {
			if !hasInt(u.Badges, b) {
				u.Badges = append(u.Badges, b)
			}
		}
	}
	gs := make([]*group, 0, len(by))
	for _, g := range by {
		gs = append(gs, g)
	}
	sort.Slice(gs, func(i, j int) bool {
		if gs[i].n != gs[j].n {
			return gs[i].n > gs[j].n
		}
		return gs[i].role+gs[i].typ < gs[j].role+gs[j].typ
	})
	for _, g := range gs {
		what := map[string]string{"core": "core node", "task": "task node", "master": "primary node"}[g.role]
		if what == "" {
			what = "node"
		}
		s := model.Plural(g.n, what, what+"s")
		if g.typ != "" {
			s += " (" + g.typ + ")"
		}
		u.Groups = append(u.Groups, s)
	}
	sort.Ints(u.Badges)
	return u
}

// detailJVM picks the executor worth drawing in full: one killed for
// memory, else the one with the highest heap.
func detailJVM(r *model.Report, a *anatomy) *anatJVM {
	c := r.Memory.Config
	if c.HeapBytes == 0 {
		return nil
	}
	var pick *model.ExecMemory
	why := ""
	killed := map[string]bool{}
	for _, x := range r.Executors.Executors {
		if x != nil && x.RemovalKind == model.RemovalMemoryKill {
			killed[x.ID] = true
		}
	}
	for i := range r.Memory.Executors {
		m := &r.Memory.Executors[i]
		switch {
		case killed[m.ID] && (pick == nil || !killed[pick.ID]):
			pick, why = m, "the one killed for using too much memory"
		case pick == nil || (!killed[pick.ID] && m.PeakHeap > pick.PeakHeap):
			pick, why = m, "the one with the highest heap"
		}
	}
	if pick == nil {
		j := configJVM(r)
		j.Title = "An executor as configured (none started)"
		return j
	}
	j := execJVM(r, pick.ID)
	if len(r.Memory.Executors) > 1 {
		j.Why = why
	}
	return j
}

// configJVM is an executor as configured: its container, heap and
// fractions, with no peaks.
func configJVM(r *model.Report) *anatJVM {
	c := r.Memory.Config
	j := &anatJVM{Container: c.ContainerBytes, Overhead: c.OverheadBytes, Heap: c.HeapBytes, PySpark: c.PySparkBytes, OffHeap: c.OffHeapBytes,
		MemoryFraction: c.MemoryFraction, StorageFraction: c.StorageFraction, Cores: c.Cores, GCShare: r.Memory.GCShare}
	if j.Container == 0 {
		j.Container = j.Heap + j.Overhead + j.PySpark + j.OffHeap
	}
	return j
}

// execJVM is executor id region by region: its own heap when it differs
// from the configuration, and how far each region peaked.
func execJVM(r *model.Report, id string) *anatJVM {
	j := configJVM(r)
	j.Title, j.Href = "Inside executor "+id, "executor:"+id
	j.GCShare = 0
	for _, m := range r.Memory.Executors {
		if m.ID == id {
			if m.HeapBytes > 0 && m.HeapBytes != j.Heap {
				j.Container += m.HeapBytes - j.Heap
				j.Heap = m.HeapBytes
			}
			j.PeakHeap, j.PeakExecution, j.PeakStorage, j.GCShare = m.PeakHeap, m.PeakExecution, m.PeakStorage, m.GCShare
		}
	}
	for _, x := range r.Executors.Executors {
		if x != nil && x.ID == id {
			j.PeakPython = x.Peak.ProcessPythonRSS
			if x.Cores > 0 {
				j.Cores = x.Cores
			}
		}
	}
	for _, u := range r.CPU.Executors {
		if u.ID == id {
			j.CPUShare = u.Share
		}
	}
	return j
}

// maxExecPanels caps the executors the explorer draws a panel for.
const maxExecPanels = 300

// execPanels draws, for the explorer, each executor region by region as
// the diagram draws one: a panel per executor, by ID, with the badges of
// the findings about it. Nil when the executors' size is not known.
func execPanels(r *model.Report, a *anatomy, l anatLinks) map[string]string {
	if a == nil || r.Memory.Config.HeapBytes == 0 {
		return nil
	}
	badges := map[string][]int{}
	for _, n := range append(append([]*anatNode{}, a.Nodes...), a.Primary) {
		if n == nil {
			continue
		}
		for _, x := range n.Execs {
			badges[x.ID] = x.Badges
		}
	}
	out := map[string]string{}
	for _, x := range r.Executors.Executors {
		if x == nil || x.ID == "driver" || len(out) == maxExecPanels {
			continue
		}
		j := execJVM(r, x.ID)
		j.Badges = badges[x.ID]
		body := &svgw{}
		h := drawJVM(body, a, j, anPad, l) + anPad
		out[x.ID] = fmt.Sprintf(`<svg class="anat" viewBox="0 0 %.0f %.0f" role="img" aria-label="Inside executor %s: its container and Java heap region by region">`, anW, h, esc(x.ID)) + body.String() + `</svg>`
	}
	return out
}

// pinFindings numbers the findings and pins each to the parts it is about.
func pinFindings(a *anatomy, r *model.Report) {
	execAt := map[string]*anatExec{}
	nodes := append([]*anatNode{}, a.Nodes...)
	if a.Primary != nil {
		nodes = append(nodes, a.Primary)
	}
	for _, n := range nodes {
		for i := range n.Execs {
			execAt[n.Execs[i].ID] = &n.Execs[i]
		}
	}
	var detail []int
	for i, f := range r.Findings {
		num := i + 1
		a.Badges = append(a.Badges, anatBadge{N: num, Sev: f.Severity, Title: f.Title})
		pinned := false
		for _, e := range f.Evidence {
			if id, ok := strings.CutPrefix(e.Ref, "executor:"); ok {
				if x := execAt[id]; x != nil && !hasInt(x.Badges, num) {
					x.Badges = append(x.Badges, num)
					pinned = true
				}
			}
			if host, ok := strings.CutPrefix(e.Ref, "node:"); ok {
				for _, n := range nodes {
					if (n.Name == host || n.Short == shortHost(host)) && !hasInt(n.Badges, num) {
						n.Badges = append(n.Badges, num)
						pinned = true
					}
				}
			}
		}
		switch f.Rule {
		case "waited-for-capacity", "shared-cluster":
			a.RM.Badges = append(a.RM.Badges, num)
			continue
		case "host-cpu-saturated", "host-memory-pressure", "idle-nodes", "spot-interrupted", "executor-fit", "executor-decommissioned":
			for _, n := range nodes {
				if mentions(f, n) && !hasInt(n.Badges, num) {
					n.Badges = append(n.Badges, num)
					pinned = true
				}
			}
			if f.Rule == "executor-fit" {
				detail = append(detail, num)
				pinned = true
			}
		case "memory-gc-pressure", "memory-heap-near-limit", "memory-over-provisioned", "out-of-memory", "memory-spill", "executor-memory-kill", "cpu-low", "cpu-idle-executors":
			// Also on the executor drawn in full: its chip may be folded
			// into a card of look-alike nodes.
			detail = append(detail, num)
			pinned = true
		}
		if !pinned {
			a.Unpinned = append(a.Unpinned, num)
		}
	}
	a.detailBadges = detail
}

// pinDetail puts the executor-level findings on the executor drawn in
// full, or, with none drawn, lists those no other part shows.
func (a *anatomy) pinDetail() {
	if a.Detail != nil {
		a.Detail.Badges = a.detailBadges
		return
	}
	shown := map[int]bool{}
	for _, n := range append(append([]*anatNode{}, a.Nodes...), a.Primary) {
		if n == nil {
			continue
		}
		for _, b := range n.Badges {
			shown[b] = true
		}
		for _, x := range n.Execs {
			for _, b := range x.Badges {
				shown[b] = true
			}
		}
	}
	for _, b := range a.detailBadges {
		if !shown[b] && !hasInt(a.Unpinned, b) {
			a.Unpinned = append(a.Unpinned, b)
		}
	}
	a.detailBadges = nil
	sort.Ints(a.Unpinned)
}

// mentions reports whether a finding's evidence names the node.
func mentions(f model.Finding, n *anatNode) bool {
	for _, e := range f.Evidence {
		if strings.Contains(e.Text, n.Name) || (n.Short != "" && strings.Contains(e.Text, n.Short+".")) {
			return true
		}
	}
	return false
}

// nodeOrder is where a node goes in the diagram: its group (0 ran only
// the driver, 1 ran executors, 2 ran nothing of this application) and,
// for group 1, its lowest executor ID.
func nodeOrder(n *anatNode) (int, string) {
	if len(n.Execs) == 0 {
		if n.DriverBytes > 0 || n.ClientDriver {
			return 0, ""
		}
		return 2, ""
	}
	first := n.Execs[0].ID
	for _, x := range n.Execs[1:] {
		if idLess(x.ID, first) {
			first = x.ID
		}
	}
	return 1, first
}

func nodeRank(n *anatNode) int {
	switch {
	case n.DriverBytes > 0 || n.ClientDriver:
		return 0
	case len(n.Badges) > 0:
		return 1
	case len(n.Execs) == 0:
		return 3
	}
	for _, x := range n.Execs {
		if x.bad() {
			return 1
		}
	}
	return 2
}

func cpuPoints(r *model.Report, instance string) []float64 {
	if r.Metrics == nil {
		return nil
	}
	start, end := r.Application.Start, r.Application.End
	for _, s := range r.Metrics.Hosts {
		if s.Name != "CPUUtilization" || s.Stat != "Average" || s.Scope != instance {
			continue
		}
		var out []float64
		for _, p := range s.Points {
			if !start.IsZero() && (p.T.Before(start.Truncate(5*time.Minute)) || (!end.IsZero() && p.T.After(end))) {
				continue
			}
			out = append(out, p.V)
		}
		return out
	}
	return nil
}

func shortHost(h string) string {
	if i := strings.IndexByte(h, '.'); i > 0 {
		return h[:i]
	}
	return h
}

func idLess(a, b string) bool {
	var x, y int
	if _, err := fmt.Sscan(a, &x); err == nil {
		if _, err := fmt.Sscan(b, &y); err == nil {
			return x < y
		}
	}
	return a < b
}

func hasInt(s []int, v int) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// ---------- drawing ----------

// anatLinks turns the diagram's references into links for the page it is
// drawn in; an empty result leaves the part unlinked.
type anatLinks struct {
	Finding func(n int) string
	Ref     func(ref string) string // "executor:3", "node:<host>"
}

const (
	anW       = 1100.0
	anPad     = 16.0
	anGap     = 14.0
	anChipW   = 168.0
	anChipH   = 76.0
	anSquare  = 20.0
	anMinWide = 900 // phones scroll the diagram rather than shrink its text
)

type svgw struct{ strings.Builder }

// anatIcons are the node card's icons, drawn as plain grey strokes so
// they label a row without adding a colour: a chip with pins for CPU, a
// memory stick with its chips and contacts for memory.
const anatIcons = `<symbol id="anat-cpu" viewBox="0 0 12 12"><rect x="3" y="3" width="6" height="6" rx="1"/><path d="M5 1v2M7 1v2M5 9v2M7 9v2M1 5h2M1 7h2M9 5h2M9 7h2"/></symbol>` +
	`<symbol id="anat-mem" viewBox="0 0 12 12"><rect x="0.6" y="2.5" width="10.8" height="6" rx="1"/><path d="M3 4.5v2M6 4.5v2M9 4.5v2M2 8.5v2M4 8.5v2M8 8.5v2M10 8.5v2"/></symbol>`

// icon draws one of anatIcons ("cpu" or "mem") with its baseline at y,
// in line with text drawn at the same y.
func (b *svgw) icon(name string, x, y float64) {
	b.f(`<use class="ico" href="#anat-%s" x="%.1f" y="%.1f" width="12" height="12"/>`, name, x, y-10)
}

func (b *svgw) f(format string, args ...any) { fmt.Fprintf(&b.Builder, format, args...) }

func (b *svgw) text(x, y float64, cls, anchor, s string) {
	if anchor == "" {
		anchor = "start"
	}
	b.f(`<text class="%s" x="%.1f" y="%.1f" text-anchor="%s">%s</text>`, cls, x, y, anchor, esc(s))
}

// textW estimates the width of s at the given font size; bold text runs
// wider. It errs on the wide side (measured in Chromium with the widest
// common fallback font: about 0.60 of the size per character, 0.63 bold) so
// labels that pass it do not overlap.
func textW(s string, size float64, bold bool) float64 {
	f := 0.6
	if bold {
		f = 0.64
	}
	return float64(len([]rune(s))) * size * f
}

// fitText cuts s to roughly fit w units at the given font size.
// wrapText breaks s into lines that fit w at the given size, at spaces.
func wrapText(s string, w, size float64) []string {
	var out []string
	line := ""
	for _, word := range strings.Fields(s) {
		next := word
		if line != "" {
			next = line + " " + word
		}
		if line != "" && textW(next, size, false) > w {
			out = append(out, line)
			next = word
		}
		line = next
	}
	if line != "" {
		out = append(out, line)
	}
	return out
}

func fitText(s string, w, size float64) string {
	n := int(w / (size * 0.56))
	return clipLabel(s, max(n, 4))
}

func (b *svgw) link(href string, draw func()) {
	if href == "" {
		draw()
		return
	}
	b.f(`<a href="%s">`, esc(href))
	draw()
	b.WriteString(`</a>`)
}

// badges draws numbered finding badges from right to left ending at x.
func (b *svgw) badges(a *anatomy, nums []int, x, y float64, l anatLinks) {
	shown := nums
	if len(shown) > 4 {
		shown = shown[:3]
	}
	for i := len(shown) - 1; i >= 0; i-- {
		n := shown[i]
		sev := model.Info
		title := ""
		if n-1 < len(a.Badges) {
			sev, title = a.Badges[n-1].Sev, a.Badges[n-1].Title
		}
		b.link(l.Finding(n), func() {
			b.f(`<g class="badge %s" data-finding="%d"><title>Finding %d: %s</title><circle cx="%.1f" cy="%.1f" r="10"/>`, sevClass(sev), n, n, esc(title), x, y)
			b.text(x, y+4, "bn", "middle", fmt.Sprint(n))
			b.WriteString(`</g>`)
		})
		x -= 24
	}
	if len(nums) > len(shown) {
		b.text(x+6, y+4, "m", "end", fmt.Sprintf("+%d", len(nums)-len(shown)))
	}
}

func sevClass(s model.Severity) string {
	return map[model.Severity]string{model.Critical: "crit", model.Warning: "warn", model.Info: "info"}[s]
}

// anatomySVG draws the diagram; it returns "" when there is nothing to draw.
func anatomySVG(a *anatomy, l anatLinks) string {
	if a == nil {
		return ""
	}
	a.pinDetail()
	var b svgw
	body := &svgw{}
	y := anPad

	// The cluster frame's header.
	head := a.Cluster
	if head == "" {
		head = "Cluster"
	}
	body.text(anPad+14, y+22, "h1", "", head)
	if a.ClusterNote != "" {
		body.text(anW-anPad-14, y+22, "m", "end", a.ClusterNote)
	}
	y += 40
	y = drawKey(body, a, y)

	// Top row: the primary node with the ResourceManager, and YARN's totals.
	// tall enough for the capacity card's lines of facts
	px, pw := anPad+14, 330.0
	rx := px + pw + anGap
	rw := anW - anPad - 14 - rx
	// the capacity card's facts, each wrapped to the card's width
	var facts [][]string
	for _, line := range rmLines(a.RM) {
		facts = append(facts, wrapText(line, rw-44, 11))
	}
	lines := 0
	for _, f := range facts {
		lines += len(f)
	}
	if !a.RM.Known {
		lines += len(a.NoCapacity)
	}
	topH := max(118.0, 62+16*float64(lines-1)+12)
	if a.Primary != nil {
		n := a.Primary
		body.link(l.Ref("node:"+n.Name), func() {
			body.f(`<g class="node" data-node="%s"><title>%s</title><rect class="nbox" x="%.1f" y="%.1f" width="%.1f" height="%.1f" rx="8"/>`, esc(n.Name), esc(n.Name), px, y, pw, topH)
			body.text(px+12, y+20, "h", "", "Primary · "+fitText(n.Short, 150, 12))
			body.text(px+12, y+37, "m", "", instanceLine(n))
			body.f(`<rect class="rm" x="%.1f" y="%.1f" width="%.1f" height="30" rx="6"/>`, px+12, y+48, pw-24)
			body.text(px+22, y+68, "b", "", "YARN ResourceManager")
			if n.ClientDriver {
				body.f(`<rect class="drv" x="%.1f" y="%.1f" width="%.1f" height="24" rx="5"/>`, px+12, y+84, pw-24)
				body.text(px+22, y+100, "b", "", "Driver (client mode, outside YARN)")
			} else if n.HasCPU {
				body.text(px+12, y+102, "m", "", fmt.Sprintf("CPU %.0f%% on average, %.0f%% at peak", n.CPUAvg, n.CPUPeak))
			}
			body.WriteString(`</g>`)
		})
	} else {
		body.f(`<rect class="nbox unknown" x="%.1f" y="%.1f" width="%.1f" height="%.1f" rx="8"/>`, px, y, pw, topH)
		body.text(px+12, y+20, "h", "", "Primary node")
		body.text(px+12, y+40, "m", "", "Runs YARN's ResourceManager, which")
		body.text(px+12, y+56, "m", "", "places every container on the workers.")
		body.text(px+12, y+80, "m", "", "Not known for this run.")
	}
	body.f(`<g class="rmpanel"><rect class="panel" x="%.1f" y="%.1f" width="%.1f" height="%.1f" rx="8"/>`, rx, y, rw, topH)
	body.text(rx+14, y+22, "h", "", "Cluster capacity used")
	ly := y + 62.0 // the first line of facts
	if a.RM.Known {
		bw := rw - 28
		held := bw * math.Min(1, float64(a.RM.HeldBytes)/float64(a.RM.OfferedBytes))
		body.f(`<rect class="free" x="%.1f" y="%.1f" width="%.1f" height="14" rx="3"/><rect class="held" x="%.1f" y="%.1f" width="%.1f" height="14" rx="3"><title>Used: %s of %s</title></rect>`,
			rx+14, y+32, bw, rx+14, y+32, held, model.Bytes(a.RM.HeldBytes), model.Bytes(a.RM.OfferedBytes))
	} else {
		for _, line := range a.NoCapacity {
			body.text(rx+14, ly-14, "m", "", line)
			ly += 16
		}
	}
	// One fact per line, as bullets.
	for _, f := range facts {
		body.text(rx+14, ly, "m", "", "•")
		for _, part := range f {
			body.text(rx+26, ly, "", "", part)
			ly += 16
		}
	}
	body.badges(a, a.RM.Badges, rx+rw-16, y+18, l)
	body.WriteString(`</g>`)
	y += topH + anGap

	// Worker nodes.
	if len(a.Nodes) > 0 {
		cols, nw := nodeCols(len(a.Nodes)), nodeWidth(len(a.Nodes))
		maxYARN := int64(0)
		for _, n := range a.Nodes {
			maxYARN = max(maxYARN, n.YARNBytes, n.DriverBytes+int64(n.AtOnce)*n.ExecBytes)
		}
		for i := 0; i < len(a.Nodes); i += cols {
			row := a.Nodes[i:min(i+cols, len(a.Nodes))]
			h := 0.0
			for _, n := range row {
				h = max(h, nodeHeight(n, nw))
			}
			for j, n := range row {
				drawNode(body, a, n, anPad+14+float64(j)*(nw+anGap), y, nw, h, maxYARN, l)
			}
			y += h + anGap
		}
		if a.Folded != "" {
			body.text(anPad+14, y+8, "m", "", a.Folded+".")
			y += 24
		}
		if a.Unused != nil {
			y = drawUnused(body, a, a.Unused, y, l) + anGap
		}
	} else {
		body.text(anPad+14, y+16, "m", "", "No worker node is known for this run: the event log names the hosts that ran executors.")
		y += 30
	}

	// The executor and the driver, region by region.
	for _, j := range []*anatJVM{a.Detail, a.Driver} {
		if j == nil || j.Heap == 0 {
			continue
		}
		y = drawJVM(body, a, j, y, l) + anGap
	}

	// Pinned findings, and those about something else.
	if len(a.Badges) > 0 {
		y = drawBadgeKey(body, a, y, l)
	}
	if a.PeakNote != "" {
		body.text(anPad+14, y+14, "m", "", a.PeakNote)
		y += 24
	}
	y += anPad

	b.f(`<svg class="anat" viewBox="0 0 %.0f %.0f" role="img" aria-label="The cluster, its nodes, the containers YARN placed on them and the memory inside an executor">`, anW, y)
	b.WriteString(`<defs><pattern id="anat-free" width="7" height="7" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect width="7" height="7" class="freebg"/><line x1="0" y1="0" x2="0" y2="7" class="hatch"/></pattern>` + anatIcons + `</defs>`)
	b.f(`<rect class="cluster" x="%.1f" y="%.1f" width="%.1f" height="%.1f" rx="12"/>`, anPad, anPad, anW-2*anPad, y-2*anPad)
	b.WriteString(body.String())
	b.WriteString(`</svg>`)
	return b.String()
}

func instanceLine(n *anatNode) string {
	var p []string
	if n.Type != "" {
		p = append(p, n.Type)
	}
	if n.VCPU > 0 {
		p = append(p, fmt.Sprintf("%d vCPU", n.VCPU))
	}
	if n.MemBytes > 0 {
		p = append(p, model.Bytes(n.MemBytes))
	}
	if n.Market != "" {
		p = append(p, n.Market)
	}
	if len(p) == 0 {
		return "Instance not known"
	}
	return strings.Join(p, " · ")
}

// Executor chips per node before they shrink to squares.
// sameCores is the executors' core count when they all have the same, else 0.
func sameCores(xs []anatExec) int {
	c := 0
	for i, x := range xs {
		if i > 0 && x.Cores != c {
			return 0
		}
		c = x.Cores
	}
	return c
}

// nodeCols is how many node boxes go in a row, for n nodes: two, three
// past six, one alone.
func nodeCols(n int) int {
	switch {
	case n == 1:
		return 1
	case n > 6:
		return 3
	}
	return 2
}

// nodeWidth is how wide each of n node boxes is drawn.
func nodeWidth(n int) float64 {
	cols := nodeCols(n)
	return (anW - 2*(anPad+14) - float64(cols-1)*anGap) / float64(cols)
}

// drawUnused draws the worker nodes the application did not use as one
// box: how many, of which roles and types, and what YARN offered on them.
func drawUnused(b *svgw, a *anatomy, u *anatUnused, y float64, l anatLinks) float64 {
	x, w := anPad+14, anW-2*anPad-28
	// the groups, on as many lines as they need
	var lines []string
	cur := ""
	for _, g := range u.Groups {
		next := g
		if cur != "" {
			next = cur + " · " + g
		}
		if cur != "" && textW(next, 12, false) > w-28 {
			lines, cur = append(lines, cur), g
			continue
		}
		cur = next
	}
	lines = append(lines, cur)
	var notes []string
	if u.Known > 0 {
		offered := fmt.Sprintf("Their NodeManagers offered %s and %d vcores", model.Bytes(u.YARNBytes), u.YARNCores)
		if u.Known < u.Count {
			offered += fmt.Sprintf(" (the %d there throughout whose offer the logs give)", u.Known)
		}
		notes = append(notes, offered+".")
	}
	var moved []string
	if u.Left > 0 {
		moved = append(moved, fmt.Sprintf("%d went away", u.Left))
	}
	if u.Joined > 0 {
		moved = append(moved, fmt.Sprintf("%d joined", u.Joined))
	}
	if len(moved) > 0 {
		notes = append(notes, strings.Join(moved, " and ")+" while the application ran.")
	}
	h := 50 + 16*float64(len(lines)+len(notes))
	b.f(`<g class="unused"><rect class="nbox idle" x="%.1f" y="%.1f" width="%.1f" height="%.1f" rx="8"/>`, x, y, w, h)
	b.text(x+12, y+20, "h", "", fmt.Sprintf("Not used by this application: %s", model.Plural(u.Count, "worker node", "worker nodes")))
	b.text(x+12, y+37, "m", "", "No executor or driver of this application ran on them; other applications may have.")
	for i, line := range lines {
		b.text(x+12, y+56+16*float64(i), "", "", line)
	}
	for i, note := range notes {
		b.text(x+12, y+56+16*float64(len(lines)+i), "m", "", note)
	}
	b.badges(a, u.Badges, x+w-16, y+18, l)
	b.WriteString(`</g>`)
	return y + h
}

// drawKey says what each colour and mark in the node boxes means, for the
// ones this diagram uses, in a row (or two) under the cluster's name.
func drawKey(b *svgw, a *anatomy, y float64) float64 {
	var drv, exe, free, chips, squares, spark, cpu, bad bool
	for _, n := range a.Nodes {
		for _, e := range n.Execs {
			bad = bad || e.bad()
		}
		drv = drv || n.DriverBytes > 0
		exe = exe || n.AtOnce > 0 && n.ExecBytes > 0
		if f, _ := n.free(); f > 0 && n.YARNBytes > 0 {
			free = true
		}
		if len(n.Execs) > 0 {
			if compact(n, nodeWidth(len(a.Nodes))) {
				squares = true
			} else {
				chips = true
			}
		}
		spark = spark || n.HasCPU && len(n.CPU) > 1
		cpu = cpu || n.HasCPU
	}
	type item struct {
		draw  func(x, y float64)
		label string
	}
	var items []item
	swatch := func(cls string) func(x, y float64) {
		return func(x, y float64) {
			b.f(`<rect class="%s" x="%.1f" y="%.1f" width="18" height="12" rx="2"/>`, cls, x, y-10)
		}
	}
	if drv {
		items = append(items, item{swatch("drv"), "Driver's container"})
	}
	if exe {
		items = append(items, item{swatch("exc"), "Executor container"})
	}
	if free {
		unused := "Free YARN memory"
		if a.Shared {
			unused = "Not used by this application"
		}
		items = append(items, item{swatch("freeh"), unused})
	}
	if chips {
		items = append(items, item{func(x, y float64) {
			b.f(`<rect class="heap" x="%.1f" y="%.1f" width="22" height="8" rx="2"/><rect class="hpeak" x="%.1f" y="%.1f" width="15" height="8" rx="2"/>`, x, y-8, x, y-8)
		}, "Executor heap: fill is its peak"})
		items = append(items, item{func(x, y float64) {
			b.f(`<rect class="core" x="%.1f" y="%.1f" width="7" height="7" rx="1.5" style="opacity:.4"/><rect class="core" x="%.1f" y="%.1f" width="7" height="7" rx="1.5"/>`, x, y-8, x+9, y-8)
		}, "One core each; the more solid, the busier"})
	}
	if squares {
		items = append(items, item{func(x, y float64) {
			b.f(`<g class="sq"><rect class="sqbox" x="%.1f" y="%.1f" width="12" height="12" rx="2"/><rect class="sqfill" x="%.1f" y="%.1f" width="12" height="7" rx="1.5"/></g>`, x, y-10, x, y-5)
		}, "Executor, when many: fill is peak heap"})
	}
	if cpu {
		items = append(items, item{func(x, y float64) {
			b.f(`<rect class="gauge" x="%.1f" y="%.1f" width="22" height="7" rx="2"/><rect class="gfill" x="%.1f" y="%.1f" width="8" height="7" rx="2"/><line class="gpeak" x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f"/>`, x, y-8, x, y-8, x+17, x+17, y-10, y+1)
		}, "Node CPU: bar to the average, tick at the peak"})
	}
	if spark {
		items = append(items, item{func(x, y float64) {
			b.f(`<polyline class="spark" points="%.1f,%.1f %.1f,%.1f %.1f,%.1f %.1f,%.1f"/>`, x, y-2, x+6, y-8, x+12, y-4, x+20, y-9)
		}, "Node CPU over the run"})
	}
	if bad {
		items = append(items, item{func(x, y float64) {
			b.f(`<rect class="bad-key" x="%.1f" y="%.1f" width="18" height="12" rx="3"/>`, x, y-10)
		}, "Red outline: executor killed or lost"})
	}
	if len(items) == 0 {
		return y
	}
	x, left, right := anPad+14, anPad+14, anW-anPad-14
	y += 4
	for _, it := range items {
		w := 26 + textW(it.label, 11, false) + 18
		if x+w > right && x > left {
			x, y = left, y+18
		}
		it.draw(x, y)
		b.text(x+26, y, "s key", "", it.label)
		x += w
	}
	return y + 16
}

func chipsPerRow(nw float64) int { return max(1, int((nw-24+8)/(anChipW+8))) }

func compact(n *anatNode, nw float64) bool { return len(n.Execs) > 2*chipsPerRow(nw) }

func nodeHeight(n *anatNode, nw float64) float64 {
	h := 128.0 // header, CPU, YARN bar and its note
	switch {
	case len(n.Execs) == 0:
		h += 26
	case compact(n, nw):
		per := max(1, int((nw-24)/(anSquare+4)))
		h += 24 + float64((len(n.Execs)+per-1)/per)*(anSquare+4)
	default:
		per := chipsPerRow(nw)
		h += 22 + float64((len(n.Execs)+per-1)/per)*(anChipH+8)
	}
	return h
}

func drawNode(b *svgw, a *anatomy, n *anatNode, x, y, w, h float64, maxYARN int64, l anatLinks) {
	b.f(`<g class="node" data-node="%s"><title>%s</title>`, esc(n.Name), esc(n.Name))
	b.link(l.Ref("node:"+n.Name), func() {
		b.f(`<rect class="nbox" x="%.1f" y="%.1f" width="%.1f" height="%.1f" rx="8"/>`, x, y, w, h)
		role := map[string]string{"CORE": "Core", "TASK": "Task", "MASTER": "Primary"}[n.Role]
		if role == "" {
			role = "Node"
		}
		name := n.Short
		if n.Alike > 0 {
			name += fmt.Sprintf(" and %d more like it", n.Alike)
		}
		b.text(x+12, y+20, "h", "", role+" · "+fitText(name, w-110, 12))
	})
	if n.Alike > 0 {
		// A stacked look, so the card reads as many nodes.
		b.f(`<path class="stack" d="M%.1f %.1f h%.1f v%.1f M%.1f %.1f h%.1f v%.1f"/>`, x+4, y-4, w, h, x+8, y-8, w, h)
		b.text(x+w-12, y+20, "h stackn", "end", fmt.Sprintf("× %d", n.Alike+1))
	}
	b.text(x+12, y+37, "m", "", instanceLine(n))
	b.badges(a, n.Badges, x+w-18, y+18, l)

	// CPU: its icon, a bar filled to the average with a tick at the peak,
	// the two in words, and a sparkline of the run.
	cy := y + 58
	b.icon("cpu", x+12, cy)
	if n.HasCPU {
		gx, gw := x+56, 64.0
		b.text(x+28, cy, "", "", "CPU")
		b.f(`<rect class="gauge" x="%.1f" y="%.1f" width="%.1f" height="7" rx="2"/><rect class="gfill" x="%.1f" y="%.1f" width="%.1f" height="7" rx="2"/>`, gx, cy-7, gw, gx, cy-7, gw*math.Min(n.CPUAvg, 100)/100)
		b.f(`<line class="gpeak" x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f"><title>Peak %.0f%%</title></line>`, gx+gw*math.Min(n.CPUPeak, 100)/100, gx+gw*math.Min(n.CPUPeak, 100)/100, cy-9, cy+2, n.CPUPeak)
		b.text(gx+gw+8, cy, "", "", fmt.Sprintf("%.0f%% avg · %.0f%% peak", n.CPUAvg, n.CPUPeak))
		if len(n.CPU) > 1 {
			sx, sw := x+w-132, 118.0
			var pts []string
			for i, v := range n.CPU {
				pts = append(pts, fmt.Sprintf("%.1f,%.1f", sx+float64(i)/float64(len(n.CPU)-1)*sw, cy-math.Min(v, 100)/100*16))
			}
			b.f(`<line class="spark0" x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f"/><polyline class="spark" points="%s"><title>CloudWatch CPU while it ran: %.0f%% average</title></polyline>`, sx, sx+sw, cy, cy, strings.Join(pts, " "), n.CPUAvg)
		}
	} else {
		b.text(x+28, cy, "m", "", a.NoCPU)
	}

	// Memory: its icon and what this application held of what the
	// NodeManager offered YARN; the bar below draws the same to scale.
	by := y + 70
	bw := w - 24
	b.icon("mem", x+12, by+2)
	if n.YARNBytes > 0 {
		held := n.DriverBytes + int64(n.AtOnce)*n.ExecBytes
		b.text(x+28, by+2, "", "", fitText(fmt.Sprintf("Memory %s of %s held by this application · YARN offered %d vcores", model.Bytes(held), model.Bytes(n.YARNBytes), n.YARNCores), w-40, 11))
		by += 8
		scale := bw / float64(max(maxYARN, 1))
		cx := x + 12
		b.f(`<rect class="yarn" x="%.1f" y="%.1f" width="%.1f" height="22" rx="4"/>`, cx, by, float64(n.YARNBytes)*scale)
		if n.DriverBytes > 0 {
			dw := float64(n.DriverBytes) * scale
			b.f(`<rect class="drv" x="%.1f" y="%.1f" width="%.1f" height="22" rx="3"><title>Driver's container: %s</title></rect>`, cx, by, dw, model.Bytes(n.DriverBytes))
			if dw > 44 {
				b.text(cx+6, by+15, "cl", "", fitText("Driver "+model.Bytes(n.DriverBytes), dw-8, 11))
			}
			cx += dw
		}
		for i := 0; i < n.AtOnce; i++ {
			ew := float64(n.ExecBytes) * scale
			b.f(`<rect class="exc" x="%.1f" y="%.1f" width="%.1f" height="22" rx="3"><title>Executor container: %s</title></rect>`, cx+1, by, math.Max(ew-2, 1), model.Bytes(n.ExecBytes))
			switch label := "Executor " + model.Bytes(n.ExecBytes); {
			case ew-10 >= textW(label, 11, false):
				b.text(cx+6, by+15, "cl", "", label)
			case ew-10 >= textW(model.Bytes(n.ExecBytes), 11, false):
				b.text(cx+6, by+15, "cl", "", model.Bytes(n.ExecBytes))
			}
			cx += ew
		}
		free, fits := n.free()
		if free > 0 {
			fw := float64(free) * scale
			b.f(`<rect class="freeh" x="%.1f" y="%.1f" width="%.1f" height="22" rx="3"><title>Free: %s</title></rect>`, cx, by, fw, model.Bytes(free))
		}
		// With other applications on the cluster, their containers are not
		// drawn, so unused space here is not necessarily free.
		note := fmt.Sprintf("%s free", model.Bytes(free))
		if a.Shared {
			note = fmt.Sprintf("%s not used by this application", model.Bytes(free))
		}
		cls := "m"
		switch {
		case n.Away != "" && free > 0:
			note += ", but the node " + n.Away
		case n.ExecBytes > 0 && free > 0 && !fits:
			note += fmt.Sprintf(": too small for another %s executor", model.Bytes(n.ExecBytes))
			cls = "warnt"
		case fits && free > 0 && a.Shared:
			note += " (other applications' containers are not shown)"
		case fits && free > 0:
			note += fmt.Sprintf(": room for %d more executor(s)", free/max(n.ExecBytes, 1))
		}
		b.text(x+12, by+38, cls, "", note)
	} else {
		b.f(`<rect class="yarn unknown" x="%.1f" y="%.1f" width="%.1f" height="22" rx="4"/>`, x+12, by+8, bw)
		b.text(x+20, by+23, "m", "", a.NoNodeCapacity)
		if n.DriverBytes > 0 || n.ClientDriver {
			b.text(x+12, by+46, "", "", "Runs the driver")
		}
	}

	// The executors that ran here.
	ey := y + 128
	switch {
	case len(n.Execs) == 0:
		msg := "No executors ran here"
		if n.DriverBytes > 0 || n.ClientDriver {
			msg = "Only the driver ran here"
		}
		b.text(x+12, ey+12, "m", "", msg)
	case compact(n, w):
		each := ""
		if n.Alike > 0 {
			each = " (on each)"
		}
		size := ""
		if s := execSize(sameCores(n.Execs), n.ExecBytes); s != "" {
			size = ", each " + s
		}
		b.text(x+12, ey+12, "m", "", fitText(fmt.Sprintf("%s ran here%s, %d at once%s; fill is peak heap", model.Plural(len(n.Execs), "executor", "executors"), each, n.AtOnce, size), w-24, 12))
		per := max(1, int((w-24)/(anSquare+4)))
		for i, e := range n.Execs {
			sx := x + 12 + float64(i%per)*(anSquare+4)
			sy := ey + 20 + float64(i/per)*(anSquare+4)
			e := e
			b.link(l.Ref("executor:"+e.ID), func() {
				cls := "sq"
				if e.bad() {
					cls += " bad"
				} else if e.Kind != "" {
					cls += " gone"
				}
				b.f(`<g class="%s" data-exec="%s"><title>%s</title><rect x="%.1f" y="%.1f" width="%.0f" height="%.0f" rx="3" class="sqbox"/>`, cls, esc(e.ID), esc(execTip(e)), sx, sy, anSquare, anSquare)
				if e.Heap > 0 && e.PeakHeap > 0 {
					fh := anSquare * math.Min(1, float64(e.PeakHeap)/float64(e.Heap))
					b.f(`<rect x="%.1f" y="%.1f" width="%.0f" height="%.1f" rx="2" class="sqfill"/>`, sx, sy+anSquare-fh, anSquare, fh)
				}
				b.WriteString(`</g>`)
			})
		}
	default:
		b.text(x+12, ey+12, "m", "", fmt.Sprintf("%s ran here", model.Plural(len(n.Execs), "executor", "executors")))
		per := chipsPerRow(w)
		for i, e := range n.Execs {
			cx := x + 12 + float64(i%per)*(anChipW+8)
			cy := ey + 22 + float64(i/per)*(anChipH+8)
			drawChip(b, a, e, n.ExecBytes, cx, cy, l)
		}
	}
	b.WriteString(`</g>`)
}

// execSize is what an executor was given, as its chip shows it: "2 cores ·
// 1.5 GiB container", or whichever half is known.
func execSize(cores int, container int64) string {
	var parts []string
	if cores > 0 {
		parts = append(parts, model.Plural(cores, "core", "cores"))
	}
	if container > 0 {
		parts = append(parts, model.Bytes(container)+" container")
	}
	return strings.Join(parts, " · ")
}

func execTip(e anatExec) string {
	s := "Executor " + e.ID
	if e.Cores > 0 {
		s += fmt.Sprintf(", %d cores", e.Cores)
	}
	if e.Heap > 0 && e.PeakHeap > 0 {
		s += fmt.Sprintf(", peak heap %s of %s", model.Bytes(e.PeakHeap), model.Bytes(e.Heap))
	}
	if e.Tasks > 0 {
		s += fmt.Sprintf(", %d tasks", e.Tasks)
	}
	switch {
	case e.Kind == "":
		s += ", ran to the end"
	case e.Reason != "":
		s += ". Removed: " + e.Reason
	}
	return s
}

// drawChip draws one executor: its heap's peak against its size, a square
// per core shaded by CPU share, and how it ended.
func drawChip(b *svgw, a *anatomy, e anatExec, container int64, x, y float64, l anatLinks) {
	cls := "chip"
	status := ""
	switch {
	case e.bad():
		cls += " bad"
		status = map[string]string{model.RemovalMemoryKill: "killed: memory", model.RemovalLost: "lost", model.RemovalDecommissioned: "node removed"}[e.Kind]
	case e.Kind == model.RemovalIdle:
		cls += " gone"
		status = "released when idle"
	case e.Kind != "":
		cls += " gone"
		status = "removed"
	}
	// The name and the status share the chip's top line: shorten them
	// until they fit, so long IDs never run into the status. The tooltip
	// keeps the full text.
	name := "Executor " + clipLabel(e.ID, 8)
	if status != "" {
		room := anChipW - 16 - 8 // inside the chip, less a gap between the two
		if textW(name, 11, true)+textW(status, 10, true) > room {
			name = "Exec " + clipLabel(e.ID, 8)
		}
		if left := room - textW(name, 11, true); textW(status, 10, true) > left {
			status = clipLabel(status, max(4, int(left/textW("m", 10, true))))
		}
	}
	b.f(`<g class="%s" data-exec="%s"><title>%s</title>`, cls, esc(e.ID), esc(execTip(e)))
	b.link(l.Ref("executor:"+e.ID), func() {
		b.f(`<rect class="cbox" x="%.1f" y="%.1f" width="%.0f" height="%.0f" rx="6"/>`, x, y, anChipW, anChipH)
		b.text(x+8, y+15, "b", "", name)
	})
	if status != "" {
		b.text(x+anChipW-8, y+15, "m st", "end", status)
	}
	// Heap: peak against configured.
	hw := anChipW - 16
	b.f(`<rect class="heap" x="%.1f" y="%.1f" width="%.1f" height="9" rx="2"/>`, x+8, y+22, hw)
	if e.Heap > 0 && e.PeakHeap > 0 {
		f := math.Min(1, float64(e.PeakHeap)/float64(e.Heap))
		hc := "hpeak"
		if f >= 0.9 {
			hc += " hot"
		}
		b.f(`<rect class="%s" x="%.1f" y="%.1f" width="%.1f" height="9" rx="2"/>`, hc, x+8, y+22, hw*f)
		b.text(x+8, y+43, "s", "", fmt.Sprintf("heap %s of %s", model.Bytes(e.PeakHeap), model.Bytes(e.Heap)))
	} else {
		b.text(x+8, y+43, "s", "", "heap peak not logged")
	}
	// What it was given: cores and the container YARN placed.
	if size := execSize(e.Cores, container); size != "" {
		b.text(x+8, y+57, "s", "", size)
	}
	// Cores, shaded by CPU share.
	for i := 0; i < min(e.Cores, 16); i++ {
		b.f(`<rect class="core" x="%.1f" y="%.1f" width="7" height="7" rx="1.5" style="opacity:%.2f"/>`, x+8+float64(i)*9, y+63, 0.25+0.75*e.CPUShare)
	}
	if e.CPUShare > 0 {
		b.text(x+anChipW-8, y+70, "s", "end", fmt.Sprintf("CPU %.0f%%", 100*e.CPUShare))
	}
	b.badges(a, e.Badges, x+anChipW-4, y-2, l)
	b.WriteString(`</g>`)
}

// drawJVM draws one JVM's memory, region by region, to scale across the
// width: the container, the heap's parts and what peaked where.
func drawJVM(b *svgw, a *anatomy, j *anatJVM, y float64, l anatLinks) float64 {
	x := anPad + 14
	w := anW - 2*x
	h := 196.0
	if j.Cores == 0 {
		h = 168
	}
	b.f(`<g class="jvm"><rect class="panel" x="%.1f" y="%.1f" width="%.1f" height="%.1f" rx="8"/>`, x, y, w, h)
	title := j.Title
	b.link(l.Ref(j.Href), func() { b.text(x+14, y+22, "h", "", title) })
	if j.Why != "" {
		b.text(x+14+float64(len(title))*7.6+10, y+22, "m", "", "("+j.Why+")")
	}
	b.badges(a, j.Badges, x+w-18, y+18, l)

	total := max(j.Container, j.Heap+j.Overhead+j.PySpark+j.OffHeap)
	bx, bw := x+14, w-28
	scale := bw / float64(total)
	ty, bh := y+56, 34.0
	type region struct {
		name, cls string
		bytes     int64
		tip       string
	}
	reserved := min(int64(anatReservedMiB)<<20, j.Heap)
	usable := j.Heap - reserved
	frac := j.MemoryFraction
	if frac == 0 {
		frac = 0.6
	}
	unified := int64(float64(usable) * frac)
	user := usable - unified
	regions := []region{
		{"Reserved", "r-res", reserved, "Spark keeps 300 MiB of the heap for itself."},
		{"User memory", "r-user", user, "Your code's own objects and Spark's internal metadata: the heap left after Spark's working memory."},
		{"Spark's working memory", "r-uni", unified, fmt.Sprintf("(heap − 300 MiB) × spark.memory.fraction (%.2g): shared by execution (shuffles, joins, sorts) and storage (cache).", frac)},
	}
	if j.Overhead > 0 {
		regions = append(regions, region{"Overhead", "r-ovh", j.Overhead, "Memory outside the heap in the same container: thread stacks, network buffers and native libraries."})
	}
	if j.OffHeap > 0 {
		regions = append(regions, region{"Off-heap", "r-off", j.OffHeap, "spark.memory.offHeap.size, outside the Java heap."})
	}
	if j.PySpark > 0 {
		regions = append(regions, region{"Python workers", "r-py", j.PySpark, "spark.executor.pyspark.memory, for the Python processes beside the JVM."})
	}
	// The heap bracket and the container bracket above the bar.
	b.f(`<path class="brk" d="M%.1f %.1f v-6 h%.1f v6"/>`, bx, ty-4, float64(j.Heap)*scale)
	b.text(bx+float64(j.Heap)*scale/2, ty-14, "b", "middle", "Java heap "+model.Bytes(j.Heap))
	if j.Container > j.Heap {
		b.text(bx+bw-float64(len(j.Badges))*24, y+22, "m", "end", "Container "+model.Bytes(j.Container)+" (what YARN placed)")
	}
	cx := bx
	for _, rg := range regions {
		rw := float64(rg.bytes) * scale
		b.f(`<rect class="%s" x="%.1f" y="%.1f" width="%.1f" height="%.0f"><title>%s: %s. %s</title></rect>`, rg.cls, cx, ty, math.Max(rw-1, 0.5), bh, esc(rg.name), model.Bytes(rg.bytes), esc(rg.tip))
		if rw > 70 {
			b.text(cx+6, ty+14, "rl", "", fitText(rg.name, rw-10, 11))
			b.text(cx+6, ty+28, "rl m", "", model.Bytes(rg.bytes))
		}
		// Peaks inside Spark's working memory: execution from the left,
		// storage from the right.
		if rg.cls == "r-uni" {
			if j.PeakExecution > 0 {
				pw := math.Min(rw, float64(j.PeakExecution)*scale)
				b.f(`<rect class="p-exec" x="%.1f" y="%.1f" width="%.1f" height="6"><title>Execution memory peak: %s</title></rect>`, cx, ty+bh-6, pw, model.Bytes(j.PeakExecution))
			}
			if j.PeakStorage > 0 {
				pw := math.Min(rw, float64(j.PeakStorage)*scale)
				b.f(`<rect class="p-stor" x="%.1f" y="%.1f" width="%.1f" height="6"><title>Storage (cache) peak: %s</title></rect>`, cx+rw-pw-1, ty, pw, model.Bytes(j.PeakStorage))
			}
			if j.StorageFraction > 0 {
				sx := cx + rw*(1-j.StorageFraction)
				b.f(`<line class="sfrac" x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f"><title>spark.memory.storageFraction (%.2g): cached data below this share is safe from eviction</title></line>`, sx, sx, ty, ty+bh, j.StorageFraction)
			}
		}
		cx += rw
	}
	// The peak heap line across the heap.
	if j.PeakHeap > 0 {
		px := bx + math.Min(float64(j.PeakHeap), float64(j.Heap))*scale
		cls := "peak"
		if float64(j.PeakHeap) >= 0.9*float64(j.Heap) {
			cls += " hot"
		}
		b.f(`<line class="%s" x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f"/>`, cls, px, px, ty-6, ty+bh+8)
		// The label gets its own classes: sharing the line's "peak" class
		// gave the text the line's 2px stroke, which drew it doubled.
		tcls := "peakt"
		if float64(j.PeakHeap) >= 0.9*float64(j.Heap) {
			tcls += " hott"
		}
		label := fmt.Sprintf("peak heap %s (%.0f%%)", model.Bytes(j.PeakHeap), 100*float64(j.PeakHeap)/float64(j.Heap))
		// Centred on the line, but kept inside the strip: a peak near
		// either end would otherwise push the label off the picture.
		anchor, tx, half := "middle", px, textW(label, 11, true)/2
		switch {
		case px-half < bx:
			anchor, tx = "start", bx
		case px+half > bx+bw:
			anchor, tx = "end", bx+bw
		}
		b.text(tx, ty+bh+22, tcls, anchor, label)
	}
	if j.PeakPython > 0 && j.PySpark == 0 {
		b.text(bx+bw, ty+bh+22, "m", "end", "Python workers peaked at "+model.Bytes(j.PeakPython)+", outside the heap")
	}

	// Key lines under the bar: what the colours mean, then cores and GC.
	ky := ty + bh + 44
	keys := []struct{ cls, label string }{{"p-exec", "execution peak (shuffles, joins, sorts)"}, {"p-stor", "storage peak (cache)"}, {"r-ovh", "overhead, outside the heap"}}
	kx := bx
	for _, k := range keys {
		b.f(`<rect class="%s" x="%.1f" y="%.1f" width="12" height="10"/>`, k.cls, kx, ky-9)
		b.text(kx+18, ky, "m", "", k.label)
		kx += 26 + float64(len(k.label))*6.2
	}
	cy := ky + 22
	if j.Cores > 0 {
		b.text(bx, cy, "", "", fmt.Sprintf("%d cores: each runs one task at a time, so each task has about %s of heap", j.Cores, model.Bytes(j.Heap/int64(j.Cores))))
		for i := 0; i < min(j.Cores, 32); i++ {
			b.f(`<rect class="core" x="%.1f" y="%.1f" width="12" height="12" rx="2" style="opacity:%.2f"/>`, bx+float64(i)*16, cy+8, 0.25+0.75*j.CPUShare)
		}
		var st []string
		if j.CPUShare > 0 {
			st = append(st, fmt.Sprintf("computing %.0f%% of task time", 100*j.CPUShare))
		}
		if j.GCShare > 0 {
			st = append(st, fmt.Sprintf("garbage collection %.1f%%", 100*j.GCShare))
		}
		if len(st) > 0 {
			b.text(bx+float64(min(j.Cores, 32))*16+10, cy+18, "m", "", strings.Join(st, " · "))
		}
	} else if j.Why != "" || j.Title == "The driver" {
		b.text(bx, cy, "m", "", "It plans the work and gathers results; tasks run on the executors.")
	}
	b.WriteString(`</g>`)
	return y + h
}

// drawBadgeKey lists the numbered findings under the diagram.
func drawBadgeKey(b *svgw, a *anatomy, y float64, l anatLinks) float64 {
	x := anPad + 14
	b.text(x, y+14, "h", "", "Findings on the diagram")
	y += 26
	col := (anW - 2*x) / 2
	for i, bd := range a.Badges {
		cx := x + float64(i%2)*col
		cy := y + float64(i/2)*24
		bd := bd
		b.link(l.Finding(bd.N), func() {
			b.f(`<g class="badge %s" data-finding="%d"><circle cx="%.1f" cy="%.1f" r="9"/>`, sevClass(bd.Sev), bd.N, cx+9, cy)
			b.text(cx+9, cy+4, "bn", "middle", fmt.Sprint(bd.N))
			b.WriteString(`</g>`)
			b.text(cx+26, cy+4, "", "", fitText(bd.Title, col-40, 12))
		})
	}
	y += float64((len(a.Badges)+1)/2)*24 + 6
	if len(a.Unpinned) > 0 {
		var ns []string
		for _, n := range a.Unpinned {
			ns = append(ns, fmt.Sprint(n))
		}
		which := "Finding " + ns[0] + " is"
		if len(ns) > 1 {
			which = "Findings " + strings.Join(ns[:len(ns)-1], ", ") + " and " + ns[len(ns)-1] + " are"
		}
		b.text(x, y+8, "m", "", which+" about the work itself (jobs, stages, settings), not where it ran, so no badge marks them above.")
		y += 22
	}
	return y
}

// anatomyHTML is the report's section content.
// anatGuide explains the run-at-a-glance diagram; the explorer shows the same.
var anatGuide = chartGuide{
	Axes: [][2]string{
		{"Boxes", "The cluster's nodes: the primary node runs YARN's ResourceManager, and each worker node that ran this application's driver or executors shows what it did. The worker nodes it did not use add up to one box, counted by role and instance type."},
		{"Bar in each node", "The memory the node offered YARN, to scale, with this application's driver and executor containers placed on it at the application's busiest moment."},
		{"Cards", "One per executor on the node: its peak heap against the heap it was given, its cores and container size, and how busy its cores were."},
		{"Bottom panels", "Inside one executor (the one with the highest heap) and the driver: the Java heap's regions, to scale, with how far each peaked."},
		{"Badges", "Numbered findings, pinned to the part they are about."},
	},
	Read: []string{
		"Read it top down: the cluster, then each node, then inside one executor.",
		"Hatched space on a node is memory nobody used; if it is narrower than an executor, it could not hold one.",
		"Red outlines are executors that were killed or lost.",
		"Inside an executor, a peak line near the end of the heap means it nearly ran out; a short execution bar with spill elsewhere means each task had too little memory.",
	},
	Note: "Hover anything for exact values; click a badge to read its finding.",
}

func anatomyHTML(r *model.Report, explorer string) template.HTML {
	a := buildAnatomy(r)
	svg := anatomySVG(a, anatLinks{
		Finding: func(n int) string { return fmt.Sprintf("#finding-%d", n) },
		Ref: func(ref string) string {
			kind, id, _ := strings.Cut(ref, ":")
			switch {
			case kind == "executor" && explorer != "":
				return explorer + "#executor/" + id
			case kind == "executor":
				return "#executors"
			case kind == "node":
				return "#nodes"
			}
			return ""
		},
	})
	if svg == "" {
		return ""
	}
	return template.HTML(`<div class="chart anatbox">` + svg + string(anatGuide.html()) + `</div>`)
}
