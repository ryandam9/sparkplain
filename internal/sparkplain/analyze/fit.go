package analyze

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// fitNode is one worker node's YARN capacity and what of the application
// sat on it.
type fitNode struct {
	host   string
	memMB  int64
	vcores int
	amMB   int64 // the driver's (application master's) container, when here
	fits   int64 // executors of the requested size that fit beside it
	src    model.Source
}

// fitFindings works out how many executors of the size Spark asked for fit
// on the cluster's nodes, and says so when Spark wanted more than fit.
// EMR's capacity scheduler places containers by memory alone (it recorded
// vCores:1 for executors that asked for 4), so memory decides the fit.
func fitFindings(c *ctx, r *model.Report) {
	nodes := map[string]*fitNode{}
	var execMB, heapMB, overheadMB int64
	var execCores, wanted, desired int
	var desiredSrc model.Source
	var amMB int64
	var amHost string
	var reqSrc, wantSrc, amSrc model.Source
	for _, h := range c.logs.hits {
		f := h.l.Fields
		num := func(k string) int64 { n, _ := strconv.ParseInt(f[k], 10, 64); return n }
		switch h.l.Kind {
		case model.LogNodeCapacity:
			k := hostKey(f["host"])
			if nodes[k] == nil {
				nodes[k] = &fitNode{host: f["host"], memMB: num("memoryMB"), vcores: int(num("vcores")), src: h.l.Source}
			}
		case model.LogYarnRequest:
			switch f["what"] {
			case "executors":
				if execMB == 0 {
					execMB, execCores, reqSrc = num("memoryMB"), int(num("cores")), h.l.Source
				}
				if n := int(num("count")); n > wanted {
					wanted, wantSrc = n, h.l.Source
				}
			case "most-desired":
				// What dynamic allocation asked for from the task backlog; the
				// allocator's opening request can be far larger and is cut
				// back within seconds.
				if n := int(num("total")); n > desired {
					desired, desiredSrc = n, h.l.Source
				}
			case "launch":
				heapMB, overheadMB = num("heapMB"), num("overheadMB")
				if execMB == 0 {
					execMB, execCores, reqSrc = heapMB+overheadMB, int(num("cores")), h.l.Source
				}
			case "am":
				if amMB == 0 {
					amMB, amSrc = num("memoryMB"), h.l.Source
				}
			}
		case model.LogContainerAssigned:
			// The last attempt's first container is its application master.
			if strings.HasSuffix(f["container"], "_000001") {
				amMB, amHost, amSrc = num("memoryMB"), f["host"], h.l.Source
			}
		}
	}
	if desired > 0 {
		wanted, wantSrc = desired, desiredSrc
	}
	// What Spark asked YARN for settles the overhead: EMR raises Spark's
	// overhead factor (0.1875 on EMR 7) without it reaching the event log,
	// so the 10% default would understate it. Checked on EMR 7.3.0: a
	// 9486 MB heap came with 11264 MB containers.
	if m := &r.Memory.Config; m.HeapBytes > 0 {
		others := m.HeapBytes + m.PySparkBytes + m.OffHeapBytes
		switch {
		case overheadMB > 0:
			m.OverheadBytes, m.OverheadFrom = overheadMB<<20, "the executor launch command in the logs"
		case execMB > 0 && execMB<<20 > others:
			m.OverheadBytes = execMB<<20 - others
			m.OverheadFrom = fmt.Sprintf("the %s container Spark asked YARN for, less the heap (%s:%d)", mb(execMB), reqSrc.File, reqSrc.Line)
		}
		m.ContainerBytes = others + m.OverheadBytes
	}
	// Show each host's YARN capacity, and what of this application YARN
	// placed on it, in the Nodes table and chart.
	for i := range r.Nodes.Hosts {
		h := &r.Nodes.Hosts[i]
		if n := nodes[hostKey(h.Name)]; n != nil {
			h.YARNMemoryBytes, h.YARNVCores = n.memMB<<20, n.vcores
		}
		if len(h.Executors) > 0 && execMB > 0 {
			h.ExecutorContainerBytes = execMB << 20
			h.PeakExecutors = peakAlive(c, h.Name)
		}
		if amHost != "" && hostKey(h.Name) == hostKey(amHost) {
			h.DriverContainerBytes = amMB << 20
		}
	}
	if len(nodes) == 0 || execMB <= 0 {
		return
	}
	var list []*fitNode
	var total int64
	for k, n := range nodes {
		if k == hostKey(amHost) {
			n.amMB = amMB
		}
		if n.memMB > n.amMB {
			n.fits = (n.memMB - n.amMB) / execMB
		}
		total += n.fits
		list = append(list, n)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].host < list[j].host })
	launched := 0
	if c.has() {
		for _, x := range c.log.Executors {
			if x.ID != "driver" {
				launched++
			}
		}
	}
	if wanted <= int(total) || wanted <= launched {
		return // everything Spark asked for could be placed
	}

	var ev []model.Evidence
	if !wantSrc.IsZero() {
		ev = append(ev, model.Evidence{Source: wantSrc, Text: fmt.Sprintf("the driver asked YARN for up to %d executors", wanted)})
	}
	ev = append(ev, model.Evidence{Source: reqSrc, Text: fmt.Sprintf("each executor container: %s, %d cores", mb(execMB), execCores)})
	var zero []string
	for _, n := range list {
		text := fmt.Sprintf("%s offers YARN %s", n.host, mb(n.memMB))
		src := n.src
		if n.amMB > 0 {
			text += fmt.Sprintf("; the driver's %s container leaves %s", mb(n.amMB), mb(n.memMB-n.amMB))
			if !amSrc.IsZero() {
				src = amSrc
			}
		}
		text += fmt.Sprintf(", room for %s", model.Plural(int(n.fits), "executor", "executors"))
		if n.fits == 0 {
			zero = append(zero, n.host)
		}
		ev = append(ev, model.Evidence{Source: src, Text: text})
	}
	expl := fmt.Sprintf("Spark wanted up to %d executors of %s each, but the %s YARN could use had room for only %d at that size", wanted, mb(execMB), model.Plural(len(list), "node", "nodes"), total)
	if len(zero) > 0 {
		expl += fmt.Sprintf("; %s had room for none", joinAnd(zero))
	}
	expl += ". YARN places containers by memory, and each executor asks for its heap plus overhead in one piece, so memory left over on a node is wasted when it is smaller than an executor."
	fix := "Use smaller executors so several fit on each node, or add nodes."
	if s := suggestSize(list, amMB, heapMB, overheadMB, execCores); s != "" {
		fix = s
	}
	c.add(model.Finding{Rule: "executor-fit", Severity: model.Warning, Section: "nodes",
		Title:       fmt.Sprintf("Spark wanted %d executors; the cluster had room for %d", wanted, total),
		Explanation: expl, Evidence: ev, Fix: fix})
	if f := c.finding("idle-nodes"); f != nil {
		f.Fix = "See the finding “Spark wanted " + strconv.Itoa(wanted) + " executors; the cluster had room for " + strconv.FormatInt(total, 10) + "”: executors of a size that fits more than one per node would put these nodes to work."
	}
}

// suggestSize proposes an executor size that fits two per node beside the
// driver's container, keeping the overhead share the run used.
func suggestSize(nodes []*fitNode, amMB, heapMB, overheadMB int64, cores int) string {
	var minMem int64
	vcores := 0
	for _, n := range nodes {
		if minMem == 0 || n.memMB < minMem {
			minMem = n.memMB
		}
		if vcores == 0 || n.vcores < vcores {
			vcores = n.vcores
		}
	}
	room := (minMem - amMB) / 2 // two executors on the node that also runs the driver
	if room < 1024 {
		return ""
	}
	factor := 0.1875 // EMR 7's spark.executor.memoryOverheadFactor
	if heapMB > 0 && overheadMB > 0 {
		factor = float64(overheadMB) / float64(heapMB)
	}
	heap := int64(float64(room) / (1 + factor))
	heap -= heap % 256
	if over := int64(float64(heap) * factor); over < 384 {
		heap = room - 384 - (room-384)%256
	}
	if heap < 512 {
		return ""
	}
	perNode := int64(2)
	if other := minMem / (heap + max(384, int64(float64(heap)*factor))); other > perNode {
		perNode = other
	}
	c := max(1, vcores/int(perNode))
	if cores > 0 && c > cores {
		c = cores
	}
	return fmt.Sprintf("Size executors so two fit beside the driver: spark.executor.memory=%dm and spark.executor.cores=%d, about %s each with overhead. Then the node running the driver holds 2 and every other node %d. Or add nodes.",
		heap, c, mb(heap+max(384, int64(float64(heap)*factor))), perNode)
}

func mb(n int64) string { return model.Bytes(n << 20) }

// peakAlive is the most executors on host that were alive at once.
func peakAlive(c *ctx, host string) int {
	if !c.has() {
		return 0
	}
	type edge struct {
		t time.Time
		d int
	}
	var es []edge
	for _, x := range c.log.Executors {
		if x.ID == "driver" || hostKey(x.Host) != hostKey(host) {
			continue
		}
		start, end := c.lifetime(x)
		es = append(es, edge{start, 1}, edge{end, -1})
	}
	sort.Slice(es, func(i, j int) bool {
		if es[i].t.Equal(es[j].t) {
			return es[i].d < es[j].d // an executor leaving frees its slot before another arrives
		}
		return es[i].t.Before(es[j].t)
	})
	n, peak := 0, 0
	for _, e := range es {
		n += e.d
		peak = max(peak, n)
	}
	return peak
}
