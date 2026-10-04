package analyze

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

const (
	mib            = int64(1) << 20
	reservedMemory = 300 * mib // Spark keeps this much heap for itself
	minOverhead    = 384 * mib
)

// memoryConfig works out what each executor asked YARN for.
func memoryConfig(c *ctx) model.MemoryConfig {
	m := model.MemoryConfig{MemoryFraction: 0.6, StorageFraction: 0.5}
	var rp *model.ResourceProfile
	for i := range c.log.ResourceProfiles {
		if c.log.ResourceProfiles[i].ID == 0 {
			rp = &c.log.ResourceProfiles[i]
		}
	}
	m.HeapBytes, m.HeapFrom = 1<<30, "Spark default (1g)"
	if v := parseSize(c.conf["spark.executor.memory"], mib); v > 0 {
		m.HeapBytes, m.HeapFrom = v, "spark.executor.memory = "+c.conf["spark.executor.memory"]
	}
	if rp != nil && rp.ExecutorMemoryMB > 0 {
		m.HeapBytes = rp.ExecutorMemoryMB * mib
	}
	factor := 0.10
	if f, err := strconv.ParseFloat(c.conf["spark.executor.memoryOverheadFactor"], 64); err == nil && f > 0 {
		factor = f
	}
	switch {
	case parseSize(c.conf["spark.executor.memoryOverhead"], mib) > 0:
		m.OverheadBytes = parseSize(c.conf["spark.executor.memoryOverhead"], mib)
		m.OverheadFrom = "spark.executor.memoryOverhead = " + c.conf["spark.executor.memoryOverhead"]
	case rp != nil && rp.OverheadMB > 0:
		m.OverheadBytes, m.OverheadFrom = rp.OverheadMB*mib, "resource profile 0"
	default:
		m.OverheadBytes = max(minOverhead, int64(factor*float64(m.HeapBytes)))
		m.OverheadFrom = fmt.Sprintf("default: %s of the heap, at least 384 MiB", model.Percent(factor))
	}
	if c.confBool("spark.memory.offHeap.enabled", false) {
		m.OffHeapBytes = max(0, parseSize(c.conf["spark.memory.offHeap.size"], 1))
	}
	m.PySparkBytes = max(0, parseSize(c.conf["spark.executor.pyspark.memory"], mib))
	m.ContainerBytes = m.HeapBytes + m.OverheadBytes + m.OffHeapBytes + m.PySparkBytes
	if f, err := strconv.ParseFloat(c.conf["spark.memory.fraction"], 64); err == nil {
		m.MemoryFraction = f
	}
	if f, err := strconv.ParseFloat(c.conf["spark.memory.storageFraction"], 64); err == nil {
		m.StorageFraction = f
	}
	m.UnifiedBytes = int64(float64(max(0, m.HeapBytes-reservedMemory)) * m.MemoryFraction)
	m.Cores = r0cores(c)
	m.DriverHeapBytes = 1 << 30
	if v := parseSize(c.conf["spark.driver.memory"], mib); v > 0 {
		m.DriverHeapBytes = v
	}
	return m
}

func r0cores(c *ctx) int {
	if n, err := strconv.Atoi(c.conf["spark.executor.cores"]); err == nil && n > 0 {
		return n
	}
	counts := map[int]int{}
	best, cores := 0, 0
	for _, x := range c.log.Executors {
		counts[x.Cores]++
		if counts[x.Cores] > best {
			best, cores = counts[x.Cores], x.Cores
		}
	}
	return cores
}

func analyzeMemory(c *ctx, r *model.Report) {
	s := &r.Memory
	if !c.metrics() {
		s.Coverage = model.NeedsEventLog
		s.Missing = []string{"Configured memory per executor", "Peak heap, off-heap and process memory per executor", "Spill to memory and disk per stage", "Garbage collection time"}
		if c.rebuilt {
			s.Missing = append([]string{rebuiltNote}, s.Missing...)
		}
		return
	}
	s.Coverage = model.Partial
	s.Missing = []string{"Host memory over time (needs the CloudWatch agent)"}
	if c.logs == nil {
		s.Missing = append([]string{"Java OutOfMemoryError and YARN memory-kill lines (needs the container logs: -cluster-id or -from)"}, s.Missing...)
	}
	m := memoryConfig(c)
	s.Config = m
	var totalGC, totalRun int64
	for _, x := range c.log.Executors {
		em := model.ExecMemory{
			ID: x.ID, Host: x.Host, HeapBytes: m.HeapBytes, PeakHeap: x.Peak.JVMHeap, PeakOffHeap: x.Peak.JVMOffHeap,
			PeakRSS:     x.Peak.ProcessJVMRSS + x.Peak.ProcessPythonRSS + x.Peak.ProcessOtherRSS,
			PeakStorage: x.Peak.OnHeapStorage + x.Peak.OffHeapStorage, PeakExecution: x.Peak.OnHeapExecution + x.Peak.OffHeapExecution,
			GCShare: share(x.Tasks.GCTimeMs, x.Tasks.RunTimeMs), Source: x.Peak.HeapSource,
		}
		if em.PeakHeap > 0 {
			s.HeapKnown = true
		}
		if em.PeakRSS > 0 {
			s.RSSKnown = true
		}
		totalGC += x.Tasks.GCTimeMs
		totalRun += x.Tasks.RunTimeMs
		s.Executors = append(s.Executors, em)
	}
	if d := c.log.Driver; d != nil {
		s.Driver = &model.ExecMemory{ID: "driver", Host: d.Host, HeapBytes: m.DriverHeapBytes, PeakHeap: d.Peak.JVMHeap, PeakOffHeap: d.Peak.JVMOffHeap,
			PeakRSS: d.Peak.ProcessJVMRSS + d.Peak.ProcessPythonRSS + d.Peak.ProcessOtherRSS, Source: d.Peak.HeapSource}
	}
	if !s.HeapKnown {
		s.Missing = append([]string{"Peak heap, off-heap and execution memory per executor: the event log holds no memory samples (set spark.eventLog.logStageExecutorMetrics=true to record them)"}, s.Missing...)
	}
	s.GCShare = share(totalGC, totalRun)
	for _, st := range c.log.Stages {
		s.TotalMemSpill += st.Totals.MemorySpillBytes
		s.TotalDiskSpill += st.Totals.DiskSpillBytes
		if st.Totals.DiskSpillBytes > 0 || st.Totals.MemorySpillBytes > 0 {
			s.Spill = append(s.Spill, model.StageSpill{StageID: st.ID, Attempt: st.Attempt, Name: st.Name, MemoryBytes: st.Totals.MemorySpillBytes,
				DiskBytes: st.Totals.DiskSpillBytes, ShuffleWrite: st.Totals.ShuffleWriteBytes, Source: st.TaskSource})
		}
	}
	sort.SliceStable(s.Spill, func(i, j int) bool { return s.Spill[i].DiskBytes > s.Spill[j].DiskBytes })
	s.Lede = memoryLede(c, m)
	memoryFindings(c, s)
}

func memoryLede(c *ctx, m model.MemoryConfig) string {
	lede := fmt.Sprintf("Each executor asked YARN for about %s: a %s Java heap plus %s overhead", model.Bytes(m.ContainerBytes), model.Bytes(m.HeapBytes), model.Bytes(m.OverheadBytes))
	if m.OffHeapBytes > 0 {
		lede += fmt.Sprintf(", %s off-heap", model.Bytes(m.OffHeapBytes))
	}
	if m.PySparkBytes > 0 {
		lede += fmt.Sprintf(", %s for Python", model.Bytes(m.PySparkBytes))
	}
	lede += fmt.Sprintf(". Spark uses %s of the heap for shuffles, joins and cached data; the rest holds your code's objects.", model.Bytes(m.UnifiedBytes))
	if c.pyspark {
		lede += " This is a PySpark job, so its Python worker processes live in the overhead."
	}
	return lede
}

func memoryFindings(c *ctx, s *model.MemorySection) {
	t := c.t
	m := s.Config
	// Over-provisioned: the busiest executor stayed well under its heap.
	var maxHeap int64
	var maxSrc model.Source
	var maxRef string
	var runTotal int64
	for _, e := range s.Executors {
		if e.PeakHeap > maxHeap {
			maxHeap, maxSrc, maxRef = e.PeakHeap, e.Source, model.ExecutorRef(e.ID)
		}
	}
	for _, x := range c.log.Executors {
		runTotal += x.Tasks.RunTimeMs
	}
	if maxHeap > 0 && m.HeapBytes > 0 && runTotal >= t.MinRunTime.Milliseconds() {
		sh := share(maxHeap, m.HeapBytes)
		if sh < t.MemoryUsedShare {
			c.add(model.Finding{
				Rule: "memory-over-provisioned", Severity: model.Info, Section: "memory",
				Title: fmt.Sprintf("Executors used at most %s of their %s heap", model.Percent(sh), model.Bytes(m.HeapBytes)),
				Explanation: fmt.Sprintf("The highest heap use on an executor was %s. Spark measures memory only at heartbeats and when a task ends, so it can miss short peaks. But a difference this large usually shows that smaller executors are sufficient.",
					model.Bytes(maxHeap)),
				Evidence: []model.Evidence{{Source: maxSrc, Ref: maxRef, Text: "highest JVMHeapMemory sample: " + model.Bytes(maxHeap)}},
				Fix:      fmt.Sprintf("Set spark.executor.memory to about %s. Then compare the run time and the spill with this run.", model.Bytes(roundUpGiB(int64(float64(maxHeap)*1.5)))),
			})
		} else if sh > 0.9 {
			c.add(model.Finding{
				Rule: "memory-heap-near-limit", Severity: model.Warning, Section: "memory",
				Title:       fmt.Sprintf("An executor's heap reached %s of its limit", model.Percent(sh)),
				Explanation: "When the heap use is this near to the limit, garbage collection pauses become long. If the heap use increases more, the executor stops with an OutOfMemoryError.",
				Evidence:    []model.Evidence{{Source: maxSrc, Ref: maxRef, Text: "highest JVMHeapMemory sample: " + model.Bytes(maxHeap) + " of " + model.Bytes(m.HeapBytes)}},
				Fix:         "Do one of these:\n- Increase spark.executor.memory.\n- Increase spark.sql.shuffle.partitions, so that each task holds less data.\n- Use fewer cores for each executor.",
			})
		}
	}
	// GC pressure per executor.
	var gcBad []model.ExecMemory
	for i, x := range c.log.Executors {
		if x.Tasks.RunTimeMs >= t.MinRunTime.Milliseconds() && s.Executors[i].GCShare > t.GCShare {
			gcBad = append(gcBad, s.Executors[i])
		}
	}
	if len(gcBad) > 0 {
		sort.Slice(gcBad, func(i, j int) bool { return gcBad[i].GCShare > gcBad[j].GCShare })
		var ev []model.Evidence
		for i, e := range gcBad {
			if i == 5 {
				break
			}
			ev = append(ev, model.Evidence{Source: e.Source, Ref: model.ExecutorRef(e.ID), Text: fmt.Sprintf("executor %s on %s: %s of task time in garbage collection", e.ID, e.Host, model.Percent(e.GCShare))})
		}
		c.add(model.Finding{
			Rule: "memory-gc-pressure", Severity: model.Warning, Section: "memory",
			Title:       fmt.Sprintf("%s spent over %s of task time in garbage collection", model.Plural(len(gcBad), "executor", "executors"), model.Percent(t.GCShare)),
			Explanation: "In garbage collection, the JVM makes unused memory free again. When it uses this much time, tasks wait for memory and do not do work. Usually the heap is too small for the data that each task holds.",
			Evidence:    ev,
			Fix:         "Do one of these:\n- Give the executors more heap.\n- Use fewer cores for each executor.\n- Increase spark.sql.shuffle.partitions, so that each task holds less data.\n- Cache data in serialized form (MEMORY_AND_DISK_SER).",
		})
	}
	// Spill: disk spill over the threshold share of shuffle write (SPEC §5).
	// Stages that spill but write no shuffle data (sorts before a file
	// write, for example) are flagged too.
	var spilled []model.StageSpill
	var total int64
	for _, sp := range s.Spill {
		if sp.DiskBytes == 0 || (sp.ShuffleWrite > 0 && share(sp.DiskBytes, sp.ShuffleWrite) <= t.SpillShare) {
			continue
		}
		spilled = append(spilled, sp)
		total += sp.DiskBytes
	}
	if len(spilled) == 0 {
		return
	}
	var ev []model.Evidence
	for i, sp := range spilled {
		if i == 5 {
			ev = append(ev, model.Evidence{Text: fmt.Sprintf("… and %d more stages", len(spilled)-5)})
			break
		}
		ev = append(ev, model.Evidence{Source: sp.Source, Ref: model.StageRef(sp.StageID, sp.Attempt), Text: fmt.Sprintf("stage %d (%s): %s spilled to disk, %s", sp.StageID, sp.Name, model.Bytes(sp.DiskBytes), spillComparison(sp))})
	}
	title := fmt.Sprintf("Stage %d spilled %s to disk", spilled[0].StageID, model.Bytes(total))
	if len(spilled) > 1 {
		title = fmt.Sprintf("%d stages spilled %s to disk", len(spilled), model.Bytes(total))
	}
	fix := "Do one of these:\n- Increase spark.sql.shuffle.partitions, so that each task sorts less data.\n- Give the executors more memory."
	if m.MemoryFraction < 0.6 {
		fix = fmt.Sprintf("spark.memory.fraction is %.2f, which is less than the Spark default of 0.6. As a result, Spark has less memory before it spills. Do one of these:\n- Set spark.memory.fraction back to 0.6.\n- Increase spark.sql.shuffle.partitions.\n- Give the executors more memory.", m.MemoryFraction)
	}
	c.add(model.Finding{
		Rule: "memory-spill", Severity: model.Warning, Section: "memory",
		Title:       title,
		Explanation: "Spill occurs when data does not fit in the memory of Spark during a sort, a join or an aggregation. Spark then writes the data to local disk and reads it back. Spill makes tasks slower and can fill the disks of the node.",
		Evidence:    ev,
		Fix:         fix,
	})
}

func spillComparison(sp model.StageSpill) string {
	switch r := share(sp.DiskBytes, sp.ShuffleWrite); {
	case sp.ShuffleWrite == 0:
		return "and it wrote no shuffle data"
	case r >= 10:
		return fmt.Sprintf("far more than the %s it wrote to shuffle", model.Bytes(sp.ShuffleWrite))
	default:
		return fmt.Sprintf("%s of the %s it wrote to shuffle", model.Percent(r), model.Bytes(sp.ShuffleWrite))
	}
}

func roundUpGiB(n int64) int64 {
	const g = int64(1) << 30
	return max(g, (n+g-1)/g*g)
}
