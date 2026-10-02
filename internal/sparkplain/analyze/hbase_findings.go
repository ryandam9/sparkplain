package analyze

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// Limits of the HBase scan findings: a stage reads the same data again
// when it shares sameRegions of the smaller stage's regions; a scan is
// worth reporting as full when it reads at least fullScanRegions regions
// or fullScanBytes; regions are tiny when at least tinyMinRegions sized
// ones have a median under tinyBytes.
const (
	sameRegions     = 0.8
	fullScanRegions = 10
	fullScanBytes   = 10 << 30
	tinyMinRegions  = 20
	tinyBytes       = 32 << 20
)

// hbaseScanFindings reports, from the tasks table and the scans, the same
// table read again by later stages (hbase-repeated-scan), a scan of a
// whole large table (hbase-full-scan), a scan of many tiny regions
// (hbase-tiny-regions), and regions read more than once because their
// task failed (hbase-retried-regions).
func hbaseScanFindings(c *ctx, h *model.HBaseSection) {
	repeatedScans(c, h)
	fullScans(c, h)
	tinyRegions(c, h)
	retriedRegions(c, h)
}

// repeatedScans finds a table whose same regions several stages read: an
// RDD read from HBase and used by two actions without being cached is
// read twice.
func repeatedScans(c *ctx, h *model.HBaseSection) {
	type read struct {
		stage   int
		regions map[string]bool
		ms      int64
		src     model.Source
	}
	byTable := map[string]map[int]*read{}
	for _, t := range h.Tasks {
		if t.Stage < 0 {
			continue
		}
		if byTable[t.Table] == nil {
			byTable[t.Table] = map[int]*read{}
		}
		rd := byTable[t.Table][t.Stage] // a stage's attempts are one read
		if rd == nil {
			rd = &read{stage: t.Stage, regions: map[string]bool{}, src: t.Source}
			byTable[t.Table][t.Stage] = rd
		}
		rd.regions[t.StartRow+"\x00"+t.EndRow] = true
		rd.ms += t.DurationMs
	}
	type repeat struct {
		table string
		reads []*read
	}
	var worst *repeat
	tables := make([]string, 0, len(byTable))
	for tb := range byTable {
		tables = append(tables, tb)
	}
	sort.Strings(tables)
	for _, tb := range tables {
		var reads []*read
		for _, rd := range byTable[tb] {
			reads = append(reads, rd)
		}
		sort.Slice(reads, func(i, j int) bool { return reads[i].stage < reads[j].stage })
		first := reads[0]
		same := []*read{first}
		for _, rd := range reads[1:] {
			n := 0
			for k := range rd.regions {
				if first.regions[k] {
					n++
				}
			}
			if float64(n) >= sameRegions*float64(min(len(rd.regions), len(first.regions))) {
				same = append(same, rd)
			}
		}
		if len(same) > 1 && (worst == nil || len(same) > len(worst.reads)) {
			worst = &repeat{tb, same}
		}
	}
	if worst == nil {
		return
	}
	var stages []string
	var again int64
	var ev []model.Evidence
	for i, rd := range worst.reads {
		stages = append(stages, fmt.Sprint(rd.stage))
		if i > 0 {
			again += rd.ms
		}
		if len(ev) < 4 {
			ev = append(ev, model.Evidence{Source: rd.src, Text: fmt.Sprintf("stage %d read %s of %s", rd.stage, model.Plural(len(rd.regions), "region", "regions"), worst.table)})
		}
	}
	sev := model.Info
	if again >= c.t.MinRunTime.Milliseconds() {
		sev = model.Warning
	}
	c.add(model.Finding{Rule: "hbase-repeated-scan", Severity: sev, Section: "stages",
		Title: fmt.Sprintf("Stages %s each read the same regions of %s: the table was scanned %d times", listAnd(stages), worst.table, len(worst.reads)),
		Explanation: fmt.Sprintf("Each of these stages read the same key range of %s from HBase again, which cost %s of task time after the first read. Spark does not keep data between actions unless told to: an RDD read from HBase and used by two actions (a count, then a save) is read from HBase once per action.",
			worst.table, model.Duration(again)),
		Evidence: ev,
		Fix:      "Cache the RDD right after reading it when the code uses it more than once (rdd.persist(StorageLevel.MEMORY_AND_DISK), and unpersist when done), or restructure the job to read the table once and derive everything from that read."})
}

// fullScans finds a scan with no start or stop row over many regions or
// much data: a filter does not narrow which regions are read.
func fullScans(c *ctx, h *model.HBaseSection) {
	for _, sc := range h.Scans {
		n := len(sc.Regions)
		if n == 0 || sc.Regions[0].StartRow != "" || sc.Regions[n-1].EndRow != "" || n < fullScanRegions && sc.SizeBytes < fullScanBytes {
			continue
		}
		if sc.Scan != nil && (sc.Scan.StartRow != "" || sc.Scan.StopRow != "") {
			continue
		}
		size := ""
		if sc.SizedRegions > 0 {
			size = fmt.Sprintf(" (about %s, HBase's estimate)", model.Bytes(sc.SizeBytes))
		}
		filter := "If the job needs only part of the table, the region servers still read every row."
		if sc.Scan != nil && sc.Scan.Filter != nil {
			filter = "The scan has a filter (" + sc.Scan.Filter.String() + "), but a filter does not narrow which regions are read: the region servers read every row and drop those that do not match."
		}
		c.add(model.Finding{Rule: "hbase-full-scan", Severity: model.Info, Section: "stages",
			Title: fmt.Sprintf("Stage %d scanned all of %s: %s from the first row to the last", sc.StageID, sc.Table, model.Plural(n, "region", "regions")),
			Explanation: fmt.Sprintf("The scan's split lines cover %s from its first row to its last%s, so the scan set no start or stop row. %s",
				sc.Table, size, filter),
			Evidence: []model.Evidence{{Source: sc.Regions[0].Source, Text: fmt.Sprintf("the first region's split: %s from the first row", sc.Table)},
				{Source: sc.Regions[n-1].Source, Text: fmt.Sprintf("the last region's split: %s to the last row", sc.Table)}},
			Fix: "When the rows needed share a key prefix or range, set the scan's start and stop rows (Scan.withStartRow and withStopRow, or hbase.mapreduce.scan.row.start and hbase.mapreduce.scan.row.stop) so only the regions holding them are read. Row keys that start with what the job selects on (a date, a customer) make that possible."})
		return
	}
}

// tinyRegions finds a scan of many small regions: each region is a task,
// so the stage pays a task's start and a scanner's opening per region.
func tinyRegions(c *ctx, h *model.HBaseSection) {
	for _, sc := range h.Scans {
		var sizes []int64
		var ms []int64
		for _, g := range sc.Regions {
			if g.SizeBytes > 0 {
				sizes = append(sizes, g.SizeBytes)
			}
			if g.Task != nil {
				ms = append(ms, g.Task.DurationMs)
			}
		}
		if len(sizes) < tinyMinRegions {
			continue
		}
		slices.Sort(sizes)
		median := sizes[len(sizes)/2]
		if median >= tinyBytes {
			continue
		}
		small := 0
		for _, s := range sizes {
			if s < tinyBytes {
				small++
			}
		}
		var ev []model.Evidence
		for _, g := range sc.Regions {
			if g.SizeBytes > 0 && g.SizeBytes < tinyBytes && len(ev) < 3 {
				src := g.SizeSource
				if src.File == "" {
					src = g.Source
				}
				ev = append(ev, model.Evidence{Source: src, Text: fmt.Sprintf("region %s of %s: %s", g.Region, sc.Table, model.Bytes(g.SizeBytes))})
			}
		}
		took := ""
		if len(ms) > 0 {
			slices.Sort(ms)
			took = fmt.Sprintf(" The median region took %s to read.", model.Duration(ms[len(ms)/2]))
		}
		c.add(model.Finding{Rule: "hbase-tiny-regions", Severity: model.Info, Section: "stages",
			Title: fmt.Sprintf("Stage %d read %d small regions of %s: the median region holds %s", sc.StageID, len(sizes), sc.Table, model.Bytes(median)),
			Explanation: fmt.Sprintf("%d of the %d regions with a known size are under %d MiB, as TableInputFormat estimated them. TableInputFormat makes one task per region, so the stage starts a task, and opens a scanner on a region server, for each small piece of data.%s",
				small, len(sizes), tinyBytes>>20, took),
			Evidence: ev,
			Fix:      "Merge small regions (HBase shell: merge_region, or turn on the region normalizer for the table: normalizer_switch true and alter '" + sc.Table + "', NORMALIZATION_ENABLED => 'true'). Small regions often come from a table pre-split for more data than it holds, or from splits that no longer match how the data grew."})
		return
	}
}

// retriedRegions finds regions read more than once in a stage because
// their task failed, with what the region's server logged meanwhile.
func retriedRegions(c *ctx, h *model.HBaseSection) {
	type key struct{ stage, part int }
	attempts := map[key][]int{}
	for i, t := range h.Tasks {
		if t.Stage >= 0 && t.Partition >= 0 {
			k := key{t.Stage, t.Partition}
			attempts[k] = append(attempts[k], i)
		}
	}
	var keys []key
	for k, xs := range attempts {
		failed := slices.ContainsFunc(xs, func(i int) bool { return h.Tasks[i].Outcome == "failed" })
		if len(xs) > 1 && failed {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := len(attempts[keys[i]]), len(attempts[keys[j]])
		if a != b {
			return a > b
		}
		return keys[i].stage < keys[j].stage || keys[i].stage == keys[j].stage && keys[i].part < keys[j].part
	})
	var lines []string
	var ev []model.Evidence
	for n, k := range keys {
		xs := attempts[k]
		t := h.Tasks[xs[0]]
		var seen []string
		for _, i := range xs {
			for _, e := range h.Tasks[i].Events {
				if ev := h.RegionEvents[e].Event; causeEvents[ev] && !slices.Contains(seen, ev) {
					seen = append(seen, ev)
				}
			}
		}
		if n < maxSlowRegions {
			why := ""
			if len(seen) > 0 {
				why = "; its server logged " + strings.Join(seen, ", ") + " on it meanwhile"
			}
			lines = append(lines, fmt.Sprintf("partition %d of stage %d read region %s of %s on %s %d times%s", k.part, k.stage, t.Region, t.Table, shortServer(t.Server), len(xs), why))
		}
		for _, i := range xs {
			if h.Tasks[i].Outcome == "failed" && len(ev) < 6 {
				ev = append(ev, model.Evidence{Source: h.Tasks[i].EndSource, Text: fmt.Sprintf("task %s failed reading region %s", taskName(h.Tasks[i]), t.Region)})
			}
		}
	}
	if len(keys) > maxSlowRegions {
		lines = append(lines, fmt.Sprintf("and %d more in the HBase tasks table", len(keys)-maxSlowRegions))
	}
	c.add(model.Finding{Rule: "hbase-retried-regions", Severity: model.Warning, Section: "stages",
		Title:       fmt.Sprintf("%s read from HBase more than once because %s failed", model.Plural(len(keys), "region was", "regions were"), map[bool]string{true: "its task", false: "their tasks"}[len(keys) == 1]),
		Explanation: "A failed task's region is read again from the start by the next attempt: " + strings.Join(lines, "; ") + ".",
		Evidence:    ev,
		Fix:         "Read the failed attempt's error (linked). HBase reads usually fail on a scanner lease expiring (the task took longer than hbase.client.scanner.timeout.period between calls: lower the scan's caching, or raise the timeout), on a region moving or splitting under the scan, or on its region server being down."})
}

// listAnd joins short items as "1, 2 and 3".
func listAnd(xs []string) string {
	if len(xs) < 2 {
		return strings.Join(xs, "")
	}
	return strings.Join(xs[:len(xs)-1], ", ") + " and " + xs[len(xs)-1]
}
