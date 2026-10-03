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
// read twice. PySpark's newAPIHadoopRDD first takes one record to check it
// can be sent to Python ("take at SerDeUtil.scala"), reading the first
// region again; that probe is not a second read of the table.
func repeatedScans(c *ctx, h *model.HBaseSection) {
	type read struct {
		stage   int
		regions map[string]bool
		ms      int64
		src     model.Source
	}
	probe := map[int]bool{}
	if c.log != nil {
		for _, s := range c.log.Stages {
			if strings.HasPrefix(s.Name, "take at SerDeUtil.scala") {
				probe[s.ID] = true
			}
		}
	}
	byTable := map[string]map[int]*read{}
	for _, t := range h.Tasks {
		if t.Stage < 0 || probe[t.Stage] {
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
		Title: fmt.Sprintf("Stages %s each read the same regions of %s, and the job scanned the table %d times", listAnd(stages), worst.table, len(worst.reads)),
		Explanation: fmt.Sprintf("Each of these stages read the same key range of %s from HBase again. This used %s of task time after the first read. Spark keeps no data between actions, if the code does not tell it to. When two actions use an RDD from HBase (for example a count and then a save), Spark reads HBase one time for each action.",
			worst.table, model.Duration(again)),
		Evidence: ev,
		Fix:      "Do one of these:\n- Cache the RDD immediately after the read, if the code uses it two or more times. Use rdd.persist(StorageLevel.MEMORY_AND_DISK), and unpersist it at the end.\n- Change the job so that it reads the table one time and gets all results from that read."})
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
		filter := "If the job uses only a part of the table, the region servers still read all rows."
		if sc.Scan != nil && sc.Scan.Filter != nil {
			filter = "The scan has a filter (" + sc.Scan.Filter.String() + "). But a filter does not decrease the number of regions that HBase reads. The region servers read all rows and drop the rows that do not match."
		}
		c.add(model.Finding{Rule: "hbase-full-scan", Severity: model.Info, Section: "stages",
			Title: fmt.Sprintf("Stage %d scanned all of %s, %s from the first row to the last", sc.StageID, sc.Table, model.Plural(n, "region", "regions")),
			Explanation: fmt.Sprintf("The split lines of the scan cover %s from its first row to its last row%s. As a result, the scan set no start row and no stop row. %s",
				sc.Table, size, filter),
			Evidence: []model.Evidence{{Source: sc.Regions[0].Source, Text: fmt.Sprintf("the first region's split: %s from the first row", sc.Table)},
				{Source: sc.Regions[n-1].Source, Text: fmt.Sprintf("the last region's split: %s to the last row", sc.Table)}},
			Fix: "Set a start row and a stop row on the scan. Do this if the necessary rows share a key prefix or a key range. Then HBase reads only the regions that hold them. Use one of these:\n- Scan.withStartRow and Scan.withStopRow.\n- hbase.mapreduce.scan.row.start and hbase.mapreduce.scan.row.stop.\nThis is possible when the row keys start with the value that the job selects, for example a date."})
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
			took = fmt.Sprintf(" The read of the median region took %s.", model.Duration(ms[len(ms)/2]))
		}
		c.add(model.Finding{Rule: "hbase-tiny-regions", Severity: model.Info, Section: "stages",
			Title: fmt.Sprintf("Stage %d read %d small regions of %s, and the median region holds %s", sc.StageID, len(sizes), sc.Table, model.Bytes(median)),
			Explanation: fmt.Sprintf("%d of the %d regions with a known size are less than %d MiB, as TableInputFormat estimated them. TableInputFormat makes one task for each region. As a result, for each small piece of data, the stage starts a task and opens a scanner on a region server.%s",
				small, len(sizes), tinyBytes>>20, took),
			Evidence: ev,
			Fix:      "Merge the small regions. Do one of these in the HBase shell:\n- Use merge_region.\n- Start the region normalizer for the table: normalizer_switch true, and alter '" + sc.Table + "', NORMALIZATION_ENABLED => 'true'.\nSmall regions often come from a pre-split for more data than the table holds. They also come from splits that do not agree with the growth of the data."})
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
				why = ". At the same time, its server logged " + strings.Join(seen, ", ") + " on it"
			}
			lines = append(lines, fmt.Sprintf("Partition %d of stage %d read region %s of %s on %s %d times%s.", k.part, k.stage, t.Region, t.Table, shortServer(t.Server), len(xs), why))
		}
		for _, i := range xs {
			if h.Tasks[i].Outcome == "failed" && len(ev) < 6 {
				ev = append(ev, model.Evidence{Source: h.Tasks[i].EndSource, Text: fmt.Sprintf("task %s failed reading region %s", taskName(h.Tasks[i]), t.Region)})
			}
		}
	}
	if len(keys) > maxSlowRegions {
		lines = append(lines, fmt.Sprintf("The HBase tasks table shows %d more regions.", len(keys)-maxSlowRegions))
	}
	c.add(model.Finding{Rule: "hbase-retried-regions", Severity: model.Warning, Section: "stages",
		Title:       fmt.Sprintf("Spark read %s from HBase two or more times because %s failed", model.Plural(len(keys), "region", "regions"), map[bool]string{true: "its task", false: "their tasks"}[len(keys) == 1]),
		Explanation: "When a task fails, the next attempt reads its region again from the start.\n- " + strings.Join(lines, "\n- "),
		Evidence:    ev,
		Fix:         "Read the error of the failed attempt (linked). HBase reads usually fail for one of these causes:\n- The scanner lease expired. Decrease the scan caching, or increase hbase.client.scanner.timeout.period.\n- The region moved or split during the scan.\n- The region server stopped."})
}

// listAnd joins short items as "1, 2 and 3".
func listAnd(xs []string) string {
	if len(xs) < 2 {
		return strings.Join(xs, "")
	}
	return strings.Join(xs[:len(xs)-1], ", ") + " and " + xs[len(xs)-1]
}
