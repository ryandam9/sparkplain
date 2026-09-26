package analyze

import (
	"sort"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

func analyzeIO(c *ctx, r *model.Report) {
	s := &r.IO
	if !c.has() {
		s.Coverage = model.NeedsEventLog
		s.Missing = []string{"Bytes and rows read and written per stage", "Shuffle read and write", "Cached data", "Tables and paths from SQL plans"}
		return
	}
	s.Coverage = model.Complete
	s.BlockKinds = c.log.BlockKinds
	for _, st := range c.log.Stages {
		s.Totals.Add(st.Totals)
		t := st.Totals
		if t.InputBytes+t.OutputBytes+t.ShuffleReadBytes+t.ShuffleWriteBytes == 0 {
			continue
		}
		s.Stages = append(s.Stages, model.StageIO{StageID: st.ID, Attempt: st.Attempt, Name: st.Name, Totals: t, Source: st.TaskSource})
	}
	moved := func(t model.TaskTotals) int64 {
		return t.InputBytes + t.OutputBytes + t.ShuffleReadBytes + t.ShuffleWriteBytes
	}
	sort.SliceStable(s.Stages, func(i, j int) bool { return moved(s.Stages[i].Totals) > moved(s.Stages[j].Totals) })
	if len(s.Stages) > 20 {
		s.Stages = s.Stages[:20]
	}
	s.Cached = c.log.RDDs
	if s.Cached == nil {
		s.Cached = []*model.CachedRDD{}
	}
	seen := map[string]bool{}
	add := func(d model.DataRef) {
		k := d.Access + "|" + d.Kind + "|" + d.Name
		if !seen[k] {
			seen[k] = true
			s.Data = append(s.Data, d)
		}
	}
	for _, q := range c.log.SQL {
		for _, d := range q.Reads {
			add(d)
		}
		for _, d := range q.Writes {
			add(d)
		}
	}
	for _, d := range c.log.CatalogEvents {
		add(d)
	}
	order := map[string]int{"read": 0, "write": 1, "create": 2, "alter": 3, "rename": 4, "drop": 5}
	sort.SliceStable(s.Data, func(i, j int) bool {
		if order[s.Data[i].Access] != order[s.Data[j].Access] {
			return order[s.Data[i].Access] < order[s.Data[j].Access]
		}
		return s.Data[i].Name < s.Data[j].Name
	})
	s.Missing = []string{"S3 request counts and throttling (needs CloudTrail data events or S3 server logs)"}
	sizeUnknown := false
	for _, rdd := range s.Cached {
		if !rdd.SizeKnown {
			sizeUnknown = true
		}
	}
	if sizeUnknown {
		s.Coverage = model.Partial
		s.Missing = append(s.Missing, "Cached data sizes: Spark only logs them when spark.eventLog.logBlockUpdates.enabled=true")
	}
}
