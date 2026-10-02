package analyze

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// analyzeStageData gives each stage the folders and tables it read and
// wrote, from four sources that each know part of it:
//   - the executors' logs: the files and file ranges its tasks read and
//     the files they wrote, by folder, with counts and sizes;
//   - the SQL plans: a scan's table or path goes to the stages whose RDDs
//     ran that scan (Spark names their scope after the plan's node, as in
//     "Scan parquet db.t"), and a write's to the stages that ran WriteFiles,
//     or else to the final stage of the query's last job;
//   - the HBase section: the tables a stage scanned or wrote.
//
// The same folder or table from several sources is one entry that names
// them all.
func analyzeStageData(c *ctx, r *model.Report) {
	if c.log == nil || len(c.log.Stages) == 0 {
		return
	}
	byID := map[int][]*model.Stage{}
	for _, st := range c.log.Stages {
		st.Data = nil
		byID[st.ID] = append(byID[st.ID], st)
	}
	index := map[string]int{} // stage key and entry key → index in its stage's Data
	add := func(st *model.Stage, d model.StageData, from string) {
		d.Stage, d.StageAttempt = st.ID, st.Attempt
		d.Name = strings.TrimSuffix(d.Name, "/")
		key := func(name string) string {
			return strconv.Itoa(st.ID) + "." + strconv.Itoa(st.Attempt) + " " + d.Access + " " + d.Kind + " " + name
		}
		k := key(d.Name)
		if _, ok := index[k]; !ok && d.Kind == model.DataPath {
			// A plan that names files: the folder the logs name for them.
			if dir, files := folderOf(d.Name); dir != "" {
				if _, ok := index[key(dir)]; ok {
					k, d.Paths = key(dir), files
				}
			}
		}
		if i, ok := index[k]; ok {
			e := &st.Data[i]
			e.Parts += d.Parts
			e.Sized += d.Sized
			e.Bytes += d.Bytes
			if e.Format == "" {
				e.Format = d.Format
			}
			for _, p := range d.Paths {
				if len(e.Paths) < model.MaxDataPaths && !slices.Contains(e.Paths, p) {
					e.Paths = append(e.Paths, p)
				}
			}
			if !slices.Contains(e.From, from) {
				e.From = append(e.From, from)
			}
			return
		}
		index[k] = len(st.Data)
		d.From = []string{from}
		st.Data = append(st.Data, d)
	}
	untied := 0
	if r.Logs != nil {
		for _, f := range r.Logs.Files {
			if f.EarlierAttempt > 0 {
				continue
			}
			untied += f.DataUntied
			for _, d := range f.StageData {
				for _, st := range byID[d.Stage] {
					if st.Attempt == d.StageAttempt {
						d.Source.File = f.Location
						add(st, d, "executor logs")
					}
				}
			}
		}
	}
	for _, q := range c.log.SQL {
		sqlStageData(c, q, byID, add)
	}
	if r.HBase != nil {
		for _, hs := range r.HBase.Stages {
			for _, st := range byID[hs.StageID] {
				if st.Attempt != hs.Attempt {
					continue
				}
				for _, t := range hs.Reads {
					add(st, model.StageData{Access: model.DataRead, Kind: model.DataHBase, Name: t, Source: hs.Source}, "HBase")
				}
				for _, t := range hs.Writes {
					add(st, model.StageData{Access: model.DataWrite, Kind: model.DataHBase, Name: t, Source: hs.Source}, "HBase")
				}
			}
		}
	}
	for _, st := range c.log.Stages {
		sort.SliceStable(st.Data, func(i, j int) bool {
			a, b := st.Data[i], st.Data[j]
			if a.Access != b.Access {
				return a.Access == model.DataRead
			}
			return a.Name < b.Name
		})
	}
	if untied > 0 {
		r.Jobs.Missing = append(r.Jobs.Missing, fmt.Sprintf("Which stage read or wrote %d files or file ranges: their executors were running tasks of more than one stage when they logged them, in a layout that does not print the task's thread", untied))
	}
}

// sqlStageData places a query's reads and writes on the stages that ran
// them. Skipped stages read and wrote nothing.
func sqlStageData(c *ctx, q *model.SQLQuery, byID map[int][]*model.Stage, add func(*model.Stage, model.StageData, string)) {
	if len(q.Reads)+len(q.Writes) == 0 {
		return
	}
	var stages []*model.Stage
	var last []*model.Stage // the stages of the query's last job
	jobs := map[int]bool{}
	for _, id := range q.JobIDs {
		jobs[id] = true
	}
	lastJob := -1
	for _, j := range c.log.Jobs {
		if jobs[j.ID] && j.ID > lastJob {
			lastJob = j.ID
		}
	}
	for _, j := range c.log.Jobs {
		if !jobs[j.ID] {
			continue
		}
		for _, id := range j.StageIDs {
			for _, st := range byID[id] {
				if st.Status == model.StatusSkipped {
					continue
				}
				stages = append(stages, st)
				if j.ID == lastJob {
					last = append(last, st)
				}
			}
		}
	}
	ref := func(d model.DataRef, access string) model.StageData {
		kind := model.DataPath
		if d.Kind == "table" {
			kind = model.DataTable
		}
		return model.StageData{Access: access, Kind: kind, Name: d.Name, Format: d.Format, Source: d.Source}
	}
	for _, d := range q.Reads {
		if d.Format == "hbase" || d.Node == "" {
			continue // the HBase section ties the connector's tables to stages
		}
		for _, st := range stages {
			if runs(st, d.Node) {
				add(st, ref(d, model.DataRead), "SQL plan")
			}
		}
	}
	var writers []*model.Stage
	for _, st := range stages {
		if runs(st, "WriteFiles") {
			writers = append(writers, st)
		}
	}
	if len(writers) == 0 && len(last) > 0 {
		final := last[0]
		for _, st := range last {
			if st.ID > final.ID {
				final = st
			}
		}
		writers = []*model.Stage{final}
	}
	for _, d := range q.Writes {
		if d.Format == "hbase" {
			continue
		}
		for _, st := range writers {
			add(st, ref(d, model.DataWrite), "SQL plan")
		}
	}
}

// runs reports whether one of the stage's RDDs was made by the plan node
// named op.
func runs(st *model.Stage, op string) bool {
	for _, rdd := range st.RDDs {
		if rdd.Operation == op {
			return true
		}
	}
	return false
}

// folderOf is the one folder a plan's comma-separated files are in, and
// the files; "" when they are in more than one.
func folderOf(names string) (string, []string) {
	var dir string
	var files []string
	for _, p := range strings.Split(names, ", ") {
		f := model.DataFolder(p)
		if f == "" || f == p || dir != "" && f != dir {
			return "", nil
		}
		dir = f
		if len(files) < model.MaxDataPaths {
			files = append(files, p)
		}
	}
	return dir, files
}
