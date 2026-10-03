package analyze

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// maxFlowSteps caps the points of each series over time.
const maxFlowSteps = 120

// analyzeFlows lays the task stories out over time: shuffle data read on
// each task's own node and over the network, data spilled and cached, and
// each executor's storage memory left; the broadcast variables read, and
// spills by stage. Then the findings these show: shuffle-network,
// cache-evicted, broadcast-large, task-spill and commit-slow.
func analyzeFlows(c *ctx, r *model.Report) {
	s := r.TaskStories
	if s == nil {
		return
	}
	type stepOf struct {
		st   model.TaskStep
		exec int // index into f.Executors
		task *model.TaskLog
		file string
	}
	f := &model.FlowSection{}
	execAt := map[string]int{}
	var steps []stepOf
	addExec := func(id, host, file string) int {
		if i, ok := execAt[id]; ok {
			return i
		}
		execAt[id] = len(f.Executors)
		f.Executors = append(f.Executors, model.ExecFlow{Executor: id, Host: host, Source: model.Source{File: file}})
		return len(f.Executors) - 1
	}
	for _, x := range s.Executors {
		i := addExec(x.Executor, x.Host, x.Source.File)
		for _, st := range x.Untied.Steps {
			steps = append(steps, stepOf{st: st, exec: i, file: x.Source.File})
		}
	}
	for k := range s.Tasks {
		t := &s.Tasks[k]
		id := t.Executor
		if id == "" {
			continue
		}
		i := addExec(id, t.Host, t.Source.File)
		for _, st := range t.Steps {
			steps = append(steps, stepOf{st: st, exec: i, task: t, file: t.Source.File})
		}
	}
	steps = slices.DeleteFunc(steps, func(x stepOf) bool { return x.st.T.IsZero() })
	sort.SliceStable(steps, func(i, j int) bool { return steps[i].st.T.Before(steps[j].st.T) })

	var from, to time.Time
	for _, x := range steps {
		switch x.st.Kind {
		case model.StepShuffle, model.StepSpill, model.StepCache:
			if from.IsZero() || x.st.T.Before(from) {
				from = x.st.T
			}
			if x.st.T.After(to) {
				to = x.st.T
			}
		}
	}
	n := 0
	if !from.IsZero() {
		f.StepMs = max(int64(1000), (to.Sub(from).Milliseconds()/maxFlowSteps+999)/1000*1000)
		f.From = from
		n = int(to.Sub(from).Milliseconds()/f.StepMs) + 1
	}
	series := func() []model.Point {
		ps := make([]model.Point, n)
		for i := range ps {
			ps[i].T = from.Add(time.Duration(int64(i)*f.StepMs) * time.Millisecond)
		}
		return ps
	}
	if n > 0 {
		f.Local, f.Remote, f.Spill, f.Cached = series(), series(), series(), series()
	}
	at := func(t time.Time) int { return int(t.Sub(from).Milliseconds() / f.StepMs) }
	bcasts := map[string]*model.BroadcastRead{}
	bcastExec := map[string]map[int]bool{}
	remoteMax := map[int]int64{}
	for _, x := range steps {
		st, e := x.st, &f.Executors[x.exec]
		src := model.Source{File: x.file, Line: st.Line}
		switch st.Kind {
		case model.StepShuffle:
			local := st.Bytes - st.Remote
			k := at(st.T)
			f.Local[k].V += float64(local)
			f.Remote[k].V += float64(st.Remote)
			f.LocalBytes += local
			f.RemoteBytes += st.Remote
			e.LocalBytes += local
			e.RemoteBytes += st.Remote
			if st.Remote > 0 {
				if e.Remote == nil {
					e.Remote = series()
				}
				e.Remote[k].V += float64(st.Remote)
				if st.Remote > remoteMax[x.exec] {
					remoteMax[x.exec], e.RemoteSource = st.Remote, src
				}
			}
		case model.StepSpill:
			f.Spill[at(st.T)].V += float64(st.Bytes)
			f.SpillBytes += st.Bytes
			e.SpillBytes += st.Bytes
		case model.StepCache, model.StepDrop:
			if st.Kind == model.StepCache {
				f.Cached[at(st.T)].V += float64(st.Bytes)
				f.CachedBytes += st.Bytes
				e.CachedBytes += st.Bytes
			} else {
				f.Dropped += st.N
				e.Dropped += st.N
				if e.EvictSource.File == "" {
					e.EvictSource = src
				}
			}
			e.Free = append(e.Free, model.Point{T: st.T, V: float64(st.Free)})
			if len(e.Free) == 1 || st.Free < e.MinFree {
				e.MinFree, e.MinFreeAt, e.MinFreeSource = st.Free, st.T, src
			}
		case model.StepNoRoom:
			f.NotCached++
			e.NotCached++
			if e.EvictSource.File == "" {
				e.EvictSource = src
			}
		case model.StepBroadcast:
			b := bcasts[st.Name]
			if b == nil {
				b = &model.BroadcastRead{Name: st.Name, First: st.T, Source: src}
				bcasts[st.Name], bcastExec[st.Name] = b, map[int]bool{}
			}
			b.Bytes, b.Pieces = max(b.Bytes, st.Bytes), max(b.Pieces, st.N)
			bcastExec[st.Name][x.exec] = true
		case model.StepBcastRead:
			b := bcasts[st.Name]
			if b == nil {
				continue // its start was not kept
			}
			b.ReadMs += st.Ms
			if st.Ms > b.MaxMs {
				b.MaxMs, b.SlowestOn, b.MaxSource = st.Ms, e.Executor, src
			}
		}
	}
	for name, b := range bcasts {
		b.Executors = len(bcastExec[name])
		f.Broadcasts = append(f.Broadcasts, *b)
	}
	sort.Slice(f.Broadcasts, func(i, j int) bool {
		a, b := f.Broadcasts[i], f.Broadcasts[j]
		return a.Bytes > b.Bytes || a.Bytes == b.Bytes && a.First.Before(b.First)
	})
	f.Spills = stageSpills(s)
	sort.SliceStable(f.Executors, func(i, j int) bool {
		a, b := f.Executors[i], f.Executors[j]
		if a.RemoteBytes != b.RemoteBytes {
			return a.RemoteBytes > b.RemoteBytes
		}
		return execLess(a.Executor, b.Executor)
	})
	commitSlow(c, s)
	if n == 0 && len(f.Broadcasts) == 0 && f.Dropped+f.NotCached == 0 {
		return
	}
	r.Flows = f
	shuffleNetwork(c, f)
	cacheEvicted(c, f)
	broadcastLarge(c, f)
	if !c.metrics() { // with the event log, memory-spill says it from exact bytes
		taskSpill(c, f)
	}
}

// stageSpills adds up spills by stage, from the tasks that logged them
// and, as stage -1, the lines no task could be found for.
func stageSpills(s *model.TaskStorySection) []model.StageSpillLog {
	type key struct{ stage, attempt int }
	by := map[key]*model.StageSpillLog{}
	add := func(k key, t *model.TaskLog) {
		x := by[k]
		if x == nil {
			x = &model.StageSpillLog{Stage: k.stage, Attempt: k.attempt, Source: t.Source}
			by[k] = x
		}
		x.Spills += t.Spills
		x.Bytes += t.SpillBytes
		if t.TaskID >= 0 {
			x.Tasks++
			if t.SpillBytes > x.MostBytes {
				x.MostBytes, x.MostTask, x.Source = t.SpillBytes, t.TaskID, t.Source
			}
		}
	}
	for i := range s.Tasks {
		if t := &s.Tasks[i]; t.Spills > 0 {
			add(key{t.Stage, t.StageAttempt}, t)
		}
	}
	for i := range s.Executors {
		if u := &s.Executors[i].Untied; u.Spills > 0 {
			add(key{-1, 0}, u)
		}
	}
	var out []model.StageSpillLog
	for _, x := range by {
		out = append(out, *x)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Bytes > out[j].Bytes })
	return out
}

// shuffleNetwork reports shuffle data read over the network, from at
// least network-min of it: a warning when one executor read far more
// than the others, otherwise a note of how much crossed between nodes.
func shuffleNetwork(c *ctx, f *model.FlowSection) {
	if f.RemoteBytes < c.t.NetworkMin || f.RemoteBytes == 0 {
		return
	}
	all := f.LocalBytes + f.RemoteBytes
	var reads []int64
	for _, e := range f.Executors {
		if e.RemoteBytes > 0 {
			reads = append(reads, e.RemoteBytes)
		}
	}
	top := f.Executors[0] // busiest over the network first
	ev := []model.Evidence{{Source: top.RemoteSource, Text: fmt.Sprintf("executor %s's largest shuffle read over the network", top.Executor)}}
	if len(f.Executors) > 1 && f.Executors[1].RemoteSource.File != "" {
		ev = append(ev, model.Evidence{Source: f.Executors[1].RemoteSource, Text: fmt.Sprintf("executor %s's largest shuffle read over the network", f.Executors[1].Executor)})
	}
	rest := slices.Clone(reads[1:])
	slices.Sort(rest)
	if len(rest) > 0 && rest[len(rest)/2] > 0 && float64(top.RemoteBytes) >= 3*float64(rest[len(rest)/2]) {
		med := rest[len(rest)/2]
		c.add(model.Finding{Rule: "shuffle-network", Severity: model.Warning, Section: "tasklogs",
			Title: fmt.Sprintf("Executor %s read %s of shuffle data over the network, %.0f× the median of the other executors", top.Executor, model.Bytes(top.RemoteBytes), float64(top.RemoteBytes)/float64(med)),
			Explanation: fmt.Sprintf("Its tasks fetched %s from other nodes. The median of the other executors that fetched data was %s. A task reads one partition of the output of the stage before it from each node that wrote the partition. As a result, when one executor fetches much more, its tasks had the largest partitions. A few keys held much of the data.\nIn all, %s of the %s of shuffle reads in the run (%s) went over the network. The sizes are estimates that Spark makes from the map outputs.",
				model.Bytes(top.RemoteBytes), model.Bytes(med), model.Bytes(f.RemoteBytes), model.Bytes(all), model.Percent(share(f.RemoteBytes, all))),
			Evidence: ev,
			Fix:      "Find the keys that have too many rows. For example, count the rows for each key before the shuffle. Then do one of these:\n- Add a random salt to the key.\n- Remove null or default keys first.\n- Set spark.sql.adaptive.skewJoin.enabled=true."})
		return
	}
	c.add(model.Finding{Rule: "shuffle-network", Severity: model.Info, Section: "tasklogs",
		Title: fmt.Sprintf("%s of shuffle data crossed the network between nodes (%s of shuffle reads)", model.Bytes(f.RemoteBytes), model.Percent(share(f.RemoteBytes, all))),
		Explanation: fmt.Sprintf("Tasks read %s of output from the stages before them. They read %s from their own node and %s from other nodes over the network, in %s. Data that moves between nodes uses network time and disk reads on the other nodes. When a job shuffles less data, it runs faster. The sizes are estimates that Spark makes from the map outputs.",
			model.Bytes(all), model.Bytes(f.LocalBytes), model.Bytes(f.RemoteBytes), model.Plural(len(reads), "executor", "executors")),
		Evidence: ev,
		Fix:      "Shuffle less data:\n- Filter the rows and select the columns before joins and aggregations.\n- Broadcast the small side of a join, so that Spark does not shuffle both sides (spark.sql.autoBroadcastJoinThreshold).\n- Use the same partitioning again. Do not repartition again."})
}

// cacheEvicted reports cached blocks dropped from memory, or that did not
// fit: Spark computes them again when they are next used.
func cacheEvicted(c *ctx, f *model.FlowSection) {
	if f.Dropped+f.NotCached == 0 {
		return
	}
	var ev []model.Evidence
	var where []string
	for _, e := range f.Executors {
		if e.Dropped+e.NotCached == 0 {
			continue
		}
		if len(ev) < 4 {
			ev = append(ev, model.Evidence{Source: e.EvictSource, Text: fmt.Sprintf("executor %s's first block dropped or not cached", e.Executor)})
		}
		if len(where) < 5 {
			where = append(where, fmt.Sprintf("executor %s (%s dropped, %s did not fit; least storage memory free %s)", e.Executor, model.Num(int64(e.Dropped)), model.Num(int64(e.NotCached)), model.Bytes(e.MinFree)))
		}
	}
	var what []string
	if f.Dropped > 0 {
		what = append(what, model.Plural(f.Dropped, "cached block was", "cached blocks were")+" dropped from memory to make room")
	}
	if f.NotCached > 0 {
		what = append(what, model.Plural(f.NotCached, "block did not fit and was not cached", "blocks did not fit and were not cached"))
	}
	c.add(model.Finding{Rule: "cache-evicted", Severity: model.Warning, Section: "tasklogs",
		Title: "Cached data did not fit in memory (" + strings.Join(what, ", and ") + ")",
		Explanation: "When Spark drops a cached partition, or does not store it, Spark calculates the partition again from its source when the code uses it next. This can take as much time as the first calculation. This occurred on " + listAnd(where) +
			". The executors cached " + model.Bytes(f.CachedBytes) + " in all, as Spark estimated it.",
		Evidence: ev,
		Fix:      "Cache only the data that the code uses two or more times. Unpersist it when the code no longer uses it.\nThen do one of these:\n- Store it in serialized form, or let it spill to disk (persist(StorageLevel.MEMORY_AND_DISK_SER)).\n- Give the executors more memory (spark.executor.memory).\n- Give storage a larger part of the memory (spark.memory.storageFraction)."})
}

// broadcastLarge reports a broadcast variable of at least broadcast-large,
// or one an executor took at least broadcast-slow to read.
func broadcastLarge(c *ctx, f *model.FlowSection) {
	for _, b := range f.Broadcasts {
		big, slow := b.Bytes >= c.t.BroadcastLarge, b.MaxMs >= c.t.BroadcastSlow.Milliseconds()
		if !big && !slow {
			continue
		}
		title := fmt.Sprintf("Broadcast variable %s is %s, and %s read it", strings.TrimPrefix(b.Name, "broadcast "), model.Bytes(b.Bytes), model.Plural(b.Executors, "executor", "executors"))
		if !big {
			title = fmt.Sprintf("Executor %s took %s to read broadcast variable %s (%s)", b.SlowestOn, model.Duration(b.MaxMs), strings.TrimPrefix(b.Name, "broadcast "), model.Bytes(b.Bytes))
		}
		ev := []model.Evidence{{Source: b.Source, Text: fmt.Sprintf("the first read of %s: %s in %s", b.Name, model.Bytes(b.Bytes), model.Plural(b.Pieces, "piece", "pieces"))}}
		if b.MaxSource.File != "" {
			ev = append(ev, model.Evidence{Source: b.MaxSource, Text: fmt.Sprintf("the slowest read, on executor %s: %s", b.SlowestOn, model.Duration(b.MaxMs))})
		}
		c.add(model.Finding{Rule: "broadcast-large", Severity: model.Warning, Section: "tasklogs",
			Title: title,
			Explanation: fmt.Sprintf("Each executor that uses a broadcast variable fetches all of it and keeps it in memory. The driver held it first. This variable is %s, as Spark estimated it. Its reads took %s in all, and the slowest read took %s. A large broadcast makes the stages that use it slower. It also takes memory from cache and execution on each executor.",
				model.Bytes(b.Bytes), model.Duration(b.ReadMs), model.Duration(b.MaxMs)),
			Evidence: ev,
			Fix:      "Broadcast only small tables:\n- Decrease spark.sql.autoBroadcastJoinThreshold, or remove the broadcast hint. Then Spark joins a large side with a shuffle.\n- Select only the necessary columns before the broadcast.\n- Do not broadcast large Python or Java objects from the driver."})
		return
	}
}

// taskSpill reports, without the event log, data spilled to disk of at
// least spill-min.
func taskSpill(c *ctx, f *model.FlowSection) {
	if f.SpillBytes < c.t.SpillMin || len(f.Spills) == 0 {
		return
	}
	top := f.Spills[0]
	where := "lines that named no task"
	if top.Stage >= 0 {
		where = fmt.Sprintf("stage %d (%s, the most by task %d: %s)", top.Stage, model.Plural(top.Tasks, "task", "tasks"), top.MostTask, model.Bytes(top.MostBytes))
	}
	c.add(model.Finding{Rule: "task-spill", Severity: model.Warning, Section: "tasklogs",
		Title: fmt.Sprintf("Tasks spilled %s from memory to disk", model.Bytes(f.SpillBytes)),
		Explanation: fmt.Sprintf("When a sort, a join or an aggregation cannot keep its data in memory, it writes a part of the data to local disk. Then it reads that data back. This is slow. The largest spill was in %s, with %s. The sizes are the sizes of the data in memory.",
			where, model.Bytes(top.Bytes)),
		Evidence: []model.Evidence{{Source: top.Source, Text: "a task that spilled"}},
		Fix:      "Do one of these:\n- Give each task less data. Increase spark.sql.shuffle.partitions, or let adaptive execution coalesce the partitions.\n- Give each task more memory. Increase spark.executor.memory, or use fewer cores for each executor."})
}

// commitSlow reports output commits that took at least commit-share of
// the writing tasks' time, and at least min-run-time in all.
func commitSlow(c *ctx, s *model.TaskStorySection) {
	var commitMs, taskMs, worst int64
	var src model.Source
	writers := 0
	for i := range s.Tasks {
		t := &s.Tasks[i]
		if t.Commits == 0 {
			continue
		}
		writers++
		commitMs += t.CommitMs
		taskMs += t.DurationMs()
		for _, st := range t.Steps {
			if st.Kind == model.StepCommit && st.Ms > worst {
				worst, src = st.Ms, model.Source{File: t.Source.File, Line: st.Line}
			}
		}
	}
	if writers == 0 || taskMs <= 0 || commitMs < c.t.MinRunTime.Milliseconds() || share(commitMs, taskMs) < c.t.CommitShare {
		return
	}
	c.add(model.Finding{Rule: "commit-slow", Severity: model.Warning, Section: "tasklogs",
		Title: fmt.Sprintf("Output commits took %s of the time of the tasks that wrote output (%s in all)", model.Percent(share(commitMs, taskMs)), model.Duration(commitMs)),
		Explanation: fmt.Sprintf("%s committed output. Each task moves its files to their final place when it ends. The commits took %s of their %s, and the slowest commit took %s. On S3, a commit that renames files copies them. As a result, many small files make the commit slow.",
			model.Plural(writers, "task", "tasks"), model.Duration(commitMs), model.Duration(taskMs), model.Duration(worst)),
		Evidence: []model.Evidence{{Source: src, Text: "the slowest commit: " + model.Duration(worst)}},
		Fix:      "Write fewer and larger files. Use coalesce or repartition before the write.\nOn EMR, keep the EMRFS S3-optimized committer on (spark.sql.parquet.fs.optimized.committer.optimization-enabled). Then commits do not copy files."})
}
