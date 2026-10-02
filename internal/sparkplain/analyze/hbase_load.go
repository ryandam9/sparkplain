package analyze

import (
	"fmt"
	"sort"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// Load over time is kept as the most tasks at once per step, at most
// maxLoadSteps steps, each at least a second; a stretch counts as hot
// only with at least minHotTasks scan tasks running on the server.
const (
	maxLoadSteps = 240
	minHotTasks  = 4
)

// hbaseLoad works out, from the tasks table's start and end times, how many
// TableInputFormat tasks each region server was serving at once over the
// run, and when one served most of those running (at least the hotspot
// share) for a while: hbase-server-load.
func hbaseLoad(c *ctx, h *model.HBaseSection) {
	type ev struct {
		t      time.Time
		server string
		d      int
		task   int
	}
	var evs []ev
	for i, t := range h.Tasks {
		if t.Start.IsZero() || !t.End.After(t.Start) || t.Server == "" {
			continue
		}
		evs = append(evs, ev{t.Start, t.Server, 1, i}, ev{t.End, t.Server, -1, i})
	}
	if len(evs) == 0 {
		return
	}
	// Ends before starts at the same instant: back-to-back tasks are not
	// at once.
	sort.SliceStable(evs, func(i, j int) bool {
		if !evs[i].t.Equal(evs[j].t) {
			return evs[i].t.Before(evs[j].t)
		}
		return evs[i].d < evs[j].d
	})
	from, to := evs[0].t, evs[len(evs)-1].t
	// Whole seconds, rounded up so there are no more than maxLoadSteps.
	step := (to.Sub(from)/maxLoadSteps + time.Second).Truncate(time.Second)
	nSteps := int(to.Sub(from)/step) + 1

	type state struct {
		load      *model.HBaseServerLoad
		n         int
		busySince time.Time
		steps     []int
		hot       *model.HBaseHot // the stretch going on
	}
	servers := map[string]*state{}
	var order []*state
	for _, e := range evs {
		if servers[e.server] == nil {
			s := &state{load: &model.HBaseServerLoad{Server: e.server}, steps: make([]int, nSteps)}
			servers[e.server], order = s, append(order, s)
		}
		if e.d > 0 {
			t := h.Tasks[e.task]
			servers[e.server].load.Tasks++
			servers[e.server].load.TaskMs += t.End.Sub(t.Start).Milliseconds()
		}
	}
	total, totals := 0, make([]int, nSteps)
	stepOf := func(t time.Time) int { return min(int(t.Sub(from)/step), nSteps-1) }
	// endHot closes a server's hot stretch at t, keeping the longest.
	endHot := func(s *state, t time.Time) {
		if s.hot == nil {
			return
		}
		s.hot.To = t
		if old := s.load.Hot; old == nil || s.hot.To.Sub(s.hot.From) > old.To.Sub(old.From) {
			s.load.Hot = s.hot
		}
		s.hot = nil
	}
	// fill carries each server's count through the steps with no event.
	last := 0
	fill := func(upTo int) {
		for b := last + 1; b <= upTo; b++ {
			for _, s := range order {
				s.steps[b] = max(s.steps[b], s.n)
			}
			totals[b] = max(totals[b], total)
		}
		last = max(last, upTo)
	}
	for i := 0; i < len(evs); {
		t := evs[i].t
		fill(stepOf(t))
		for ; i < len(evs) && evs[i].t.Equal(t); i++ {
			e := evs[i]
			s := servers[e.server]
			if s.n == 0 && e.d > 0 {
				s.busySince = t
			}
			s.n += e.d
			total += e.d
			if s.n == 0 && e.d < 0 {
				s.load.BusyMs += t.Sub(s.busySince).Milliseconds()
			}
			if s.n > s.load.Peak {
				s.load.Peak, s.load.PeakAt = s.n, t
			}
			b := stepOf(t)
			s.steps[b] = max(s.steps[b], s.n)
			totals[b] = max(totals[b], total)
		}
		// Which server, if any, serves most of what runs now.
		for _, s := range order {
			hot := len(order) > 1 && s.n >= minHotTasks && float64(s.n) >= c.t.HBaseHotspot*float64(total)
			switch {
			case hot && s.hot == nil:
				s.hot = &model.HBaseHot{From: t, Tasks: s.n, All: total, At: t}
			case hot && s.n > s.hot.Tasks:
				s.hot.Tasks, s.hot.All, s.hot.At = s.n, total, t
			case !hot:
				endHot(s, t)
			}
		}
	}
	for _, s := range order {
		endHot(s, to)
		for b, v := range s.steps {
			s.load.Points = append(s.load.Points, model.Point{T: from.Add(time.Duration(b) * step), V: float64(v)})
		}
		h.Load = append(h.Load, *s.load)
	}
	for b, v := range totals {
		h.LoadTotal = append(h.LoadTotal, model.Point{T: from.Add(time.Duration(b) * step), V: float64(v)})
	}
	sort.SliceStable(h.Load, func(i, j int) bool {
		a, b := h.Load[i], h.Load[j]
		if a.TaskMs != b.TaskMs {
			return a.TaskMs > b.TaskMs
		}
		return a.Server < b.Server
	})
	h.LoadFrom, h.LoadTo, h.LoadStepMs = from, to, step.Milliseconds()
	serverLoad(c, h)
}

// serverLoad reports the longest stretch, of at least hbase-load-min, when
// one region server served at least the hotspot share of the scan tasks
// running: its regions' reads queued on that one server while the others
// had little to do.
func serverLoad(c *ctx, h *model.HBaseSection) {
	var worst *model.HBaseServerLoad
	for i := range h.Load {
		l := &h.Load[i]
		if l.Hot != nil && l.Hot.To.Sub(l.Hot.From) >= c.t.HBaseLoadMin && (worst == nil || l.Hot.To.Sub(l.Hot.From) > worst.Hot.To.Sub(worst.Hot.From)) {
			worst = l
		}
	}
	if worst == nil {
		return
	}
	hot := worst.Hot
	var ev []model.Evidence
	for _, t := range h.Tasks {
		if t.Server == worst.Server && !t.Start.After(hot.At) && t.End.After(hot.At) && len(ev) < 3 {
			ev = append(ev, model.Evidence{Source: t.Source, Text: fmt.Sprintf("task %s read region %s of %s on %s, %s to %s", taskName(t), t.Region, t.Table, worst.Server, c.clock(t.Start), c.clock(t.End))})
		}
	}
	c.add(model.Finding{Rule: "hbase-server-load", Severity: model.Warning, Section: "stages",
		Title: fmt.Sprintf("Region server %s served %d of the %d HBase scan tasks running at %s", worst.Server, hot.Tasks, hot.All, c.clock(hot.At)),
		Explanation: fmt.Sprintf("From %s to %s (%s), %s served at least %s of the run's TableInputFormat tasks running at each moment, while %s other region server%s had the rest. Each task reads one region, so the reads queued on one server's handlers and disks instead of being spread across the cluster. Over the whole run it served %s, %s of task time.",
			c.clock(hot.From), c.clock(hot.To), model.Duration(hot.To.Sub(hot.From).Milliseconds()), worst.Server, model.Percent(c.t.HBaseHotspot), model.Num(int64(len(h.Load)-1)), plural(len(h.Load)-1),
			model.Plural(worst.Tasks, "task", "tasks"), model.Duration(worst.TaskMs)),
		Evidence: ev,
		Fix:      "Spread the regions the job reads: check that HBase's balancer is on (HBase shell: balance_switch true, then balancer), move or split the hot regions, or pre-split the table so its key range lands on several servers. Row keys that start with a date or a counter put the newest rows on one server; salting or hashing a prefix spreads them."})
}

// taskName names a tasks-table row as Spark does: 41.0 in stage 172.0 (TID
// 6429), or as much of it as is known.
func taskName(t model.HBaseTaskRead) string {
	if t.Stage < 0 || t.Partition < 0 {
		return "(not known)"
	}
	return fmt.Sprintf("%d.%d in stage %d.%d (TID %d)", t.Partition, t.Attempt, t.Stage, t.StageAttempt, t.TaskID)
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
