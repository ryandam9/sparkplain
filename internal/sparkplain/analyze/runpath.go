package analyze

import (
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// pathSlack forgives driver clocks: a parent logged as finishing a moment
// after its child started still held the child up.
const pathSlack = 50 * time.Millisecond

// runPath traces what held up the application's completion, backwards
// from its end. The last thing to finish was the stage attempt that
// finished last; what held that stage up was its parent that finished last
// before it started or, when it has none that ran (the first stage of a
// job, or parents skipped because their output existed), whatever stage
// finished last before it started: usually the previous job, since the
// driver submits jobs one after another. Time between two links of the
// chain is a step of its own: driver time when no job was running, or
// scheduling when one was. The steps cover the whole run, so they add up
// to its wall time. JobsSection.CriticalPath, the longest job's slowest
// chain, stays for readers of the JSON; the pages use this.
func runPath(c *ctx) []model.PathStep {
	start, end := c.log.Application.Start, c.end
	if start.IsZero() || !end.After(start) {
		return nil
	}
	type attempt struct {
		st       *model.Stage
		from, to time.Time
	}
	var all []*attempt
	byID := map[int][]*attempt{}
	for _, st := range c.log.Stages {
		if st.Submitted.IsZero() {
			continue
		}
		to := st.Completed
		if to.IsZero() || to.Before(st.Submitted) {
			to = end // still running when the log ends
		}
		a := &attempt{st, st.Submitted, to}
		all = append(all, a)
		byID[st.ID] = append(byID[st.ID], a)
	}
	if len(all) == 0 {
		return nil
	}
	jobRunning := func(t time.Time) bool {
		for _, j := range c.log.Jobs {
			if j.Submitted.IsZero() || t.Before(j.Submitted) {
				continue
			}
			if j.Completed.IsZero() || t.Before(j.Completed) {
				return true
			}
		}
		return false
	}
	var rev []model.PathStep
	gap := func(from, to time.Time) {
		if !to.After(from) {
			return
		}
		kind := model.PathDriver
		if jobRunning(from.Add(to.Sub(from) / 2)) {
			kind = model.PathWaiting
		}
		rev = append(rev, model.PathStep{Kind: kind, Start: from, End: to})
	}
	used := map[*attempt]bool{}
	// latest is the unused attempt among cands that started before t and
	// finished by t (give or take the slack), finishing last.
	latest := func(cands []*attempt, t time.Time) *attempt {
		var best *attempt
		for _, a := range cands {
			if used[a] || !a.from.Before(t) || a.to.After(t.Add(pathSlack)) {
				continue
			}
			if best == nil || a.to.After(best.to) || a.to.Equal(best.to) && a.from.Before(best.from) {
				best = a
			}
		}
		return best
	}
	t := end
	var cur *attempt
	for len(used) < len(all) && t.After(start) {
		var pick *attempt
		if cur != nil {
			var parents []*attempt
			for _, p := range cur.st.ParentIDs {
				parents = append(parents, byID[p]...)
			}
			pick = latest(parents, t)
		}
		if pick == nil {
			pick = latest(all, t)
		}
		if pick == nil {
			break
		}
		to := pick.to
		if to.After(t) {
			to = t
		}
		gap(to, t)
		from := pick.from
		if from.Before(start) {
			from = start
		}
		rev = append(rev, model.PathStep{Kind: model.PathStage, StageID: pick.st.ID, Attempt: pick.st.Attempt, Start: from, End: to})
		used[pick], t, cur = true, from, pick
	}
	gap(start, t)
	out := make([]model.PathStep, 0, len(rev))
	for i := len(rev) - 1; i >= 0; i-- {
		s := rev[i]
		// two stretches of the same kind side by side are one
		if n := len(out); n > 0 && s.Kind != model.PathStage && out[n-1].Kind == s.Kind {
			out[n-1].End = s.End
			continue
		}
		out = append(out, s)
	}
	return out
}
