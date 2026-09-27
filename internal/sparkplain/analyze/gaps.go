package analyze

import (
	"fmt"
	"sort"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// maxDriverGaps caps the gaps kept in the report, longest first; the total
// still counts them all.
const maxDriverGaps = 200

// driverGaps finds the stretches between the application's start and end
// when no job was running, from the union of every job's submit-to-finish
// span (a job still running when the log ends runs to the end). Gaps under
// a second are counted in the total but not listed. With no job at all
// there is nothing to compare, so no gaps.
func driverGaps(c *ctx) ([]model.DriverGap, int64) {
	start, end := c.log.Application.Start, c.end
	type span struct {
		from, to time.Time
		id       int
	}
	var jobs []span
	for _, j := range c.log.Jobs {
		if j.Submitted.IsZero() {
			continue
		}
		to := j.Completed
		if to.IsZero() || to.Before(j.Submitted) {
			to = end
		}
		jobs = append(jobs, span{j.Submitted, to, j.ID})
	}
	if len(jobs) == 0 || start.IsZero() || !end.After(start) {
		return nil, 0
	}
	sort.Slice(jobs, func(i, k int) bool { return jobs[i].from.Before(jobs[k].from) })
	var gaps []model.DriverGap
	var total int64
	add := func(from, to time.Time, before, after *int) {
		if !to.After(from) {
			return
		}
		ms := to.Sub(from).Milliseconds()
		total += ms
		if ms >= 1000 {
			gaps = append(gaps, model.DriverGap{Start: from, End: to, Before: before, After: after})
		}
	}
	at, last := start, (*int)(nil) // at: when the jobs so far last finished; last: the job that finished then
	for i := range jobs {
		j := jobs[i]
		if j.from.After(at) {
			id := j.id
			add(at, j.from, last, &id)
		}
		if j.to.After(at) {
			at = j.to
			id := j.id
			last = &id
		}
	}
	if end.After(at) {
		add(at, end, last, nil)
	}
	sort.SliceStable(gaps, func(i, k int) bool { return gaps[i].DurationMs() > gaps[k].DurationMs() })
	if len(gaps) > maxDriverGaps {
		gaps = gaps[:maxDriverGaps]
	}
	return gaps, total
}

// driverGapFinding reports a run that spent much of its time with no job
// running: executors held but idle while the driver worked alone.
func driverGapFinding(c *ctx, s *model.JobsSection) {
	run := c.end.Sub(c.log.Application.Start).Milliseconds()
	if run <= 0 || s.DriverGapMs < c.t.DriverGapMin.Milliseconds() || share(s.DriverGapMs, run) <= c.t.DriverGapShare {
		return
	}
	jobs := map[int]*model.Job{}
	for _, j := range c.log.Jobs {
		jobs[j.ID] = j
	}
	name := func(id *int) string {
		if id == nil {
			return ""
		}
		if j := jobs[*id]; j != nil && j.Name != "" {
			return fmt.Sprintf("job %d (%s)", *id, j.Name)
		}
		return fmt.Sprintf("job %d", *id)
	}
	var ev []model.Evidence
	for i, g := range s.DriverGaps {
		if i == 3 {
			break
		}
		var where string
		switch {
		case g.Before == nil:
			where = "before the first job, " + name(g.After)
		case g.After == nil:
			where = "after the last job, " + name(g.Before)
		default:
			where = "between " + name(g.Before) + " and " + name(g.After)
		}
		e := model.Evidence{Text: fmt.Sprintf("%s with no job running, %s", model.Duration(g.DurationMs()), where)}
		// cite the job that ended the gap, where the driver picked Spark back up
		if g.After != nil && jobs[*g.After] != nil {
			e.Source, e.Ref = jobs[*g.After].Source, model.JobRef(*g.After)
		} else if g.Before != nil && jobs[*g.Before] != nil {
			e.Source, e.Ref = jobs[*g.Before].EndSource, model.JobRef(*g.Before)
		}
		ev = append(ev, e)
	}
	c.add(model.Finding{Rule: "driver-gaps", Severity: model.Warning, Section: "timeline",
		Title: fmt.Sprintf("No Spark job ran for %s (%s) of the run: the cluster waited on the driver",
			model.Duration(s.DriverGapMs), model.Percent(share(s.DriverGapMs, run))),
		Explanation: "Between jobs the executors sit idle while the driver works on its own: planning queries, listing files, running Python or Scala code outside Spark, or handling results it collected. Executors YARN still holds cost the same whether they work or wait.",
		Evidence:    ev,
		Fix:         "Look at the code just before the job each gap ends at (its call site is on the job). Common causes: listing many files in S3 (use a table format, or fewer, larger files), collect() or toPandas() followed by work on the driver, many small actions run one after another (combine them), and sleeps or calls to outside services. If the gap cannot go, dynamic allocation lets idle executors be released."})
}
