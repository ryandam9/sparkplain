package analyze

import (
	"strings"
	"testing"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

func TestDriverGaps(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	job := func(id, from, to int) *model.Job {
		j := &model.Job{ID: id, Name: "count at etl.py:" + strings.Repeat("1", id+1), Submitted: at(from), Status: model.StatusSucceeded}
		if to >= 0 {
			j.Completed = at(to)
		}
		return j
	}
	c := &ctx{t: DefaultThresholds(), end: at(600), log: &model.EventLog{
		Application: model.Application{Start: at(0)},
		// 0–30 idle; job 0 runs 30–100; jobs 1 and 2 overlap 100–250; 250–550
		// idle; job 3 still running when the log ends (to = -1).
		Jobs: []*model.Job{job(0, 30, 100), job(1, 100, 200), job(2, 150, 250), job(3, 550, -1)},
	}}
	gaps, total := driverGaps(c)
	if total != 330_000 {
		t.Errorf("total %d ms, want 330000", total)
	}
	if len(gaps) != 2 || gaps[0].DurationMs() != 300_000 || *gaps[0].Before != 2 || *gaps[0].After != 3 ||
		gaps[1].Before != nil || *gaps[1].After != 0 {
		t.Fatalf("gaps %+v", gaps)
	}
	s := &model.JobsSection{DriverGaps: gaps, DriverGapMs: total}
	driverGapFinding(c, s)
	if len(c.findings) != 1 || c.findings[0].Rule != "driver-gaps" || !strings.Contains(c.findings[0].Title, "5 min 30 s (55%)") ||
		!strings.Contains(c.findings[0].Evidence[0].Text, "between job 2 (count at etl.py:111) and job 3") || c.findings[0].Evidence[0].Ref != "job:3" {
		t.Errorf("finding %+v", c.findings)
	}

	// Under the share threshold: no finding.
	c.findings = nil
	driverGapFinding(c, &model.JobsSection{DriverGapMs: 100_000})
	if len(c.findings) != 0 {
		t.Errorf("finding at 17%%: %+v", c.findings)
	}
	// No jobs: nothing to compare.
	c.log.Jobs = nil
	if g, n := driverGaps(c); g != nil || n != 0 {
		t.Errorf("no jobs gave %v %d", g, n)
	}
}
