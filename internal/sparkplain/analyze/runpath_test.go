package analyze

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

func TestRunPath(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	stage := func(id, from, to int, parents ...int) *model.Stage {
		return &model.Stage{ID: id, Submitted: at(from), Completed: at(to), ParentIDs: parents, Status: model.StatusSucceeded}
	}
	// Job 0 runs 5–40 s: stages 0 (5–20 s) and 1 (5–15 s) in parallel, then
	// stage 2 (21–40 s) reads both. The driver works alone 40–50 s. Job 1
	// runs 50–90 s as stage 3, which reads nothing earlier. The driver
	// finishes up 90–100 s.
	c := &ctx{t: DefaultThresholds(), end: at(100), log: &model.EventLog{
		Application: model.Application{Start: at(0)},
		Jobs:        []*model.Job{{ID: 0, Submitted: at(5), Completed: at(40)}, {ID: 1, Submitted: at(50), Completed: at(90)}},
		Stages:      []*model.Stage{stage(0, 5, 20), stage(1, 5, 15), stage(2, 21, 40, 0, 1), stage(3, 50, 90)},
	}}
	got := runPath(c)
	var b strings.Builder
	var total int64
	for _, s := range got {
		fmt.Fprintf(&b, "%s", s.Kind)
		if s.Kind == model.PathStage {
			fmt.Fprintf(&b, " %d", s.StageID)
		}
		fmt.Fprintf(&b, " %d–%d; ", int(s.Start.Sub(t0).Seconds()), int(s.End.Sub(t0).Seconds()))
		total += s.DurationMs()
	}
	// stage 1 finished first, so it did not hold stage 2 up
	want := "driver 0–5; stage 0 5–20; scheduling 20–21; stage 2 21–40; driver 40–50; stage 3 50–90; driver 90–100; "
	if b.String() != want {
		t.Errorf("path\n got %s\nwant %s", b.String(), want)
	}
	if total != 100_000 {
		t.Errorf("steps add up to %d ms, want the run's 100000", total)
	}

	// A parent logged as ending a moment after its child started still
	// held it up; its step ends when the child starts, leaving no gap.
	c.log.Stages[0].Completed = at(21).Add(20 * time.Millisecond)
	if p := runPath(c); len(p) < 3 || p[1].StageID != 0 || !p[1].End.Equal(at(21)) || p[2].Kind != model.PathStage || p[2].StageID != 2 {
		t.Errorf("slack: %+v", p)
	}

	// A stage still running when the log ends runs to the end, and no
	// stages at all means no path.
	c.log.Stages = []*model.Stage{{ID: 7, Submitted: at(10)}}
	if p := runPath(c); len(p) != 2 || p[1].StageID != 7 || !p[1].End.Equal(at(100)) {
		t.Errorf("running stage: %+v", p)
	}
	c.log.Stages = nil
	if p := runPath(c); p != nil {
		t.Errorf("no stages: %+v", p)
	}
}
