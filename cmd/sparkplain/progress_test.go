package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/source"
)

// A step's progress reads as the share of bytes read with the time left,
// and the files read; a step that counts nothing shows nothing.
func TestProgressText(t *testing.T) {
	t.Parallel()
	if got := progressText(nil, time.Minute, "  "); got != "" {
		t.Errorf("nil progress = %q", got)
	}
	p := &source.Progress{}
	p.Add(600 << 20)
	p.Add(400 << 20)
	p.Read(250 << 20)
	p.Done()
	got := progressText(p, 30*time.Second, "  ")
	for _, want := range []string{"  25% · 250 MiB of 1000 MiB", "about 1 min 30 s left", "1 of 2 files"} {
		if !strings.Contains(got, want) {
			t.Errorf("progress = %q, want it to hold %q", got, want)
		}
	}
	if got := progressText(p, 2*time.Second, ""); strings.Contains(got, "left") {
		t.Errorf("a guess of the time left after 2 s: %q", got)
	}
}

// When the console is not a terminal (a script, nohup, CI), a long step
// says now and then on stderr that it is still working, with its counts,
// and stops when the step ends.
func TestHeartbeatWhenNotATerminal(t *testing.T) {
	saved := heartbeat
	t.Cleanup(func() { heartbeat = saved })
	heartbeat = 5 * time.Millisecond
	var out, errs bytes.Buffer
	c := newConsole(&out, &errs)
	c.step("cluster logs", "reading container, step and node logs")
	p := &source.Progress{}
	p.Add(10)
	p.Add(10)
	p.Done()
	c.track(p)
	time.Sleep(60 * time.Millisecond)
	c.clear()
	got := errs.String()
	if !strings.Contains(got, "sparkplain: still reading container, step and node logs (") || !strings.Contains(got, "1 of 2 files") {
		t.Errorf("stderr = %q", got)
	}
	n := strings.Count(got, "\n")
	time.Sleep(30 * time.Millisecond)
	if strings.Count(errs.String(), "\n") != n {
		t.Error("the heartbeat went on after the step ended")
	}
}

// The closing line says where the time went: steps of a second or more,
// the same step's parts added up, slowest first.
func TestTookLine(t *testing.T) {
	t.Parallel()
	c := &console{steps: []stepTime{{"event log", 21 * time.Second}, {"cluster logs", 20 * time.Minute}, {"analysis", 300 * time.Millisecond}, {"cluster logs", 8 * time.Minute}, {"explorer", 4 * time.Second}}}
	if got := c.tookLine(false); got != "cluster logs 28 min 0 s · event log 21 s · explorer 4.0 s" {
		t.Errorf("took = %q", got)
	}
	if got := (&console{steps: []stepTime{{"analysis", time.Millisecond}}}).tookLine(false); got != "" {
		t.Errorf("a fast run's took line = %q", got)
	}
}
