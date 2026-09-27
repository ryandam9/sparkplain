package model

import "testing"

func TestTimeSplit(t *testing.T) {
	for _, c := range []struct {
		name string
		in   TaskTotals
		want TimeSplit
	}{
		{"parts fit in run time", TaskTotals{SchedulerDelayMs: 50, DeserializeMs: 20, RunTimeMs: 1000, CPUTimeNs: 600e6, GCTimeMs: 100,
			ShuffleFetchWaitMs: 150, ShuffleWriteTimeNs: 50e6, ResultSerializationMs: 5, GettingResultMs: 3},
			TimeSplit{SchedulerDelayMs: 50, DeserializeMs: 20, ComputeMs: 600, GCMs: 100, ShuffleFetchMs: 150, ShuffleWriteMs: 50, ResultMs: 8, OtherMs: 100}},
		{"overlap scaled to fit", TaskTotals{RunTimeMs: 1000, CPUTimeNs: 900e6, GCTimeMs: 200, ShuffleWriteTimeNs: 100e6},
			TimeSplit{ComputeMs: 751, GCMs: 166, ShuffleWriteMs: 83}}, // 900:200:100 of 1000, the remainder to the largest
		{"nothing ran", TaskTotals{}, TimeSplit{}},
		{"negative values ignored", TaskTotals{RunTimeMs: 100, CPUTimeNs: -5, GCTimeMs: -1, SchedulerDelayMs: -3}, TimeSplit{OtherMs: 100}},
	} {
		got := c.in.TimeSplit()
		if got != c.want {
			t.Errorf("%s: got %+v, want %+v", c.name, got, c.want)
		}
		want := max(c.in.SchedulerDelayMs, 0) + max(c.in.DeserializeMs, 0) + max(c.in.RunTimeMs, 0) + c.in.ResultSerializationMs + c.in.GettingResultMs
		if got.Total() != want {
			t.Errorf("%s: parts add up to %d, want %d", c.name, got.Total(), want)
		}
	}
}
