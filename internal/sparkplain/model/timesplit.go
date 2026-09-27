package model

// TimeSplit divides task time (every task's duration, added up) into what
// the tasks spent it on, in milliseconds. The parts add up to the task
// time, so a stacked bar of them is as long as the time it explains.
//
// Spark reports duration as scheduler delay + deserializing + running +
// serializing the result + fetching it, and inside running it measures CPU
// time on the task thread, garbage collection, waiting for shuffle data
// and blocking on shuffle writes. Those four are measured separately and
// can overlap a little (a shuffle write also uses CPU), so when they add up
// to more than run time they are scaled down to fit it; what they leave of
// run time is Other: file I/O, Python workers, locks, anything unmeasured.
type TimeSplit struct {
	SchedulerDelayMs int64 `json:"schedulerDelayMs"` // launch overhead and waiting on the driver
	DeserializeMs    int64 `json:"deserializeMs"`    // unpacking the task on the executor
	ComputeMs        int64 `json:"computeMs"`        // CPU time on the task thread
	GCMs             int64 `json:"gcMs"`             // JVM garbage collection
	ShuffleFetchMs   int64 `json:"shuffleFetchMs"`   // waiting for shuffle data from other executors
	ShuffleWriteMs   int64 `json:"shuffleWriteMs"`   // blocked writing shuffle output
	ResultMs         int64 `json:"resultMs"`         // serializing the result and the driver fetching it
	OtherMs          int64 `json:"otherMs"`          // the rest of run time
}

// Total is the task time the split explains.
func (s TimeSplit) Total() int64 {
	return s.SchedulerDelayMs + s.DeserializeMs + s.ComputeMs + s.GCMs + s.ShuffleFetchMs + s.ShuffleWriteMs + s.ResultMs + s.OtherMs
}

// TimeSplit splits the totals' task time; see the type.
func (t TaskTotals) TimeSplit() TimeSplit {
	s := TimeSplit{
		SchedulerDelayMs: max(t.SchedulerDelayMs, 0),
		DeserializeMs:    max(t.DeserializeMs, 0),
		ResultMs:         max(t.ResultSerializationMs, 0) + max(t.GettingResultMs, 0),
	}
	run := max(t.RunTimeMs, 0)
	parts := []int64{max(t.CPUTimeNs/1_000_000, 0), max(t.GCTimeMs, 0), max(t.ShuffleFetchWaitMs, 0), max(t.ShuffleWriteTimeNs/1_000_000, 0)}
	var sum int64
	for _, p := range parts {
		sum += p
	}
	if sum > run {
		// Overlapping measurements: scale to fit, giving the rounding
		// remainder to the largest part so the parts add up to run exactly.
		var got int64
		big := 0
		for i, p := range parts {
			parts[i] = p * run / sum
			got += parts[i]
			if parts[i] > parts[big] {
				big = i
			}
		}
		parts[big] += run - got
		sum = run
	}
	s.ComputeMs, s.GCMs, s.ShuffleFetchMs, s.ShuffleWriteMs = parts[0], parts[1], parts[2], parts[3]
	s.OtherMs = run - sum
	return s
}
