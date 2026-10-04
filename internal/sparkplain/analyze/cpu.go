package analyze

import (
	"fmt"
	"sort"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

func analyzeCPU(c *ctx, r *model.Report) {
	s := &r.CPU
	s.Missing = []string{"Host CPU for each node over time (needs the CloudWatch agent)"}
	if !c.metrics() {
		s.Coverage = model.NeedsEventLog
		s.Missing = append([]string{"CPU time per stage and per executor"}, s.Missing...)
		if c.rebuilt {
			s.Missing = append([]string{rebuiltNote}, s.Missing...)
		}
		return
	}
	s.Coverage = model.Partial
	for _, x := range c.log.Executors {
		cpu := x.Tasks.CPUTimeNs / 1e6
		s.CPUMs += cpu
		s.RunMs += x.Tasks.RunTimeMs
		start, end := c.lifetime(x)
		s.AllocatedCoreMs += int64(x.Cores) * max(0, end.Sub(start).Milliseconds())
		s.Executors = append(s.Executors, model.Utilisation{ID: x.ID, Label: x.Host, CPUMs: cpu, RunMs: x.Tasks.RunTimeMs, Share: share(cpu, x.Tasks.RunTimeMs), Source: x.AddedSource})
	}
	if d := c.log.Driver; d != nil && d.Tasks.Tasks > 0 { // local mode runs tasks in the driver
		cpu := d.Tasks.CPUTimeNs / 1e6
		s.CPUMs += cpu
		s.RunMs += d.Tasks.RunTimeMs
	}
	s.Share = share(s.CPUMs, s.RunMs)
	s.BusyShare = share(s.RunMs, s.AllocatedCoreMs)
	for _, st := range c.log.Stages {
		if st.Totals.RunTimeMs == 0 {
			continue
		}
		cpu := st.Totals.CPUTimeNs / 1e6
		s.Stages = append(s.Stages, model.Utilisation{ID: fmt.Sprintf("%d.%d", st.ID, st.Attempt), Label: st.Name, CPUMs: cpu, RunMs: st.Totals.RunTimeMs, Share: share(cpu, st.Totals.RunTimeMs), Source: st.TaskSource})
	}
	sort.SliceStable(s.Stages, func(i, j int) bool { return s.Stages[i].RunMs > s.Stages[j].RunMs })
	if len(s.Stages) > 15 {
		s.Stages = s.Stages[:15]
	}
	cpuFindings(c, s)
}

func cpuFindings(c *ctx, s *model.CPUSection) {
	t := c.t
	if s.RunMs < t.MinRunTime.Milliseconds() {
		return
	}
	if s.Share < t.LowCPUShare {
		why := "In the remaining time, the tasks waited. They read input, fetched shuffle data, did garbage collection or wrote output."
		if c.pyspark {
			why += "\nThis is a PySpark job. Spark counts only the CPU time of the JVM, so the work in Python UDFs shows as waiting time. As a result, a low CPU share can only mean that the work is in Python."
		}
		c.add(model.Finding{
			Rule: "cpu-low", Severity: model.Info, Section: "cpu",
			Title:       fmt.Sprintf("Tasks used the CPU for only %s of their run time", model.Percent(s.Share)),
			Explanation: fmt.Sprintf("All tasks together used %s of CPU time in %s of run time. %s", model.Duration(s.CPUMs), model.Duration(s.RunMs), why),
			Evidence:    []model.Evidence{{Text: "sum of “Executor CPU Time” / sum of “Executor Run Time” over all task end events"}},
			Fix:         "Look at the stages with the lowest CPU share.\n- A long shuffle fetch wait shows a problem with the network or skew.\n- A long garbage collection time shows a problem with memory.",
		})
	}
	if s.AllocatedCoreMs > 0 && s.BusyShare < t.LowCPUShare {
		c.add(model.Finding{
			Rule: "cpu-idle-executors", Severity: model.Info, Section: "cpu",
			Title:       fmt.Sprintf("Executor cores were busy only %s of the time they were held", model.Percent(s.BusyShare)),
			Explanation: fmt.Sprintf("The executors held %s of core time, but they ran tasks for only %s. In the remaining time, the cores did no work. This often occurs between jobs while the driver works alone, or at the end of skewed stages.", model.Duration(s.AllocatedCoreMs), model.Duration(s.RunMs)),
			Evidence:    []model.Evidence{{Text: "executor cores × (removed − added), against the sum of task run time"}},
			Fix:         "Do one of these:\n- Set spark.dynamicAllocation.enabled=true, so that Spark releases executors that do no work.\n- Put the work into fewer and larger batches.",
		})
	}
}
