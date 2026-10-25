package analyze

import (
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// localClusterRE reads a local-cluster master URL: local-cluster[workers,
// cores per worker, MiB per worker].
var localClusterRE = regexp.MustCompile(`^local-cluster\[\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\]$`)

// workerCapacity fills in what the Spark Standalone workers on each host
// offered Spark, for a run not on YARN. A worker's log says it at each
// start ("Starting Spark worker localhost:41579 with 4 cores, 4.0 GiB
// RAM"); for each worker the start in force when the application ran is
// taken (the last before its end), and a host's workers add up. A
// local-cluster run's master URL gives it instead, all on one host.
func workerCapacity(c *ctx, r *model.Report) {
	if c.onYARN() {
		return
	}
	r.Nodes.Manager = "standalone"
	// The most executors alive at once on each host, as the YARN path has
	// it from the containers: Standalone starts new ones for lost ones, so
	// how many it started is not how many ran together.
	for i := range r.Nodes.Hosts {
		if h := &r.Nodes.Hosts[i]; len(h.Executors) > 0 && h.PeakExecutors == 0 {
			h.PeakExecutors = peakAlive(c, h.Name)
		}
	}
	if c.log != nil {
		if u, err := url.Parse(c.log.Application.Master); err == nil && u.Scheme == "spark" {
			r.Nodes.MasterHost = u.Hostname()
		}
		if m := localClusterRE.FindStringSubmatch(c.log.Application.Master); m != nil {
			r.Nodes.Manager = "local-cluster"
			n, _ := strconv.Atoi(m[1])
			cores, _ := strconv.Atoi(m[2])
			mib, _ := strconv.ParseInt(m[3], 10, 64)
			for i := range r.Nodes.Hosts {
				if h := &r.Nodes.Hosts[i]; len(h.Executors) > 0 {
					h.Workers, h.WorkerCores, h.WorkerMemoryBytes = n, n*cores, int64(n)*mib<<20
					h.WorkerSource = c.confSrc
				}
			}
			return
		}
	}
	type start struct {
		t     time.Time
		host  string
		cores int
		mem   int64
		src   model.Source
	}
	latest := map[string]start{} // by worker: host and process ("vm/worker-1")
	for _, f := range c.in.Logs {
		for _, l := range f.Found {
			if l.Kind != model.LogWorkerCapacity {
				continue
			}
			if !c.end.IsZero() && l.Time.After(c.end) {
				continue // started after the application ended
			}
			key := f.Host + "/" + l.Fields["worker"]
			cores, _ := strconv.Atoi(l.Fields["cores"])
			s := start{t: l.Time, host: l.Fields["host"], cores: cores, mem: parseSize(l.Fields["memory"], 1), src: l.Source}
			if old, ok := latest[key]; !ok || s.t.After(old.t) {
				latest[key] = s
			}
		}
	}
	for _, s := range latest {
		for i := range r.Nodes.Hosts {
			if h := &r.Nodes.Hosts[i]; hostKey(h.Name) == hostKey(s.host) {
				h.Workers++
				h.WorkerCores += s.cores
				h.WorkerMemoryBytes += max(s.mem, 0)
				h.WorkerSource = s.src
			}
		}
	}
	// A host that ran executors with no worker's log read: say where its
	// capacity is.
	for _, h := range r.Nodes.Hosts {
		if len(h.Executors) > 0 && h.Workers == 0 {
			r.Nodes.Missing = append(r.Nodes.Missing, "The cores and memory each worker offered Spark (in the worker's log, under $SPARK_HOME/logs)")
			if r.Nodes.Coverage == model.Complete {
				r.Nodes.Coverage = model.Partial
			}
			return
		}
	}
}
