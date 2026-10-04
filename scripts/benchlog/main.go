// Command benchlog writes a large synthetic event log for the performance
// budget in SPEC §4 (a 1 GB log parsed in under 60 s using under 1 GB RAM).
// It stamps real events from the fixture with new IDs, times and hosts, so
// the line mix and field layout match what Spark 3.5 writes.
//
//	go run ./scripts/benchlog -out /tmp/big.log -tasks 1000000 -slim
//
// With -logs it also writes each executor's container log for the same
// tasks (Running, shuffle fetch and Finished lines, and the usual noise),
// as a -from folder, for the cost of reading the cluster's logs.
package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

func main() {
	fixture := flag.String("fixture", "testdata/eventlog/application_1790380000000_0042", "plain fixture to take events from")
	out := flag.String("out", "", "output file")
	tasks := flag.Int("tasks", 1000000, "number of tasks")
	perStage := flag.Int("tasks-per-stage", 500, "tasks per stage")
	executors := flag.Int("executors", 200, "number of executors")
	hosts := flag.Int("hosts", 25, "number of hosts")
	slim := flag.Bool("slim", false, "drop SQL accumulables from task events (about 1 KB per task)")
	logsDir := flag.String("logs", "", "also write the executors' container logs for these tasks under this folder (a -from copy)")
	flag.Parse()
	if *out == "" {
		log.Fatal("-out is required")
	}
	data, err := os.ReadFile(*fixture)
	if err != nil {
		log.Fatal(err)
	}
	var header []string
	tmpl := map[string]string{}
	for _, l := range strings.Split(string(data), "\n") {
		ev := eventOf(l)
		switch ev {
		case "SparkListenerLogStart", "SparkListenerResourceProfileAdded", "SparkListenerEnvironmentUpdate", "SparkListenerApplicationStart":
			header = append(header, l)
		case "SparkListenerTaskEnd":
			if _, ok := tmpl[ev]; !ok && strings.Contains(l, `"Reason":"Success"`) && strings.Contains(l, `"Bytes Read"`) {
				tmpl[ev] = l
			}
		case "SparkListenerTaskStart", "SparkListenerJobStart", "SparkListenerJobEnd", "SparkListenerStageSubmitted", "SparkListenerStageCompleted", "SparkListenerApplicationEnd":
			if _, ok := tmpl[ev]; !ok {
				tmpl[ev] = l
			}
		}
	}
	if *slim {
		tmpl["SparkListenerTaskEnd"] = regexp.MustCompile(`"Accumulables":\[.*?\]\}`).ReplaceAllString(tmpl["SparkListenerTaskEnd"], `"Accumulables":[]}`)
	}
	task := format(tmpl["SparkListenerTaskEnd"], "Stage ID", "Task ID", "Index", "Launch Time", "Executor ID", "Host", "Finish Time", "Executor Run Time", "Executor CPU Time", "Bytes Read")
	taskStart := format(tmpl["SparkListenerTaskStart"], "Stage ID", "Task ID", "Index", "Launch Time", "Executor ID", "Host")

	f, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}
	w := bufio.NewWriterSize(f, 4<<20)
	const t0 = int64(1790386000000)
	appStart := regexp.MustCompile(`"Timestamp":\d+`)
	for _, h := range header {
		// The run below starts at t0, so the application must start just
		// before it, not when the fixture's application did.
		if eventOf(h) == "SparkListenerApplicationStart" {
			h = appStart.ReplaceAllString(h, fmt.Sprintf(`"Timestamp":%d`, t0-5000))
		}
		fmt.Fprintln(w, h)
	}
	for e := 0; e < *executors; e++ {
		host := fmt.Sprintf("ip-10-0-%d-%d.ec2.internal", e%*hosts/250, 10+e%*hosts)
		fmt.Fprintf(w, `{"Event":"SparkListenerExecutorAdded","Timestamp":%d,"Executor ID":"%d","Executor Info":{"Host":"%s","Total Cores":4,"Log Urls":{},"Attributes":{},"Resources":{},"Resource Profile Id":0,"Registration Time":%d}}`+"\n", t0+int64(e), e, host, t0+int64(e))
		fmt.Fprintf(w, `{"Event":"SparkListenerBlockManagerAdded","Block Manager ID":{"Executor ID":"%d","Host":"%s","Port":40000},"Maximum Memory":2000000000,"Timestamp":%d,"Maximum Onheap Memory":2000000000,"Maximum Offheap Memory":0}`+"\n", e, host, t0+int64(e))
	}
	now := t0 + 10000
	stage := 0
	var runs []taskRun // what the executors' logs say, when -logs is set
	for id := 0; id < *tasks; stage++ {
		fmt.Fprintf(w, `{"Event":"SparkListenerJobStart","Job ID":%d,"Submission Time":%d,"Stage Infos":[{"Stage ID":%d,"Stage Attempt ID":0,"Stage Name":"collect at job.py:%d","Number of Tasks":%d,"RDD Info":[],"Parent IDs":[],"Details":"","Accumulables":[],"Resource Profile Id":0}],"Stage IDs":[%d],"Properties":{"spark.job.description":"batch %d"}}`+"\n", stage, now, stage, stage, *perStage, stage, stage)
		fmt.Fprintf(w, `{"Event":"SparkListenerStageSubmitted","Stage Info":{"Stage ID":%d,"Stage Attempt ID":0,"Stage Name":"collect at job.py:%d","Number of Tasks":%d,"RDD Info":[],"Parent IDs":[],"Details":"","Submission Time":%d,"Accumulables":[],"Resource Profile Id":0},"Properties":{}}`+"\n", stage, stage, *perStage, now)
		start := now
		for i := 0; i < *perStage && id < *tasks; i, id = i+1, id+1 {
			e := id % *executors
			host := fmt.Sprintf("ip-10-0-%d-%d.ec2.internal", e%*hosts/250, 10+e%*hosts)
			run := int64(200 + (id*7919)%1800)
			if i == 0 && stage%10 == 0 {
				run *= 20 // some skew
			}
			launch := start + int64(i/(*executors))*10
			fmt.Fprintf(w, taskStart+"\n", stage, id, i, launch, fmt.Sprint(e), host)
			fmt.Fprintf(w, task+"\n", stage, id, i, launch, fmt.Sprint(e), host, launch+run+30, run, run*700000, int64(id%97)*1000000)
			if *logsDir != "" {
				runs = append(runs, taskRun{id: int32(id), stage: int32(stage), index: int32(i), launch: launch, end: launch + run + 30})
			}
			now = max(now, launch+run+30)
		}
		fmt.Fprintf(w, `{"Event":"SparkListenerStageCompleted","Stage Info":{"Stage ID":%d,"Stage Attempt ID":0,"Stage Name":"collect at job.py:%d","Number of Tasks":%d,"RDD Info":[],"Parent IDs":[],"Details":"","Submission Time":%d,"Completion Time":%d,"Accumulables":[],"Resource Profile Id":0}}`+"\n", stage, stage, *perStage, start, now)
		fmt.Fprintf(w, `{"Event":"SparkListenerJobEnd","Job ID":%d,"Completion Time":%d,"Job Result":{"Result":"JobSucceeded"}}`+"\n", stage, now)
	}
	fmt.Fprintf(w, `{"Event":"SparkListenerApplicationEnd","Timestamp":%d}`+"\n", now+100)
	if err := w.Flush(); err != nil {
		log.Fatal(err)
	}
	st, _ := f.Stat()
	f.Close()
	fmt.Printf("wrote %s: %d tasks, %d stages, %.2f GB\n", *out, *tasks, stage, float64(st.Size())/1e9)
	if *logsDir != "" {
		n, size := writeExecutorLogs(*logsDir, appIDOf(header), runs, *executors, *hosts)
		fmt.Printf("wrote %d executor logs under %s: %.2f GB gzipped\n", n, *logsDir, float64(size)/1e9)
	}
}

// taskRun is one task as its executor's log tells it.
type taskRun struct {
	id, stage, index int32
	launch, end      int64
}

// appIDOf reads the application ID from the fixture's start event.
func appIDOf(header []string) string {
	re := regexp.MustCompile(`"App ID":"([^"]+)"`)
	for _, h := range header {
		if m := re.FindStringSubmatch(h); m != nil {
			return m[1]
		}
	}
	log.Fatal("the fixture has no application ID")
	return ""
}

// writeExecutorLogs writes one gzipped stderr per executor, its lines in
// time order: Spark's default layout, which prints no thread, so lines are
// tied to their tasks by the TIDs they name, as on a real cluster.
func writeExecutorLogs(dir, appID string, runs []taskRun, executors, hosts int) (int, int64) {
	ts := strings.TrimPrefix(appID, "application_")
	stamp := func(ms int64) string { return time.UnixMilli(ms).UTC().Format("06/01/02 15:04:05") }
	type entry struct {
		at   int64
		line string
	}
	var size int64
	for e := 0; e < executors; e++ {
		host := fmt.Sprintf("ip-10-0-%d-%d.ec2.internal", e%hosts/250, 10+e%hosts)
		var lines []entry
		for i := e; i < len(runs); i += executors {
			r := runs[i]
			name := fmt.Sprintf("%d.0 in stage %d.0 (TID %d)", r.index, r.stage, r.id)
			kb := 40 + int(r.id%900)
			lines = append(lines,
				entry{r.launch, fmt.Sprintf("INFO YarnCoarseGrainedExecutorBackend: Got assigned task %d", r.id)},
				entry{r.launch, "INFO Executor: Running task " + name},
				entry{r.launch, fmt.Sprintf("INFO ShuffleBlockFetcherIterator: Getting 32 (%d.0 KiB) non-empty blocks including 24 (%d.0 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 8 (%d.0 KiB) remote blocks", kb, kb*3/4, kb-kb*3/4)},
				entry{r.launch, fmt.Sprintf("INFO ShuffleBlockFetcherIterator: Started 8 remote fetches in %d ms", 1+r.id%9)},
				entry{r.launch, fmt.Sprintf("INFO BlockManager: Found block rdd_14_%d locally", r.index)},
				entry{r.launch, fmt.Sprintf("INFO CodeGenerator: Code generated in %d.%03d ms", 5+r.id%30, r.id%1000)},
				entry{r.end, "INFO Executor: Finished task " + name + fmt.Sprintf(". %d bytes result sent to driver", 2000+r.id%3000)})
		}
		sort.SliceStable(lines, func(a, b int) bool { return lines[a].at < lines[b].at })
		p := filepath.Join(dir, "containers", appID, fmt.Sprintf("container_%s_01_%06d", ts, e+2), "stderr.gz")
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			log.Fatal(err)
		}
		f, err := os.Create(p)
		if err != nil {
			log.Fatal(err)
		}
		zw := gzip.NewWriter(f)
		bw := bufio.NewWriterSize(zw, 1<<20)
		start := int64(1790386000000) + int64(e)
		fmt.Fprintf(bw, "%s INFO CoarseGrainedExecutorBackend: Started daemon with process name: %d@%s\n", stamp(start), 7000+e, host)
		fmt.Fprintf(bw, "%s INFO Executor: Starting executor ID %d on host %s\n", stamp(start), e, host)
		for _, l := range lines {
			bw.WriteString(stamp(l.at) + " " + l.line + "\n")
		}
		if err := bw.Flush(); err != nil {
			log.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			log.Fatal(err)
		}
		st, _ := f.Stat()
		size += st.Size()
		f.Close()
	}
	return executors, size
}

func eventOf(l string) string {
	const p = `{"Event":"`
	if !strings.HasPrefix(l, p) {
		return ""
	}
	rest := l[len(p):]
	return rest[:strings.IndexByte(rest, '"')]
}

// format turns the first occurrence of each key's value into a verb.
func format(line string, keys ...string) string {
	line = strings.ReplaceAll(line, "%", "%%")
	for _, k := range keys {
		re := regexp.MustCompile(`"` + regexp.QuoteMeta(k) + `":("[^"]*"|-?\d+)`)
		loc := re.FindStringSubmatchIndex(line)
		if loc == nil {
			log.Fatalf("template has no %q", k)
		}
		verb := "%d"
		if line[loc[2]] == '"' {
			verb = `"%s"`
		}
		line = line[:loc[2]] + verb + line[loc[3]:]
	}
	return line
}
