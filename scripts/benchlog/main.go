// Command benchlog writes a large synthetic event log for the performance
// budget in SPEC §8 (a 1 GB log parsed in under 60 s using under 1 GB RAM).
// It stamps real events from the fixture with new IDs, times and hosts, so
// the line mix and field layout match what Spark 3.5 writes.
//
//	go run ./scripts/benchlog -out /tmp/big.log -tasks 1000000 -slim
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
)

func main() {
	fixture := flag.String("fixture", "testdata/eventlog/application_1790380000000_0042", "plain fixture to take events from")
	out := flag.String("out", "", "output file")
	tasks := flag.Int("tasks", 1000000, "number of tasks")
	perStage := flag.Int("tasks-per-stage", 500, "tasks per stage")
	executors := flag.Int("executors", 200, "number of executors")
	hosts := flag.Int("hosts", 25, "number of hosts")
	slim := flag.Bool("slim", false, "drop SQL accumulables from task events (about 1 KB per task)")
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
