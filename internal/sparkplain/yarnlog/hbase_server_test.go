package yarnlog

import (
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/source"
)

// HBase's daemon logs are named by role and host, and rolled each hour.
func TestDescribeHBaseLogs(t *testing.T) {
	t.Parallel()
	for key, want := range map[string]string{
		"node/i-0fee0000000000001/applications/hbase/hbase-hbase-master-ip-10-0-2-11.us-east-1.compute.internal.log.gz":                     "hbase-master ip-10-0-2-11.us-east-1.compute.internal 0001-01-01T00:00:00Z",
		"node/i-0fee0000000000002/applications/hbase/hbase-hbase-regionserver-ip-10-0-2-10.us-east-1.compute.internal.log.2026-09-29-06.gz": "hbase-regionserver ip-10-0-2-10.us-east-1.compute.internal 2026-09-29T06:00:00Z",
		"node/i-0fee0000000000002/applications/hbase/hbase-hbase-regionserver-ip-10-0-2-10.us-east-1.compute.internal.out.gz":               " 0001-01-01T00:00:00Z",
		"node/i-0fee0000000000002/applications/hbase/SecurityAuth.audit.gz":                                                                 " 0001-01-01T00:00:00Z",
	} {
		f := Describe(key)
		if got := fmt.Sprintf("%s %s %s", f.Kind, f.Host, f.Hour.Format(time.RFC3339)); strings.TrimSpace(got) != strings.TrimSpace(want) {
			t.Errorf("%s: %q, want %q", key, got, want)
		}
	}
}

// What HBase's servers logged, kept to the application's time: lines from
// the phase 5 test cluster (scrubbed), and HBase 2.4's own messages for
// the events it did not have (an abort, a JVM pause, a slow call, a
// store-file pile-up).
func TestHBaseServerLines(t *testing.T) {
	t.Parallel()
	const master = "node/i-0fee0000000000001/applications/hbase/hbase-hbase-master-ip-10-0-2-11.us-east-1.compute.internal.log.gz"
	const rs = "node/i-0fee0000000000002/applications/hbase/hbase-hbase-regionserver-ip-10-0-2-10.us-east-1.compute.internal.log.gz"
	window := Options{From: time.Date(2026, 9, 29, 5, 57, 0, 0, time.UTC), To: time.Date(2026, 9, 29, 6, 12, 0, 0, time.UTC)}
	res := classifyText(t, master, `2026-09-29 05:56:59,000 INFO  [PEWorker-4] procedure2.ProcedureExecutor: Finished pid=29, state=SUCCESS; TransitRegionStateProcedure table=sp_orders, region=554f89fb3a319d50d2ea3299dc1270c1, REOPEN/MOVE in 793 msec
2026-09-29 05:57:52,211 INFO  [PEWorker-4] procedure2.ProcedureExecutor: Finished pid=30, state=SUCCESS; TransitRegionStateProcedure table=sp_orders, region=554f89fb3a319d50d2ea3299dc1270c1, REOPEN/MOVE in 793 msec
2026-09-29 05:57:53,211 INFO  [PEWorker-4] procedure2.ProcedureExecutor: Finished pid=31, state=SUCCESS; TransitRegionStateProcedure table=sp_orders, region=17c39c635aba9b9b66fc8733de14ff83, REOPEN/MOVE in 12 msec
2026-09-29 05:58:06,572 ERROR [RpcServer.default.FPBQ.Fifo.handler=28,queue=1,port=16000] master.ServerManager:
2026-09-29 05:58:27,950 INFO  [PEWorker-12] procedure2.ProcedureExecutor: Finished pid=79, state=SUCCESS; SplitTableRegionProcedure table=sp_orders, parent=456201869c63248e892eb7dff56d6e04, daughterA=badc9208fa9494fbceded48178567f71, daughterB=5fcfa80c357be1677b821fa2c5085382 in 920 msec
2026-09-29 06:09:41,525 INFO  [RegionServerTracker-0] assignment.AssignmentManager: Scheduled ServerCrashProcedure pid=335 for ip-10-0-2-10.us-east-1.compute.internal,16020,1790655896660 (carryingMeta=true) ip-10-0-2-10.us-east-1.compute.internal,16020,1790655896660/CRASHED/regionCount=8/lock=java.util.concurrent.locks.ReentrantReadWriteLock@4f64c3ed[Write locks = 1, Read locks = 0], oldState=ONLINE
2026-09-29 06:12:01,000 INFO  [PEWorker-4] procedure2.ProcedureExecutor: Finished pid=90, state=SUCCESS; TransitRegionStateProcedure table=sp_orders, region=554f89fb3a319d50d2ea3299dc1270c1, REOPEN/MOVE in 793 msec
`, window)
	want := []string{
		"hbase-server/warning moved x2 table=sp_orders",
		"hbase-server/warning split x1 table=sp_orders",
		"hbase-server/critical server-lost x1 meta=true regions=8 server=ip-10-0-2-10.us-east-1.compute.internal",
	}
	if got := serverLines(res); got != strings.Join(want, "\n") {
		t.Errorf("master:\n%s\nwant\n%s", got, strings.Join(want, "\n"))
	}
	res = classifyText(t, rs, `2026-09-29 05:59:34,362 INFO  [regionserver/ip-10-0-2-10:16020.leaseChecker] regionserver.RSRpcServices: Scanner lease 6006178542713313347 expired clientIPAndPort=10.0.2.12:47002, userName=hadoop, regionInfo=sp_events,,1790657036204.25b1eb06c8a7d4dde9f2f0c1c7afcf3f.
2026-09-29 06:07:47,856 WARN  [RpcServer.default.FPBQ.Fifo.handler=27,queue=0,port=16020] regionserver.HRegion: Region is too busy due to exceeding memstore size limit.
2026-09-29 06:07:48,856 WARN  [RpcServer.default.FPBQ.Fifo.handler=28,queue=1,port=16020] regionserver.HRegion: Region is too busy due to exceeding memstore size limit.
2026-09-29 06:07:49,334 INFO  [MemStoreFlusher.0] regionserver.HRegion: Flushing 1595e783b53d99cd5eef43b6debb2682 1/1 column families, dataSize=1.16 MB heapSize=1.47 MB
2026-09-29 06:07:50,131 INFO  [regionserver/ip-10-0-2-10:16020-shortCompactions-0] regionserver.CompactSplit: Completed compaction region=sp_hot,,1790662068160.f342f1af5b1add7627559ca12e067774., storeName=f342f1af5b1add7627559ca12e067774/d, priority=12, startTime=1790662262895; duration=0sec
2026-09-29 06:07:51,000 WARN  [MemStoreFlusher.1] regionserver.MemStoreFlusher: sp_hot,,1790662068160.f342f1af5b1add7627559ca12e067774. has too many store files(17); delaying flush up to 90000 ms
2026-09-29 06:08:00,000 WARN  [JvmPauseMonitor] util.JvmPauseMonitor: Detected pause in JVM or host machine (eg GC): pause of approximately 12345ms
GC pool 'G1 Young Generation' had collection(s): count=1 time=12200ms
2026-09-29 06:08:30,000 WARN  [RpcServer.default.FPBQ.Fifo.handler=3,queue=0,port=16020] ipc.RpcServer: (responseTooSlow): {"call":"Scan(org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos$ScanRequest)","starttimems":"1790662110000","responsesize":"8","method":"Scan","param":"region= sp_events,,1790657036204.25b1eb06c8a7d4dde9f2f0c1c7afcf3f., scanner_id= 1 number_of_rows= 2000","processingtimems":14002,"client":"10.0.2.12:47002","queuetimems":0,"class":"HRegionServer"}
2026-09-29 06:09:39,984 INFO  [RpcServer.priority.RWQ.Fifo.read.handler=11,queue=1,port=16020] regionserver.HRegionServer: STOPPED: Called by admin client hconnection-0x169d1f92
2026-09-29 06:09:40,000 ERROR [regionserver/ip-10-0-2-10:16020] regionserver.HRegionServer: ***** ABORTING region server ip-10-0-2-10.us-east-1.compute.internal,16020,1790655896660: Replay of WAL required. Forcing server shutdown *****
`, window)
	want = []string{
		"hbase-server/warning scanner x1 client=10.0.2.12 table=sp_events user=hadoop",
		"hbase-server/warning busy x2",
		"hbase-server/info flush x1",
		"hbase-server/info compaction x1 table=sp_hot",
		"hbase-server/warning store-files x1 table=sp_hot",
		"hbase-server/warning pause x1 ms=12345",
		"hbase-server/warning slow-call x1 method=Scan ms=14002 table=sp_events",
		"hbase-server/critical server-stopped x1 reason=Called by admin client hconnection-0x169d1f92 server=ip-10-0-2-10.us-east-1.compute.internal",
		"hbase-server/critical server-stopped x1 aborted=true reason=Replay of WAL required. Forcing server shutdown server=ip-10-0-2-10.us-east-1.compute.internal",
	}
	if got := serverLines(res); got != strings.Join(want, "\n") {
		t.Errorf("region server:\n%s\nwant\n%s", got, strings.Join(want, "\n"))
	}
}

// serverLines prints each server event with its fields, but its host.
func serverLines(res Result) string {
	var out []string
	for _, l := range res.Lines {
		s := fmt.Sprintf("%s/%s %s x%d", l.Kind, l.Severity, l.Fields["event"], l.Count)
		var keys []string
		for k := range l.Fields {
			if k != "event" && k != "host" {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			s += " " + k + "=" + l.Fields[k]
		}
		out = append(out, s)
	}
	return strings.Join(out, "\n")
}

// The collector reads HBase's logs on every node, skips the hours before
// the application, and keeps the rest to its time. The phase 5 cluster's
// region server stop ran from 06:08:47 to 06:10:11.
func TestCollectHBaseLogs(t *testing.T) {
	t.Parallel()
	st := source.NewLocalStore(filepath.Join(emrlogs, "j-FIXTURE0083CLUSTER"))
	c := Collect(context.Background(), st, Plan{AppID: "application_1790380000000_0092",
		Since: time.Date(2026, 9, 29, 6, 8, 47, 0, time.UTC), Until: time.Date(2026, 9, 29, 6, 10, 11, 0, time.UTC)})
	var row *struct{ status, detail string }
	listed := 0
	for _, s := range c.Sources {
		if s.Name == "HBase server logs" {
			row = &struct{ status, detail string }{s.Status, s.Detail}
			for _, f := range s.Files {
				if f.Status == "skipped" {
					listed++
				}
			}
		}
	}
	// The hours before the run are counted, not listed one by one: a
	// long-lived cluster has thousands.
	if row == nil || row.status != "read" || !strings.Contains(row.detail, "kept to the application's time (2026-09-29 06:08–06:10 UTC). 6 hourly logs outside it skipped.") || listed != 0 {
		t.Fatalf("HBase logs row = %+v, %d skipped files listed", row, listed)
	}
	events := map[string]int{}
	for _, f := range c.Files {
		for _, l := range f.Found {
			if l.Kind == "hbase-server" {
				events[l.Fields["event"]] += l.Count
				if l.Time.Before(time.Date(2026, 9, 29, 6, 8, 47, 0, time.UTC)) || l.Time.After(time.Date(2026, 9, 29, 6, 10, 11, 0, time.UTC)) {
					t.Errorf("a line outside the window: %s %s", l.Time, l.Text)
				}
			}
		}
	}
	if events["server-lost"] != 1 || events["server-stopped"] != 1 {
		t.Errorf("events = %v", events)
	}
}

// listed records which prefixes a collection listed.
type listed struct {
	source.Store
	mu       sync.Mutex
	prefixes []string
}

func (l *listed) List(ctx context.Context, prefix string) ([]source.Object, error) {
	l.mu.Lock()
	l.prefixes = append(l.prefixes, prefix)
	l.mu.Unlock()
	return l.Store.List(ctx, prefix)
}

// HBase's logs are listed only on nodes up during the application: a
// long-lived cluster that scales has thousands of instances it once had.
func TestHBaseLogsOnlyFromNodesUpDuringTheRun(t *testing.T) {
	t.Parallel()
	st := &listed{Store: source.NewLocalStore(filepath.Join(emrlogs, "j-FIXTURE0083CLUSTER"))}
	since, until := time.Date(2026, 9, 29, 6, 8, 47, 0, time.UTC), time.Date(2026, 9, 29, 6, 10, 11, 0, time.UTC)
	Collect(context.Background(), st, Plan{AppID: "application_1790380000000_0092", Since: since, Until: until,
		Instances: []string{"i-0fee0000000000001"},
		Others:    map[string]string{"ip-10-0-2-10": "i-0fee0000000000002", "ip-10-0-2-12": "i-0fee0000000000003", "ip-10-0-2-99": "i-0fee0000000000099", "ip-10-0-2-98": "i-0fee0000000000098"},
		Lifetimes: map[string][2]time.Time{
			"i-0fee0000000000002": {since.Add(-time.Hour), time.Time{}},               // still up
			"i-0fee0000000000003": {since.Add(-time.Hour), since.Add(time.Minute)},    // ended during the run
			"i-0fee0000000000099": {since.Add(-3 * time.Hour), since.Add(-time.Hour)}, // gone before it
			"i-0fee0000000000098": {until.Add(time.Hour), time.Time{}},                // joined after it
		}})
	var hbase []string
	for _, p := range st.prefixes {
		if strings.HasSuffix(p, "/applications/hbase/") {
			hbase = append(hbase, p)
		}
	}
	sort.Strings(hbase)
	want := []string{"node/i-0fee0000000000001/applications/hbase/", "node/i-0fee0000000000002/applications/hbase/", "node/i-0fee0000000000003/applications/hbase/"}
	if strings.Join(hbase, " ") != strings.Join(want, " ") {
		t.Errorf("listed %v, want %v", hbase, want)
	}
}

// When a node's applications/hbase/ folder holds no Master or region
// server log, the collection says how many nodes it looked at and what
// was there instead, so the run can tell named-otherwise from absent.
func TestCollectHBaseSaysWhatItFound(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for _, f := range []string{"node/i-1/applications/hbase/hbase-hbase-master-ip-10-0-0-1.out.gz", "node/i-2/applications/hbase/SecurityAuth.audit.gz"} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(dir, f)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, f), []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	c := CollectHBase(context.Background(), source.NewLocalStore(dir), Plan{Instances: []string{"i-1", "i-2", "i-3"}})
	if len(c.Sources) != 0 || c.HBaseNodes != 3 || strings.Join(c.HBaseOther, ",") != "hbase-hbase-master-ip-10-0-0-1.out.gz,SecurityAuth.audit.gz" {
		t.Errorf("sources %+v, nodes %d, other %q", c.Sources, c.HBaseNodes, c.HBaseOther)
	}
}

// Without the application's time, HBase's logs would all qualify (every
// hour the cluster ever logged) and the file cap would keep the oldest:
// none is read, and the row says why and what to pass.
func TestHBaseLogsNeedTheApplicationsTime(t *testing.T) {
	t.Parallel()
	st := source.NewLocalStore(filepath.Join(emrlogs, "j-FIXTURE0083CLUSTER"))
	c := CollectHBase(context.Background(), st, Plan{Instances: []string{"i-0fee0000000000001", "i-0fee0000000000003"}})
	if len(c.Sources) != 1 || c.Sources[0].Status != "not-supplied" || len(c.Files) != 0 ||
		!strings.Contains(c.Sources[0].Detail, "Found 8 HBase logs, but nothing says when the application ran") || !strings.Contains(c.Sources[0].Detail, "Pass -eventlog") {
		t.Fatalf("sources %+v, %d files read", c.Sources, len(c.Files))
	}
	// Given the run's time, only its hours are read.
	since := time.Date(2026, 9, 29, 6, 8, 47, 0, time.UTC)
	c = CollectHBase(context.Background(), st, Plan{Instances: []string{"i-0fee0000000000001", "i-0fee0000000000003"}, Since: since, Until: since.Add(90 * time.Second)})
	if len(c.Sources) != 1 || c.Sources[0].Status != "read" || len(c.Files) != 4 {
		t.Fatalf("sources %+v, %d files read", c.Sources, len(c.Files))
	}
}

// Each TableInputFormat task's split is kept with its key range, region
// server and region, and its size when the size line can be told apart.
// In the 0084 run one executor ran all five tasks two at a time: the first
// pair logged equal sizes (50 M each), so both get it; the second pair
// logged 57 M and 50 M, which cannot be told apart, so neither does.
func TestSplitsFromExecutorLog(t *testing.T) {
	t.Parallel()
	path := filepath.Join(emrlogs, "j-FIXTURE0083CLUSTER/containers/application_1790380000000_0084/container_1790380000000_0084_01_000003/stderr.gz")
	fh, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()
	zr, err := gzip.NewReader(fh)
	if err != nil {
		t.Fatal(err)
	}
	res, err := Classify(zr, path, Describe("containers/application_1790380000000_0084/container_1790380000000_0084_01_000003/stderr.gz"), Options{AppID: "application_1790380000000_0084"})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, s := range res.Splits {
		got = append(got, fmt.Sprintf("%s [%s,%s) %s %s %d @%d", s.Table, s.StartRow, s.EndRow, strings.SplitN(s.Server, ".", 2)[0], s.Region[:6], s.SizeBytes>>20, s.Source.Line))
	}
	want := []string{
		"sp_orders [4,6) ip-10-0-2-12 456201 50 @61",
		"sp_orders [2,4) ip-10-0-2-12 17c39c 50 @62",
		"sp_orders [6,8) ip-10-0-2-12 5281bb 0 @106",
		"sp_orders [,2) ip-10-0-2-10 554f89 0 @109",
		"sp_orders [8,) ip-10-0-2-10 a4cad2 50 @131",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("splits:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if res.Splits[4].SizeSource.Line != 139 {
		t.Errorf("size source = %+v", res.Splits[4].SizeSource)
	}
}

// A log4j layout that prints the thread first and the full logger
// (%d [%t] %-5p %c - %m) is read like Spark's own, and its thread names
// each split's task, so sizes are tied exactly even when two tasks'
// lines interleave with different sizes.
func TestThreadLayoutTiesSplitsToTasks(t *testing.T) {
	t.Parallel()
	task := func(p, tid int) string {
		return fmt.Sprintf("[Executor task launch worker for task %d.0 in stage 172.0 (TID %d)]", p, tid)
	}
	log := strings.Join([]string{
		"2026-10-02 09:17:17,589 " + task(41, 6429) + " INFO  org.apache.spark.rdd.NewHadoopRDD  - Input split: Split(tablename=ns:orders, startrow=k41, endrow=k42, regionLocation=ip-10-0-2-12.example.internal, regionname=aaa949cedc)",
		"2026-10-02 09:17:17,590 " + task(42, 6430) + " INFO  org.apache.spark.rdd.NewHadoopRDD  - Input split: Split(tablename=ns:orders, startrow=k42, endrow=k43, regionLocation=ip-10-0-2-10.example.internal, regionname=bbb949cedc)",
		"2026-10-02 09:17:17,601 " + task(42, 6430) + " INFO  org.apache.hadoop.hbase.mapreduce.TableInputFormatBase  - Input split length: 57 M bytes.",
		"2026-10-02 09:17:17,602 " + task(41, 6429) + " INFO  org.apache.hadoop.hbase.mapreduce.TableInputFormatBase  - Input split length: 50 M bytes.",
		"2026-10-02 09:18:02,001 " + task(42, 6430) + " ERROR org.apache.spark.executor.Executor  - Exception in task 42.0 in stage 172.0 (TID 6430)",
	}, "\n") + "\n"
	res, err := Classify(strings.NewReader(log), "stderr", File{Kind: ContainerStderr, Container: "container_1_0001_01_000002"}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, s := range res.Splits {
		got = append(got, fmt.Sprintf("%s [%s,%s) %s %dM size@%d task %d.%d stage %d.%d TID %d", s.Table, s.StartRow, s.EndRow, s.Region, s.SizeBytes>>20,
			s.SizeSource.Line, s.Task.Partition, s.Task.Attempt, s.Task.Stage, s.Task.StageAttempt, s.Task.TaskID))
	}
	want := []string{
		"ns:orders [k41,k42) aaa949cedc 50M size@4 task 41.0 stage 172.0 TID 6429",
		"ns:orders [k42,k43) bbb949cedc 57M size@3 task 42.0 stage 172.0 TID 6430",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("splits:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	var taskErr bool
	for _, l := range res.Lines {
		if l.Kind == model.LogTaskError && l.Fields["tid"] == "6430" && l.Time.Equal(time.Date(2026, 10, 2, 9, 18, 2, 0, time.UTC)) {
			taskErr = true
		}
	}
	if !taskErr {
		t.Errorf("task error not read from the thread layout: %+v", res.Lines)
	}
	if h, ok := parseHeader(ContainerStderr, "2026-10-02 09:17:17,589 [main] WARN  org.apache.spark.SparkConf  - The configuration key is deprecated"); !ok ||
		h.thread != "main" || h.level != "WARN" || h.logger != "org.apache.spark.SparkConf" || h.msg != "The configuration key is deprecated" {
		t.Errorf("header = %+v", h)
	}
}

// A container log dates itself from any line that starts with a time,
// whatever log4j pattern follows it, so a run with no event log and no
// recognised line still has a time for HBase's logs.
func TestLogFileTimesAnyPattern(t *testing.T) {
	t.Parallel()
	log := "2026-10-02 05:12:03,123 [Executor task launch worker for task 0.0] INFO  org.apache.spark.executor.Executor - Running task 0.0\n" +
		"  at some.Frame(Frame.java:1)\n" +
		"2026-10-02 05:31:40,001 [main] INFO  org.apache.spark.executor.Executor - Finished\n"
	res, err := Classify(strings.NewReader(log), "stderr", File{Kind: ContainerStderr, Container: "container_1_0001_01_000002"}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if res.FirstTime != time.Date(2026, 10, 2, 5, 12, 3, 0, time.UTC) || res.LastTime != time.Date(2026, 10, 2, 5, 31, 40, 0, time.UTC) {
		t.Errorf("times %s – %s", res.FirstTime, res.LastTime)
	}
	res, _ = Classify(strings.NewReader("26/10/02 05:12:03 INFO Executor: Running task\n"), "stderr", File{Kind: ContainerStderr}, Options{})
	if res.FirstTime != time.Date(2026, 10, 2, 5, 12, 3, 0, time.UTC) {
		t.Errorf("Spark's own layout: %s", res.FirstTime)
	}
}

// The application's time without an event log: YARN's application summary
// when the logs hold it (exact), else the times its containers' logs cover.
func TestWindowFromLogs(t *testing.T) {
	t.Parallel()
	containers := []model.LogFile{
		{Container: "c2", FirstTime: time.Date(2026, 10, 2, 5, 12, 3, 0, time.UTC), LastTime: time.Date(2026, 10, 2, 5, 30, 0, 0, time.UTC)},
		{Container: "c3", FirstTime: time.Date(2026, 10, 2, 5, 13, 0, 0, time.UTC), LastTime: time.Date(2026, 10, 2, 5, 31, 40, 0, time.UTC)},
		{Instance: "i-1", FirstTime: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}, // a node log covers far more than the run
	}
	since, until, from := Window(containers, "application_1_0001")
	if since != containers[0].FirstTime || until != containers[1].LastTime || from != "the times its container logs cover" {
		t.Errorf("containers: %s – %s from %q", since, until, from)
	}
	rm := model.LogFile{Instance: "i-1", Found: []model.LogLine{{Kind: model.LogAppSummary, Fields: map[string]string{"appId": "application_1_0001",
		"startTime": "1790917920000", "finishTime": "1790919000000"}}}}
	since, until, from = Window(append(containers, rm), "application_1_0001")
	if since != time.UnixMilli(1790917920000).UTC() || until != time.UnixMilli(1790919000000).UTC() || from != "YARN's application summary" {
		t.Errorf("summary: %s – %s from %q", since, until, from)
	}
	if since, _, from := Window(nil, ""); !since.IsZero() || from != "" {
		t.Errorf("nothing: %s %q", since, from)
	}
}
