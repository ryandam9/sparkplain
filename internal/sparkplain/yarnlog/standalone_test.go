package yarnlog

import (
	"slices"
	"strings"
	"testing"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// A Spark Standalone executor's log is <app-id>/<executor-id>/stderr or
// stdout under a worker's work folder; the folder names the executor, and
// the folder above (not work) names the machine of copies from several.
func TestDescribeStandalone(t *testing.T) {
	t.Parallel()
	const app = "app-20261025032316-0000"
	for key, want := range map[string]File{
		app + "/0/stderr":                       {Kind: ContainerStderr, App: app, Executor: "0"},
		"work/" + app + "/12/stdout":            {Kind: ContainerStdout, App: app, Executor: "12"},
		"worker-a/work/" + app + "/3/stderr":    {Kind: ContainerStderr, App: app, Executor: "3", Host: "worker-a"},
		"worker-b/" + app + "/1/stderr":         {Kind: ContainerStderr, App: app, Executor: "1", Host: "worker-b"},
		"work/" + app + "/0/core.123":           {}, // not a log
		"work/" + app + "/driver-1/stderr":      {}, // a driver, not an executor (step 4)
		"work/app-20261025032411-0000/0/stderr": {}, // another application
		"work/" + app + "/0/stderr.gz":          {}, // Standalone does not compress them
		app + "/stderr":                         {},
	} {
		if got := DescribeStandalone(key, app); got != want {
			t.Errorf("%s: got %+v, want %+v", key, got, want)
		}
	}
}

// A cluster-mode driver's log is driver-<id>/stderr or stdout at the top
// of a worker's work folder; which application it ran is not in the path.
func TestDescribeStandaloneDriver(t *testing.T) {
	t.Parallel()
	for key, want := range map[string]File{
		"work/driver-20261025043052-0000/stderr":    {Kind: ContainerStderr, Executor: "driver"},
		"driver-20261025043052-0000/stdout":         {Kind: ContainerStdout, Executor: "driver"},
		"w1/work/driver-20261025043052-0000/stderr": {Kind: ContainerStderr, Executor: "driver", Host: "w1"},
		"work/driver-20261025043052-0000/app.jar":   {},
		"work/app-20261025043058-0000/0/stderr":     {},
	} {
		if got := DescribeStandaloneDriver(key); got != want {
			t.Errorf("%s: got %+v, want %+v", key, got, want)
		}
	}
}

// spark-daemon.sh names a Standalone daemon's log after its class and host,
// and rolls it to .out.1 to .out.5 at each start.
func TestDescribeStandaloneDaemon(t *testing.T) {
	t.Parallel()
	for key, want := range map[string]File{
		"logs/spark-sparkuser-org.apache.spark.deploy.worker.Worker-1-vm.out":              {Kind: StandaloneWorker, Host: "vm"},
		"w2/logs/spark-ec2-user-org.apache.spark.deploy.master.Master-1-ip-10-0-0-5.out.2": {Kind: StandaloneMaster, Host: "ip-10-0-0-5"},
		"logs/spark-sparkuser-org.apache.spark.deploy.history.HistoryServer-1-vm.out":      {},
		"logs/worker.log": {},
	} {
		if got := DescribeStandaloneDaemon(key); got != want {
			t.Errorf("%s: got %+v, want %+v", key, got, want)
		}
	}
}

// A worker's log says how each of the application's executors ended: a
// death (EXITED with a non-zero status) is kept with what it means, while
// the driver's own releases (KILLED), the shutdown's "Unknown Executor"
// repeats and other applications' executors are not. The master's log
// says where each executor ran.
func TestStandaloneDaemonLines(t *testing.T) {
	t.Parallel()
	const app = "app-20261025054247-0000"
	worker := strings.Join([]string{
		"26/10/25 05:43:20 INFO Worker: Executor " + app + "/1 finished with state EXITED message Command exited with code 137 exitStatus 137",
		"26/10/25 05:43:40 INFO Worker: Executor " + app + "/3 finished with state EXITED message Command exited with code 52 exitStatus 52",
		"26/10/25 05:44:00 INFO Worker: Executor " + app + "/5 finished with state KILLED exitStatus 143",
		"26/10/25 05:44:01 INFO Worker: Unknown Executor " + app + "/3 finished with state EXITED message Worker shutting down exitStatus 52",
		"26/10/25 05:44:02 INFO Worker: Executor app-20261025000000-0000/0 finished with state EXITED message Command exited with code 1 exitStatus 1",
		"26/10/25 05:44:03 ERROR Worker: something about another app-20261025000000-0000/0",
	}, "\n")
	res, err := Classify(strings.NewReader(worker), "worker.out", File{Kind: StandaloneWorker, Host: "vm"}, Options{AppID: app})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, l := range res.Lines {
		got = append(got, string(l.Kind)+" "+l.Fields["executor"]+" "+l.Fields["exitCode"]+" "+l.Fields["host"])
		if l.Fields["exitCode"] == "137" && strings.Contains(l.Fields["meaning"], "YARN") && !strings.Contains(l.Fields["meaning"], "without YARN") {
			t.Errorf("137 on Standalone reads as YARN's: %q", l.Fields["meaning"])
		}
	}
	if want := []string{"container-exit 1 137 vm", "container-exit 3 52 vm"}; !slices.Equal(got, want) {
		t.Errorf("worker lines: %q, want %q", got, want)
	}
	master := "26/10/25 05:42:47 INFO Master: Launching executor " + app + "/2 on worker worker-20261025054235-ip-10-0-0-7-41579\n"
	res, _ = Classify(strings.NewReader(master), "master.out", File{Kind: StandaloneMaster, Host: "vm"}, Options{AppID: app})
	if len(res.Lines) != 1 || res.Lines[0].Kind != model.LogExecutorHost || res.Lines[0].Fields["executor"] != "2" || res.Lines[0].Fields["host"] != "ip-10-0-0-7" {
		t.Errorf("master lines: %+v", res.Lines)
	}
}
