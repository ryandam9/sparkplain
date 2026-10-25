package yarnlog

import "testing"

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
