package analyze

import (
	"strings"
	"testing"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// A cluster like the NOAA test run: two core nodes, the driver alone on
// one, the only executor on the other, and a spot task node that went away
// mid-run taking an executor with it.
func TestNodesJoinInstances(t *testing.T) {
	start := time.Unix(1_790_000_000, 0).UTC()
	exec1 := &model.Executor{ID: "1", Host: "ip-10-0-0-2.us-east-1.compute.internal", Cores: 4}
	exec2 := &model.Executor{ID: "2", Host: "ip-10-0-0-4.ec2.internal", Cores: 4, Removed: start.Add(20 * time.Minute), RemovedReason: "Executor decommission.",
		RemovalKind: model.RemovalDecommissioned, RemovedSource: model.Source{File: "ev", Line: 40}}
	l := synthetic(nil, exec1, exec2)
	l.Driver = &model.Executor{ID: "driver", Host: "ip-10-0-0-3.us-east-1.compute.internal"}
	l.Application.DeployMode = "cluster"
	inst := func(id, dns, role, market string, created, ended time.Time) model.Instance {
		return model.Instance{ID: id, PrivateDNS: dns, Role: role, Type: "m5.xlarge", VCPU: 4, MemoryBytes: 16 << 30, Market: market, Created: created, Ended: ended,
			Primary: role == "MASTER", StateReason: "Instance was terminated."}
	}
	cl := &model.Cluster{ID: "j-1", Instances: []model.Instance{
		inst("i-1", "ip-10-0-0-1.us-east-1.compute.internal", "MASTER", "ON_DEMAND", start.Add(-time.Hour), time.Time{}),
		inst("i-2", "ip-10-0-0-2.us-east-1.compute.internal", "CORE", "ON_DEMAND", start.Add(-time.Hour), time.Time{}),
		inst("i-3", "ip-10-0-0-3.us-east-1.compute.internal", "CORE", "ON_DEMAND", start.Add(-time.Hour), time.Time{}),
		inst("i-4", "ip-10-0-0-4.us-east-1.compute.internal", "TASK", "SPOT", start.Add(-time.Hour), start.Add(21*time.Minute)),
		inst("i-5", "ip-10-0-0-5.us-east-1.compute.internal", "TASK", "SPOT", start.Add(-3*time.Hour), start.Add(-2*time.Hour)), // gone before the run
	}, Security: &model.SecurityPosture{Name: "sec", AtRestEncryption: true, S3Encryption: "SSE-KMS", InTransitEncryption: false, RuntimeRoles: true, Source: "EMR DescribeSecurityConfiguration sec"},
		SecurityConfig: "sec", InstanceProfile: "EMR_EC2_DefaultRole"}
	r := Run(Input{Tool: "t", EventLog: l, EventSource: model.SourceStatus{Name: "Spark event log", Status: "read"}, Cluster: cl,
		Steps: []model.Step{{ID: "s-1", AppID: "application_1_1", ExecutionRole: "arn:aws:iam::000000000000:role/etl"}}})

	if r.Nodes.Coverage != model.Complete || len(r.Nodes.Hosts) != 4 {
		t.Fatalf("nodes = %v, %d hosts: %+v", r.Nodes.Coverage, len(r.Nodes.Hosts), r.Nodes.Hosts)
	}
	for _, h := range r.Nodes.Hosts {
		if h.Instance == nil {
			t.Errorf("host %s not joined to an instance", h.Name)
		}
		if h.Name == "ip-10-0-0-4.ec2.internal" && h.Instance.ID != "i-4" {
			t.Errorf("hosts should match by short name across domains: %+v", h)
		}
	}
	if !strings.Contains(r.Nodes.Lede, "The cluster had 4 nodes up while it ran. 1 worker node ran no executors.") {
		t.Errorf("lede = %q", r.Nodes.Lede)
	}
	got := rules(r)
	idle := got["idle-nodes"]
	if idle.Title != "1 worker node of 3 ran no executors" || len(idle.Evidence) != 1 || !strings.Contains(idle.Evidence[0].Text, "i-3") || !strings.Contains(idle.Evidence[0].Text, "ran only the driver") {
		t.Errorf("idle = %+v", idle)
	}
	spot := got["spot-interrupted"]
	if spot.Title != "Spot node i-4 went away while the application ran" || len(spot.Evidence) != 2 || spot.Evidence[1].Ref != "executor:2" {
		t.Errorf("spot = %+v", spot)
	}
	facts := map[string]string{}
	for _, f := range r.Identity.Facts {
		facts[f.Label] = f.Value
	}
	for label, want := range map[string]string{"Encryption at rest": "on: S3 SSE-KMS", "Encryption in transit": "off", "Runtime roles": "on",
		"Lake Formation": "off", "Step runtime role": "arn:aws:iam::000000000000:role/etl", "EC2 instance profile": "EMR_EC2_DefaultRole"} {
		if facts[label] != want {
			t.Errorf("%s = %q, want %q", label, facts[label], want)
		}
	}
}

// Without the EMR API, the Nodes section says what it is missing and no
// node finding appears.
func TestNodesWithoutCluster(t *testing.T) {
	r := Run(Input{Tool: "t", EventLog: synthetic(nil, &model.Executor{ID: "1", Host: "h1", Cores: 2}), EventSource: model.SourceStatus{Name: "Spark event log", Status: "read"}})
	if r.Nodes.Coverage != model.Partial || len(r.Nodes.Missing) != 2 {
		t.Errorf("nodes = %v %v", r.Nodes.Coverage, r.Nodes.Missing)
	}
	if _, ok := rules(r)["idle-nodes"]; ok {
		t.Error("idle-nodes needs the EMR API")
	}
}

// The NOAA test run: 12,288 MB nodes, the driver's 2,432 MB container on
// one, and executors of 11,264 MB, so only one executor fitted while
// dynamic allocation wanted up to 42.
func TestExecutorFit(t *testing.T) {
	rm := logFile(t, rmLog, `2024-01-01 10:00:00,000 INFO org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService (IPC Server handler 0 on default port 8025): NodeManager from node ip-10-0-0-2.ec2.internal(cmPort: 8041 httpPort: 8042) registered with capability: <memory:12288, vCores:4>, assigned nodeId ip-10-0-0-2.ec2.internal:8041
2024-01-01 10:00:01,000 INFO org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService (IPC Server handler 1 on default port 8025): NodeManager from node ip-10-0-0-3.ec2.internal(cmPort: 8041 httpPort: 8042) registered with capability: <memory:12288, vCores:4>, assigned nodeId ip-10-0-0-3.ec2.internal:8041
2024-01-01 10:01:00,000 INFO org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode (SchedulerEventDispatcher:Event Processor): Assigned container container_1_1_01_000001 of capacity <memory:2432, max memory:12288, vCores:1, max vCores:4> on host ip-10-0-0-3.ec2.internal:8041, which has 1 containers, <memory:2432, vCores:1> used and <memory:9856, vCores:3> available after allocation
`)
	drv := logFile(t, driverErr, `24/01/01 10:01:10 INFO YarnAllocator: Will request 50 executor container(s) for  ResourceProfile Id: 0, each with 4 core(s) and 11264 MB memory. with custom resources: <memory:11264, max memory:2147483647, vCores:4, max vCores:2147483647>
24/01/01 10:01:11 INFO YarnAllocator: Launching executor with 9485m of heap (plus 1779m overhead/off heap) and 4 cores
24/01/01 10:01:15 INFO YarnAllocator: Canceling requests for 49 executor container(s) to have a new desired total 1 executors.
24/01/01 10:01:20 INFO YarnAllocator: Driver requested a total number of 42 executor(s) for resource profile id: 0.
24/01/01 10:01:25 INFO YarnAllocator: Driver requested a total number of 7 executor(s) for resource profile id: 0.
`)
	l := synthetic(nil, &model.Executor{ID: "1", Host: "ip-10-0-0-2.ec2.internal", Cores: 4})
	r := runWithLogs(l, nil, rm, drv)
	f := rules(r)["executor-fit"]
	if f.Title != "Spark wanted 42 executors; the cluster had room for 1" || !strings.Contains(f.Explanation, "ip-10-0-0-3.ec2.internal had room for none") ||
		!strings.Contains(f.Fix, "spark.executor.memory=4096m and spark.executor.cores=2") {
		t.Errorf("fit = %+v", f)
	}
	if len(f.Evidence) != 4 || f.Evidence[0].Source.Line != 4 {
		t.Errorf("evidence = %+v", f.Evidence)
	}
	for _, h := range r.Nodes.Hosts {
		if h.Name == "ip-10-0-0-2.ec2.internal" && (h.YARNMemoryBytes != 12288<<20 || h.YARNVCores != 4) {
			t.Errorf("host capacity = %+v", h)
		}
	}
	// Room for all it wanted: no finding.
	few := logFile(t, driverErr, `24/01/01 10:01:10 INFO YarnAllocator: Will request 1 executor container(s) for  ResourceProfile Id: 0, each with 4 core(s) and 11264 MB memory.
`)
	if _, ok := rules(runWithLogs(l, nil, rm, few))["executor-fit"]; ok {
		t.Error("a demand the cluster could meet raised executor-fit")
	}
}

// On EMR the event log does not carry the overhead factor EMR sets
// (0.1875 on EMR 7), so the 10% default understates the overhead; the
// driver's log says what Spark asked YARN for. Checked on the NOAA run: a
// 9486 MB heap with 11264 MB containers.
func TestOverheadFromTheContainerRequest(t *testing.T) {
	l := synthetic(map[string]string{"spark.executor.memory": "9486m"}, &model.Executor{ID: "1", Host: "ip-10-0-0-2.ec2.internal", Cores: 4})
	if r := runWithLogs(l, nil); r.Memory.Config.OverheadBytes>>20 != 948 {
		t.Fatalf("without logs, overhead = %d MiB, want the 10%% default", r.Memory.Config.OverheadBytes>>20)
	}
	launch := logFile(t, driverErr, `24/01/01 10:01:11 INFO YarnAllocator: Launching executor with 9486m of heap (plus 1778m overhead/off heap) and 4 cores
`)
	if c := runWithLogs(l, nil, launch).Memory.Config; c.OverheadBytes != 1778<<20 || c.ContainerBytes != 11264<<20 || c.OverheadFrom != "the executor launch command in the logs" {
		t.Errorf("from the launch line: %+v", c)
	}
	request := logFile(t, driverErr, `24/01/01 10:01:10 INFO YarnAllocator: Will request 1 executor container(s) for  ResourceProfile Id: 0, each with 4 core(s) and 11264 MB memory.
`)
	if c := runWithLogs(l, nil, request).Memory.Config; c.OverheadBytes != 1778<<20 || !strings.Contains(c.OverheadFrom, "11.0 GiB container Spark asked YARN for") {
		t.Errorf("from the request: %+v", c)
	}
}

// Settings the cluster's configuration or the step's spark-submit set are
// marked; a job that overrode the cluster's value is credited to the job.
func TestSettingOrigins(t *testing.T) {
	l := synthetic(map[string]string{"spark.executor.memory": "4g", "spark.sql.shuffle.partitions": "400", "spark.eventLog.dir": "s3://b/e/", "spark.executor.cores": "2", "spark.myapp.db.password": "[redacted]"})
	l.Config = append(l.Config, model.ConfigEntry{Key: "fs.s3.maxConnections", Value: "200", Group: "Hadoop", Origin: "Hadoop Properties"})
	cl := &model.Cluster{ID: "j-1", Configurations: map[string]string{"spark-defaults/spark.eventLog.dir": "s3://b/e/", "spark-defaults/spark.executor.memory": "8g",
		"emrfs-site/fs.s3.maxConnections": "200"}}
	steps := []model.Step{{ID: "s-1", AppID: "application_1_1", Args: []string{"spark-submit", "--deploy-mode", "cluster", "--executor-memory", "4g", "--conf", "spark.sql.shuffle.partitions=400",
		"--conf", "spark.myapp.db.password=[redacted]", "s3://code/job.py", "--conf", "spark.executor.cores=2"}}}
	r := Run(Input{Tool: "t", EventLog: l, EventSource: model.SourceStatus{Name: "Spark event log", Status: "read"}, Cluster: cl, Steps: steps})
	got := map[string]string{}
	for _, g := range r.Config.Groups {
		for _, e := range g.Entries {
			got[e.Key] = e.SetBy
		}
	}
	for k, want := range map[string]string{"spark.executor.memory": "spark-submit", "spark.sql.shuffle.partitions": "spark-submit", "spark.eventLog.dir": "cluster configuration",
		"fs.s3.maxConnections": "cluster configuration", "spark.myapp.db.password": "spark-submit", "spark.executor.cores": ""} {
		if got[k] != want {
			t.Errorf("%s set by %q, want %q", k, got[k], want)
		}
	}
	if !strings.Contains(r.Config.Missing[0], "EMR itself set") {
		t.Errorf("missing = %v", r.Config.Missing)
	}
}

// An application that failed before any executor started does not get an
// idle-nodes finding: its failure is the story.
func TestNoIdleNodesWithoutExecutors(t *testing.T) {
	l := synthetic(nil)
	cl := &model.Cluster{ID: "j-1", Instances: []model.Instance{{ID: "i-2", PrivateDNS: "ip-10-0-0-2.ec2.internal", Role: "CORE"}}}
	if _, ok := rules(Run(Input{Tool: "t", EventLog: l, EventSource: model.SourceStatus{Name: "Spark event log", Status: "read"}, Cluster: cl}))["idle-nodes"]; ok {
		t.Error("idle-nodes for an application with no executors")
	}
}
