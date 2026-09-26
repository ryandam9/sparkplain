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
