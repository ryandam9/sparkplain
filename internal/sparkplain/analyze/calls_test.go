package analyze

import (
	"strings"
	"testing"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

func callsSection() *model.AWSCallsSection {
	at := time.Unix(1_790_000_060, 0).UTC()
	deny := func(action, res string) model.AWSEvent {
		return model.AWSEvent{Time: at, User: "i-1", Role: "arn:aws:iam::000000000000:role/EMR_EC2_DefaultRole", Service: "sts.amazonaws.com", Action: action,
			ErrorCode: "AccessDenied", Resources: []string{res}, EventID: "e1", Source: "CloudTrail LookupEvents Username=i-1 event e1"}
	}
	return &model.AWSCallsSection{Coverage: model.Complete, Users: []string{"i-1"}, Events: 5,
		Calls:   []model.AWSCall{{Service: "sts.amazonaws.com", Action: "AssumeRole", Count: 2, Errors: 2}, {Service: "glue.amazonaws.com", Action: "GetTable", Count: 3}},
		Denied:  []model.AWSEvent{deny("AssumeRole", "arn:aws:iam::000000000000:role/nope"), deny("AssumeRole", "arn:aws:iam::000000000000:role/nope")},
		Missing: []string{"S3 object reads and writes: data events."}}
}

// Refusals only CloudTrail saw become the access-denied finding.
func TestCallsMakeAccessFinding(t *testing.T) {
	r := Run(Input{Tool: "t", EventLog: synthetic(nil), EventSource: model.SourceStatus{Name: "Spark event log", Status: "read"},
		Cluster: &model.Cluster{ID: "j-1", InstanceProfile: "EMR_EC2_DefaultRole"}, AWSCalls: callsSection()})
	f := rules(r)["access-denied"]
	if f.Title != "AWS refused 2 calls: sts.amazonaws.com AssumeRole on arn:aws:iam::000000000000:role/nope" || f.Severity != model.Critical ||
		!strings.Contains(f.Evidence[0].Text, "refused (AccessDenied), 2 times, as arn:aws:iam::000000000000:role/EMR_EC2_DefaultRole") ||
		!strings.Contains(f.Fix, "EMR_EC2_DefaultRole") {
		t.Errorf("finding = %+v", f)
	}
	var calls string
	for _, x := range r.Identity.Facts {
		if x.Label == "AWS calls" {
			calls = x.Value
		}
	}
	if calls != "5 calls to 2 actions; 2 refused" {
		t.Errorf("AWS calls fact = %q", calls)
	}
	if len(r.Identity.Missing) == 0 || r.Identity.Missing[0] != "S3 object reads and writes: data events." || strings.Contains(strings.Join(r.Identity.Missing, " "), "phase 3") {
		t.Errorf("missing = %v", r.Identity.Missing)
	}
}

// When the logs already showed the refusal, CloudTrail adds evidence to
// the same finding rather than a second one.
func TestCallsJoinLogFinding(t *testing.T) {
	drv := logFile(t, driverErr, `24/01/01 10:00:00 ERROR Client: failed
com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException: User: arn:aws:sts::000000000000:assumed-role/EMR_EC2_DefaultRole/i-1 is not authorized to perform: sts:AssumeRole on resource: arn:aws:iam::000000000000:role/nope (Service: AWSSecurityTokenService; Status Code: 403; Error Code: AccessDenied)
`)
	r := Run(Input{AppID: "application_1_1", Tool: "t", TimeZone: "UTC", EventLog: synthetic(nil), EventSource: model.SourceStatus{Name: "Spark event log", Status: "read"},
		Logs: []model.LogFile{drv}, LogsRead: true, AWSCalls: callsSection()})
	n := 0
	for _, f := range r.Findings {
		if f.Rule == "access-denied" {
			n++
			if !strings.HasPrefix(f.Title, "AWS refused access 1 time: sts:AssumeRole") || len(f.Evidence) != 2 || !strings.HasPrefix(f.Evidence[1].Text, "CloudTrail:") {
				t.Errorf("finding = %+v", f)
			}
		}
	}
	if n != 1 {
		t.Errorf("%d access-denied findings", n)
	}
}
