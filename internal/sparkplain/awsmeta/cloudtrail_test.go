package awsmeta

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	cttypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
)

// stubCT returns pages of events per user: n allowed calls, then one
// refused, one throttled, and one with a planted secret in its message.
type stubCT struct {
	perUser int
	calls   int
}

func event(id, user, source, name string, doc map[string]any) cttypes.Event {
	b, _ := json.Marshal(doc)
	return cttypes.Event{EventId: aws.String(id), Username: aws.String(user), EventSource: aws.String(source), EventName: aws.String(name),
		EventTime: aws.Time(time.Date(2026, 9, 26, 15, 50, 0, 0, time.UTC)), ReadOnly: aws.String("true"), CloudTrailEvent: aws.String(string(b))}
}

func (s *stubCT) LookupEvents(_ context.Context, in *cloudtrail.LookupEventsInput, _ ...func(*cloudtrail.Options)) (*cloudtrail.LookupEventsOutput, error) {
	s.calls++
	user := aws.ToString(in.LookupAttributes[0].AttributeValue)
	var all []cttypes.Event
	for i := 0; i < s.perUser; i++ {
		all = append(all, event(user+"-ok-"+strconv.Itoa(i), user, "glue.amazonaws.com", "GetTable", map[string]any{}))
	}
	role := map[string]any{"sessionContext": map[string]any{"sessionIssuer": map[string]any{"arn": "arn:aws:iam::000000000000:role/EMR_EC2_DefaultRole"}}}
	denied := event(user+"-denied", user, "sts.amazonaws.com", "AssumeRole", map[string]any{"errorCode": "AccessDenied", "errorMessage": "not authorized", "userIdentity": role})
	denied.Resources = []cttypes.Resource{{ResourceName: aws.String("arn:aws:iam::000000000000:role/nope")}}
	all = append(all, denied,
		event(user+"-throttled", user, "glue.amazonaws.com", "GetTable", map[string]any{"errorCode": "ThrottlingException"}),
		event(user+"-kms", user, "kms.amazonaws.com", "Decrypt", map[string]any{"errorCode": "AccessDeniedException", "errorMessage": "denied for token=FAKE-TRAIL-SECRET-0015"}))
	start := 0
	if in.NextToken != nil {
		start, _ = strconv.Atoi(aws.ToString(in.NextToken))
	}
	end := min(start+int(aws.ToInt32(in.MaxResults)), len(all))
	out := &cloudtrail.LookupEventsOutput{Events: all[start:end]}
	if end < len(all) {
		out.NextToken = aws.String(strconv.Itoa(end))
	}
	return out, nil
}

func TestCalls(t *testing.T) {
	LookupInterval = 0
	defer func() { LookupInterval = 500 * time.Millisecond }()
	now := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	api := &stubCT{perUser: 60}
	sec, err := Calls(context.Background(), api, []string{"i-1", "i-2"}, now.Add(-2*time.Hour), now.Add(-time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	if api.calls != 4 || sec.Events != 126 || sec.Coverage != "complete" {
		t.Errorf("calls %d, events %d, coverage %v", api.calls, sec.Events, sec.Coverage)
	}
	if c := sec.Calls[0]; c.Service != "glue.amazonaws.com" || c.Action != "GetTable" || c.Count != 122 || c.Errors != 2 {
		t.Errorf("top call = %+v", c)
	}
	if len(sec.Denied) != 4 {
		t.Fatalf("denied = %+v", sec.Denied)
	}
	d := sec.Denied[0]
	if d.Action != "AssumeRole" || d.ErrorCode != "AccessDenied" || d.Role != "arn:aws:iam::000000000000:role/EMR_EC2_DefaultRole" ||
		strings.Join(d.Resources, ",") != "arn:aws:iam::000000000000:role/nope" || !strings.Contains(d.Source, "Username=i-1") {
		t.Errorf("denied = %+v", d)
	}
	b, _ := json.Marshal(sec)
	if strings.Contains(string(b), "FAKE-TRAIL-SECRET") {
		t.Error("a secret in an error message was kept")
	}
	if !strings.Contains(strings.Join(sec.Missing, " "), "data events") {
		t.Errorf("missing = %v", sec.Missing)
	}

	old, _ := Calls(context.Background(), api, []string{"i-1"}, now.Add(-100*24*time.Hour), now.Add(-99*24*time.Hour), now)
	if old.Coverage != "no-data" || old.Events != 0 {
		t.Errorf("100 days ago = %+v", old)
	}
	many, _ := Calls(context.Background(), &stubCT{perUser: 2100}, []string{"i-1"}, now.Add(-time.Hour), now, now)
	if !many.Truncated || many.Coverage != "partial" || many.Events != 2000 {
		t.Errorf("cap: truncated %v, coverage %v, events %d", many.Truncated, many.Coverage, many.Events)
	}
}
