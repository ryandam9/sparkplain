package awsmeta

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	cttypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/redact"
)

// CloudTrailAPI is the part of the CloudTrail client sparkplain uses: one
// read call. Tests stub it.
type CloudTrailAPI interface {
	LookupEvents(ctx context.Context, in *cloudtrail.LookupEventsInput, opts ...func(*cloudtrail.Options)) (*cloudtrail.LookupEventsOutput, error)
}

// NewCloudTrail makes a client from a loaded AWS config.
func NewCloudTrail(cfg aws.Config) CloudTrailAPI { return cloudtrail.NewFromConfig(cfg) }

// LookupInterval spaces LookupEvents calls: CloudTrail allows 2 a second
// per account and region. Tests set it to zero.
var LookupInterval = 500 * time.Millisecond

// Caps on what one run reads from CloudTrail.
const (
	maxEventsPerUser = 2000
	maxEvents        = 10000
	lookupDays       = 90 // LookupEvents keeps 90 days of management events
)

// Calls looks up what CloudTrail recorded for each user (instance IDs:
// EC2 names an instance-profile session after its instance) from from to
// to, counting calls per service and action and keeping every call AWS
// refused. Request parameters are never read; messages are redacted.
func Calls(ctx context.Context, api CloudTrailAPI, users []string, from, to, now time.Time) (*model.AWSCallsSection, error) {
	sec := &model.AWSCallsSection{From: from, To: to, Users: users, Coverage: model.Complete, Calls: []model.AWSCall{}, Denied: []model.AWSEvent{}}
	sec.Missing = append(sec.Missing, "S3 object reads and writes: CloudTrail records them only as data events, which LookupEvents never returns.",
		"Some refusals: an STS AssumeRole refused for a role that does not exist never reached CloudTrail's event history in testing, so the container logs are checked too.")
	if now.Sub(from) > lookupDays*24*time.Hour {
		sec.Coverage = model.NoData
		sec.Missing = append(sec.Missing, fmt.Sprintf("CloudTrail's event history keeps %d days; this run is older.", lookupDays))
		return sec, nil
	}
	type key struct{ service, action string }
	calls := map[key]*model.AWSCall{}
	var last time.Time
	for _, user := range users {
		n := 0
		p := cloudtrail.NewLookupEventsPaginator(api, &cloudtrail.LookupEventsInput{StartTime: aws.Time(from), EndTime: aws.Time(to), MaxResults: aws.Int32(50),
			LookupAttributes: []cttypes.LookupAttribute{{AttributeKey: cttypes.LookupAttributeKeyUsername, AttributeValue: aws.String(user)}}})
		for p.HasMorePages() {
			if wait := LookupInterval - time.Since(last); wait > 0 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(wait):
				}
			}
			last = time.Now()
			page, err := p.NextPage(ctx)
			if err != nil {
				return nil, fmt.Errorf("CloudTrail LookupEvents %s: %w", user, err)
			}
			for _, e := range page.Events {
				n++
				sec.Events++
				service, action := aws.ToString(e.EventSource), aws.ToString(e.EventName)
				k := key{service, action}
				c := calls[k]
				if c == nil {
					c = &model.AWSCall{Service: service, Action: action, ReadOnly: aws.ToString(e.ReadOnly) == "true"}
					calls[k] = c
				}
				c.Count++
				var doc struct {
					ErrorCode    string `json:"errorCode"`
					ErrorMessage string `json:"errorMessage"`
					UserIdentity struct {
						SessionContext struct {
							SessionIssuer struct {
								Arn string `json:"arn"`
							} `json:"sessionIssuer"`
						} `json:"sessionContext"`
					} `json:"userIdentity"`
				}
				_ = json.Unmarshal([]byte(aws.ToString(e.CloudTrailEvent)), &doc)
				if doc.ErrorCode == "" {
					continue
				}
				c.Errors++
				if !denied(doc.ErrorCode) {
					continue
				}
				ev := model.AWSEvent{Time: aws.ToTime(e.EventTime).UTC(), User: user, Role: doc.UserIdentity.SessionContext.SessionIssuer.Arn,
					Service: service, Action: action, ErrorCode: doc.ErrorCode, Message: redact.Text(doc.ErrorMessage), EventID: aws.ToString(e.EventId),
					Source: "CloudTrail LookupEvents Username=" + user + " event " + aws.ToString(e.EventId)}
				for _, r := range e.Resources {
					if name := aws.ToString(r.ResourceName); name != "" {
						ev.Resources = append(ev.Resources, redact.Text(name))
					}
				}
				sec.Denied = append(sec.Denied, ev)
			}
			if n >= maxEventsPerUser || sec.Events >= maxEvents {
				sec.Truncated = true
				break
			}
		}
		if sec.Events >= maxEvents {
			break
		}
	}
	for _, c := range calls {
		sec.Calls = append(sec.Calls, *c)
	}
	sort.Slice(sec.Calls, func(i, j int) bool {
		a, b := sec.Calls[i], sec.Calls[j]
		if a.Errors != b.Errors {
			return a.Errors > b.Errors
		}
		if a.Count != b.Count {
			return a.Count > b.Count
		}
		return a.Service+a.Action < b.Service+b.Action
	})
	sort.SliceStable(sec.Denied, func(i, j int) bool { return sec.Denied[i].Time.Before(sec.Denied[j].Time) })
	if sec.Truncated {
		sec.Coverage = model.Partial
		sec.Missing = append(sec.Missing, fmt.Sprintf("Events past the first %d per node (%d in all) were not read.", maxEventsPerUser, maxEvents))
	}
	return sec, nil
}

// denied reports whether a CloudTrail error code means a permission was
// refused, as against a throttle or a missing resource.
func denied(code string) bool {
	c := strings.ToLower(code)
	return strings.Contains(c, "accessdenied") || strings.Contains(c, "unauthorized") || strings.Contains(c, "notauthorized") || strings.Contains(c, "forbidden")
}
