package model

import "time"

// MetricsSection holds CloudWatch metrics over the run's window: the
// cluster's own (AWS/ElasticMapReduce), each node's (AWS/EC2), and the
// CloudWatch agent's memory and disk metrics when it publishes them.
type MetricsSection struct {
	Coverage Coverage  `json:"coverage"`
	From     time.Time `json:"from"`
	To       time.Time `json:"to"`
	Cluster  []Series  `json:"cluster"`
	Hosts    []Series  `json:"hosts"`
	Missing  []string  `json:"missing,omitempty"`
	// Summary is what the report shows in words, worked out by analyze.
	Summary []Fact `json:"summary,omitempty"`
}

// Series is one metric over time.
type Series struct {
	Namespace string  `json:"namespace"`
	Name      string  `json:"name"`
	Stat      string  `json:"stat"`             // Maximum, Average, Sum
	Scope     string  `json:"scope"`            // the cluster ID or an instance ID
	Unit      string  `json:"unit"`             // percent, count, MB, bytes
	PeriodS   int     `json:"periodSeconds"`    // seconds between points
	Points    []Point `json:"points"`           // in time order
	Source    string  `json:"source"`           // the query, for people
	Status    string  `json:"status,omitempty"` // CloudWatch's status when not Complete
}

// Point is one value at one time.
type Point struct {
	T time.Time `json:"t"`
	V float64   `json:"v"`
}

// Max is the series' largest value, and whether it has any.
func (s Series) Max() (float64, bool) {
	if len(s.Points) == 0 {
		return 0, false
	}
	m := s.Points[0].V
	for _, p := range s.Points[1:] {
		m = max(m, p.V)
	}
	return m, true
}

// Mean is the series' average value.
func (s Series) Mean() (float64, bool) {
	if len(s.Points) == 0 {
		return 0, false
	}
	var t float64
	for _, p := range s.Points {
		t += p.V
	}
	return t / float64(len(s.Points)), true
}

// AWSCallsSection is what CloudTrail recorded for the nodes the
// application ran on, in its time window: management events only, since
// LookupEvents never returns S3 object calls.
type AWSCallsSection struct {
	Coverage  Coverage   `json:"coverage"`
	From      time.Time  `json:"from"`
	To        time.Time  `json:"to"`
	Users     []string   `json:"users"` // the CloudTrail user names looked up (instance IDs)
	Events    int        `json:"events"`
	Calls     []AWSCall  `json:"calls"`
	Denied    []AWSEvent `json:"denied"`
	Truncated bool       `json:"truncated,omitempty"` // stopped at the event cap
	Missing   []string   `json:"missing,omitempty"`
}

// AWSCall is how often one action was called.
type AWSCall struct {
	Service  string `json:"service"` // such as glue.amazonaws.com
	Action   string `json:"action"`  // such as GetTable
	Count    int    `json:"count"`
	Errors   int    `json:"errors"`
	ReadOnly bool   `json:"readOnly"`
}

// AWSEvent is one call AWS refused.
type AWSEvent struct {
	Time      time.Time `json:"time"`
	User      string    `json:"user"`
	Role      string    `json:"role,omitempty"` // the IAM role the session belonged to
	Service   string    `json:"service"`
	Action    string    `json:"action"`
	ErrorCode string    `json:"errorCode"`
	Message   string    `json:"message,omitempty"` // redacted
	Resources []string  `json:"resources,omitempty"`
	EventID   string    `json:"eventId"`
	Source    string    `json:"source"`
}
