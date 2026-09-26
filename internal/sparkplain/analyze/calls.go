package analyze

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// analyzeCalls turns CloudTrail's record of the nodes' AWS calls into the
// access-denied finding's evidence (or the finding itself when the logs
// showed no refusal) and Identity facts.
func analyzeCalls(c *ctx, r *model.Report) {
	a := r.AWSCalls
	if a == nil || a.Coverage == model.NoData {
		return
	}
	actions := 0
	errors := 0
	for _, call := range a.Calls {
		actions++
		errors += call.Errors
	}
	r.Identity.Facts = append(r.Identity.Facts, model.Fact{Label: "AWS calls",
		Value:   fmt.Sprintf("%s to %s; %d refused", model.Plural(a.Events, "call", "calls"), model.Plural(actions, "action", "actions"), len(a.Denied)),
		Explain: fmt.Sprintf("What CloudTrail recorded for the sessions of %s while the application ran. S3 object reads and writes are not included.", model.Plural(len(a.Users), "node", "nodes")),
		Source:  model.Source{File: "CloudTrail LookupEvents"}})
	if len(a.Denied) == 0 {
		return
	}
	type group struct {
		what  string
		first model.AWSEvent
		n     int
	}
	groups := map[string]*group{}
	var order []string
	for _, e := range a.Denied {
		what := e.Service + " " + e.Action
		if len(e.Resources) > 0 {
			what += " on " + e.Resources[0]
		}
		g := groups[what]
		if g == nil {
			g = &group{what: what, first: e}
			groups[what] = g
			order = append(order, what)
		}
		g.n++
	}
	sort.SliceStable(order, func(i, j int) bool { return groups[order[i]].n > groups[order[j]].n })
	var ev []model.Evidence
	for _, w := range order {
		if len(ev) == 5 {
			ev = append(ev, model.Evidence{Text: fmt.Sprintf("… and %d more kinds of refused call in CloudTrail", len(order)-5)})
			break
		}
		g := groups[w]
		text := fmt.Sprintf("CloudTrail: %s refused (%s)", w, g.first.ErrorCode)
		if g.n > 1 {
			text += fmt.Sprintf(", %d times", g.n)
		}
		if g.first.Role != "" {
			text += ", as " + g.first.Role
		}
		ev = append(ev, model.Evidence{Source: model.Source{File: g.first.Source}, Text: text})
	}
	if f := c.finding("access-denied"); f != nil {
		f.Evidence = append(f.Evidence, ev...)
		f.Explanation += " CloudTrail recorded the refusals too, with the role that made each call."
		return
	}
	role := "the cluster's instance profile"
	if r.Cluster != nil && r.Cluster.InstanceProfile != "" {
		role = "the instance profile " + r.Cluster.InstanceProfile
	}
	c.add(model.Finding{Rule: "access-denied", Severity: model.Critical, Section: "access",
		Title:       fmt.Sprintf("AWS refused %s: %s", model.Plural(len(a.Denied), "call", "calls"), strings.Join(order[:min(len(order), 3)], "; ")),
		Explanation: "CloudTrail recorded calls from the application's nodes that AWS refused. The logs sparkplain read did not show the error, so the application may have caught it or retried.",
		Evidence:    ev,
		Fix:         "Grant " + role + " (or the step's runtime role) the refused actions on those resources, or stop the job making the call."})
}
