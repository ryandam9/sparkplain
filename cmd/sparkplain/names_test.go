package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"

	"github.com/ryandam9/sparkplain/internal/sparkplain/awsmeta"
)

// With the cluster's name (and the profile) in the config file, a run needs
// only -app-id. Of three clusters with the name, it reads the one that was
// up when the application's YARN started (the time in its ID), not the
// newest, and says how it chose.
func TestClusterByNameFromConfig(t *testing.T) {
	bucket, stub := hbaseCluster0083(t, false)
	fakeAWS(t, map[string]string{"logs": bucket}, stub.clusters)
	yarnStart := appClusterStart("application_1790380000000_0092")
	summary := func(id string, state types.ClusterState, created, ended time.Time) types.ClusterSummary {
		c := types.ClusterSummary{Id: aws.String(id), Name: aws.String("etl"),
			Status: &types.ClusterStatus{State: state, Timeline: &types.ClusterTimeline{CreationDateTime: aws.Time(created)}}}
		if !ended.IsZero() {
			c.Status.Timeline.EndDateTime = aws.Time(ended)
		}
		return c
	}
	withClusters := func(cs ...types.ClusterSummary) {
		s := stub
		s.summaries = cs
		awsDeps.emr = func(aws.Config) awsmeta.EMRAPI { return s }
	}
	cfg := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(cfg, []byte("cluster-name: etl\nprofile: test\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	withClusters(
		summary("j-OLDER", types.ClusterStateTerminated, yarnStart.Add(-72*time.Hour), yarnStart.Add(-48*time.Hour)),
		summary(hbaseCluster, types.ClusterStateTerminated, yarnStart.Add(-5*time.Minute), yarnStart.Add(3*time.Hour)),
		summary("j-NEWER", types.ClusterStateWaiting, yarnStart.Add(24*time.Hour), time.Time{}),
	)
	code, out, errs := runCLI(t, "-app-id", "application_1790380000000_0092", "-config", cfg, "-check")
	flatOut := flat(out)
	if code == exitFatal || !strings.Contains(flatOut, "Cluster "+hbaseCluster) ||
		!strings.Contains(flatOut, flat("found by name etl: the one up when the application's YARN started, 2026-09-25 23:46 UTC")) {
		t.Errorf("exit %d\nstdout: %s\nstderr: %s", code, out, errs)
	}

	// Two up then, created minutes apart: it cannot tell which ran the
	// application, so it says so and lists them, with a command to see
	// them all.
	withClusters(
		summary(hbaseCluster, types.ClusterStateWaiting, yarnStart.Add(-5*time.Minute), time.Time{}),
		summary("j-TWIN", types.ClusterStateWaiting, yarnStart.Add(-3*time.Minute), time.Time{}),
	)
	_, out, _ = runCLI(t, "-app-id", "application_1790380000000_0092", "-config", cfg, "-check")
	if f := flat(out); !strings.Contains(f, "more than one was up") || !strings.Contains(f, "j-TWIN (waiting") || !strings.Contains(f, "Pass the ID of the one you mean") ||
		!strings.Contains(f, "aws emr list-clusters") {
		t.Errorf("ambiguous name:\n%s", out)
	}

	// A flag overrides the file, and a local -eventlog run ignores the
	// file's cluster name.
	if code, out, _ := runCLI(t, "-app-id", "application_1790380000000_0042", "-config", cfg, "-eventlog", filepath.Join(fx, "application_1790380000000_0042"), "-out", t.TempDir()); code != exitOK || !strings.Contains(out, "offline") {
		t.Errorf("an -eventlog run went online from the config's cluster name: exit %d\n%s", code, out)
	}
}

func TestAppClusterStart(t *testing.T) {
	if got := appClusterStart("application_1790380000000_0092"); !got.Equal(time.UnixMilli(1790380000000)) {
		t.Errorf("appClusterStart = %v", got)
	}
	if !appClusterStart("app-20260925-0001").IsZero() {
		t.Error("an ID with no time gave one")
	}
}
