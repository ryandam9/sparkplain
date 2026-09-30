package main

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"

	"github.com/ryandam9/sparkplain/internal/sparkplain/analyze"
	"github.com/ryandam9/sparkplain/internal/sparkplain/awsmeta"
	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
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

// A config file can hold a set of keys per environment; with -env the run
// takes that set's clusters, profile and region over the top-level ones,
// so -app-id and -env are all it needs.
func TestEnvironmentsInConfig(t *testing.T) {
	bucket, stub := hbaseCluster0083(t, false)
	fakeAWS(t, map[string]string{"logs": bucket}, stub.clusters)
	var profiles []string
	awsDeps.config = func(_ context.Context, profile, region string) (aws.Config, error) {
		profiles = append(profiles, profile+"/"+region)
		return aws.Config{Region: firstNonEmpty(region, "us-east-1")}, nil
	}
	yarnStart := appClusterStart("application_1790380000000_0092")
	up := func(id, name string) types.ClusterSummary {
		return types.ClusterSummary{Id: aws.String(id), Name: aws.String(name),
			Status: &types.ClusterStatus{State: types.ClusterStateWaiting, Timeline: &types.ClusterTimeline{CreationDateTime: aws.Time(yarnStart.Add(-time.Hour))}}}
	}
	stub.summaries = []types.ClusterSummary{up(hbaseCluster, "etl-prod"), up("j-NONPROD", "etl-nonprod")}
	awsDeps.emr = func(aws.Config) awsmeta.EMRAPI { return stub }
	cfg := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(cfg, []byte(`profile: default
region: us-east-1
environments:
  prod:
    cluster-name: etl-prod
    profile: prod-emr
    region: ap-southeast-2
  nonprod:
    cluster-name: etl-nonprod
    profile: dev-emr
`), 0o600); err != nil {
		t.Fatal(err)
	}
	code, out, errs := runCLI(t, "-app-id", "application_1790380000000_0092", "-config", cfg, "-env", "prod", "-check")
	if code == exitFatal || !strings.Contains(flat(out), "Cluster "+hbaseCluster) || !strings.Contains(flat(out), "found by name etl-prod") ||
		len(profiles) == 0 || profiles[0] != "prod-emr/ap-southeast-2" {
		t.Errorf("-env prod: exit %d, profiles %v\n%s\n%s", code, profiles, out, errs)
	}
	// An environment that is not there names the ones that are.
	if code, _, errs := runCLI(t, "-app-id", "application_1790380000000_0092", "-config", cfg, "-env", "staging"); code != exitFatal ||
		!strings.Contains(errs, "no environment of that name; it has nonprod, prod") {
		t.Errorf("-env staging: exit %d: %s", code, errs)
	}
}

// The starter config parses with the strict reader as written, and again
// with every commented-out setting switched on; the thresholds and
// explorer limits it lists are the defaults. -init-config writes it and
// never over an existing file.
func TestStarterConfig(t *testing.T) {
	dir := t.TempDir()
	parse := func(text string) fileConfig {
		t.Helper()
		p := filepath.Join(dir, "c.yaml")
		if err := os.WriteFile(p, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
		c, err := loadConfig(p, true)
		if err != nil {
			t.Fatalf("%v\n%s", err, text)
		}
		return c
	}
	c := parse(starterConfig)
	if c.Envs["prod"].ClusterName == "" || c.Envs["nonprod"].Profile == "" {
		t.Errorf("starter environments = %+v", c.Envs)
	}
	on := regexp.MustCompile(`(?m)^(\s*)# (\s*[a-z-]+:.*)$`).ReplaceAllString(starterConfig, "$1$2")
	all := parse(on)
	if all.ClusterName == "" || all.TimeZone == "" || all.MaxSize == "" || all.Envs["prod"].HBaseClusterName == "" {
		t.Errorf("uncommented settings missing: %+v", all)
	}
	if got := all.Thresholds.apply(analyze.Thresholds{}); got != analyze.DefaultThresholds() {
		t.Errorf("starter thresholds %+v differ from the defaults %+v", got, analyze.DefaultThresholds())
	}
	if all.Explorer != model.DefaultExplorerLimits() {
		t.Errorf("starter explorer limits %+v differ from the defaults", all.Explorer)
	}

	path := filepath.Join(dir, "sub", "config.yaml")
	code, out, _ := runCLI(t, "-init-config", "-config", path)
	written, _ := os.ReadFile(path)
	if code != exitOK || string(written) != starterConfig || !strings.Contains(out, "-env prod -check") {
		t.Errorf("-init-config: exit %d\n%s", code, out)
	}
	if err := os.WriteFile(path, []byte("out: ~/mine\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code, _, errs := runCLI(t, "-init-config", "-config", path); code != exitFatal || !strings.Contains(errs, "already exists") {
		t.Errorf("-init-config over an existing file: exit %d: %s", code, errs)
	}
	if kept, _ := os.ReadFile(path); string(kept) != "out: ~/mine\n" {
		t.Error("-init-config overwrote an existing file")
	}
}

func TestExpandHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	for in, want := range map[string]string{"~/Downloads": filepath.Join(home, "Downloads"), "~": home,
		"s3://b/p/": "s3://b/p/", "/abs/x": "/abs/x", "rel/~x": "rel/~x", "~other/x": "~other/x", "": ""} {
		if got := expandHome(in); got != want {
			t.Errorf("expandHome(%q) = %q, want %q", in, got, want)
		}
	}
}

// The default config file is where the docs say, ~/.config/sparkplain, on
// macOS too, where os.UserConfigDir would give ~/Library/Application Support.
func TestDefaultConfigPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows keeps %AppData%")
	}
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", "")
	if got, want := defaultConfigPath(), filepath.Join(home, ".config", "sparkplain", "config.yaml"); got != want {
		t.Errorf("defaultConfigPath() = %q, want %q", got, want)
	}
	xdg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", xdg)
	if got, want := defaultConfigPath(), filepath.Join(xdg, "sparkplain", "config.yaml"); got != want {
		t.Errorf("with XDG_CONFIG_HOME: defaultConfigPath() = %q, want %q", got, want)
	}
	t.Setenv("XDG_CONFIG_HOME", "relative/dir") // not absolute: ignored, as the XDG spec says
	if got, want := defaultConfigPath(), filepath.Join(home, ".config", "sparkplain", "config.yaml"); got != want {
		t.Errorf("with a relative XDG_CONFIG_HOME: defaultConfigPath() = %q, want %q", got, want)
	}
}
