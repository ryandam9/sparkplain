package analyze

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

func TestRulePoorLocality(t *testing.T) {
	t.Parallel()
	stage := func(id int, input, process, node, rack, anyHost int64) *model.Stage {
		return &model.Stage{ID: id, Name: "load at job.py:2", Status: model.StatusSucceeded, TaskSource: model.Source{File: "f", Line: int64(20 + id)},
			Totals: model.TaskTotals{Tasks: process + node + rack + anyHost, InputBytes: input,
				LocalityProcess: process, LocalityNode: node, LocalityRack: rack, LocalityAny: anyHost}}
	}
	l := synthetic(nil)
	// 7 of the 20 input tasks ran away from their data: 35%
	l.Stages = []*model.Stage{stage(1, 1<<30, 2, 2, 1, 5), stage(2, 1<<30, 9, 0, 0, 1)}
	f, ok := runSynthetic(l)["poor-locality"]
	if !ok || f.Severity != model.Info || !strings.Contains(f.Title, "35%") {
		t.Fatalf("poor-locality = %+v", f)
	}
	// only stage 1 is over the threshold (6 of 10 away from their data)
	if len(f.Evidence) != 1 || f.Evidence[0].Ref != "stage:1.0" || !strings.Contains(f.Evidence[0].Text, "6 of 10 input tasks") {
		t.Errorf("evidence %+v", f.Evidence)
	}
	// Tasks that read no input (shuffle only) have nothing to be local to;
	// a stage with few tasks is too small to judge.
	l.Stages = []*model.Stage{stage(3, 0, 0, 0, 0, 10), stage(4, 1<<30, 0, 0, 0, 3)}
	if f, ok := runSynthetic(l)["poor-locality"]; ok {
		t.Errorf("fired on shuffle-only or tiny stages: %+v", f)
	}
}

func TestRuleStageRetried(t *testing.T) {
	t.Parallel()
	l := synthetic(nil)
	l.Stages = []*model.Stage{
		{ID: 3, Attempt: 0, Name: "count at job.py:5", Status: model.StatusFailed, FailureReason: "org.apache.spark.shuffle.FetchFailedException: Failed to connect to ip-10-0-0-9\n\tat ..."},
		{ID: 3, Attempt: 1, Name: "count at job.py:5", Status: model.StatusSucceeded, Source: model.Source{File: "f", Line: 77}},
		{ID: 4, Attempt: 0, Name: "save at job.py:9", Status: model.StatusSucceeded},
	}
	var got []model.Finding
	for _, f := range Run(Input{Tool: "t", EventLog: l, EventSource: model.SourceStatus{Name: "Spark event log", Status: "read"}}).Findings {
		if f.Rule == "stage-retried" {
			got = append(got, f)
		}
	}
	if len(got) != 1 {
		t.Fatalf("want one stage-retried finding, got %+v", got)
	}
	f := got[0]
	if f.Severity != model.Warning || f.Title != "Stage 3 ran again (attempt 2)" || !strings.Contains(f.Explanation, "FetchFailedException") ||
		f.Evidence[0].Ref != "stage:3.1" || f.Evidence[0].Source.Line != 77 {
		t.Errorf("stage-retried = %+v", f)
	}
}

// Every finding rule is exercised by at least one test (HISTORY.md, phase 4):
// a rule named in analyze but in no test file fails the build.
func TestEveryRuleHasATest(t *testing.T) {
	t.Parallel()
	defined := map[string]bool{}
	ruleRE := regexp.MustCompile(`Rule:\s+"([a-z0-9-]+)"`)
	files, _ := filepath.Glob("*.go")
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range ruleRE.FindAllStringSubmatch(string(b), -1) {
			defined[m[1]] = true
		}
	}
	if len(defined) < 30 {
		t.Fatalf("found only %d rules; the pattern no longer matches analyze", len(defined))
	}
	// Tests anywhere in the module count: the CLI's recorded-cluster tests
	// pin several rules.
	var tests strings.Builder
	root := filepath.Join("..", "..", "..")
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == ".git" || d.Name() == "testdata" || d.Name() == "node_modules") {
			return filepath.SkipDir
		}
		if strings.HasSuffix(p, "_test.go") {
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			tests.Write(b)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var missing []string
	for r := range defined {
		if !strings.Contains(tests.String(), `"`+r+`"`) {
			missing = append(missing, r)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("finding rules with no test: %v", missing)
	}
}
