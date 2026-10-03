package report

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// The script spark-submit ran is loaded from -source even when Spark
// recorded no line of it, as with PySpark's DataFrame actions.
func TestLoadSourcesMainScript(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "orders.py"), []byte("print('hi')\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	r := &model.Report{}
	r.Jobs.Jobs = []*model.Job{{ID: 0, Code: []model.CodeLocation{{File: "NativeMethodAccessorImpl.java"}}}}
	r.Application.Script = "orders.py"
	srcs, _, err := LoadSources(r, []string{dir})
	if err != nil {
		t.Fatal(err)
	}
	if len(srcs) != 1 || srcs[0].Logged[0] != "orders.py" || len(srcs[0].Lines) != 1 {
		t.Fatalf("sources %+v", srcs)
	}
	r.Application.Script = ""
	if srcs, _, _ := LoadSources(r, []string{dir}); len(srcs) != 0 {
		t.Errorf("without the script nothing names orders.py: %+v", srcs)
	}
}
