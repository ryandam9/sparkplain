package eventlog

import (
	"testing"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// A file write's format follows its partition columns when it has any, as
// Spark 3.5 prints InsertIntoHadoopFsRelationCommand's arguments.
func TestWriteFormat(t *testing.T) {
	for args, want := range map[string]string{
		"s3://b/t, false, Parquet, [path=s3://b/t], Overwrite, [id]":                          "parquet",
		"s3://b/t, false, [year#386], Parquet, [__partition_columns=[\"year\"]], Append, [a]": "parquet",
		"s3://b/t, false, [year#1, month#2], ORC, [path=s3://b/t], Append, [a, b]":            "orc",
	} {
		n := planNode{NodeName: "Execute InsertIntoHadoopFsRelationCommand", SimpleString: "Execute InsertIntoHadoopFsRelationCommand " + args}
		refs := writeRefs(n, "", model.Source{})
		if len(refs) != 1 || refs[0].Name != "s3://b/t" || refs[0].Format != want {
			t.Errorf("%s: %+v, want format %q", args, refs, want)
		}
	}
}
