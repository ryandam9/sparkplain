package eventlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The script a driver ran, from its command line: the YARN application
// master's --primary-py-file in cluster mode, SparkSubmit's first argument
// that is not an option in client mode, and nothing for a jar or a shell.
func TestMainScript(t *testing.T) {
	t.Parallel()
	for _, c := range []struct{ cmd, want string }{
		{"org.apache.spark.deploy.yarn.ApplicationMaster --class org.apache.spark.deploy.PythonRunner --primary-py-file orders_hbase.py --arg 10 --properties-file /mnt/x/__spark_conf__.properties", "orders_hbase.py"},
		{"org.apache.spark.deploy.yarn.ApplicationMaster --class org.apache.spark.deploy.RRunner --primary-r-file model.R", "model.R"},
		{"org.apache.spark.deploy.SparkSubmit --master yarn --deploy-mode client --conf spark.x=1 --py-files deps.zip,util.py --verbose /home/hadoop/jobs/etl.py 2026-10-01", "/home/hadoop/jobs/etl.py"},
		{"org.apache.spark.deploy.SparkSubmit --master=yarn s3://code/jobs/etl.py", "s3://code/jobs/etl.py"},
		{"org.apache.spark.deploy.SparkSubmit --master yarn --class com.example.Etl /home/hadoop/etl.jar run.py", ""},
		{"org.apache.spark.deploy.SparkSubmit --master yarn pyspark-shell", ""},
		{"org.apache.spark.deploy.yarn.ApplicationMaster --class com.example.Etl --jar etl.jar", ""},
		{"", ""},
	} {
		if got := mainScript(c.cmd); got != c.want {
			t.Errorf("mainScript(%q) = %q, want %q", c.cmd, got, c.want)
		}
	}
}

// A job with no call site of its own takes its final stage's (the highest
// ID), not the last one Spark lists, and none when the final stage has
// none: in a PySpark run, job 4 listed a skipped parent named after an
// earlier collect() last, and job 5's final stage named only
// NativeMethodAccessorImpl.java:0. The environment's driver command line
// gives the script.
func TestJobCodeFromFinalStage(t *testing.T) {
	t.Parallel()
	py := "collect at /mnt/yarn/app/container_1/orders.py:121"
	stage := func(id int, name string) string {
		return fmt.Sprintf(`{"Stage ID":%d,"Stage Attempt ID":0,"Stage Name":%q,"Number of Tasks":1,"RDD Info":[],"Parent IDs":[],"Details":""}`, id, name)
	}
	log := strings.Join([]string{
		`{"Event":"SparkListenerLogStart","Spark Version":"3.5.1"}`,
		`{"Event":"SparkListenerApplicationStart","App Name":"x","App ID":"app_1","Timestamp":1000,"User":"u"}`,
		`{"Event":"SparkListenerEnvironmentUpdate","JVM Information":{},"Spark Properties":{},"Hadoop Properties":{},"System Properties":{"sun.java.command":"org.apache.spark.deploy.yarn.ApplicationMaster --class org.apache.spark.deploy.PythonRunner --primary-py-file orders.py --arg 1"},"Classpath Entries":{}}`,
		`{"Event":"SparkListenerJobStart","Job ID":4,"Submission Time":2000,"Stage Infos":[` + stage(9, py) + `,` + stage(11, "runJob at SparkHadoopWriter.scala:83") + `,` + stage(8, py) + `],"Stage IDs":[9,11,8],"Properties":{}}`,
		`{"Event":"SparkListenerStageSubmitted","Stage Info":` + stage(11, "runJob at SparkHadoopWriter.scala:83") + `,"Properties":{}}`,
		`{"Event":"SparkListenerStageSubmitted","Stage Info":` + stage(8, py) + `,"Properties":{}}`,
		`{"Event":"SparkListenerJobEnd","Job ID":4,"Completion Time":3000,"Job Result":{"Result":"JobSucceeded"}}`,
		`{"Event":"SparkListenerJobStart","Job ID":5,"Submission Time":3000,"Stage Infos":[` + stage(15, "parquet at NativeMethodAccessorImpl.java:0") + `,` + stage(12, py) + `],"Stage IDs":[15,12],"Properties":{}}`,
		`{"Event":"SparkListenerStageSubmitted","Stage Info":` + stage(12, py) + `,"Properties":{}}`,
		`{"Event":"SparkListenerStageSubmitted","Stage Info":` + stage(15, "parquet at NativeMethodAccessorImpl.java:0") + `,"Properties":{}}`,
		`{"Event":"SparkListenerJobEnd","Job ID":5,"Completion Time":4000,"Job Result":{"Result":"JobSucceeded"}}`,
		`{"Event":"SparkListenerApplicationEnd","Timestamp":5000}`,
	}, "\n") + "\n"
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "app_1"), []byte(log), 0o600); err != nil {
		t.Fatal(err)
	}
	in, err := Resolve(filepath.Join(dir, "app_1"), "app_1", Limits{})
	if err != nil {
		t.Fatal(err)
	}
	l, _ := Parse(context.Background(), in, Options{})
	if len(l.Jobs) != 2 || len(l.Jobs[0].Code) == 0 || l.Jobs[0].Code[0].File != "SparkHadoopWriter.scala" {
		t.Fatalf("job code %+v, want its final stage's (SparkHadoopWriter.scala)", l.Jobs)
	}
	if len(l.Jobs[1].Code) != 0 {
		t.Errorf("job 5's final stage recorded no line, but the job got %+v", l.Jobs[1].Code)
	}
	if l.Application.Script != "orders.py" || l.Application.ScriptSource.Line != 3 {
		t.Errorf("script %q from %+v, want orders.py from line 3", l.Application.Script, l.Application.ScriptSource)
	}
}
