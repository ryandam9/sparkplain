package eventlog

import "testing"

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
