package analyze

import (
	"path"
	"strings"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

// Runtime table groups, in display order.
const (
	groupVersions  = "Versions"
	groupRuntime   = "Runtime"
	groupLocations = "Locations"
)

// runtimeRows builds the "Runtime environment" table: the versions, runtime
// settings and locations the driver recorded. The event log only holds the
// driver's environment, so executors on other hosts may differ.
func runtimeRows(c *ctx) []model.RuntimeRow {
	var rows []model.RuntimeRow
	src := c.confSrc
	add := func(group, label, value, from, explain string, s model.Source) {
		if value == "" {
			rows = append(rows, model.RuntimeRow{Group: group, Label: label, Value: "not recorded", From: from, Explain: explain, Missing: true})
			return
		}
		rows = append(rows, model.RuntimeRow{Group: group, Label: label, Value: value, From: from, Explain: explain, Source: s})
	}
	comp := map[string]model.Component{}
	for _, x := range c.log.Components {
		comp[x.Name] = x
	}
	fromJar := func(name string) (string, string, model.Source) {
		x, ok := comp[name]
		if !ok {
			return "", "", model.Source{}
		}
		return x.Version, path.Base(x.Path), x.Source
	}
	a := c.log.Application

	// Versions.
	sparkFrom := "SparkListenerLogStart"
	sparkVal := a.SparkVersion
	if v, jar, _ := fromJar("Spark (jars)"); v != "" && v != a.SparkVersion {
		sparkVal += " (build " + v + ")"
		sparkFrom += ", " + jar
	}
	add(groupVersions, "Spark", sparkVal, sparkFrom, "The Spark release that ran the application. An -amzn- build suffix means Amazon's EMR build.", a.VersionSrc)
	add(groupVersions, "Scala", strings.TrimPrefix(c.sys["Scala Version"], "version "), "JVM Information › Scala Version", "The Scala version Spark was built with. Libraries added to the job must match it.", src)
	java := c.sys["java.version"]
	if java == "" {
		java = c.sys["Java Version"]
	}
	if vendor := c.sys["java.vendor"]; vendor != "" && java != "" {
		java += " (" + vendor + ")"
	}
	if vm := c.sys["java.vm.name"]; vm != "" && java != "" {
		java += ", " + vm
	}
	add(groupVersions, "Java", java, "System Properties › java.version, java.vendor, java.vm.name", "The Java runtime of the driver. Libraries and JVM flags must suit this version.", src)
	v, jar, s := fromJar("Hadoop")
	add(groupVersions, "Hadoop", v, jarFrom(jar), "The Hadoop client libraries Spark used for HDFS, S3A and YARN. Read from the jar name, since the event log does not record it directly.", s)
	for _, name := range []string{"Hive", "EMRFS", "AWS SDK for Java v1", "AWS SDK for Java v2", "Apache Iceberg", "Apache Hudi", "Delta Lake", "HBase client", "HBase Spark connector"} {
		if v, jar, s := fromJar(name); v != "" {
			add(groupVersions, name, v, jarFrom(jar), componentExplain[name], s)
		}
	}
	add(groupVersions, "EMR release", "", "EMR API (phase 3)", "The event log does not name the EMR release; reading it from the EMR API comes in phase 3. A version ending in -amzn-N above means an Amazon build.", model.Source{})
	pyVer, pyJar, pySrc := fromJar("Py4J (PySpark bridge)")
	switch {
	case c.pyspark && pyVer != "":
		add(groupVersions, "PySpark", "yes, Py4J "+pyVer, "spark.submit.pyFiles, "+pyJar, "The application was submitted from Python. The Python version itself is not in the event log.", pySrc)
	case c.pyspark:
		add(groupVersions, "PySpark", "yes", "spark.submit.pyFiles", "The application was submitted from Python. The Python version itself is not in the event log.", src)
	default:
		add(groupVersions, "PySpark", "no", "spark.submit.pyFiles not set", "No sign of a Python driver: the application ran on the JVM only.", src)
	}

	// Runtime.
	add(groupRuntime, "Master", c.conf["spark.master"], "spark.master", "Where Spark asked for executors: yarn on EMR, local or local-cluster for a test run.", src)
	add(groupRuntime, "Deploy mode", orDefault(c.conf["spark.submit.deployMode"], "client"), "spark.submit.deployMode", "client runs the driver where spark-submit ran; cluster runs it inside a YARN container.", src)
	add(groupRuntime, "Scheduler mode", orDefault(c.conf["spark.scheduler.mode"], "FIFO"), "spark.scheduler.mode", "How jobs submitted at the same time share executors: FIFO runs them in order, FAIR shares.", src)
	osVal := strings.TrimSpace(strings.Join(nonEmpty(c.sys["os.name"], c.sys["os.version"], c.sys["os.arch"]), " "))
	add(groupRuntime, "Operating system", osVal, "System Properties › os.name, os.version, os.arch", "The driver host's operating system and CPU architecture (amd64 or aarch64 for Graviton).", src)
	add(groupRuntime, "JVM time zone", c.sys["user.timezone"], "System Properties › user.timezone", "The zone the driver used to read and write dates and timestamps without an explicit zone.", src)
	add(groupRuntime, "File encoding", c.sys["file.encoding"], "System Properties › file.encoding", "The character set the driver used for text files without an explicit encoding.", src)
	add(groupRuntime, "Default filesystem", c.conf["fs.defaultFS"], "Hadoop Properties › fs.defaultFS", "Where paths without a scheme point: HDFS on the cluster, or the local disk.", src)
	add(groupRuntime, "Hadoop authentication", orDefault(c.conf["hadoop.security.authentication"], "simple"), "Hadoop Properties › hadoop.security.authentication", "simple trusts the user name, and kerberos asks for a ticket.", src)
	add(groupRuntime, "Table catalog", orDefault(c.conf["spark.sql.catalogImplementation"], "in-memory"), "spark.sql.catalogImplementation", "hive means a Hive metastore or the AWS Glue Data Catalog; in-memory keeps tables for this run only.", src)

	// Locations.
	javaHome := c.sys["java.home"]
	if javaHome == "" {
		javaHome = c.sys["Java Home"]
	}
	add(groupLocations, "Java home", javaHome, "JVM Information › Java Home", "The Java installation the driver ran from.", src)
	sparkHome, sparkHomeSrc := "", model.Source{}
	if x, ok := comp["Spark (jars)"]; ok {
		if dir := path.Dir(x.Path); path.Base(dir) == "jars" {
			sparkHome, sparkHomeSrc = path.Dir(dir), x.Source
		}
	}
	add(groupLocations, "Spark home", sparkHome, "Classpath Entries (folder holding spark-core)", "Where Spark is installed on the driver host.", sparkHomeSrc)
	add(groupLocations, "Driver working directory", c.sys["user.dir"], "System Properties › user.dir", "The folder the driver started in. Relative local paths in the code resolve from here.", src)
	add(groupLocations, "Driver host", c.conf["spark.driver.host"], "spark.driver.host", "The machine the driver ran on.", src)
	add(groupLocations, "Event log directory", c.conf["spark.eventLog.dir"], "spark.eventLog.dir", "Where Spark wrote this event log. On EMR the default is HDFS, which is lost when the cluster ends.", src)
	add(groupLocations, "SQL warehouse", c.conf["spark.sql.warehouse.dir"], "spark.sql.warehouse.dir", "Where managed tables are stored when no location is given.", src)
	local := c.conf["spark.local.dir"]
	localFrom, localExplain := "spark.local.dir", "Scratch disk for shuffle files and spill."
	if local == "" {
		localExplain += " Not set, so on YARN each executor uses the node's YARN local directories, and the driver uses the JVM temp directory."
	}
	add(groupLocations, "Local scratch directories", local, localFrom, localExplain, src)
	add(groupLocations, "JVM temp directory", c.sys["java.io.tmpdir"], "System Properties › java.io.tmpdir", "Where the driver JVM writes temporary files.", src)
	return rows
}

var componentExplain = map[string]string{
	"Hive":                  "Hive libraries Spark uses to talk to a Hive metastore or the Glue Data Catalog.",
	"EMRFS":                 "Amazon's S3 filesystem for EMR (s3:// paths).",
	"AWS SDK for Java v1":   "AWS client library used by EMRFS, S3A and other AWS integrations.",
	"AWS SDK for Java v2":   "AWS client library used by newer AWS integrations, including S3A in Hadoop 3.4.",
	"Apache Iceberg":        "Table format library for Iceberg tables.",
	"Apache Hudi":           "Table format library for Hudi tables.",
	"Delta Lake":            "Table format library for Delta tables.",
	"HBase client":          "Client library for reading and writing HBase tables.",
	"HBase Spark connector": "The hbase-spark connector, which reads and writes HBase tables as DataFrames (format org.apache.hadoop.hbase.spark).",
}

func jarFrom(jar string) string {
	if jar == "" {
		return "Classpath Entries"
	}
	return "Classpath Entries › " + jar
}

func orDefault(v, def string) string {
	if v == "" {
		return def + " (default)"
	}
	return v
}

func nonEmpty(v ...string) []string {
	var out []string
	for _, s := range v {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}
