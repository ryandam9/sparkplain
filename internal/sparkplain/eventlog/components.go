package eventlog

import (
	"path"
	"regexp"
	"sort"
	"strings"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/redact"
)

// jarRE splits a jar file name into artifact and version, for example
// spark-core_2.12-3.5.1-amzn-0.jar -> spark-core_2.12, 3.5.1-amzn-0.
// PySpark ships Py4J as py4j-<version>-src.zip, so zips are read too.
var jarRE = regexp.MustCompile(`^([A-Za-z][\w.\-]*?)-(\d+(?:\.\d+)+(?:[-.][\w.\-]+)?)\.(?:jar|zip)$`)

// componentSpecs are the libraries worth naming in the runtime table. The
// event log records no library versions directly, so they come from the
// jar names on the driver's classpath. The first matching artifact wins.
var componentSpecs = []struct {
	name     string
	artifact *regexp.Regexp
}{
	{"Spark (jars)", regexp.MustCompile(`^spark-core_[\d.]+$`)},
	{"Scala library", regexp.MustCompile(`^scala-library$`)},
	{"Hadoop", regexp.MustCompile(`^hadoop-(client-api|common)$`)},
	{"Hive", regexp.MustCompile(`^hive-(exec|metastore)$`)},
	{"EMRFS", regexp.MustCompile(`^emrfs-hadoop-assembly$`)},
	{"AWS SDK for Java v1", regexp.MustCompile(`^aws-java-sdk-(bundle|core)$`)},
	{"AWS SDK for Java v2", regexp.MustCompile(`^(bundle|awssdk-bundle)$`)},
	{"Apache Iceberg", regexp.MustCompile(`^iceberg-spark-runtime-[\w.\-]+$`)},
	{"Apache Hudi", regexp.MustCompile(`^hudi-spark[\w.\-]*-bundle[\w.\-]*$`)},
	{"Delta Lake", regexp.MustCompile(`^delta-(core|spark)_[\d.]+$`)},
	{"HBase client", regexp.MustCompile(`^hbase-(shaded-)?client$`)},
	{"HBase Spark connector", regexp.MustCompile(`^hbase-spark$`)},
	{"Py4J (PySpark bridge)", regexp.MustCompile(`^py4j$`)},
}

// shippedJars adds the jars the job shipped (--jars, --packages) to the
// classpath entries: in cluster mode they reach the classpath through
// YARN, and the environment lists them only in these settings.
func shippedJars(classpath, spark map[string]string) map[string]string {
	out := make(map[string]string, len(classpath))
	for k, v := range classpath {
		out[k] = v
	}
	for _, k := range []string{"spark.jars", "spark.yarn.dist.jars", "spark.yarn.secondary.jars"} {
		for _, j := range strings.Split(spark[k], ",") {
			if j = strings.TrimSpace(j); strings.HasSuffix(j, ".jar") {
				if _, ok := out[j]; !ok {
					out[j] = "Shipped with the job (" + k + ")"
				}
			}
		}
	}
	return out
}

// components picks library versions out of the classpath entries.
func components(classpath map[string]string, src model.Source) []model.Component {
	paths := make([]string, 0, len(classpath))
	for p := range classpath {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	var out []model.Component
	for _, spec := range componentSpecs {
		for _, p := range paths {
			m := jarRE.FindStringSubmatch(path.Base(p))
			if m == nil || !spec.artifact.MatchString(m[1]) {
				continue
			}
			m[2] = strings.TrimSuffix(m[2], "-src")
			if spec.name == "AWS SDK for Java v2" && !strings.HasPrefix(m[2], "2.") {
				continue
			}
			out = append(out, model.Component{Name: spec.name, Artifact: m[1], Version: redact.Text(m[2]), Path: redact.Text(p), Source: src})
			break
		}
	}
	return out
}
