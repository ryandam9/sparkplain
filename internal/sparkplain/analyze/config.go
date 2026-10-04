package analyze

import (
	"sort"
	"strconv"
	"strings"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

var groupOrder = []string{"Spark", "Hadoop", "Hive", "HBase", "JVM"}

func analyzeConfig(c *ctx, r *model.Report) {
	s := &r.Config
	if !c.metrics() {
		s.Coverage = model.NeedsEventLog
		s.Missing = []string{"The effective configuration, including settings made inside the code"}
		return
	}
	s.ResourceProfiles = c.log.ResourceProfiles
	s.Coverage = model.Partial
	setBy := settingOrigins(r)
	s.Missing = []string{
		"Which settings came from EMR defaults, cluster configuration or spark-submit (needs -cluster-id)",
		"The EMR release label and the Python version (not in the event log)",
		"Executors' own JVM and OS details (the event log records the driver's environment only)",
	}
	s.Runtime = runtimeRows(c)
	byGroup := map[string][]model.ConfigView{}
	for _, e := range c.log.Config {
		v := model.ConfigView{ConfigEntry: e, SetBy: setBy(e)}
		if st, ok := settingByKey[e.Key]; ok && e.Origin == "Spark Properties" {
			v.Default, v.Explain = st.def, st.explain
			v.NonDefault = !e.Redacted && !sameValue(st, e.Value)
		}
		if e.Redacted {
			s.Redacted++
		}
		s.Total++
		byGroup[e.Group] = append(byGroup[e.Group], v)
	}
	for _, g := range groupOrder {
		if es := byGroup[g]; len(es) > 0 {
			s.Groups = append(s.Groups, model.ConfigGroup{Name: g, Entries: es})
		}
	}
	for _, st := range keySettings {
		v, ok := c.conf[st.key]
		if !ok {
			continue
		}
		view := model.ConfigView{
			ConfigEntry: model.ConfigEntry{Key: st.key, Value: v, Group: "Spark", Origin: "Spark Properties", Source: c.confSrc, Redacted: v == "[redacted]"},
			Default:     st.def, Explain: st.explain,
		}
		view.NonDefault = !view.Redacted && !sameValue(st, v)
		view.Risk = riskOf(c, st.key, v)
		view.SetBy = setBy(view.ConfigEntry)
		if view.NonDefault {
			s.NonDefault++
		}
		s.Key = append(s.Key, view)
	}
	sort.SliceStable(s.Key, func(i, j int) bool { return s.Key[i].NonDefault && !s.Key[j].NonDefault })
	if r.Cluster != nil {
		s.Missing[0] = "Which unmarked settings come from EMR and which come from the code. The EMR API does not show the EMR defaults for the release and instance type"
	}
	configFindings(c)
}

// submitFlags are the spark-submit flags that set a Spark property.
var submitFlags = map[string]string{"--executor-memory": "spark.executor.memory", "--executor-cores": "spark.executor.cores", "--num-executors": "spark.executor.instances",
	"--driver-memory": "spark.driver.memory", "--driver-cores": "spark.driver.cores", "--deploy-mode": "spark.submit.deployMode", "--master": "spark.master",
	"--name": "spark.app.name", "--queue": "spark.yarn.queue", "--py-files": "spark.submit.pyFiles", "--jars": "spark.jars", "--packages": "spark.jars.packages",
	"--files": "spark.files", "--archives": "spark.archives", "--principal": "spark.kerberos.principal", "--keytab": "spark.kerberos.keytab"}

// hadoopClassifications are the EMR classifications whose keys reach the
// event log's Hadoop properties unprefixed.
var hadoopClassifications = []string{"core-site", "hdfs-site", "yarn-site", "mapred-site", "hive-site", "spark-hive-site", "emrfs-site"}

// settingOrigins says, for each setting, whether the cluster's EMR
// configuration (DescribeCluster) or the application's step (its
// spark-submit arguments) set it. A value must match to count, so a job
// that overrode the cluster's value is not credited to the cluster.
func settingOrigins(r *model.Report) func(model.ConfigEntry) string {
	cluster := map[string]string{}
	if r.Cluster != nil {
		cluster = r.Cluster.Configurations
	}
	submit := map[string]string{}
	for _, st := range r.Steps {
		if st.AppID == "" {
			continue
		}
		args := st.Args
		for i, a := range args {
			if a == "spark-submit" || strings.HasSuffix(a, "/spark-submit") {
				args = args[i+1:]
				break
			}
		}
		for i := 0; i < len(args); i++ {
			a := args[i]
			name, val, eq := strings.Cut(a, "=")
			if !strings.HasPrefix(a, "-") {
				if i > 0 && args[i-1] == "--conf" {
					continue
				}
				break // the application and its own arguments
			}
			if !eq && i+1 < len(args) {
				val = args[i+1]
				i++
			}
			switch {
			case name == "--conf" || name == "-c":
				if k, v, ok := strings.Cut(val, "="); ok {
					submit[k] = v
				}
			case submitFlags[name] != "":
				submit[submitFlags[name]] = val
			}
		}
	}
	return func(e model.ConfigEntry) string {
		if v, ok := submit[e.Key]; ok && (e.Redacted || v == e.Value || v == "[redacted]") {
			return "spark-submit"
		}
		var keys []string
		switch e.Origin {
		case "Spark Properties":
			keys = []string{"spark-defaults/" + e.Key}
		case "Hadoop Properties":
			for _, cl := range hadoopClassifications {
				keys = append(keys, cl+"/"+e.Key)
			}
		}
		for _, k := range keys {
			if v, ok := cluster[k]; ok && (e.Redacted || v == e.Value || v == "[redacted]") {
				return "cluster configuration"
			}
		}
		return ""
	}
}

func riskOf(c *ctx, key, v string) string {
	switch key {
	case "spark.driver.maxResultSize":
		if parseSize(v, 1) == 0 {
			return "No limit: one large collect() can run the driver out of memory."
		}
	case "spark.sql.adaptive.enabled":
		if v == "false" {
			return "Adaptive execution is off, so Spark cannot fix skewed or badly sized shuffles at run time."
		}
	case "spark.sql.adaptive.skewJoin.enabled":
		if v == "false" {
			return "Spark will not split skewed join partitions."
		}
	case "spark.memory.fraction":
		if f, err := strconv.ParseFloat(v, 64); err == nil && f < 0.6 {
			return "Lower than the default, so less memory for shuffles and caching; expect more spill."
		}
	case "spark.dynamicAllocation.enabled":
		if v == "true" && !c.confBool("spark.shuffle.service.enabled", false) && !c.confBool("spark.dynamicAllocation.shuffleTracking.enabled", false) {
			return "Dynamic allocation without the shuffle service or shuffle tracking loses shuffle files when executors are removed."
		}
	}
	return ""
}

func configFindings(c *ctx) {
	src := c.confSrc
	if v, ok := c.conf["spark.driver.maxResultSize"]; ok && parseSize(v, 1) == 0 {
		c.add(model.Finding{
			Rule: "config-unlimited-result", Severity: model.Warning, Section: "config",
			Title:       "The driver accepts results of any size",
			Explanation: "spark.driver.maxResultSize is 0. As a result, an action such as collect() or toPandas() can bring more data than the driver heap holds, and the driver stops.",
			Evidence:    []model.Evidence{{Source: src, Text: "spark.driver.maxResultSize = 0"}},
			Fix:         "Set spark.driver.maxResultSize to a limit that fits in the driver heap, for example 2g.\nWrite large results to storage. Do not collect them.",
		})
	}
	if c.confBool("spark.dynamicAllocation.enabled", false) && !c.confBool("spark.shuffle.service.enabled", false) && !c.confBool("spark.dynamicAllocation.shuffleTracking.enabled", false) {
		c.add(model.Finding{
			Rule: "config-dynalloc-no-shuffle", Severity: model.Warning, Section: "config",
			Title:       "Dynamic allocation is on without a way to keep shuffle files",
			Explanation: "When dynamic allocation removes an executor, Spark loses its shuffle files. Only the external shuffle service or shuffle tracking keeps them. Without them, later stages must calculate that data again.",
			Evidence:    []model.Evidence{{Source: src, Text: "spark.dynamicAllocation.enabled = true, spark.shuffle.service.enabled and spark.dynamicAllocation.shuffleTracking.enabled not set"}},
			Fix:         "Do one of these:\n- Set spark.shuffle.service.enabled=true. EMR runs the service on all nodes.\n- Set spark.dynamicAllocation.shuffleTracking.enabled=true.",
		})
	}
	if v := c.conf["spark.sql.adaptive.enabled"]; v == "false" {
		c.add(model.Finding{
			Rule: "config-aqe-off", Severity: model.Info, Section: "config",
			Title:       "Adaptive query execution is off",
			Explanation: "Adaptive execution makes a new plan for SQL queries from the real data sizes. It merges very small shuffle partitions, divides skewed partitions and changes joins to broadcast joins. It is on by default in Spark 3.5.",
			Evidence:    []model.Evidence{{Source: src, Text: "spark.sql.adaptive.enabled = false"}},
			Fix:         "Remove the setting, or set spark.sql.adaptive.enabled=true.\nKeep it off only if you know of a problem that it causes.",
		})
	}
}
