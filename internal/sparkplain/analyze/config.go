package analyze

import (
	"sort"
	"strconv"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
)

var groupOrder = []string{"Spark", "Hadoop", "Hive", "HBase", "JVM"}

func analyzeConfig(c *ctx, r *model.Report) {
	s := &r.Config
	if !c.has() {
		s.Coverage = model.NeedsEventLog
		s.Missing = []string{"The effective configuration, including settings made inside the code"}
		return
	}
	s.ResourceProfiles = c.log.ResourceProfiles
	s.Coverage = model.Partial
	s.Missing = []string{
		"Which settings came from EMR defaults, cluster configuration or spark-submit (needs the EMR API and step logs)",
		"The EMR release label and the Python version (not in the event log)",
		"Executors' own JVM and OS details (the event log records the driver's environment only)",
	}
	s.Runtime = runtimeRows(c)
	byGroup := map[string][]model.ConfigView{}
	for _, e := range c.log.Config {
		v := model.ConfigView{ConfigEntry: e}
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
		if view.NonDefault {
			s.NonDefault++
		}
		s.Key = append(s.Key, view)
	}
	sort.SliceStable(s.Key, func(i, j int) bool { return s.Key[i].NonDefault && !s.Key[j].NonDefault })
	configFindings(c)
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
			Explanation: "spark.driver.maxResultSize is 0, so an action such as collect() or toPandas() can pull more data than the driver's heap holds and crash it.",
			Evidence:    []model.Evidence{{Source: src, Text: "spark.driver.maxResultSize = 0"}},
			Fix:         "Set spark.driver.maxResultSize to a limit that fits the driver heap, such as 2g, and write large results to storage instead of collecting them.",
		})
	}
	if c.confBool("spark.dynamicAllocation.enabled", false) && !c.confBool("spark.shuffle.service.enabled", false) && !c.confBool("spark.dynamicAllocation.shuffleTracking.enabled", false) {
		c.add(model.Finding{
			Rule: "config-dynalloc-no-shuffle", Severity: model.Warning, Section: "config",
			Title:       "Dynamic allocation is on without a way to keep shuffle files",
			Explanation: "When dynamic allocation removes an executor, its shuffle files go with it unless the external shuffle service or shuffle tracking keeps them. Later stages then have to recompute that data.",
			Evidence:    []model.Evidence{{Source: src, Text: "spark.dynamicAllocation.enabled = true, spark.shuffle.service.enabled and spark.dynamicAllocation.shuffleTracking.enabled not set"}},
			Fix:         "Set spark.shuffle.service.enabled=true (EMR runs the service on every node) or spark.dynamicAllocation.shuffleTracking.enabled=true.",
		})
	}
	if v := c.conf["spark.sql.adaptive.enabled"]; v == "false" {
		c.add(model.Finding{
			Rule: "config-aqe-off", Severity: model.Info, Section: "config",
			Title:       "Adaptive query execution is off",
			Explanation: "Adaptive execution re-plans SQL queries with real data sizes: it merges tiny shuffle partitions, splits skewed ones and switches to broadcast joins. It is on by default in Spark 3.5.",
			Evidence:    []model.Evidence{{Source: src, Text: "spark.sql.adaptive.enabled = false"}},
			Fix:         "Remove the setting or set spark.sql.adaptive.enabled=true unless a known problem needs it off.",
		})
	}
}
