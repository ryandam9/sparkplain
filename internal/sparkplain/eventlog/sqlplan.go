package eventlog

import (
	"regexp"
	"strings"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/redact"
)

var (
	backtickTable = regexp.MustCompile("(?:`[^`]+`\\.)*`[^`]+`")
	locationPaths = regexp.MustCompile(`\[([^\[\]]+)\]\s*$`)
)

// writeNodes are plan nodes that write data. V1 commands appear as
// "Execute <Command>".
var writeNodes = []string{
	"InsertIntoHadoopFsRelationCommand", "InsertIntoHiveTable", "InsertIntoHiveDirCommand",
	"CreateDataSourceTableAsSelectCommand", "CreateHiveTableAsSelectCommand", "OptimizedCreateHiveTableAsSelectCommand",
	"SaveIntoDataSourceCommand", "InsertIntoDataSourceCommand", "InsertIntoDataSourceDirCommand",
	"AppendData", "OverwriteByExpression", "OverwritePartitionsDynamic", "ReplaceData", "WriteDelta",
	"AtomicCreateTableAsSelect", "AtomicReplaceTableAsSelect", "CreateTableAsSelect", "ReplaceTableAsSelect",
	"WriteToDataSourceV2",
}

func isWriteNode(name string) bool {
	name = strings.TrimPrefix(name, "Execute ")
	for _, w := range writeNodes {
		if name == w {
			return true
		}
	}
	return false
}

// planData walks a physical plan and returns the tables and paths it reads
// and writes. planText is the formatted plan, used for write targets that
// the node summary leaves out (CTAS nodes print only their name).
func planData(root planNode, planText string, src model.Source) (reads, writes []model.DataRef) {
	var walk func(n planNode)
	walk = func(n planNode) {
		switch {
		case strings.HasPrefix(n.NodeName, "Scan "):
			if r, ok := scanRef(n, src); ok {
				reads = append(reads, r)
			}
		case isWriteNode(n.NodeName):
			writes = append(writes, writeRefs(n, planText, src)...)
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(root)
	return dedupe(reads), dedupe(writes)
}

func scanRef(n planNode, src model.Source) (model.DataRef, bool) {
	rest := strings.TrimSpace(strings.TrimPrefix(n.NodeName, "Scan "))
	format, target, _ := strings.Cut(rest, " ")
	target = strings.TrimSpace(target)
	switch {
	case strings.HasPrefix(format, "JDBCRelation("):
		name := strings.TrimSuffix(strings.TrimPrefix(format, "JDBCRelation("), ")")
		return model.DataRef{Kind: "table", Access: "read", Name: redact.Text(name), Format: "jdbc", Source: src}, true
	case format == "ExistingRDD" || format == "OneRowRelation" || format == "In-memory" || format == "":
		return model.DataRef{}, false
	case target != "" && !strings.HasPrefix(target, "["):
		return model.DataRef{Kind: "table", Access: "read", Name: redact.Text(target), Format: strings.ToLower(format), Source: src}, true
	}
	if loc := n.Metadata["Location"]; loc != "" {
		if m := locationPaths.FindStringSubmatch(loc); m != nil {
			return model.DataRef{Kind: "path", Access: "read", Name: redact.Text(m[1]), Format: strings.ToLower(format), Source: src}, true
		}
	}
	return model.DataRef{}, false
}

func writeRefs(n planNode, planText string, src model.Source) []model.DataRef {
	cmd := strings.TrimPrefix(n.NodeName, "Execute ")
	args := strings.TrimSpace(strings.TrimPrefix(n.SimpleString, n.NodeName))
	if args == "" {
		args = planArguments(planText, n.NodeName)
	}
	var out []model.DataRef
	if t := backtickTable.FindString(args); t != "" {
		out = append(out, model.DataRef{Kind: "table", Access: "write", Name: redact.Text(strings.ReplaceAll(t, "`", "")), Source: src})
	} else if cmd == "InsertIntoHadoopFsRelationCommand" {
		parts := strings.Split(args, ", ")
		ref := model.DataRef{Kind: "path", Access: "write", Name: redact.Text(parts[0]), Source: src}
		if len(parts) > 2 {
			ref.Format = strings.ToLower(parts[2])
		}
		out = append(out, ref)
	}
	return out
}

// planArguments finds the "Arguments:" line of a node in the formatted plan.
func planArguments(plan, nodeName string) string {
	i := strings.Index(plan, ") "+nodeName+"\n")
	if i < 0 {
		return ""
	}
	rest := plan[i:]
	j := strings.Index(rest, "Arguments: ")
	if j < 0 {
		return ""
	}
	line, _, _ := strings.Cut(rest[j+len("Arguments: "):], "\n")
	return line
}

func dedupe(refs []model.DataRef) []model.DataRef {
	seen := map[string]bool{}
	out := refs[:0]
	for _, r := range refs {
		k := r.Kind + "|" + r.Access + "|" + r.Name
		if !seen[k] {
			seen[k] = true
			out = append(out, r)
		}
	}
	return out
}
