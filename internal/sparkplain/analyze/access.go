package analyze

import (
	"strings"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/source"
)

// sourceNeeds says, for each source, the permission sparkplain needs to
// read it and what the report cannot show without it.
var sourceNeeds = map[string][2]string{
	"Spark event log":  {"s3:ListBucket and s3:GetObject on the event log folder (and kms:Decrypt if it is encrypted), or read access to the local file", "jobs, stages, tasks, executors, memory, CPU, storage and configuration"},
	"EMR API":          {"elasticmapreduce:DescribeCluster, ListClusters, ListSteps, ListInstances, ListInstanceGroups, DescribeStep and DescribeSecurityConfiguration", "the cluster's details, its steps and nodes, its security configuration, and where its logs are"},
	"EC2 API":          {"ec2:DescribeInstanceTypes", "each node's vCPU and memory"},
	"Container logs":   {"s3:ListBucket and s3:GetObject on the cluster's log folder (and kms:Decrypt if it is encrypted)", "the errors, out-of-memory and memory-kill evidence in the driver's and executors' own logs"},
	"Step logs":        {"s3:ListBucket and s3:GetObject on the cluster's log folder", "the spark-submit command, the step's result and YARN's final report"},
	"Node logs":        {"s3:ListBucket and s3:GetObject on the cluster's log folder", "each node's YARN capacity, container exits and memory kills from YARN's own logs"},
	"CloudWatch":       {"cloudwatch:GetMetricData and cloudwatch:ListMetrics", "each node's CPU, containers waiting and other applications on the cluster"},
	"CloudTrail":       {"cloudtrail:LookupEvents", "the AWS calls the nodes made and every refusal"},
	"Application code": {"s3:GetObject on the application's script", "the application's code beside its jobs and stages in the explorer"},
}

// accessGaps lists the sources sparkplain was not allowed to read, so the
// report and the command line can say what is missing and why.
func accessGaps(r *model.Report) {
	for _, s := range r.Sources {
		if s.Class != source.ClassAccessDenied {
			continue
		}
		need, ok := sourceNeeds[s.Name]
		if !ok {
			need = [2]string{"read access", "what this source holds"}
		}
		if s.Location != "" && !strings.HasPrefix(s.Location, "s3://") && !strings.HasPrefix(s.Location, "EMR ") {
			need[0] = "read permission on " + s.Location
		}
		r.AccessGaps = append(r.AccessGaps, model.AccessGap{Source: s.Name, Needs: need[0], Missing: need[1], Detail: s.Detail})
	}
}
