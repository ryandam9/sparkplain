#!/usr/bin/env bash
# EMR step: run one mode of HBaseJob in cluster mode. Usage: run.sh <mode> [args]
# A failing mode is expected for missing and badquorum, so the step still succeeds.
set -uxo pipefail
W=/home/hadoop/sparkplain-hbase
spark-submit --deploy-mode cluster --class HBaseJob \
  --jars "$(cat $W/jars.txt)" --files /etc/hbase/conf/hbase-site.xml \
  "$W/hbase-job.jar" "$@"
echo "spark-submit exit $?"
