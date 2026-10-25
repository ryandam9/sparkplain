#!/bin/bash
# Makes testdata/standalone/local-cluster and its event log: job.py in
# local-cluster[4,1,1024] mode (4 workers of 1 core and 1 GiB, which
# spark-submit starts itself). Their executors write $SPARK_HOME/work,
# which is moved to the output folder.
#
#   SPARK_HOME=/path/to/spark-3.5.3-bin-hadoop3 ./run-local-cluster.sh [out-dir]
#
# Scrub the output before you commit it, as for run-standalone.sh.
set -u
: "${SPARK_HOME:?set SPARK_HOME to an unpacked Spark 3.5 distribution}"
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=$(mkdir -p "${1:-./out-lc}" && cd "${1:-./out-lc}" && pwd)
rm -rf "$OUT"/{events,work,driver.txt} "$SPARK_HOME/work"; mkdir -p "$OUT/events"
export PYSPARK_PYTHON=${PYSPARK_PYTHON:-python3} SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost
"$SPARK_HOME/bin/spark-submit" --master "local-cluster[4,1,1024]" \
  --conf spark.executor.memory=512m --conf spark.task.maxFailures=2 --conf spark.sql.shuffle.partitions=8 \
  --conf spark.shuffle.spill.numElementsForceSpillThreshold=20000 \
  --conf spark.eventLog.enabled=true --conf spark.eventLog.dir="file://$OUT/events" \
  "$HERE/job.py" > "$OUT/driver.txt" 2>&1
echo "spark-submit exited $?"
[ -d "$SPARK_HOME/work" ] && mv "$SPARK_HOME/work" "$OUT/work"
