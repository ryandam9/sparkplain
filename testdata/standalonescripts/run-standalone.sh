#!/bin/bash
# Makes testdata/standalone/cluster and its event log: a 1-master, 1-worker
# Spark Standalone cluster on this machine, started with the distribution's
# sbin scripts, runs job.py with 4 executors of 1 core. The driver runs in
# client mode, so its console is saved as driver.txt.
#
#   SPARK_HOME=/path/to/spark-3.5.3-bin-hadoop3 ./run-standalone.sh [out-dir]
#
# Then scrub the output (host names, paths, the user name) before you
# commit it; HISTORY.md (2026-10-25) lists what was replaced.
set -u
: "${SPARK_HOME:?set SPARK_HOME to an unpacked Spark 3.5 distribution}"
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=$(mkdir -p "${1:-./out}" && cd "${1:-./out}" && pwd)
rm -rf "$OUT"/{events,logs,work,driver.txt}; mkdir -p "$OUT"/{events,logs,work}
export PYSPARK_PYTHON=${PYSPARK_PYTHON:-python3} SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost
export SPARK_LOG_DIR=$OUT/logs SPARK_PID_DIR=$OUT SPARK_WORKER_DIR=$OUT/work
"$SPARK_HOME/sbin/start-master.sh" --host localhost
sleep 6
"$SPARK_HOME/sbin/start-worker.sh" spark://localhost:7077 --cores 4 --memory 4g
sleep 8
"$SPARK_HOME/bin/spark-submit" --master spark://localhost:7077 \
  --conf spark.executor.cores=1 --conf spark.cores.max=4 --conf spark.executor.memory=1g \
  --conf spark.task.maxFailures=2 --conf spark.sql.shuffle.partitions=8 \
  --conf spark.shuffle.spill.numElementsForceSpillThreshold=20000 \
  --conf spark.eventLog.enabled=true --conf spark.eventLog.dir="file://$OUT/events" \
  "$HERE/job.py" > "$OUT/driver.txt" 2>&1
echo "spark-submit exited $?"
sleep 3
"$SPARK_HOME/sbin/stop-worker.sh"
"$SPARK_HOME/sbin/stop-master.sh"
