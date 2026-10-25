#!/bin/bash
# Makes testdata/standalone/lost-executors and its event log: lost_executors.py
# on a 1-master, 1-worker Standalone cluster with 4 executors of 1 core and
# 512 MiB. While its long tasks sleep, this script kills executor 1's JVM
# with SIGKILL (exit 137); a later stage then runs the executors out of
# heap (exit 52). Spark starts new executors, and the application goes on.
#
#   SPARK_HOME=/path/to/spark-3.5.3-bin-hadoop3 ./run-lost-executors.sh [out-dir]
#
# Scrub the output before you commit it, as for run-standalone.sh.
set -u
: "${SPARK_HOME:?set SPARK_HOME to an unpacked Spark 3.5 distribution}"
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=$(mkdir -p "${1:-./out-lost}" && cd "${1:-./out-lost}" && pwd)
rm -rf "$OUT"/{events,logs,work,driver.txt}; mkdir -p "$OUT"/{events,logs,work}
export PYSPARK_PYTHON=${PYSPARK_PYTHON:-python3} SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost
export SPARK_LOG_DIR=$OUT/logs SPARK_PID_DIR=$OUT SPARK_WORKER_DIR=$OUT/work
"$SPARK_HOME/sbin/start-master.sh" --host localhost
sleep 6
"$SPARK_HOME/sbin/start-worker.sh" spark://localhost:7077 --cores 4 --memory 4g
sleep 8
"$SPARK_HOME/bin/spark-submit" --master spark://localhost:7077 \
  --conf spark.executor.cores=1 --conf spark.cores.max=4 --conf spark.executor.memory=512m \
  --conf spark.task.maxFailures=2 --conf spark.sql.shuffle.partitions=4 \
  --conf spark.eventLog.enabled=true --conf spark.eventLog.dir="file://$OUT/events" \
  "$HERE/lost_executors.py" > "$OUT/driver.txt" 2>&1 &
SUBMIT=$!
for _ in $(seq 1 60); do grep -q "long tasks start" "$OUT/driver.txt" 2>/dev/null && break; sleep 1; done
sleep 12
# Match the executor's JVM by its binary path, so the pattern cannot match a
# shell whose command line quotes it.
PID=$(pgrep -f "^[^ ]*/java .*CoarseGrainedExecutorBackend.*executor-id 1 " | head -1)
echo "killing executor 1 (pid $PID)"
kill -9 "$PID"
wait "$SUBMIT"
echo "spark-submit exited $?"
sleep 3
"$SPARK_HOME/sbin/stop-worker.sh"
"$SPARK_HOME/sbin/stop-master.sh"
