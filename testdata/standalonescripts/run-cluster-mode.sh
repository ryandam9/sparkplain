#!/bin/bash
# Makes testdata/standalone/cluster-mode and its event log: Spark's SparkPi
# example in cluster deploy mode on a 1-master, 1-worker Standalone cluster
# (PySpark cannot run in cluster deploy mode on Standalone). The driver
# runs in the worker's work/driver-…/ folder, beside the executors'.
#
#   SPARK_HOME=/path/to/spark-3.5.3-bin-hadoop3 ./run-cluster-mode.sh [out-dir]
#
# On Java 17 or later, a driver the worker starts does not get the
# --add-opens options spark-submit gives a client-mode driver, so they are
# passed here. Delete the examples jar the worker copies into the driver's
# folder, and scrub the output, before you commit it.
set -u
: "${SPARK_HOME:?set SPARK_HOME to an unpacked Spark 3.5 distribution}"
OUT=$(mkdir -p "${1:-./out-cm}" && cd "${1:-./out-cm}" && pwd)
rm -rf "$OUT"/{events,logs,work}; mkdir -p "$OUT"/{events,logs,work}
export SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost
export SPARK_LOG_DIR=$OUT/logs SPARK_PID_DIR=$OUT SPARK_WORKER_DIR=$OUT/work
"$SPARK_HOME/sbin/start-master.sh" --host localhost
sleep 6
"$SPARK_HOME/sbin/start-worker.sh" spark://localhost:7077 --cores 4 --memory 4g
sleep 8
OPENS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
"$SPARK_HOME/bin/spark-submit" --master spark://localhost:7077 --deploy-mode cluster \
  --class org.apache.spark.examples.SparkPi \
  --conf "spark.driver.extraJavaOptions=$OPENS" \
  --conf spark.executor.cores=1 --conf spark.cores.max=3 --conf spark.executor.memory=1g \
  --conf spark.eventLog.enabled=true --conf spark.eventLog.dir="file://$OUT/events" \
  "$SPARK_HOME"/examples/jars/spark-examples_*.jar 20
echo "spark-submit exited $? (it returns once the driver starts)"
sleep 25 # the driver runs on after spark-submit returns
"$SPARK_HOME/sbin/stop-worker.sh"
"$SPARK_HOME/sbin/stop-master.sh"
