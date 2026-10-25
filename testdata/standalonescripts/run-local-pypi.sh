#!/bin/bash
# Makes testdata/local and its event log: job.py in local[4] mode with
# PySpark from PyPI (pip install pyspark==3.5.3), using that package's own
# spark-submit, so the console logs at INFO and is saved as driver.txt.
# Local mode starts no executors: the driver runs every task.
#
#   ./run-local-pypi.sh [out-dir]     (with the pyspark package installed)
#
# Scrub the output before you commit it, as for run-standalone.sh.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=$(mkdir -p "${1:-./out-local}" && cd "${1:-./out-local}" && pwd)
rm -rf "$OUT"/{events,driver.txt}; mkdir -p "$OUT/events"
PYSPARK=$(python3 -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
export SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost
"$PYSPARK/bin/spark-submit" --master "local[4]" \
  --conf spark.sql.shuffle.partitions=8 --conf spark.task.maxFailures=2 \
  --conf spark.shuffle.spill.numElementsForceSpillThreshold=20000 \
  --conf spark.eventLog.enabled=true --conf spark.eventLog.dir="file://$OUT/events" \
  "$HERE/job.py" > "$OUT/driver.txt" 2>&1
echo "spark-submit exited $?"
