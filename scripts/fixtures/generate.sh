#!/usr/bin/env bash
# Regenerates testdata/eventlog from real PySpark 3.5 runs, then scrubs them.
# Needs Java 17+ and a Python with pyspark==3.5.1. Usage:
#   SP_PYTHON=/path/to/venv/bin/python SP_SCRATCH=/some/tmp scripts/fixtures/generate.sh
# SP_SCRATCH/venv must hold the pyspark the workload runs with. SP_LOCAL_IP
# (default 192.0.2.2) is the address Spark binds to; the scrubber replaces
# it with EMR-style host names. SP_ONLY="investigate running" regenerates only
# those fixtures and leaves the others as they are.
set -euo pipefail
REPO=$(cd "$(dirname "$0")/../.." && pwd)
PY=${SP_PYTHON:-python3}
SCRATCH=${SP_SCRATCH:?set SP_SCRATCH to a scratch directory}
OUT="$REPO/testdata/eventlog"
export SP_SCRATCH="$SCRATCH" SP_REPO="$REPO"
unset JAVA_TOOL_OPTIONS  # keeps proxy settings out of the logged system properties
export SP_LOCAL_IP=${SP_LOCAL_IP:-192.0.2.2} SPARK_LOCAL_IP=${SP_LOCAL_IP:-192.0.2.2} SPARK_LOCAL_HOSTNAME=${SP_LOCAL_IP:-192.0.2.2}

run() { # mode
  rm -rf "$SCRATCH/run/ev/$1" "$SCRATCH/run/work/$1"
  mkdir -p "$SCRATCH/run/ev/$1" "$SCRATCH/run/work/$1"
  "$PY" "$REPO/scripts/fixtures/workload.py" "$1" "$SCRATCH/run/ev/$1" "$SCRATCH/run/work/$1" >"$SCRATCH/run/$1.out" 2>&1 || true
  ls -d "$SCRATCH/run/ev/$1"/*
}

extra() { # the fixtures added for phase 1c
  "$PY" "$REPO/scripts/fixtures/scrub.py" "$(run investigate)" application_1790380000000_0046 "$OUT" plainonly
  run running >/dev/null
  "$PY" "$REPO/scripts/fixtures/scrub.py" "$SCRATCH/run/ev/running/snapshot" application_1790380000000_0047 "$OUT" snapshot
  # A node excluded for a stage leaves the task nowhere to run on a one-host
  # cluster, so the job aborts: a fixture of that failure.
  "$PY" "$REPO/scripts/fixtures/scrub.py" "$(run excluded)" application_1790380000000_0048 "$OUT" plainonly
}
if [ -n "${SP_ONLY:-}" ]; then
  extra
  ls -la "$OUT"
  exit 0
fi
rm -rf "$OUT" && mkdir -p "$OUT"
"$PY" "$REPO/scripts/fixtures/scrub.py" "$(run main)" application_1790380000000_0042 "$OUT" single
"$PY" "$REPO/scripts/fixtures/scrub.py" "$(run rolling)" application_1790380000000_0043 "$OUT" rolling
failed=$(run failed)
"$PY" "$REPO/scripts/fixtures/scrub.py" "$failed" application_1790380000000_0044 "$OUT" plainonly
"$PY" "$REPO/scripts/fixtures/scrub.py" "$failed" application_1790380000000_0045 "$OUT" inprogress
extra
ls -la "$OUT"
