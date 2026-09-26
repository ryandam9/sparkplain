#!/usr/bin/env bash
# Regenerates testdata/eventlog from real PySpark 3.5 runs, then scrubs them.
# Needs Java 17+ and a Python with pyspark==3.5.1. Usage:
#   SP_PYTHON=/path/to/venv/bin/python SP_SCRATCH=/some/tmp scripts/fixtures/generate.sh
set -euo pipefail
REPO=$(cd "$(dirname "$0")/../.." && pwd)
PY=${SP_PYTHON:-python3}
SCRATCH=${SP_SCRATCH:?set SP_SCRATCH to a scratch directory}
OUT="$REPO/testdata/eventlog"
export SP_SCRATCH="$SCRATCH" SP_REPO="$REPO"
unset JAVA_TOOL_OPTIONS  # keeps proxy settings out of the logged system properties

run() { # mode
  rm -rf "$SCRATCH/run/ev/$1" "$SCRATCH/run/work/$1"
  mkdir -p "$SCRATCH/run/ev/$1" "$SCRATCH/run/work/$1"
  "$PY" "$REPO/scripts/fixtures/workload.py" "$1" "$SCRATCH/run/ev/$1" "$SCRATCH/run/work/$1" >"$SCRATCH/run/$1.out" 2>&1 || true
  ls -d "$SCRATCH/run/ev/$1"/*
}

rm -rf "$OUT" && mkdir -p "$OUT"
"$PY" "$REPO/scripts/fixtures/scrub.py" "$(run main)" application_1790380000000_0042 "$OUT" single
"$PY" "$REPO/scripts/fixtures/scrub.py" "$(run rolling)" application_1790380000000_0043 "$OUT" rolling
failed=$(run failed)
"$PY" "$REPO/scripts/fixtures/scrub.py" "$failed" application_1790380000000_0044 "$OUT" plainonly
"$PY" "$REPO/scripts/fixtures/scrub.py" "$failed" application_1790380000000_0045 "$OUT" inprogress
ls -la "$OUT"
