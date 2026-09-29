#!/usr/bin/env bash
# EMR step: one HBase trouble scenario, for real log lines. Usage:
#   scenario.sh slow    flush sp_events (so TableInputFormat sees its regions),
#                       then scan it slower than the 60 s scanner lease
#   scenario.sh moves   write and scan sp_orders while disturb.hbase moves,
#                       splits, flushes and compacts its regions
#   scenario.sh hot     write every row into sp_hot's one small region
#   scenario.sh rsdown  write sp_orders and stop a region server 45 s in
set -uxo pipefail
W=/home/hadoop/sparkplain-hbase
cd "$W"
case "$1" in
  slow)
    echo "flush 'sp_events'" | hbase shell -n
    ./run.sh slow ;;
  moves)
    hbase shell -n disturb.hbase > disturb.log 2>&1 &
    ./run.sh connector 2000000
    ./run.sh rdd
    wait; cat disturb.log ;;
  hot)
    hbase shell -n hot.hbase
    ./run.sh hot 500000 ;;
  rsdown)
    (sleep 45; hbase shell -n stoprs.hbase) > stoprs.log 2>&1 &
    ./run.sh connector 2000000
    wait; cat stoprs.log ;;
  *) echo "unknown scenario $1"; exit 2 ;;
esac
