#!/usr/bin/env bash
# Compile HBaseJob into hbase-job.jar against the jars setup.sh listed in
# jars.txt. Run on the primary node after setup.sh, or again after editing
# HBaseJob.java.
set -euxo pipefail
W=/home/hadoop/sparkplain-hbase
cd "$W"
JAVAC=$(ls /usr/lib/jvm/java-17*/bin/javac | head -1)
rm -rf classes && mkdir classes
"$JAVAC" --release 17 -cp "$(tr ',' ':' < jars.txt):/usr/lib/spark/jars/*:$(hadoop classpath)" -d classes HBaseJob.java
(cd classes && "$(dirname "$JAVAC")/jar" cf ../hbase-job.jar .)
