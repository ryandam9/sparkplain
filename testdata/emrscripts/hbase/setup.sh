#!/usr/bin/env bash
# EMR step: fetch the HBase job, compile it on the primary node, and create
# its tables. Usage: setup.sh <s3 prefix holding this folder>
set -euxo pipefail
SRC=${1%/}
W=/home/hadoop/sparkplain-hbase
rm -rf "$W" && mkdir -p "$W" && cd "$W"
aws s3 cp --recursive "$SRC/" .
chmod +x ./*.sh
# What EMR ships, for the record.
rpm -qa | grep -iE 'hbase|zookeeper|spark-core' | sort || true
find /usr/lib /usr/share/aws -name '*hbase-spark*.jar' 2>/dev/null | sort
# /usr/bin/javac is Java 8; Spark runs on Corretto 17, whose compiler is
# in a package EMR does not install.
ls /usr/lib/jvm
JAVAC=$(ls /usr/lib/jvm/java-17*/bin/javac 2>/dev/null | head -1 || true)
if [ -z "$JAVAC" ]; then
  sudo dnf install -y java-17-amazon-corretto-devel
  JAVAC=$(ls /usr/lib/jvm/java-17*/bin/javac | head -1)
fi
# EMR 7.3.0 does not ship the hbase-spark connector, so bundle it as an
# application would: the latest release (1.0.1) from Maven Central.
M=https://repo1.maven.org/maven2/org/apache/hbase/connectors/spark
for a in hbase-spark hbase-spark-protocol-shaded; do
  curl -sSfo "$W/$a-1.0.1.jar" "$M/$a/1.0.1/$a-1.0.1.jar"
done
SPARKJARS=$W/hbase-spark-1.0.1.jar,$W/hbase-spark-protocol-shaded-1.0.1.jar
# The connector needs HBase's plain (unshaded) client jars, not the
# shaded-mapreduce one `hbase mapredcp` lists.
L=/usr/lib/hbase/lib
HB=$(ls $L/hbase-{annotations,client,common,server,protocol,protocol-shaded,mapreduce,zookeeper,hadoop-compat,hadoop2-compat,metrics,metrics-api,logging,procedure,asyncfs,http,replication}-2*.jar \
  $L/hbase-shaded-{miscellaneous,netty,protobuf,gson}-*.jar $L/hbase-unsafe-*.jar $L/client-facing-thirdparty/htrace-core4-*.jar \
  | grep -v tests | paste -sd,)
# Connector 1.0.1 calls slf4j 1.x's StaticLoggerBinder, which Spark 3.5's
# slf4j 2 lacks (NoClassDefFoundError on its first log line); HBase's own
# slf4j 1.7 binding supplies it.
HB=$HB,$L/client-facing-thirdparty/slf4j-reload4j-1.7.33.jar
# HBase 2.4's client still loads unshaded protobuf 2.5 (com.google.protobuf.
# RpcChannel), which Spark 3.5 no longer ships.
HB=$HB,$(ls $L/protobuf-java-2.5*.jar /usr/lib/hadoop/lib/protobuf-java-2.5*.jar 2>/dev/null | head -1)
echo "$HB,$SPARKJARS" > jars.txt
cat jars.txt
./build.sh
hbase shell -n tables.hbase
