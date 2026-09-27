"""A PySpark run built to trip sparkplain's findings on a real EMR cluster.

Usage: spark-submit p4_findings.py <output s3 prefix> [gap seconds]
Each part is its own job with a description, so the report can name it:
  1. a cached DataFrame, read twice
  2. a sort-merge join where half the rows share one key (skew)
  3. a wide sort on small executors (spill)
  4. a task that fails on its first attempt and succeeds when retried
  5. an idle driver between jobs (driver gap)
"""
import sys
import time

from pyspark import TaskContext
from pyspark.sql import SparkSession, functions as F

out = sys.argv[1].rstrip("/")
gap = int(sys.argv[2]) if len(sys.argv) > 2 else 120
spark = SparkSession.builder.appName("sparkplain_p4_findings").getOrCreate()
sc = spark.sparkContext
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")     # force a sort-merge join
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")   # keep the skew visible
spark.conf.set("spark.sql.shuffle.partitions", "32")

# 1. Cache: an events table read by two later jobs.
sc.setJobDescription("build and cache events")
events = (spark.range(0, 20_000_000, numPartitions=32)
          .withColumn("key", F.when(F.col("id") % 2 == 0, F.lit(0)).otherwise(F.col("id") % 5000))
          .withColumn("payload", F.sha2(F.col("id").cast("string"), 256)))
events.cache()
print("events:", events.count())

# 2. Skew: key 0 holds half the rows, so one join task reads ~10M rows.
sc.setJobDescription("skewed join")
dims = spark.range(0, 5000).withColumnRenamed("id", "key").withColumn("label", F.concat(F.lit("k"), F.col("key")))
joined = events.join(dims, "key")
joined.groupBy("label").count().write.mode("overwrite").parquet(out + "/skewed_counts")

# 3. Spill: sort ~2.6 GB of rows on 1 GiB executors.
sc.setJobDescription("wide sort that spills")
(events.withColumn("payload2", F.concat("payload", "payload"))
 .repartition(16).sortWithinPartitions("payload2")
 .write.mode("overwrite").parquet(out + "/sorted"))

# 5. Driver gap: nothing runs while the driver sleeps.
time.sleep(gap)


# 4. A retried task: partition 3 fails on its first attempt only.
def flaky(rows):
    ctx = TaskContext.get()
    if ctx.partitionId() == 3 and ctx.attemptNumber() == 0:
        raise RuntimeError("sparkplain test: planned failure on first attempt")
    return rows


sc.setJobDescription("flaky task retried")
print("flaky rows:", sc.parallelize(range(80_000), 8).mapPartitions(flaky).count())

events.unpersist()
spark.stop()
