"""Small PySpark job that exercises what sparkplain reports on.

Run by generate.sh. It covers shuffle, caching, a skewed join, spill, a table
write and read, a job that fails on purpose, and one executor that is killed
mid-task so Spark records a lost executor. Every secret it plants is fake and
starts with FAKE- (or is AWS's documented example key ID) so tests can assert none of them
reach the report.
"""
import os
import signal
import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType

mode = sys.argv[1]          # "main", "rolling", "failed", "investigate", "excluded" or "running"
event_dir = sys.argv[2]
work = sys.argv[3]

b = (SparkSession.builder
     .master("local-cluster[3,1,1024]" if mode in ("investigate", "excluded") else "local-cluster[2,2,1024]")
     .appName({"main": "claims_enrich_fixture", "rolling": "rolling_fixture", "failed": "failing_fixture",
               "investigate": "investigate_fixture", "excluded": "excluded_fixture", "running": "running_fixture"}[mode])
     .config("spark.eventLog.enabled", "true")
     .config("spark.eventLog.dir", "file://" + event_dir)
     .config("spark.eventLog.logStageExecutorMetrics", "true")
     .config("spark.executor.processTreeMetrics.enabled", "true")
     .config("spark.executor.metrics.pollingInterval", "200ms")
     .config("spark.executor.heartbeatInterval", "2s")
     .config("spark.sql.warehouse.dir", work + "/warehouse")
     .config("spark.sql.autoBroadcastJoinThreshold", "-1")
     .config("spark.sql.adaptive.skewJoin.enabled", "false")
     .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
     .config("spark.sql.shuffle.partitions", "16")
     .config("spark.memory.fraction", "0.15")
     .config("spark.executor.memory", "1g")
     .config("spark.task.maxFailures", "3")
     # Planted fake secrets. Spark's own redaction is switched off so they
     # reach the event log and sparkplain's redaction has to catch them.
     .config("spark.redaction.regex", "(?i)nomatch-sparkplain-fixture")
     .config("spark.hadoop.fs.s3a.access.key", "AKIAIOSFODNN7EXAMPLE")
     .config("spark.hadoop.fs.s3a.secret.key", "FAKE-S3A-SECRET-0001")
     .config("spark.myapp.db.password", "FAKE-DB-PASSWORD-0002")
     .config("spark.executor.extraJavaOptions", "-Dapi.token=FAKE-TOKEN-0003 -XX:+UseG1GC")
     .config("spark.executorEnv.AWS_SECRET_ACCESS_KEY", "FAKE-AWS-SECRET-0005")
     .config("spark.myapp.jdbc.url", "jdbc:postgresql://svc:FAKE-URL-PASSWORD-0008@db.example.internal:5432/claims"))
if mode in ("investigate", "excluded"):
    # Exclude an executor after one failed task, and let the exclusion lapse
    # quickly so Spark also logs it being lifted. Speculate on stragglers.
    b = (b.config("spark.excludeOnFailure.enabled", "true")
          .config("spark.excludeOnFailure.task.maxTaskAttemptsPerExecutor", "1")
          .config("spark.excludeOnFailure.stage.maxFailedTasksPerExecutor", "1")
          .config("spark.excludeOnFailure.application.maxFailedTasksPerExecutor", "1")
          .config("spark.excludeOnFailure.timeout", "8s")
          .config("spark.task.maxFailures", "4"))
if mode == "investigate":
    # All local-cluster executors share one host, so node-level exclusion
    # would leave nowhere to run; "excluded" mode keeps it and shows that.
    b = (b.config("spark.excludeOnFailure.stage.maxFailedExecutorsPerNode", "10")
          .config("spark.excludeOnFailure.application.maxFailedExecutorsPerNode", "10")
          .config("spark.speculation", "true")
          .config("spark.speculation.interval", "100ms")
          .config("spark.speculation.multiplier", "1.5")
          .config("spark.speculation.quantile", "0.5")
          .config("spark.speculation.minTaskRuntime", "200ms")
          .config("spark.speculation.efficiency.enabled", "false")  # speculate on time alone
          .config("spark.eventLog.logBlockUpdates.enabled", "true")
          .config("spark.task.maxFailures", "4"))
if mode == "running":
    # Flush the event log often so a copy taken mid-stage holds TaskStart
    # events for tasks that have not ended yet.
    b = b.config("spark.eventLog.buffer.kb", "1k")
if mode == "rolling":
    b = (b.config("spark.eventLog.rolling.enabled", "true")
          .config("spark.eventLog.logBlockUpdates.enabled", "true")
          .config("spark.eventLog.rolling.maxFileSize", "10m")
          .config("spark.sql.shuffle.partitions", "200"))
spark = b.getOrCreate()
sc = spark.sparkContext


def kill_my_executor_once(v):
    """Kill the executor JVM that runs this Python worker, once per run."""
    marker = work + "/killed"
    if v == 7 and not os.path.exists(marker):
        open(marker, "w").close()
        daemon = os.getppid()
        with open("/proc/%d/stat" % daemon) as f:
            jvm = int(f.read().rsplit(")", 1)[1].split()[1])
        os.kill(jvm, signal.SIGKILL)
    return v


def fail_on_bad_row(v):
    if v == 13:
        raise ValueError("bad row 13 while loading, password=FAKE-PW-0006")
    return v


if mode == "rolling":
    cached = spark.range(0, 400_000, numPartitions=20).withColumn("s", F.sha2(F.col("id").cast("string"), 256)).cache()
    cached.count()
    for i in range(24):
        sc.setJobDescription("rolling batch %d" % i)
        spark.range(0, 400_000, numPartitions=200).groupBy((F.col("id") % 97).alias("k")).count().collect()
    spark.stop()
    sys.exit(0)

def fail_first_attempt(i, rows):
    from pyspark import TaskContext
    if i < 2 and TaskContext.get().attemptNumber() == 0:
        raise RuntimeError("first attempt of partition %d fails on purpose" % i)
    return rows


def straggle(i, rows):
    import time
    from pyspark import TaskContext
    time.sleep(12 if i == 0 and TaskContext.get().attemptNumber() == 0 else 0.4)
    return rows


if mode == "excluded":
    sc.setJobDescription("retries that exclude the only node")
    try:
        sc.parallelize(range(60), 6).mapPartitionsWithIndex(fail_first_attempt).count()
    except Exception as e:
        print("job aborted as planned:", type(e).__name__)
    spark.stop()
    sys.exit(0)

if mode == "investigate":
    sc.setJobGroup("nightly", "Nightly claims load")
    sc.setLocalProperty("spark.scheduler.pool", "etl")
    sc.setJobDescription("retries that exclude an executor")
    sc.parallelize(range(60), 6).mapPartitionsWithIndex(fail_first_attempt).count()
    import time
    time.sleep(10)  # past spark.excludeOnFailure.timeout, so the exclusion lapses
    sc.setJobDescription("after the exclusion lapsed")
    sc.parallelize(range(60), 6).map(lambda v: v * 2).sum()
    sc.setJobDescription("a straggler Spark speculates on")
    sc.parallelize(range(60), 6).mapPartitionsWithIndex(straggle).count()
    sc.setLocalProperty("callSite.short", "load_claims() at claims_job.py:120")
    sc.setJobDescription(None)
    spark.range(0, 1000).selectExpr("sum(id)").collect()
    sc.setLocalProperty("callSite.short", None)
    sc.setJobDescription("large task results")
    spark.range(0, 4, numPartitions=4).selectExpr("repeat('x', 3000000) AS big").collect()
    sc.setJobDescription("per-query settings")
    spark.conf.set("spark.sql.shuffle.partitions", "7")
    spark.conf.set("spark.myapp.session.token", "FAKE-SESSION-TOKEN-0009")
    spark.range(0, 10_000).groupBy((F.col("id") % 13).alias("k")).count().collect()
    sc.setJobDescription("tables")
    spark.sql("CREATE DATABASE IF NOT EXISTS inv")
    spark.range(0, 100).write.mode("overwrite").saveAsTable("inv.t1")
    spark.sql("ALTER TABLE inv.t1 SET TBLPROPERTIES ('team'='claims')")
    spark.sql("ALTER TABLE inv.t1 RENAME TO inv.t2")
    spark.sql("CACHE TABLE inv_top AS SELECT id FROM inv.t2 WHERE id < 50")
    spark.sql("SELECT count(*) FROM inv_top").collect()
    spark.sql("UNCACHE TABLE inv_top")
    spark.sql("DROP TABLE inv.t2")
    sc.setJobGroup("adhoc", "Ad-hoc check", interruptOnCancel=True)
    sc.setJobDescription("sub-query")
    spark.sql("SELECT id FROM range(100) WHERE id > (SELECT avg(id) FROM range(50))").collect()
    spark.stop()
    sys.exit(0)

if mode == "running":
    import glob
    import shutil
    import threading
    import time

    def slow(i, rows):
        time.sleep(25)
        return rows

    sc.setJobDescription("quick job")
    sc.parallelize(range(8), 2).count()
    # Three slow tasks on four slots. Spark buffers TaskStart events, so a
    # second job started mid-stage (job starts flush the log) makes the copy
    # hold tasks that have started but not ended.
    t = threading.Thread(target=lambda: (sc.setJobDescription("still running when copied"), sc.parallelize(range(6), 3).mapPartitionsWithIndex(slow).count()))
    t.start()
    time.sleep(5)
    sc.setJobDescription("flush")
    sc.parallelize(range(2), 1).count()
    time.sleep(2)
    live = glob.glob(os.path.join(event_dir, "*.inprogress"))[0]
    shutil.copy(live, os.path.join(event_dir, "snapshot"))
    t.join()
    spark.stop()
    sys.exit(0)

if mode == "failed":
    sc.setJobDescription("load claims")
    spark.range(0, 200_000, numPartitions=8).groupBy((F.col("id") % 10).alias("k")).count().collect()
    sc.setJobDescription("validate claims")
    bad = F.udf(fail_on_bad_row, LongType())
    try:
        spark.range(0, 40, numPartitions=4).select(bad("id").alias("v")).collect()
    except Exception as e:  # the app ends right after the failed job
        print("job failed as planned:", type(e).__name__)
        spark.stop()
        sys.exit(1)

# main
spark.sql("CREATE DATABASE IF NOT EXISTS claims")

sc.setJobDescription("write raw claims")
raw = (spark.range(0, 6_000_000, numPartitions=8)
       .withColumn("provider_id", F.col("id") % 500)
       .withColumn("amount", F.rand(7) * 1000)
       .withColumn("note", F.concat(F.lit("claim-"), F.col("id").cast("string"), F.lit("-"), F.sha2(F.col("id").cast("string"), 256))))
raw.write.mode("overwrite").parquet(work + "/lake/claims_raw")

sc.setJobDescription("write provider dimension")
(spark.range(0, 500).withColumn("provider_id", F.col("id"))
 .withColumn("region", F.concat(F.lit("r"), (F.col("id") % 7).cast("string")))
 .drop("id").write.mode("overwrite").saveAsTable("claims.provider_dim"))

sc.setJobDescription("cache claims")
claims = spark.read.parquet(work + "/lake/claims_raw").cache()
claims.count()

sc.setJobDescription("totals by provider (cached)")
claims.groupBy("provider_id").agg(F.sum("amount"), F.count("*")).collect()

sc.setJobDescription("sort claims (spills)")
claims.orderBy(F.col("note").desc()).write.mode("overwrite").parquet(work + "/lake/claims_sorted")

sc.setJobDescription("skewed join with providers")
skewed = claims.withColumn("provider_id", F.when(F.col("id") % 100 < 97, F.lit(0)).otherwise(F.col("provider_id")))
dim = spark.table("claims.provider_dim")
(skewed.join(dim, "provider_id").groupBy("region").agg(F.sum("amount").alias("total"))
 .write.mode("overwrite").saveAsTable("claims.region_totals"))

sc.setJobDescription("query with token=FAKE-TOKEN-0004")
spark.sql("SELECT region, total FROM claims.region_totals ORDER BY total DESC").collect()

sc.setJobDescription("executor loss")
killer = F.udf(kill_my_executor_once, LongType())
spark.range(0, 40, numPartitions=4).select(killer("id").alias("v")).groupBy().sum("v").collect()

sc.setJobDescription("validate claims")
bad = F.udf(fail_on_bad_row, LongType())
try:
    spark.range(0, 40, numPartitions=4).select(bad("id").alias("v")).collect()
except Exception as e:
    print("job failed as planned:", type(e).__name__)

claims.unpersist()
sc.setJobDescription("final count")
spark.read.parquet(work + "/lake/claims_sorted").count()
spark.stop()
