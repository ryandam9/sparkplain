# sparkplain phase 3 test: a steady job that runs alongside another, so
# CloudWatch shows two applications at once and busy hosts.
import time
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("p3_busy").getOrCreate()
sc = spark.sparkContext
sc.setJobDescription("busy: shuffle-heavy aggregation")
df = spark.range(0, 200_000_000, numPartitions=64).withColumn("k", F.col("id") % 100_000)
df.groupBy("k").agg(F.sum("id"), F.countDistinct(F.col("id") % 977)).count()
sc.setJobDescription("busy: CPU-bound tasks")
sc.parallelize(range(32), 32).map(lambda i: sum(x * x for x in range(3_000_000))).sum()
time.sleep(30)
spark.stop()
