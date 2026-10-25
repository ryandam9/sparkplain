# A small synthetic job for sparkplain's Standalone "lost executors"
# fixture: one executor is killed from outside while its task sleeps, and
# a stage then runs its executors out of heap. The application goes on.
import time
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("sparkplain_lost_executors").getOrCreate()
sc = spark.sparkContext

# 1. a normal stage
spark.range(0, 200000).groupBy((F.col("id") % 10).alias("k")).count().collect()

# 2. long tasks: run-lost-executors.sh kills one executor's JVM while they sleep
print("long tasks start", flush=True)
sc.parallelize(range(4), 4).map(lambda x: (time.sleep(40), x)[1]).collect()

# 3. one key with tens of millions of rows, collected into one list per key:
#    the executors run out of heap and the job fails
try:
    spark.range(0, 40000000).groupBy(F.lit(1).alias("k")).agg(F.collect_list("id").alias("ids")).selectExpr("size(ids)").collect()
except Exception as e:
    print("caught the out-of-memory failure:", type(e).__name__, flush=True)

# 4. the application goes on after both
spark.range(0, 1000).count()
spark.stop()
