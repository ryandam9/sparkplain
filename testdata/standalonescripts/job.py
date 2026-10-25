# A small synthetic job for sparkplain's Standalone fixture: shuffle,
# cache, a forced spill, a skewed join and one deliberate task failure.
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("sparkplain_standalone_test").getOrCreate()
sc = spark.sparkContext

# 1. shuffle: a group-by over generated rows
df = spark.range(0, 400000).withColumn("k", (F.col("id") % 97)).withColumn("v", F.rand(7))
df.groupBy("k").agg(F.sum("v").alias("s")).collect()

# 2. cache and reuse
cached = df.withColumn("w", F.col("v") * 2).cache()
cached.count()
cached.filter("w > 1").count()

# 3. skewed join: most rows share one key
skew = spark.range(0, 300000).withColumn("key", F.when(F.col("id") % 10 < 9, F.lit(1)).otherwise(F.col("id") % 50))
dim = spark.range(0, 50).withColumnRenamed("id", "key").withColumn("name", F.concat(F.lit("n"), F.col("key")))
skew.join(dim, "key").groupBy("name").count().collect()

# 4. an RDD shuffle in Python
sc.parallelize(range(200000), 8).map(lambda x: (x % 1000, x)).groupByKey().mapValues(len).count()

# 5. a sort that spills in the JVM (numElementsForceSpillThreshold is low)
spark.range(0, 300000).withColumn("v", F.rand(11)).sort("v").selectExpr("sum(v)").collect()

# 6. a deliberate failure: one partition raises, the job fails, the app goes on
def bad(x):
    if x == 4242:
        raise ValueError("deliberate failure for the sparkplain fixture: bad value 4242")
    return x
try:
    sc.parallelize(range(10000), 4).map(bad).count()
except Exception as e:
    print("caught the deliberate failure:", type(e).__name__)

spark.stop()
