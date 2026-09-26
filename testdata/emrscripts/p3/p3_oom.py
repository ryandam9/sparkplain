# sparkplain phase 3 test: one task gathers 60 million longs into a list,
# past a 1 GiB heap, so the executor throws OutOfMemoryError.
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("p3_oom").getOrCreate()
spark.sparkContext.setJobDescription("oom: collect_list into one task")
spark.range(0, 60_000_000, 1, 1).selectExpr("size(collect_list(id)) as n").collect()
spark.stop()
