# sparkplain phase 3 test: Python workers grow past the container's memory,
# so YARN kills the executors (exit 137) and the job fails.
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("p3_memkill").getOrCreate()
sc = spark.sparkContext
sc.setJobDescription("memkill: Python workers over the container limit")

def hog(i):
    blob = b"x" * (3 * 1024 ** 3)  # 3 GiB, written, so it is resident
    return len(blob)

sc.parallelize(range(4), 4).map(hog).sum()
spark.stop()
