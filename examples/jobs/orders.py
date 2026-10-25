# A small PySpark job to try sparkplain on: it makes fake orders, then
# groups, caches, joins and sorts them, so the report has shuffles, a
# cached table and a skewed join to show. It reads and writes no files.
#
#   spark-submit --master "local[4]" orders.py
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("sparkplain_orders_example").getOrCreate()

# 1. Fake orders: 1 million rows, 200 customers, most orders from customer 1.
orders = (
    spark.range(0, 1_000_000)
    .withColumn("customer", F.when(F.col("id") % 10 < 7, F.lit(1)).otherwise(F.col("id") % 200))
    .withColumn("amount", F.round(F.rand(42) * 100, 2))
)

# 2. A shuffle: total amount per customer.
totals = orders.groupBy("customer").agg(F.sum("amount").alias("total"))
print("customers:", totals.count())

# 3. A cached table, used twice.
big = orders.filter("amount > 50").cache()
print("big orders:", big.count())
print("big orders by customer 1:", big.filter("customer = 1").count())

# 4. A skewed join: 70% of the orders have the same key.
customers = spark.range(0, 200).withColumnRenamed("id", "customer").withColumn(
    "name", F.concat(F.lit("customer-"), F.col("customer"))
)
joined = orders.join(customers, "customer").groupBy("name").count()
print("joined rows:", joined.count())

# 5. A sort over all rows.
print("top amount:", orders.sort(F.desc("amount")).first()["amount"])

spark.stop()
