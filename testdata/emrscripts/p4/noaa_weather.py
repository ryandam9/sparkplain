"""Climate summary from NOAA GHCN-Daily (AWS Open Data), a realistic PySpark ETL.

Usage: spark-submit noaa_weather.py <output s3 prefix>
Reads two years of daily observations (about 2.6 GB of CSV), cleans them,
joins station metadata, and writes monthly summaries per country.
"""
import sys

from pyspark.sql import SparkSession, Window, functions as F

out = sys.argv[1].rstrip("/")
spark = (SparkSession.builder.appName("noaa_climate_summary")
         .enableHiveSupport().getOrCreate())
sc = spark.sparkContext

# 1. Observations: one row per station, day and element (TMAX, TMIN, PRCP, ...).
sc.setJobDescription("load observations")
obs = (spark.read.option("header", "true")
       .csv(["s3://noaa-ghcn-pds/csv/by_year/2024.csv", "s3://noaa-ghcn-pds/csv/by_year/2025.csv"]))

clean = (obs.where(F.col("Q_FLAG").isNull())                       # drop observations that failed QC
         .where(F.col("ELEMENT").isin("TMAX", "TMIN", "PRCP"))
         .select(F.col("ID").alias("station"),
                 F.to_date("DATE", "yyyyMMdd").alias("day"),
                 "ELEMENT",
                 (F.col("DATA_VALUE").cast("double") / 10).alias("value")))

# 2. One row per station-day with max/min temperature and precipitation.
daily = (clean.groupBy("station", "day").pivot("ELEMENT", ["TMAX", "TMIN", "PRCP"]).agg(F.first("value"))
         .withColumnRenamed("TMAX", "tmax_c").withColumnRenamed("TMIN", "tmin_c").withColumnRenamed("PRCP", "prcp_mm"))
daily.cache()
sc.setJobDescription("count station-days")
print("station-days:", daily.count())

# 3. Station metadata: a fixed-width text file.
sc.setJobDescription("load stations")
raw = spark.read.text("s3://noaa-ghcn-pds/ghcnd-stations.txt")
stations = raw.select(
    F.trim(F.substring("value", 1, 11)).alias("station"),
    F.substring("value", 1, 2).alias("country"),
    F.trim(F.substring("value", 13, 8)).cast("double").alias("lat"),
    F.trim(F.substring("value", 22, 9)).cast("double").alias("lon"),
    F.trim(F.substring("value", 32, 6)).cast("double").alias("elevation_m"),
    F.trim(F.substring("value", 42, 30)).alias("name"))

# 4. Join and aggregate: the US has far more stations than anywhere else,
# so this join is skewed towards one country.
spark.conf.set("spark.sql.shuffle.partitions", "64")
enriched = daily.join(stations, "station")
monthly = (enriched.withColumn("month", F.date_trunc("month", "day"))
           .groupBy("country", "month")
           .agg(F.avg("tmax_c").alias("avg_tmax_c"), F.avg("tmin_c").alias("avg_tmin_c"),
                F.sum("prcp_mm").alias("total_prcp_mm"), F.countDistinct("station").alias("stations")))

sc.setJobDescription("write monthly summary")
(monthly.withColumn("year", F.year("month"))
 .write.mode("overwrite").partitionBy("year").parquet(out + "/monthly_by_country"))

# 5. The hottest stations per country, ranked with a window.
sc.setJobDescription("rank hottest stations")
per_station = enriched.groupBy("country", "station", "name").agg(F.max("tmax_c").alias("max_tmax_c"))
w = Window.partitionBy("country").orderBy(F.desc("max_tmax_c"))
hottest = per_station.withColumn("rank", F.rank().over(w)).where("rank <= 3")
hottest.write.mode("overwrite").parquet(out + "/hottest_stations")

# 6. Read it back through Spark SQL.
sc.setJobDescription("query the summary")
spark.read.parquet(out + "/monthly_by_country").createOrReplaceTempView("monthly")
top = spark.sql("""
    SELECT country, SUM(total_prcp_mm) AS prcp_mm, AVG(avg_tmax_c) AS avg_tmax_c
    FROM monthly WHERE year = 2025
    GROUP BY country ORDER BY prcp_mm DESC LIMIT 10""").collect()
for r in top:
    print(r)
daily.unpersist()
spark.stop()
