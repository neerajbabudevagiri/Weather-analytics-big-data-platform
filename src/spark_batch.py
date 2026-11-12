from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("WeatherBatch")
         .master("local[*]").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

df = spark.read.csv("data/raw/*.csv", header=True, inferSchema=True)
df = (df.withColumn("ts", F.to_timestamp("time"))
        .withColumn("year", F.year("ts"))
        .withColumn("month", F.month("ts"))
        .dropna())

print("Total rows:", df.count())

monthly = df.groupBy("city", "year", "month").agg(
    F.round(F.avg("temperature"), 2).alias("avg_temp"),
    F.round(F.max("temperature"), 2).alias("max_temp"),
    F.round(F.sum("rainfall"), 2).alias("total_rain"),
    F.round(F.avg("humidity"), 2).alias("avg_humidity"),
    F.round(F.avg("wind_speed"), 2).alias("avg_wind"))

yearly = df.groupBy("city", "year").agg(
    F.round(F.avg("temperature"), 2).alias("avg_temp"),
    F.round(F.sum("rainfall"), 1).alias("total_rain"))

extremes = df.groupBy("city").agg(
    F.max("temperature").alias("record_high"),
    F.min("temperature").alias("record_low"),
    F.max("wind_speed").alias("max_wind"))

monthly.write.mode("overwrite").parquet("output/batch/monthly")
yearly.write.mode("overwrite").parquet("output/batch/yearly")
extremes.write.mode("overwrite").parquet("output/batch/extremes")

extremes.show()
print("Batch job finished")
spark.stop()