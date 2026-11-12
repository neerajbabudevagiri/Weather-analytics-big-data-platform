from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = (SparkSession.builder.appName("WeatherStream")
    .master("local[*]")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("city", StringType()),
    StructField("timestamp", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("rainfall", DoubleType()),
    StructField("wind_speed", DoubleType()),
])

raw = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribe", "weather-events")
    .option("startingOffsets", "latest")
    .load())

events = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
    .select("e.*")
    .withColumn("ts", F.to_timestamp("timestamp")))

agg = (events.withWatermark("ts", "30 seconds")
    .groupBy(F.window("ts", "30 seconds"), "city")
    .agg(F.round(F.avg("temperature"), 2).alias("avg_temp"),
         F.round(F.avg("humidity"), 2).alias("avg_humidity"),
         F.round(F.sum("rainfall"), 2).alias("rain"),
         F.round(F.max("wind_speed"), 1).alias("max_wind"))
    .select(F.col("window.start").alias("window_start"),
            "city", "avg_temp", "avg_humidity", "rain", "max_wind"))

query = (agg.writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", "output/stream/data")
    .option("checkpointLocation", "output/stream/_chk")
    .trigger(processingTime="10 seconds")
    .start())

print("Streaming started. Press Ctrl+C to stop.")
query.awaitTermination()