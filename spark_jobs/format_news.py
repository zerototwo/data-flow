from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

spark = SparkSession.builder.appName("FormatNews").getOrCreate()

schema = StructType() \
    .add("title", StringType()) \
    .add("publishedAt", StringType()) \
    .add("description", StringType()) \
    .add("source", StringType()) \
    .add("url", StringType())

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "raw_news") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

json_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "formatted_news") \
    .option("checkpointLocation", "/tmp/ck_format_news") \
    .start().awaitTermination()