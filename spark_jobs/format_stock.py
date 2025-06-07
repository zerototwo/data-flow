from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType

spark = SparkSession.builder.appName("FormatStock").getOrCreate()

schema = StructType() \
    .add("symbol", StringType()) \
    .add("timestamp", StringType()) \
    .add("open", FloatType()) \
    .add("high", FloatType()) \
    .add("low", FloatType()) \
    .add("close", FloatType()) \
    .add("volume", IntegerType())

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "raw_stock") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

json_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "formatted_stock") \
    .option("checkpointLocation", "/tmp/ck_format_stock") \
    .start().awaitTermination()