from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType
from pyspark.sql.functions import sum, count

class StreamProcessor:
    def __init__(self, app_name = "ClickStreamProcessor"):
        self.app_name = app_name
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2") \
            .getOrCreate()

    def read_stream(self):
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "clickstream_v2") \
            .option("startingOffsets", "earliest") \
            .load()

    def parse_events(self, df):

        schema = StructType([
            StructField("user_id", IntegerType()),
            StructField("url", StringType()),
            StructField("action", StringType()),
            StructField("session_id", StringType()),
            StructField("timestamp", LongType()),
            StructField("amount", DoubleType()),
            ])

        return (
            df
            .selectExpr("CAST(value AS STRING) as json_str")
            .select(from_json(col("json_str"), schema).alias("data"))
            .select("data.*")
        )


    def aggregate_purchases(self, df):
        return (
            df.filter(df.action == "purchase")
            .groupBy("user_id")
            .agg(
                count("*").alias("purchase_count"),
                sum("amount").alias("total_spent")
            )
        )


    def stop(self):
        self.spark.stop()