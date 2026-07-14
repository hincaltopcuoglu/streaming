"""
Spark Structured Streaming job with online (true) learning.
Connects to Kafka, consumes JSON events, computes windowed features,
and updates a logistic-regression model with one SGD step per batch.
Writes the snapshot to Redis so the API can read it.
Why not import api.state directly?
    The Spark driver already runs Python; importing additional C-extension
    libraries there can occasionally clash with PySpark's setup. We use
    a tiny CLI wrapper (python -m api.cli save_state) to write to Redis.
"""
import json
import os
import subprocess
import sys
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, max, min, when, lit, rand
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "clickstream_v2")

LEARNING_RATE = float(os.getenv("LEARNING_RATE", "0.05"))
L2_REG = float(os.getenv("L2_REG","0.01"))

FEATURE_NAMES = ["clicks_in_session", "time_on_page"]

# ---- Model state: weights, intercept, counts ----
weights = None
intercept = 0.0
update_count = 0

def save_to_redis(w, b, meta):
    """Call the api.cli subprocess to write the snapshot"""
    payload = json.dumps({"w":w, "b":b, "meta": meta})
    subprocess.run(
        [
            sys.executable, "-m", "api.cli", "save_state",
            "--weights-json", json.dumps(w),
            "--intercept", str(b),
            "--meta-json", json.dumps(meta),
        ],
        check=True
    )


def sigmoid(z):
    if z >= 0:
        return 1.0 / (1.0 + math.exp(-z))
    return math.exp(z) / (1.0 + math.exp(z))

# SparkSession and parse
spark = SparkSession.builder \
    .appName("ClickStreamOnlineLearner") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("url", StringType()),
    StructField("action", StringType()),
    StructField("session_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("amount", DoubleType()),
])

def featurize(df):
    df_w  = df.withColumn("event_window", window(col("timestamp"), "5 minutes"))
    agg = df_w.groupBy("event_window", "session_id").agg(
        count("*").alias("clicks_in_session"),
        (max(col("timestamp").cast("long")) - min(col("timestamp").cast("long"))).alias("time_on_page"),
    )
    enriched = df_w.join(agg, on=["event_window", "session_id"], how="left")
    return enriched.withColumn(
        "label",
        when(col("action") == "purchase", lit(1)).otherwise(lit(0)),
    )


def process_batch(df, batch_id):
    """Called for each micro-batch. Train, update, save."""
    global weights, intercept, update_count

    count_events = df.count()
    if count_events == 0:
        print(f"Batch {batch_id}: empty, skipping")
        return
    
    label_counts = (
        df.withColumn("label", (col("action") == "purchase").cast("int"))
        .groupBy("label").count().collect()
    )
    label_map = {row["label"]: row["count"] for row in label_counts}
    if label_map.get(1,0) == 0 or label_map.get(0,0) == 0:
        print(f"Batch {batch_id}: skipped (only one class)")
        return
    
    enriched = featurize(df)
    assembler = VectorAssembler(inputCols=FEATURE_NAMES, outputCol="features")
    assembled = assembler.transform(enriched)

    # cold start
    if weights is None:
        lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)
        model = lr.fit(assembled)
        weights = [float(c) for c in model.coefficients]
        intercept = float(model.interceptVector[0])
        update_count += 1
    else:
        # sample batch, do omne SGD step
        sampled = assembled.select("features", "label").orderBy(rand()).limit(5000).collect()
        rows = sampled
        n = len(rows)
        if n > 0:
            grad_w = [0.0] * len(weights)
            grad_b = 0.0
            for r in rows:
                x = list(r.features)
                y = float(r.label)
                z = intercept + sum(weights[i] * x[i] for i in range(len(weights)))
                p = sigmoid(z)
                err = p - y
                for i in range(len(weights)):
                    grad_w[i] += err * x[i]
                grad_b += err
            for i in range(len(weights)):
                grad = grad_w[i] / n + L2_REG * weights[i]
                weights[i] -= LEARNING_RATE * grad
            intercept -= LEARNING_RATE * (grad_b / n)
            update_count += 1


    meta = {"update_count": update_count, "last_batch_size": count_events}
    save_to_redis(weights, intercept, meta)
    print(f"Batch {batch_id} | events={count_events} | update #{update_count}")
    print(f"  weights: {dict(zip(FEATURE_NAMES, weights))}")
    print(f"  intercept: {intercept:+.4f}")

raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

parsed = raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

query = parsed.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
