from processor import StreamProcessor

CHECKPOINT_DIR = "/tmp/spark-checkpoint-clickstream-v2"

p = StreamProcessor()
raw_df = p.read_stream()
parsed_df = p.parse_events(raw_df)
agg_df = p.aggregate_purchases(parsed_df)

query = agg_df.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .start()

query.awaitTermination()