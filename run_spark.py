"""Run the streaming pipeline with continuous online ML training.

This pipeline uses TRUE online learning: the model carries its weights
across batches and updates them with one small gradient-descent step
per batch. It is not "mini-batch refitting" - the model does not forget
what it learned from previous batches.

Each micro-batch:
  1. Either bootstraps weights with a full fit (cold start) or does ONE
     SGD step on the batch to update existing weights in place.
  2. Extracts the current weights and intercept as plain Python floats.
  3. Computes accuracy by applying the current weights to the batch.
  4. Prints a clean summary.

Compare to the alternative: refitting a fresh model on each batch
makes the model "forget" everything outside the current window
(catastrophic forgetting) and is sensitive to noise in any single batch.
"""
from processor import StreamProcessor
from model import OnlinePurchasePredictor
from pyspark.sql.functions import col as c
import math
import sys

TOPIC = sys.argv[1] if len(sys.argv) > 1 else "clickstream_v2"
CHECKPOINT_DIR = f"/tmp/spark-checkpoint-{TOPIC}"

# The online model is a module-level singleton. Its weights and intercept
# survive across batches - this is the whole point of true online learning.
_online_model = None


def get_online_model():
    """Return the persistent OnlinePurchasePredictor instance.

    Created lazily on first call. Subsequent calls return the same object
    with its accumulated weights.
    """
    global _online_model
    if _online_model is None:
        _online_model = OnlinePurchasePredictor(learning_rate=0.05, l2_reg=0.01)
    return _online_model


def extract_feature_importance(model):
    """Extract weights and intercept as a dict {feature_name: value}.

    Weights are simple Python floats - safe to pass around.
    """
    try:
        feature_names = model.assembler.getInputCols()
        result = {name: float(model.weights[i]) for i, name in enumerate(feature_names)}
        result["__intercept__"] = float(model.intercept)
        return result
    except Exception as e:
        return {"error": str(e)}


def compute_training_accuracy(model, batch_df):
    """Apply the current weights to the batch and measure accuracy.

    Uses a Spark UDF to apply the current weights and intercept on the
    executor side, then compares predictions to labels in SQL. No
    driver-side collect, no nested SparkContext.
    """
    try:
        if model.weights is None:
            return None

        from pyspark.sql.functions import udf, col as c, sum as smax
        from pyspark.sql.types import DoubleType

        # Snapshot weights and intercept for the UDF (must be picklable)
        weights_list = list(model.weights)
        intercept_val = float(model.intercept)

        def predict(features):
            # features is a pyspark.ml.linalg.Vector
            z = intercept_val + sum(weights_list[i] * features[i] for i in range(len(weights_list)))
            # Sigmoid
            if z >= 0:
                p = 1.0 / (1.0 + 2.718281828 ** (-z))
            else:
                ez = 2.718281828 ** z
                p = ez / (1.0 + ez)
            return float(1.0 if p >= 0.5 else 0.0)

        predict_udf = udf(predict, DoubleType())

        featurized = model.featurize(batch_df)
        assembled = model.assembler.transform(featurized)
        predictions = assembled.withColumn(
            "pred", predict_udf(c("features"))
        )

        result = predictions.withColumn(
            "is_correct", (c("pred") == c("label")).cast("int")
        ).agg(smax("is_correct").alias("correct")).collect()[0]

        correct = result["correct"] or 0
        total = predictions.count()
        return correct / total if total > 0 else 0.0
    except Exception as e:
        print(f"  accuracy error: {e}")
        return None


def process_batch(batch_df, batch_id):
    """Called by Spark for each micro-batch - trains a fresh model."""
    try:
        event_count = batch_df.count()
        if event_count == 0:
            print(f"Batch {batch_id}: empty, skipping")
            return

        # Check that we have both positive and negative labels.
        # LogisticRegression crashes on a batch with no purchases (no positive class).
        from pyspark.sql.functions import col as c, sum as smax
        label_counts = (
            batch_df
            .withColumn("label", (c("action") == "purchase").cast("int"))
            .groupBy("label")
            .count()
            .collect()
        )
        label_map = {row["label"]: row["count"] for row in label_counts}
        positives = label_map.get(1, 0)
        negatives = label_map.get(0, 0)
        if positives == 0 or negatives == 0:
            print(f"Batch {batch_id}: skipped (only one class present: "
                  f"positives={positives}, negatives={negatives}, total={event_count})")
            return

        # 1) Update the persistent online model with one SGD step on this batch.
        # The model is a module-level singleton so its weights survive across batches.
        model = get_online_model()
        model.train_on_batch(batch_df)

        # 2) Gather metrics - all simple Python values, safe to print
        importance = extract_feature_importance(model)
        accuracy = compute_training_accuracy(model, batch_df)

        # 3) Print a clean summary
        print(f"\n{'=' * 60}")
        print(f"Batch {batch_id}  |  events: {event_count}  |  model update #{model.update_count}")
        print(f"{'=' * 60}")
        if accuracy is not None:
            print(f"Accuracy on this batch: {accuracy:.3f}")
        else:
            print(f"Accuracy: could not compute")
        print(f"Current weights (carried across batches, updated by one SGD step per batch):")
        
        if "error" not in importance:
            intercept = importance.pop("__intercept__", None)
            for feature, weight in importance.items():
                sign = "+" if weight >= 0 else ""
                bar_len = min(int(abs(weight) * 10), 30)
                bar = "#" * bar_len
                print(f"  {feature:25s} {sign}{weight:.4f}  {bar}")
            if intercept is not None:
                import math
                baseline_p = 1.0 / (1.0 + math.exp(-intercept))
                print(f"  {'__intercept__':25s} {intercept:+.4f}  (baseline p={baseline_p:.3f})")
        else:
            print(f"  Error: {importance['error']}")
    except Exception as e:
        print(f"Batch {batch_id} failed: {type(e).__name__}: {e}")


if __name__ == "__main__":
    p = StreamProcessor()
    raw_df = p.read_stream(topic=TOPIC)
    parsed_df = p.parse_events(raw_df)

    query = parsed_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()