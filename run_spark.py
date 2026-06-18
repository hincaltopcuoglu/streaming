"""Run the streaming pipeline with continuous online ML training.

Each micro-batch:
  1. Trains a fresh logistic regression model on the parsed events
  2. Extracts coefficients (feature importances) - safe Python values
  3. Computes model accuracy on the training batch via Spark SQL only
  4. Prints a clean summary
"""
from processor import StreamProcessor
from model import OnlinePurchasePredictor
from pyspark.sql.functions import col
import sys

TOPIC = sys.argv[1] if len(sys.argv) > 1 else "clickstream_v2"
CHECKPOINT_DIR = f"/tmp/spark-checkpoint-{TOPIC}"


def extract_feature_importance(model):
    """Extract logistic regression coefficients and intercept as a dict.

    Coefficients and intercept are simple Python floats - safe to pass around.
    """
    try:
        coeffs = model.model.coefficients
        feature_names = model.assembler.getInputCols()
        result = {name: float(coeffs[i]) for i, name in enumerate(feature_names)}
        # Add the intercept - it captures the class baseline probability.
        # If coefficients are all 0, the model only uses the intercept.
        result["__intercept__"] = float(model.model.interceptVector[0])
        return result
    except Exception as e:
        return {"error": str(e)}


def compute_training_accuracy(model, batch_df):
    """Compute accuracy using Spark SQL transforms only (no driver-side collect)."""
    try:
        featurized = model.featurize(batch_df)
        assembled = model.assembler.transform(featurized)
        predictions = model.model.transform(assembled)

        from pyspark.sql.functions import col as c, sum as smax
        result = predictions.withColumn(
            "is_correct", (c("prediction") == c("label")).cast("int")
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

        # 1) Train fresh model on this batch
        model = OnlinePurchasePredictor()
        model.train_on_batch(batch_df)

        # 2) Gather metrics - all simple Python values, safe to print
        importance = extract_feature_importance(model)
        accuracy = compute_training_accuracy(model, batch_df)

        # 3) Print a clean summary
        print(f"\n{'=' * 60}")
        print(f"Batch {batch_id}  |  events: {event_count}")
        print(f"{'=' * 60}")
        if accuracy is not None:
            print(f"Accuracy on this batch: {accuracy:.3f}")
        else:
            print(f"Accuracy: could not compute")
        print(f"Feature importances (coefficients):")
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