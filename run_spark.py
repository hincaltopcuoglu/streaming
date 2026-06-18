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

CHECKPOINT_DIR = "/tmp/spark-checkpoint-ml-v2"


def extract_feature_importance(model):
    """Extract logistic regression coefficients as a dict {feature: weight}.

    Coefficients are simple Python floats - safe to pass around.
    """
    try:
        coeffs = model.model.coefficients
        feature_names = model.assembler.getInputCols()
        return {name: float(coeffs[i]) for i, name in enumerate(feature_names)}
    except Exception as e:
        return {"error": str(e)}


def compute_training_accuracy(model, batch_df):
    """Compute accuracy using a SQL-only approach (no model.transform).

    Trick: we compute the linear predictor manually from coefficients,
    apply sigmoid, then compare to label. Pure Python, no Spark closures.
    """
    try:
        # Get coefficients as a dict
        coeffs_dict = extract_feature_importance(model)
        if "error" in coeffs_dict:
            return None

        # Get the intercept
        intercept = float(model.model.interceptVector[0])

        # Collect the batch to driver as plain Python (small batches only)
        featurized = model.featurize(batch_df)
        rows = featurized.select(
            "clicks_in_session", "time_on_page", "label"
        ).collect()

        if not rows:
            return None

        correct = 0
        for r in rows:
            # Manual logistic regression: P(y=1) = sigmoid(intercept + sum(coef * x))
            z = intercept
            for feat_name, coef in coeffs_dict.items():
                z += coef * getattr(r, feat_name)
            # Sigmoid
            prob = 1.0 / (1.0 + pow(2.71828, -z))
            pred = 1 if prob >= 0.5 else 0
            if pred == r.label:
                correct += 1

        return correct / len(rows)
    except Exception as e:
        return None


def process_batch(batch_df, batch_id):
    """Called by Spark for each micro-batch - trains a fresh model."""
    try:
        # 1) Train fresh model on this batch
        model = OnlinePurchasePredictor()
        model.train_on_batch(batch_df)

        # 2) Gather metrics - all simple Python values, safe to print
        event_count = batch_df.count()
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
            for feature, weight in importance.items():
                sign = "+" if weight >= 0 else ""
                bar_len = min(int(abs(weight) * 10), 30)
                bar = "#" * bar_len
                print(f"  {feature:25s} {sign}{weight:.4f}  {bar}")
        else:
            print(f"  Error: {importance['error']}")
    except Exception as e:
        print(f"Batch {batch_id} failed: {type(e).__name__}: {e}")


if __name__ == "__main__":
    p = StreamProcessor()
    raw_df = p.read_stream()
    parsed_df = p.parse_events(raw_df)

    query = parsed_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()