from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import window, col, count, max, min, when, lit

_global_model = None


def get_model():
    """Module-level singleton accessor for the online model.

    Spark's foreachBatch can run in a different execution context than the
    main script. Using a module-level variable ensures the trained model
    persists across batches.
    """
    global _global_model
    if _global_model is None:
        _global_model = OnlinePurchasePredictor()
    return _global_model


class OnlinePurchasePredictor:
    """An online ML model that trains on streaming click events.

    The model is a logistic regression that predicts whether a click
    event will lead to a purchase, based on features like clicks_in_session.
    """

    def __init__(self, learning_rate=0.05, l2_reg=0.01):
        # learning_rate: step size for the gradient descent update.
        # l2_reg:        weight decay coefficient (regularization).
        # Both are kept small so each batch only nudges the weights a little.
        self.learning_rate = learning_rate
        self.l2_reg = l2_reg
        # Weights stored as plain Python lists. Initialized only after the
        # first full fit. This is the "state" that survives across batches
        # and is the whole point of true online learning.
        self.weights = None      # list of feature coefficients
        self.intercept = 0.0     # bias term
        # How many batches the model has been updated on. Incremented each
        # call to train_on_batch. Visible to the user so they can see the
        # model is accumulating knowledge, not refitting.
        self.update_count = 0
        self.assembler = VectorAssembler(
            inputCols=["clicks_in_session", "time_on_page"],
            outputCol="features"
        )
        # Used only for the first batch (cold start) when we have no weights yet.
        self.lr = LogisticRegression(
            featuresCol="features",
            labelCol="label",
            maxIter=10
        )
        # The fitted model used to extract coefficients/accuracy.
        # Kept around only so callers can use .transform() for accuracy.
        self.model = None



    def featurize(self, df):
        # Step 1: Add a window column to the events
        df_with_window = df.withColumn(
            "event_window",
            window(col("timestamp"), "5 minutes")
        )

        # Step 2: Compute per-session aggregates within each window
        session_stats = df_with_window.groupBy("event_window", "session_id").agg(
            count("*").alias("clicks_in_session"),
            (max(col("timestamp").cast("long")) - min(col("timestamp").cast("long"))).alias("time_on_page")
        )

        # Step 3: Join back using both window AND session_id
        enriched = df_with_window.join(
            session_stats,
            on=["event_window", "session_id"],
            how="left"
        )

        # Step 4: Add label
        enriched = enriched.withColumn(
            "label",
            when(col("action") == "purchase", lit(1)).otherwise(lit(0))
        )

        return enriched


    def _sigmoid(self, z):
        # Numerically stable sigmoid
        if z >= 0:
            return 1.0 / (1.0 + 2.718281828 ** (-z))
        else:
            ez = 2.718281828 ** z
            return ez / (1.0 + ez)


    def train_on_batch(self, df):
        """Online learning: update weights with one SGD step on the batch.

        First call: do a full LogisticRegression fit (cold start, no priors).
        Subsequent calls: do exactly ONE gradient descent step on the batch
        and update self.weights / self.intercept in place. This means the
        model carries knowledge across batches and only adjusts a little
        per batch - the defining property of online learning.

        The gradient for logistic regression is:
            grad_w = (1/n) * sum( (p - y) * x_i ) + l2_reg * w
            grad_b = (1/n) * sum( (p - y) )
        where p = sigmoid(w . x + b) and y is the true label.
        """
        featurized = self.featurize(df)
        assembled = self.assembler.transform(featurized)

        # Cold start: no existing weights -> do a full fit to bootstrap.
        if self.weights is None:
            self.model = self.lr.fit(assembled)
            self.weights = list(self.model.coefficients)
            self.intercept = float(self.model.interceptVector[0])
            self.update_count += 1
            return

        self.update_count += 1

        # Online update: one SGD step on the batch.
        # Pull features and labels as plain Python lists. With sampling this
        # is cheap; for very large batches we sample to keep the driver small.
        from pyspark.sql.functions import col as c, rand
        sampled = assembled.select("features", "label").orderBy(rand()).limit(5000)
        rows = sampled.collect()

        if not rows:
            return

        n = len(rows)
        # Accumulate gradients
        grad_w = [0.0] * len(self.weights)
        grad_b = 0.0
        for r in rows:
            x = list(r.features)         # list of feature values
            y = float(r.label)
            z = self.intercept + sum(self.weights[i] * x[i] for i in range(len(self.weights)))
            p = self._sigmoid(z)
            err = p - y
            for i in range(len(self.weights)):
                grad_w[i] += err * x[i]
            grad_b += err

        # Average + L2 regularization, then step
        for i in range(len(self.weights)):
            grad = grad_w[i] / n + self.l2_reg * self.weights[i]
            self.weights[i] -= self.learning_rate * grad
        self.intercept -= self.learning_rate * (grad_b / n)


    def predict_proba(self, clicks_in_session):
        """Predict probability of purchase given clicks_in_session."""
        if self.model is None:
            return 0.5
        import pandas as pd
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        pdf = pd.DataFrame([{"clicks_in_session": clicks_in_session}])
        sdf = spark.createDataFrame(pdf)
        sdf = self.assembler.transform(sdf)
        result = self.model.transform(sdf).select("probability").collect()[0][0]
        return float(result[1])