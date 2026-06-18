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

    def __init__(self):
        self.model = None
        self.assembler = VectorAssembler(
            inputCols=["clicks_in_session"],
            outputCol="features"
        )
        self.lr = LogisticRegression(
            featuresCol="features",
            labelCol="label",
            maxIter=10
        )

    

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


    def train_on_batch(self, df):
        """Train (or update) the model on a micro-batch of events."""
        featurized = self.featurize(df)
        assembled = self.assembler.transform(featurized)
        self.model = self.lr.fit(assembled)


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