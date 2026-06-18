# ClickStream Streaming Pipeline

A complete **streaming data processing pipeline** built with Kafka, PySpark, and online machine learning. Events flow in real time from a Python producer through Kafka, get processed by Spark Structured Streaming, and feed an online logistic regression model that updates with every batch.

This project was built as a hands-on learning exercise covering **Python OOP**, **event streaming**, **distributed processing**, and **online ML**.

---

## Architecture

```
┌──────────────┐    ┌──────────────┐    ┌──────────────────┐
│  Producer    │───▶│ Kafka Topic  │───▶│ Spark Streaming  │
│  (Python)    │    │ clickstream  |  + |   foreachBatch   │
└──────────────┘    └──────────────┘    └────────┬─────────┘
                                                 │
                                                 ▼
                                    ┌────────────────────────┐
                                    │ Online ML Pipeline     │
                                    │ - Windowed features    │
                                    │ - Logistic regression  │
                                    │ - Per-batch metrics    │
                                    └────────────────────────┘
```

Each micro-batch triggers the full pipeline: Spark reads events from Kafka, parses JSON into typed columns, computes windowed session-level features, and retrains a logistic regression model. Coefficients and accuracy are printed continuously.

---

## Project Structure

| File | Purpose |
|---|---|
| `models.py` | Event blueprints: `ClickEvent`, `PurchaseEvent` (uses inheritance + polymorphism) |
| `factory.py` | `EventFactory.from_dict()` — reconstructs typed objects from raw dicts |
| `producer.py` | `EventProducer` — serializes events and sends them to Kafka |
| `consumer.py` | `EventConsumer` — reads events from Kafka, optionally returns typed objects |
| `processor.py` | `StreamProcessor` — Spark session, reads Kafka stream, parses JSON into typed DataFrame columns, exposes aggregations |
| `model.py` | `OnlinePurchasePredictor` — logistic regression trained per batch with windowed session features |
| `sent_events.py` | Test script: sends a mix of clicks and purchases to Kafka |
| `run_spark.py` | Main entry point — wires the Spark stream into the model with `foreachBatch` and prints live metrics |
| `docker-compose.yml` | Kafka and Zookeeper containers |
| `.gitignore` | Excludes `venv/`, `__pycache__/`, etc. |

---

## Prerequisites

- **macOS** (Apple Silicon or Intel)
- **Docker Desktop** running
- **Homebrew**
- **Python 3.14+** (project uses a venv)
- **Java 17** (required by Spark 3.5.x)

---

## Setup

### 1. Install Java 17

```bash
brew install openjdk@17
sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk
```

(Use `/usr/local/opt/openjdk@17/...` on Intel Macs.)

### 2. Create the virtual environment

```bash
cd /Users/hincaltopcuoglu/Desktop/streaming
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Python dependencies

```bash
pip install --upgrade pip
pip install kafka-python pyspark==3.5.1 setuptools
```

> `setuptools` is needed because PySpark 3.5.1 imports `distutils`, which was removed from Python 3.12+. `setuptools` provides a vendored `distutils` shim.

### 4. Start Kafka and Zookeeper

```bash
docker-compose up -d
docker ps  # confirm both containers are running
```

### 5. Create the Kafka topic

```bash
docker exec -it $(docker ps --format '{{.Names}}' | grep kafka) \
    kafka-topics --create --topic clickstream_v2 \
    --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

---

## Running the Pipeline

You need **two terminals**.

### Terminal 1 — Start the Spark streaming query

```bash
cd /Users/hincaltopcuoglu/Desktop/streaming
source venv/bin/activate
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
python3 run_spark.py
```

Spark will start, download the Kafka connector on first run (~50MB), and begin polling Kafka every 10 seconds. The first batch processes any historical events already in the topic.

### Terminal 2 — Send events continuously

```bash
cd /Users/hincaltopcuoglu/Desktop/streaming
source venv/bin/activate
python3 sent_events.py     # send one round (8 events)
```

Or run a continuous loop:

```bash
while true; do
    python3 sent_events.py
    sleep 2
done
```

---

## Sample Output

```
============================================================
Batch 0  |  events: 79
============================================================
Accuracy on this batch: 0.781
Feature importances (coefficients):
  clicks_in_session          +0.8328  ########
  time_on_page               -0.0123  #

============================================================
Batch 1  |  events: 24
============================================================
Accuracy on this batch: 0.750
Feature importances (coefficients):
  clicks_in_session          +0.7821  #######
  time_on_page               +0.0098
```

The model retrains on every batch, and the coefficients update to reflect the patterns in the most recent events. That is online learning in action.

---

## How the Pieces Fit Together

### Event Models (`models.py`)

`ClickEvent` and `PurchaseEvent` use OOP inheritance. `PurchaseEvent` extends `ClickEvent` and adds an `amount` field. Both override `event_type()` for polymorphic identification.

```python
class PurchaseEvent(ClickEvent):
    def __init__(self, user_id, url, action, session_id, timestamp, amount):
        super().__init__(user_id, url, action, session_id, timestamp)
        self.amount = amount

    def event_type(self):
        return "purchase"
```

### Producer (`producer.py`)

Wraps `kafka-python`'s `KafkaProducer`. Serializes events via `to_dict()` → JSON → UTF-8 bytes before sending.

### Consumer (`consumer.py`)

Wraps `KafkaConsumer`. Decodes bytes → string → dict. Optionally uses `EventFactory.from_dict()` to rebuild typed event objects.

### Spark Processor (`processor.py`)

Three methods:

| Method | What it does |
|---|---|
| `read_stream()` | Connects to Kafka as a streaming DataFrame with `startingOffsets=earliest` |
| `parse_events(df)` | Casts JSON bytes → string → parsed struct → typed columns using `from_json()` with a schema |
| `aggregate_purchases(df)` | Filters to purchase events, groups by user, returns `count` + `sum(amount)` |

### Online Model (`model.py`)

`OnlinePurchasePredictor` with:

- `featurize(df)` — adds `clicks_in_session` and `time_on_page` features using **5-minute tumbling windows** grouped by `session_id`, then joins back per event
- `train_on_batch(df)` — fits a fresh `LogisticRegression` on the batch

The windowed aggregation makes the features meaningful: each event's `clicks_in_session` reflects how many events the user produced in the same 5-minute window, not a fixed random number.

### Main Pipeline (`run_spark.py`)

Wires everything together with `foreachBatch`. Each micro-batch:

1. Trains a fresh logistic regression model
2. Extracts feature coefficients (as plain Python floats)
3. Computes accuracy using a manual sigmoid (avoids Spark serialization issues inside `foreachBatch`)
4. Prints a clean summary with visual bars for coefficient magnitudes

The `trigger(processingTime="10 seconds")` setting means Spark waits up to 10 seconds per batch, accumulating more events for meaningful training.

---

## OOP Concepts Demonstrated

| Concept | Where |
|---|---|
| Classes and objects | All model and pipeline classes |
| `__init__` and `self` | Constructors throughout |
| Methods | `to_dict()`, `is_purchase()`, `event_type()`, `send()`, `poll_one()`, etc. |
| Inheritance | `PurchaseEvent(ClickEvent)` with `super().__init__()` |
| Method overriding | `event_type()`, `to_dict()`, `is_purchase()` redefined in `PurchaseEvent` |
| Static methods | `EventFactory.from_dict()` |
| Polymorphism | Same `from_dict` call returns different object types based on data |

---

## Streaming Concepts Demonstrated

| Concept | Where |
|---|---|
| Event-driven architecture | Producer → Kafka → Consumer pattern |
| Topics and partitions | `clickstream_v2` topic |
| Serialization | JSON encoding for Kafka transport |
| Group offsets and replay | `group_id` parameter on the consumer |
| Micro-batch processing | Spark's `processingTime` trigger |
| Stateful stream processing | Windowed aggregations in `featurize` |
| Continuous machine learning | `foreachBatch` retrains on every batch |
| Online vs batch learning | Logistic regression fit on each micro-batch |

---

## Troubleshooting

### `NoBrokersAvailable`
Kafka isn't reachable. Check `docker ps` and confirm both containers are up.

### `UnsupportedClassVersionError`
Java version mismatch. Spark 3.5 requires Java 8/11/17. Use `java -version` to verify, and set `JAVA_HOME` to point at JDK 17.

### `JAVA_GATEWAY_EXITED`
JAVA_HOME isn't reaching Python. Set it in the same shell:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

### `ModuleNotFoundError: No module named 'distutils'`
Python 3.12+ removed `distutils`. Install `setuptools` which provides a shim:

```bash
pip install setuptools
```

### `Failed to find data source: kafka`
PySpark needs the Kafka connector JAR. It is downloaded automatically via the `spark.jars.packages` config in `processor.py`. First run takes a minute.

### `RecursionError: Stack overflow` in `foreachBatch`
Calling Spark operations (like `model.transform()`) inside `foreachBatch` triggers closure serialization issues. The current implementation extracts coefficients and computes accuracy with pure Python math instead.

### Spark 4.x Kafka metrics NullPointerException
Spark 4.1.2 has a known bug with the Kafka 0-10 connector that crashes the streaming query after a few batches. **Use Spark 3.5.1** to avoid this.

---

## What's Next

Some natural extensions if you want to keep building:

- **Broadcast variables** for proper model persistence across batches
- **River library** for true per-event online learning
- **Schema registry** to manage the JSON schema externally
- **Model persistence** — save trained model to disk and reload
- **Multi-class prediction** — view / click / purchase instead of binary
- **Unit tests** for each class
- **Dockerfile** to containerize the entire pipeline
- **Monitoring dashboard** with Prometheus + Grafana

---

## License

Built for educational purposes. Use freely.