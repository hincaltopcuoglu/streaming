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

Each micro-batch triggers the full pipeline: Spark reads events from Kafka, parses JSON into typed columns, computes windowed session-level features, and updates a single online logistic regression model. The model is a **stateful singleton** — its weights survive across batches and are updated by one stochastic gradient descent (SGD) step per batch. Coefficients and accuracy are printed continuously.

> **A note on "online learning"**: this is true online learning, not mini-batch refitting. The model does *not* forget its weights between batches; each batch only nudges them by a small amount. The first batch bootstraps the weights with a full `LogisticRegression.fit()`, and every subsequent batch does exactly one SGD step. This protects against catastrophic forgetting and lets the model converge stably over time.

---

## Project Structure

| File | Purpose |
|---|---|
| `models.py` | Event blueprints: `ClickEvent`, `PurchaseEvent` (uses inheritance + polymorphism) |
| `factory.py` | `EventFactory.from_dict()` — reconstructs typed objects from raw dicts |
| `producer.py` | `EventProducer` — serializes events and sends them to Kafka |
| `consumer.py` | `EventConsumer` — reads events from Kafka, optionally returns typed objects |
| `processor.py` | `StreamProcessor` — Spark session, reads Kafka stream, parses JSON into typed DataFrame columns, exposes aggregations |
| `model.py` | `OnlinePurchasePredictor` — true online learning: weights persist across batches, one SGD step per batch (with cold-start full fit on the first batch) |
| `sent_events.py` | Test script: sends a mix of clicks and purchases to Kafka |
| `loadtest.py` | High-throughput async load test — 4 threads × 4 aiokafka producers = 16 concurrent senders, generates 5 GB of realistic clickstream data on-the-fly |
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
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Python dependencies

```bash
pip install --upgrade pip
pip install kafka-python pyspark==3.5.1 setuptools aiokafka
```

> `setuptools` is needed because PySpark 3.5.1 imports `distutils`, which was removed from Python 3.12+. `setuptools` provides a vendored `distutils` shim.
>
> `aiokafka` is used by `loadtest.py` for the high-throughput async producer.

### 4. Start Kafka and Zookeeper

```bash
docker-compose up -d
docker ps  # confirm both containers are running
```

### 5. Create the Kafka topic

For the small `sent_events.py` test pipeline:

```bash
docker exec -it $(docker ps --format '{{.Names}}' | grep kafka) \
    kafka-topics --create --topic clickstream_v2 \
    --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

For the high-throughput load test (recommended, 1 partition is enough — the model trains on the whole stream at once):

```bash
docker exec -it $(docker ps --format '{{.Names}}' | grep kafka) \
    kafka-topics --create --topic clickstream_loadtest \
    --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

---

## Running the Pipeline

You need **two terminals** for either the small or large pipeline.

### Terminal 1 — Start the Spark streaming query

```bash
source venv/bin/activate
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PYSPARK_GATEWAY_TIMEOUT=300
python3 run_spark.py clickstream_loadtest    # for the load test
# OR
python3 run_spark.py                         # defaults to clickstream_v2
```

Spark will start, download the Kafka connector on first run (~50MB), and begin polling Kafka every 10 seconds.

The `PYSPARK_GATEWAY_TIMEOUT=300` setting gives the JVM 5 minutes to start (the 60s default is sometimes too short on slower systems). `startingOffsets=latest` is used in the load test pipeline so Spark only sees events produced from the moment it starts, not historical data.

### Terminal 2 — Send events

**Option A — Small test pipeline (a few events at a time):**

```bash
source venv/bin/activate
python3 sent_events.py
```

Or run it in a loop:

```bash
while true; do
    python3 sent_events.py
    sleep 2
done
```

**Option B — High-throughput load test (~9 MB/s, ~85k events/s):**

```bash
source venv/bin/activate
python3 loadtest.py
```

This generates 5 GB of realistic clickstream data on-the-fly. The data has real predictive signal: sessions with more events are more likely to end in a purchase, so the model learns a non-zero coefficient for `clicks_in_session`. See `loadtest.py` for tunable parameters (target bytes, threads, sessions, purchase rate).

---

## Sample Output

With the high-throughput load test producing realistic sessions:

```
============================================================
Batch 0  |  events: 1,800  |  model update #1
============================================================
Accuracy on this batch: 0.676
Current weights (carried across batches, updated by one SGD step per batch):
  clicks_in_session         +0.1234  #
  time_on_page              +0.0000
  __intercept__             -0.9069  (baseline p=0.288)

============================================================
Batch 1  |  events: 193,400  |  model update #2
============================================================
Accuracy on this batch: 0.680
Current weights (carried across batches, updated by one SGD step per batch):
  clicks_in_session         +0.0450
  time_on_page              -0.0925
  __intercept__             -0.9972  (baseline p=0.269)
```

Note the `model update #N` counter — it increments every batch, confirming the same model object is being updated (not a new model refit on each batch). The weights move only by small amounts per batch because the learning rate is small (0.05).

What this tells you:

- **`clicks_in_session` is positive**: more events in a session → more likely to purchase. The model learned this from the data, where the load test makes purchase probability scale with session length.
- **`time_on_page` is negative**: long gaps between events → less likely to purchase. Events cluster within sessions, so this is anti-correlated with the positive class.
- **`__intercept__` is the baseline**: `sigmoid(intercept)` ≈ actual purchase rate in the batch. The model uses this to capture the class imbalance on top of the feature signal.
- **Accuracy around 0.68** is a real prediction accuracy, not "always predict majority class". The data has substantial noise, so this is close to the realistic ceiling.
- **Weights evolve slowly**: each batch only nudges the weights by the gradient × learning rate (0.05). Without catastrophic forgetting, the model can keep refining what it learned from earlier batches.

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
- `train_on_batch(df)` — **true online learning**: on the first call (cold start) it fits a full `LogisticRegression`. On every subsequent call it does **one** stochastic gradient descent step on the batch and updates `self.weights` and `self.intercept` in place. The model therefore carries knowledge across batches and only adjusts a little per batch.

The windowed aggregation makes the features meaningful: each event's `clicks_in_session` reflects how many events the user produced in the same 5-minute window, not a fixed random number.

The SGD step uses a small learning rate (default 0.05) and L2 regularization (default 0.01), so each batch only nudges the weights slightly. This is the difference between **online learning** and **mini-batch refitting** — the latter would train a brand-new model on each batch and forget everything else.

### Main Pipeline (`run_spark.py`)

Wires everything together with `foreachBatch`. The model is a **module-level singleton** (`get_online_model()`), so its weights survive across batches — this is the key to true online learning.

Each micro-batch:

1. Skips the batch if it has only one class (LogisticRegression needs both positives and negatives)
2. Calls `train_on_batch()` on the **same** model instance — this is a single SGD step, not a fresh fit
3. Extracts the current weights and intercept (as plain Python floats)
4. Computes accuracy by wrapping the current weights in a small `LogisticRegressionModel` and calling `.transform()` for a quick comparison against labels
5. Prints a clean summary including the `model update #N` counter and a sigmoid-transformed intercept

The `trigger(processingTime="10 seconds")` setting means Spark waits up to 10 seconds per batch, accumulating more events for meaningful training.

### Load Test (`loadtest.py`)

High-throughput synthetic data generator. Key design points:

- **4 worker threads × 4 async producers per thread = 16 concurrent Kafka producers** — keeps the network saturated
- **Shared session pool** across all threads (protected by a lock) — events have a 5% chance of starting a new session, 95% chance of extending an existing one
- **Realistic purchase signal** — purchase probability scales with how many events the user has already produced in the session (so `clicks_in_session` is a real feature the model can learn)
- **aiokafka + `asyncio.gather()`** for high throughput (not the lower-level `send_batch`, which has a different API in aiokafka 0.14)
- **On-the-fly generation** — events are created in memory and discarded, so 5 GB doesn't require 5 GB of RAM
- **Live progress reporter** — prints MB/s, events/s, and total bytes every 2 seconds

Tunable parameters at the top of the file: `TARGET_BYTES`, `NUM_WORKERS`, `TASKS_PER_WORKER`, `MAX_ACTIVE_SESSIONS`, `NEW_SESSION_PROBABILITY`, `BASE_PURCHASE_PROBABILITY`, `PURCHASE_SCALING`.

Measured throughput on Apple M1 8-core: ~9.6 MB/s, ~85k events/sec with no lock contention (the simpler version) and ~3.3 MB/s after adding the session pool (the lock is the bottleneck).

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
| Topics and partitions | `clickstream_v2` and `clickstream_loadtest` topics |
| Serialization | JSON encoding for Kafka transport |
| Group offsets and replay | `group_id` parameter on the consumer |
| Asynchronous production | `aiokafka` + `asyncio.gather` in `loadtest.py` |
| Multi-threaded fan-out | `ThreadPoolExecutor` + per-thread `asyncio.run` in `loadtest.py` |
| Micro-batch processing | Spark's `processingTime` trigger |
| Stateful stream processing | Windowed aggregations in `featurize` |
| Continuous machine learning | `foreachBatch` retrains on every batch |
| Online vs batch learning | Logistic regression fit on each micro-batch |
| `startingOffsets` strategies | `earliest` for replay, `latest` for live training |

---

## Troubleshooting

### `NoBrokersAvailable`
Kafka isn't reachable. Check `docker ps` and confirm both containers are up.

### `UnsupportedClassVersionError`
Java version mismatch. Spark 3.5 requires Java 8/11/17. Use `java -version` to verify, and set `JAVA_HOME` to point at JDK 17.

### `JAVA_GATEWAY_EXITED` (or `spark-class: Operation timed out`)
JAVA_HOME isn't reaching Python, or the JVM is taking too long to start. Try:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PYSPARK_GATEWAY_TIMEOUT=300    # default is 60s, too short on some systems
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

### `LogisticRegression: requirement failed: Nothing has been added to this summarizer`
The batch contains only one class (all purchases or all non-purchases). The current `process_batch` skips such batches gracefully. If you see this in your own code, add a label check or undersample.

### Spark produces no output for several minutes
Most often caused by `startingOffsets=earliest` combined with a topic that already has millions of events. The first batch will be huge and take a long time. Use `startingOffsets=latest` to only see new events.

### `aiokafka` `send_batch` API quirks
`AIOKafkaProducer.send_batch()` in aiokafka 0.14 expects a `BatchBuilder` object, not a list of payloads. For high throughput, use `send()` in a loop and `asyncio.gather()` to wait for the batch — the producer still batches on the wire via `linger_ms` and `max_batch_size`.

---

## What's Next

Some natural extensions if you want to keep building:

- **River library** for true per-event online learning (one update per event, not per batch)
- **External state store** — save weights to Redis so they survive Spark restarts
- **Concept drift detection** — alert when a batch's gradient direction is wildly different from past ones
- **Multi-class prediction** — view / click / purchase instead of binary
- **More features** — `url` one-hot encoding, `amount` for purchases, day-of-week from `timestamp`
- **Class imbalance handling** — undersample non-purchases or use class weights in `LogisticRegression`
- **Adaptive learning rate** — decay the learning rate over batches to fine-tune convergence
- **Unit tests** for each class
- **Dockerfile** to containerize the entire pipeline
- **Monitoring dashboard** with Prometheus + Grafana

---

## License

Built for educational purposes. Use freely.
