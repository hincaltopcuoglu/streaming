# ClickStream Streaming Pipeline

An end-to-end **real-time clickstream analytics** pipeline running on Kubernetes. Events flow in through a FastAPI ingestion service, get streamed through Apache Kafka, processed by PySpark Structured Streaming, fed into an online logistic regression model, and the trained weights are served back through the API for low-latency purchase probability predictions.

This project was built as a hands-on learning exercise covering **REST API design**, **event streaming**, **distributed processing**, **online machine learning**, and **Kubernetes orchestration**.

---

## 🎯 What It Does

1. **Ingest** — A client POSTs click/purchase events to the FastAPI service.
2. **Stream** — Events are written to a Kafka topic (`clickstream_v2`).
3. **Process** — Spark Structured Streaming consumes the topic, computes windowed session features, and trains an online logistic regression model.
4. **Persist** — After each micro-batch, model weights are written to Redis as a single JSON snapshot.
5. **Serve** — The FastAPI service reads the latest weights from Redis and returns a purchase probability for any input feature vector.

```
HTTP POST /events
        │
        ▼
┌──────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   FastAPI × 2    │───▶│  Kafka (KRaft)   │───▶│  Spark Job × 1  │
│   (Deployment)   │    │   topic: v2      │    │   foreachBatch  │
└──────────────────┘    └──────────────────┘    └────────┬────────┘
        ▲                                                 │
        │                                                 │ SGD step
        │                                                 ▼
        │                                       ┌─────────────────┐
        │                                       │     Redis       │
        └────── GET /metrics/weights ──────────▶│  model state    │
        └────── GET /predictions    ──────────▶│  (JSON blob)    │
                                               └─────────────────┘
```

---

## ✨ Features

- **FastAPI** ingestion with Pydantic validation, lifespan-managed Kafka producer, automatic Swagger UI.
- **Kafka 3.7 (KRaft mode)** — Zookeeper-free, single StatefulSet, persistent volume.
- **PySpark Structured Streaming** with `foreachBatch`, windowed session features, and true online SGD (one step per batch, no catastrophic forgetting).
- **Redis** as the shared external state for model weights — survives Spark restarts, decouples training from serving.
- **Kubernetes** manifests for every component (Namespace, ConfigMap, Deployment, Service, StatefulSet, PVC) wired together with Kustomize.
- **Dockerfiles** that pin Java 17 (Eclipse Temurin) for the Spark image.
- **Liveness + readiness probes** so K8s can manage rolling updates cleanly.

---

## 📁 Project Structure

```
streaming/
├── api/
│   ├── app.py            # FastAPI app: /events, /metrics/weights, /predictions, /healthz, /readyz
│   ├── schemas.py        # Pydantic models for request/response validation
│   ├── state.py          # Redis client: load_state / save_state / health_check
│   └── cli.py            # CLI bridge so Spark can write to Redis via subprocess
├── spark/
│   └── job.py            # Structured Streaming + online logistic regression + SGD step
├── docker/
│   ├── Dockerfile.api    # python:3.12-slim + FastAPI deps
│   └── Dockerfile.spark  # python:3.12-slim + Temurin JDK 17 + PySpark
├── k8s/base/
│   ├── namespace.yaml
│   ├── configmap.yaml                    # Kafka / Redis / topic / log-level env
│   ├── kafka-statefulset.yaml            # ConfigMap + 2 Services + StatefulSet
│   ├── redis-deployment.yaml
│   ├── redis-service.yaml
│   ├── api-deployment.yaml
│   ├── api-service.yaml                  # NodePort 30080
│   ├── spark-deployment.yaml
│   └── kustomization.yaml
├── docs/
│   └── DEPLOY.md         # Step-by-step Kubernetes deployment guide
├── factory.py            # EventFactory.from_dict() — rebuilds typed objects
├── models.py             # ClickEvent / PurchaseEvent dataclasses
├── requirements.txt
└── README.md             # ← this file
```

---

## 🧰 Tech Stack

| Layer | Tech | Why |
|-------|------|-----|
| **Ingest API** | FastAPI + Pydantic | Async, typed, auto OpenAPI |
| **Message bus** | Apache Kafka 3.7 (KRaft) | Durable, replayable stream |
| **Stream processor** | PySpark 3.5.1 | Mature, expressive windowing |
| **Online ML** | LogisticRegression (cold-start) + manual SGD step | Real online learning, no refits |
| **Shared state** | Redis 7 | Single source of truth for model |
| **Containerization** | Docker (multi-stage build) | Local + K8s parity |
| **Orchestration** | Kubernetes (Docker Desktop) | Local end-to-end demo |

---

## 🚀 Quick Start (Kubernetes)

For full details see **[`docs/DEPLOY.md`](docs/DEPLOY.md)**.

```bash
# 1. Enable Kubernetes in Docker Desktop
# Settings → Kubernetes → Enable → Apply & Restart

# 2. Build images
docker build -t clickstream-api:latest -f docker/Dockerfile.api .
docker build -t clickstream-spark:latest -f docker/Dockerfile.spark .

# 3. Apply manifests
kubectl apply -k k8s/base/

# 4. Wait ~90 seconds for pods
kubectl get pods -n clickstream -w

# 5. Send an event
curl -X POST http://localhost:30080/events \
  -H "Content-Type: application/json" \
  -d '{"user_id": 42, "url": "/home", "action": "click", "session_id": "abc", "timestamp": 1721000000}'

# 6. Wait ~30s for Spark to process, then read weights
curl http://localhost:30080/metrics/weights | python3 -m json.tool

# 7. Get a prediction
curl "http://localhost:30080/predictions?clicks_in_session=5&time_on_page=180" | python3 -m json.tool
```

---

## 🔗 API Reference

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/events` | Accept a click/purchase event, write to Kafka |
| `GET` | `/metrics/weights` | Read current model weights from Redis |
| `GET` | `/predictions?clicks_in_session=X&time_on_page=Y` | Compute purchase probability |
| `GET` | `/healthz` | Liveness probe (always 200) |
| `GET` | `/readyz` | Readiness probe (200 when Kafka + Redis reachable) |
| `GET` | `/docs` | Swagger UI |

Example `POST /events` body:

```json
{
  "user_id": 7,
  "url": "/checkout",
  "action": "purchase",
  "session_id": "abc-123",
  "timestamp": 1721000010,
  "amount": 49.99
}
```

`action` must be one of `click`, `view`, `scroll`, `purchase`. `amount` is required for purchases.

---

## 🧠 The Online ML Piece

The model is a single logistic regression classifier with two features:

- `clicks_in_session` — number of events the user produced in the current 5-minute tumbling window.
- `time_on_page` — average seconds between events in the same window.

Training happens **per micro-batch** inside `spark/job.py`:

1. **First batch (cold start)** — fit a full `LogisticRegression` to bootstrap the weights.
2. **Every subsequent batch** — run **exactly one** stochastic gradient descent step on a sample of the batch and nudge the existing weights in place.

This is true online learning, not mini-batch refitting. The model carries its weights across batches and only adjusts by a small amount each time, which avoids catastrophic forgetting. The single Spark replica is intentional — multiple writers would race on Redis.

The weights are written to Redis as a single JSON blob:

```json
{
  "weights": [0.12, -0.04],
  "intercept": -0.9069,
  "meta": {"update_count": 17, "last_batch_size": 8421}
}
```

The API serves predictions by loading that blob and applying the sigmoid:

```
purchase_probability = sigmoid(intercept + w[0]*clicks_in_session + w[1]*time_on_page)
```

---

## 🧪 Verifying the Pipeline

```bash
# All pods running?
kubectl get pods -n clickstream
# NAME                       READY   STATUS    RESTARTS
# api-xxx-aaaa               1/1     Running   0
# api-xxx-bbbb               1/1     Running   0
# kafka-0                    1/1     Running   0
# redis-xxx-cccc             1/1     Running   0
# spark-xxx-dddd             1/1     Running   0

# Spark is processing?
kubectl logs -n clickstream -l app=spark --tail=20 | grep Batch
# Batch 1 | events=4 | update #1
# Batch 2 | events=12 | update #2

# Health
curl http://localhost:30080/healthz   # 200 OK
curl http://localhost:30080/readyz    # 200 OK (when Kafka + Redis reachable)
```

---

## 💡 Key Design Decisions

1. **Kafka KRaft (no Zookeeper)** — Single broker is enough for a learning setup, and KRaft removes the Zookeeper dependency entirely.
2. **Custom Kafka `server.properties` mounted via ConfigMap** — overrides the default `advertised.listeners=localhost:9092` that traps clients connecting from other pods.
3. **Spark single replica** — online learning writer that mutates Redis; multi-replica would race.
4. **Redis as external state** — model survives both Spark and API restarts; no in-process singleton fragility.
5. **Lazy Kafka producer in FastAPI lifespan** — wrapped in try/except so the API can boot and answer `/healthz` even when Kafka is down.
6. **Spark uses a CLI bridge (`api.cli`) to talk to Redis** — avoids importing the FastAPI app into the PySpark driver (which would clash with their Python environments).

---

## 🐛 Troubleshooting

Common issues and fixes are documented in detail in [`docs/DEPLOY.md`](docs/DEPLOY.md). Quick reference:

| Symptom | Fix |
|---------|-----|
| Kafka `CrashLoopBackOff` | Delete the StatefulSet + PVC, re-apply manifests |
| Spark `ModuleNotFoundError: numpy` | Add `numpy>=1.26` to `requirements.txt` and rebuild |
| API `/readyz` keeps 503 | `kubectl rollout restart deployment/api -n clickstream` |
| Kafka clients get `localhost:9092` | PVC contains stale metadata — reset the StatefulSet (see docs) |

---

## 🛣️ Possible Extensions

- **Per-event online learning** with River instead of per-batch SGD
- **More features** — one-hot URL, day-of-week from timestamp, recency
- **Class-imbalance handling** — class weights or undersampling
- **Concept-drift detection** — alert when batch gradients diverge from history
- **Multi-class prediction** — predict the next action, not just purchase
- **Prometheus + Grafana** monitoring dashboards
- **Helm chart** instead of raw Kustomize

---

## 📚 Concepts Demonstrated

| Area | Concepts |
|------|----------|
| **API design** | REST, Pydantic validation, lifespan, app.state, request injection |
| **Streaming** | Kafka topics, KRaft, producers, consumers, partitioning |
| **Distributed processing** | Structured Streaming, foreachBatch, checkpointing |
| **Online ML** | Cold-start fit, single SGD step, weight persistence |
| **State management** | Redis as shared external state, JSON snapshots |
| **Containers** | Multi-stage Dockerfiles, layer caching |
| **Kubernetes** | Namespace, ConfigMap, Deployment, Service, StatefulSet, PVC, probes, Kustomize |
| **Observability** | Health/readiness probes, structured logs |

---

## License

Built for educational purposes. Use freely.