# Deployment Guide — ClickStream Streaming Pipeline on Kubernetes

This document describes how to deploy the ClickStream pipeline end-to-end on **Docker Desktop Kubernetes**. Architecture: FastAPI → Kafka → Spark Structured Streaming → Redis → FastAPI (serving).

---

## 📋 Prerequisites

- **macOS** (Apple Silicon — also works on Intel)
- **Docker Desktop** with **Kubernetes enabled**
- **kubectl** (bundled with Docker Desktop)
- **Python 3.12+** (for local development/testing)
- Approximately **4 GB free RAM** (Spark + Kafka + Redis + 2× API)

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Docker Desktop Kubernetes                       │
│                                                                         │
│  ┌────────────────┐     ┌──────────────────┐     ┌─────────────────┐   │
│  │  FastAPI × 2   │────▶│  Kafka (KRaft)   │────▶│  Spark Job × 1  │   │
│  │  (Deployment)  │     │  (StatefulSet)   │     │  (Deployment)   │   │
│  │  NodePort:80   │     │  port: 9092      │     │                 │   │
│  └────────────────┘     └──────────────────┘     └────────┬────────┘   │
│         ▲                                                  │            │
│         │  HTTP                                    SGD     ▼            │
│         │                                            ┌──────────────┐    │
│  ┌──────┴───────┐                                    │    Redis     │    │
│  │   Client     │                                    │  (Deployment)│    │
│  │  :30080      │                                    │   port: 6379 │    │
│  └──────────────┘                                    └──────────────┘    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Services

| Service | Type | Replicas | Reason |
|---------|------|----------|--------|
| **API** | Deployment | 2 | Horizontal scalability, rolling updates |
| **Kafka** | StatefulSet | 1 (KRaft) | Persistent volume, stable network ID |
| **Spark** | Deployment | 1 | Online ML writer — multiple replicas would race on Redis writes |
| **Redis** | Deployment | 1 | Shared model state |

---

## 🚀 Deployment Steps

### 1. Enable Kubernetes

Docker Desktop → **Settings** → **Kubernetes** → ☑️ **Enable Kubernetes** → **Apply & Restart**

Verify:

```bash
kubectl cluster-info
# Kubernetes control plane is running at https://127.0.0.1:6443
```

### 2. Build Docker Images

```bash
# Spark job (Java 17 + PySpark)
docker build -t clickstream-spark:latest -f docker/Dockerfile.spark .

# FastAPI service
docker build -t clickstream-api:latest -f docker/Dockerfile.api .

docker images | grep clickstream
# clickstream-api      latest    ...
# clickstream-spark    latest    ...
```

### 3. Apply Manifests

```bash
kubectl apply -k k8s/base/
```

**Expected output:**

```
namespace/clickstream created
configmap/clickstream-config created
configmap/kafka-config created
service/kafka-headless created
service/kafka created
statefulset.apps/kafka created
deployment.apps/redis created
service/redis created
deployment.apps/api created
service/api created
deployment.apps/spark created
```

### 4. Wait for Pods to Become Ready (~60-90 seconds)

```bash
kubectl get pods -n clickstream -w
```

**All should reach `1/1 Running`:**

```
NAME                       READY   STATUS    RESTARTS   AGE
api-xxx-aaaa               1/1     Running   0          60s
api-xxx-bbbb               1/1     Running   0          60s
kafka-0                    1/1     Running   0          90s
redis-xxx-cccc             1/1     Running   0          30s
spark-xxx-dddd             1/1     Running   0          70s
```

---

## 🧪 End-to-End Test

### Send an Event

```bash
curl -X POST http://localhost:30080/events \
  -H "Content-Type: application/json" \
  -d '{"user_id": 42, "url": "/home", "action": "click", "session_id": "abc", "timestamp": 1721000000}'
# → {"status":"accepted","topic":"clickstream_v2"}
```

### Read Model Weights (from Redis)

```bash
curl http://localhost:30080/metrics/weights | python3 -m json.tool
```

**Expected** (after first batch):

```json
{
  "exists": true,
  "update_count": 1,
  "last_batch_size": 4,
  "weights": {"clicks_in_session": 0.0, "time_on_page": 0.0},
  "intercept": -1.0986
}
```

### Get a Prediction

```bash
curl "http://localhost:30080/predictions?clicks_in_session=5&time_on_page=180" | python3 -m json.tool
```

**Expected**:

```json
{"model_ready": true, "purchase_probability": 0.25}
```

> ⚠️ Weights may stay at 0 for the first batch — this is normal. As more events arrive, weights will be learned.

### Health Probes

```bash
curl http://localhost:30080/healthz    # → 200 OK (always)
curl http://localhost:30080/readyz     # → 200 OK (when Kafka + Redis are reachable)
```

---

## 📊 Log Monitoring

```bash
# Tail logs for any service
kubectl logs -n clickstream -l app=api --tail=50 -f
kubectl logs -n clickstream -l app=spark --tail=50 -f
kubectl logs -n clickstream kafka-0 --tail=50 -f

# Only Spark batch summaries
kubectl logs -n clickstream -l app=spark | grep "Batch"
# Batch 1 | events=4 | update #1
```

---

## 🔧 Troubleshooting

### Kafka `CrashLoopBackOff`

**Cause:** Storage metadata was formatted with an older config (typically after a Kafka image change).

**Fix:**

```bash
kubectl delete statefulset -n clickstream kafka
kubectl delete pvc -n clickstream data-kafka-0
kubectl apply -f k8s/base/kafka-statefulset.yaml
```

### Spark `ModuleNotFoundError: No module named 'numpy'`

**Cause:** PySpark ML packages depend on numpy, but it was missing from `requirements.txt`.

**Fix:** Add `numpy>=1.26` to `requirements.txt` and rebuild the Spark image.

### API `/readyz` keeps returning 503

**Cause:** API started before Kafka was up and failed to create the producer in its lifespan.

**Fix:**

```bash
kubectl rollout restart deployment/api -n clickstream
```

### Kafka `advertised.listeners=localhost:9092` (clients cannot connect)

**Cause:** Kafka's default `server.properties` advertises `localhost:9092`, which only resolves inside the Kafka pod itself. Our mount path `/etc/kafka-config/server.properties` overrides this.

**Fix:** The custom `server.properties` is already mounted via ConfigMap in `k8s/base/kafka-statefulset.yaml`. If you see this error, follow the "CrashLoopBackOff" fix above to start fresh storage with the correct config.

### PVC stuck in `Terminating`

**Cause:** Docker Desktop hostPath volumes require manual reclaim.

**Fix:** Delete the StatefulSet first, then the PVC:

```bash
kubectl delete statefulset -n clickstream kafka
kubectl delete pvc -n clickstream data-kafka-0
```

---

## 🛑 Stop / Clean Up

### Delete Pods Only (Data Preserved)

```bash
kubectl delete -k k8s/base/
```

### Delete Everything (Including Data)

```bash
kubectl delete namespace clickstream
```

---

## 🔁 Development Loop

### After Code Changes

```bash
# 1. Rebuild images
docker build -t clickstream-api:latest -f docker/Dockerfile.api .
docker build -t clickstream-spark:latest -f docker/Dockerfile.spark .

# 2. Roll the deployments
kubectl rollout restart deployment/api -n clickstream
kubectl rollout restart deployment/spark -n clickstream

# 3. Watch
kubectl get pods -n clickstream -w
```

### After Config Changes (Kafka, Spark ML params)

```bash
# 1. Re-apply manifests (ConfigMap updates)
kubectl apply -k k8s/base/

# 2. Restart the workloads so they pick up new env values
kubectl rollout restart deployment -n clickstream
kubectl delete pod -n clickstream kafka-0
```

---

## 📁 Project Layout

```
streaming/
├── api/
│   ├── app.py            # FastAPI endpoints
│   ├── schemas.py        # Pydantic models
│   ├── state.py          # Redis client
│   └── cli.py            # Redis CLI bridge (Spark subprocess)
├── spark/
│   └── job.py            # Structured Streaming + online SGD
├── docker/
│   ├── Dockerfile.api
│   └── Dockerfile.spark
├── k8s/base/
│   ├── namespace.yaml
│   ├── configmap.yaml
│   ├── kafka-statefulset.yaml   (ConfigMap + 2 Services + StatefulSet)
│   ├── redis-deployment.yaml
│   ├── redis-service.yaml
│   ├── api-deployment.yaml
│   ├── api-service.yaml
│   ├── spark-deployment.yaml
│   └── kustomization.yaml
├── docs/
│   └── DEPLOY.md         # ← this file
├── factory.py
├── models.py
├── requirements.txt
└── README.md
```

---

## 🔗 Endpoint Reference

| Method | Path | Description |
|--------|------|-------------|
| POST | `/events` | Accept a click/purchase event, write to Kafka |
| GET | `/metrics/weights` | Read model weights from Redis |
| GET | `/predictions?clicks_in_session=X&time_on_page=Y` | Compute purchase probability |
| GET | `/healthz` | Liveness probe (always 200) |
| GET | `/readyz` | Readiness probe (200 when Kafka + Redis reachable) |
| GET | `/docs` | Swagger UI (auto-generated by FastAPI) |

---

## 💡 Key Design Decisions

1. **Kafka KRaft (no Zookeeper)** — Sufficient for a single broker and reduces operational overhead.
2. **Spark single replica** — The online learning writer writes to Redis; multiple replicas would create race conditions.
3. **Redis as external state** — Shared model state between Spark and API. Survives process restarts.
4. **Custom Kafka `server.properties` mount** — Overrides the default `localhost:9092` advertised listener that traps clients.
5. **API lifespan lazy Kafka init** — Producer creation is wrapped in try/except so the API can boot even when Kafka is down.