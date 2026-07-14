import json
import math
import os
import logging
from contextlib import asynccontextmanager


from fastapi import FastAPI, HTTPException, Query, Request
from kafka import KafkaProducer

from api.schemas import EventIn, WeightsOut, PredictionOut
import api.state as state


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("api")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "clickstream_v2")


@asynccontextmanager
async def lifespan(app):
    """Startup: try to create Kafka Producer (lazy, don't crash if Kafka is down).
    Shutdown: close if it exists."""
    log.info("Kafka=%s topic=%s", KAFKA_BOOTSTRAP, TOPIC)
    try:
        app.state.kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000,
        )
        log.info("Kafka producer ready")
    except Exception as e:
        log.warning("Kafka producer init failed: %s — running without producer", e)
        app.state.kafka_producer = None
    try:
        yield
    finally:
        if app.state.kafka_producer is not None:
            log.info("Flushing Kafka Producer")
            try:
                app.state.kafka_producer.flush(timeout=5)
                app.state.kafka_producer.close(timeout=5)
            except Exception as e:
                log.warning("Kafka close failed: %s", e)

app = FastAPI(title="ClickStream API", version="2.0", lifespan=lifespan)

# ---- Endpoints ----

@app.post("/events")
def post_event(body: EventIn, request: Request):
    """Get Body, convert to dictionary, write to Kafka."""
    producer = request.app.state.kafka_producer
    if producer is None:
        raise HTTPException(status_code=503, detail="Kafka producer not ready")
    payload = body.model_dump()
    data = json.dumps(payload).encode("utf-8")
    producer.send(TOPIC, value=data)
    return {"status": "accepted", "topic": TOPIC}



@app.get("/metrics/weights", response_model=WeightsOut)
def get_weights():
    snap = state.load_state()
    if not snap["exists"]:
        return WeightsOut(exists=False)
    return WeightsOut(
        exists=True,
        update_count = snap["meta"].get("update_count",0),
        last_accuracy = snap["meta"].get("last_accuracy"),
        last_batch_size= snap["meta"].get("last_batch_size"),
        weights= _weights_view(snap["weights"]),
        intercept= snap["intercept"],
    )


def _weights_view(raw_list: list) -> dict:
    """[0.12, -0.04] -> {'clicks_in_session': 0.12, 'time_on_page': -0.04}"""
    names = ["clicks_in_session", "time_on_page"]
    return {n: raw_list[i] for i, n in enumerate(names) if i < len(raw_list)}


@app.get("/predictions", response_model=PredictionOut)
def get_predictions(
    clicks_in_session: int = Query(..., gt = 0),
    time_on_page: int = Query(..., ge=0)
):
    snap = state.load_state()
    if not snap["exists"]:
        return PredictionOut(model_ready=False, purchase_probability=0.0)

    w = snap["weights"]
    b = snap["intercept"]
    # Spark'taki feature engineering ile aynı: time_on_page = total_events in session
    # (saniye cinsinden değil — Spark bunu zaten normalize ediyor)
    z = b + w[0] * clicks_in_session + w[1] * time_on_page
    # Numerically stable sigmoid: avoid overflow when z is very negative
    if z >= 0:
        p = 1.0 / (1.0 + math.exp(-z))
    else:
        ez = math.exp(z)
        p = ez / (1.0 + ez)
    return PredictionOut(model_ready=True, purchase_probability=p)


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/readyz")
def readyz(request: Request):
    redis_ok = state.health_check()
    kafka_ok = request.app.state.kafka_producer is not None
    if not (redis_ok and kafka_ok):
        raise HTTPException(status_code=503, detail={"redis": redis_ok, "kafka": kafka_ok})
    return {"status": "ready", "redis": redis_ok, "kafka": kafka_ok}
