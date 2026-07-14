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
    """Startup: Create Kafka Producer. Shutdown: close."""
    log.info("Kafka=%s topic=%s", KAFKA_BOOTSTRAP, TOPIC)
    app.state.kafka_producer = KafkaProducer(bootstrap_servers = KAFKA_BOOTSTRAP)
    try:
        yield
    finally:
        log.info("Flushing Kafka Producer")
        app.state.kafka_producer.flush()
        app.state.kafka_producer.close()

app = FastAPI(title="ClickStream API", version="2.0", lifespan=lifespan)

# ---- Endpoints ----

@app.post("/events")
def post_event(body: EventIn, request: Request):
    """Get Body, convert to dictionary, write to Kafka."""
    payload = body.model_dump()
    data = json.dumps(payload).encode("utf-8")
    request.app.state.kafka_producer.send(TOPIC, value=data)
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
    z = b + w[0] * clicks_in_session + w[1] * time_on_page
    p = 1.0 / (1.0 + math.exp(-z))
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
