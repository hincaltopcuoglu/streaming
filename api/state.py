"""Redis-backed shared state for the online model.
Why this module exists:
    The online model has two readers/writers: the Spark streaming job
    updates the weights on each micro-batch, and the API reads them to
    serve predictions. They live in different processes (and later, in
    different Kubernetes pods), so they cannot share an in-process
    Python singleton.
    Redis is the single source of truth: Spark writes the full snapshot,
    the API reads it on demand.
The snapshot is stored as a single JSON blob in one key:
    clickstream:model:state -> {"weights": [...], "intercept": ..., "meta": {...}}
Why a single blob instead of three separate keys?
    So that a read returns an atomic snapshot. Three separate GETs could
    race against a concurrent write and return a weights/intercept/meta
    triple from different moments in time — the API would then mix
    "new weights" with "old intercept" and produce nonsense predictions.
    A single SET + a single GET is naturally atomic.
Auth:
    REDIS_PASSWORD is read from the environment (K8s Secret). When the
    env var is unset (local development with no auth), we pass None so
    redis-py doesn't even send an AUTH command.
"""


import json
import os
import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None



KEY_STATE = "clickstream:model:state"

_client = redis.Redis(
    host = REDIS_HOST,
    port = REDIS_PORT,
    password = REDIS_PASSWORD,
    decode_responses = True,
)

def load_state() -> dict:
    """Return the current model snapshot.
    Shape:
        {
            "weights":   list[float] | None,
            "intercept": float       | None,
            "meta":      dict,
            "exists":    bool,        # False if no model trained yet
        }
    """

    blob = _client.get(KEY_STATE)
    if blob is None:
        return {"weights": None, "intercept": None, "meta": {}, "exists": False}
    snapshot = json.loads(blob)
    snapshot["exists"] = True
    return snapshot



def save_state(weights: list, intercept: float, meta: dict) -> None:
    """Overwrite the model snapshot. Spark calls this every batch."""
    payload = {
        "weights": weights,
        "intercept": intercept,
        "meta": meta,
    }
    _client.set(KEY_STATE, json.dumps(payload))



def reset_state() -> None:
    """Drop the snapshot. Useful for cold restarts or tests."""
    _client.delete(KEY_STATE)



def health_check() -> bool:
    """True if Redis is reachable."""
    try:
        return bool(_client.ping())
    except Exception:
        return False
