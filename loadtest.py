"""High-throughput load test for the clickstream pipeline.

Architecture (Option C, scaled for Apple M1 8-core / 16GB):
  - 4 worker threads, each with its own async event loop
  - 4 aiokafka producer tasks per worker = 16 concurrent producers
  - Events generated on-the-fly (no precomputed list)
  - Stops when target bytes (default 5 GB) reached

Each event is ~1 KB of JSON, so 5 GB ~ 5 million events.
"""
import asyncio
import json
import random
import string
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from aiokafka import AIOKafkaProducer

# ---- Config ----
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "clickstream_loadtest"
TARGET_BYTES = 5 * 1024 * 1024 * 1024   # 5 GB
NUM_WORKERS = 4                          # worker threads
TASKS_PER_WORKER = 4                     # async tasks per worker
EVENTS_PER_BATCH = 100                   # events per producer.send_and_wait cycle
PROGRESS_INTERVAL_SEC = 2.0              # how often to print stats

# ---- Session model ----
# Active sessions tracked across all threads. We keep the last MAX_ACTIVE_SESSIONS
# so the pool doesn't grow without bound. Each session remembers its user_id,
# the count of events it has produced, and the start time.
MAX_ACTIVE_SESSIONS = 5000
NEW_SESSION_PROBABILITY = 0.05           # 5% chance a new event starts a new session
BASE_PURCHASE_PROBABILITY = 0.02         # baseline purchase rate per event
# Purchase probability scales with the number of events already in the session.
# With 1 event already in session, p = 0.02; with 10 events, p ~= 0.05; etc.
PURCHASE_SCALING = 0.03                  # additive per prior event in session
MAX_PURCHASE_PROBABILITY = 0.5           # cap so we don't get all purchases


# ---- Shared state across threads ----
_lock = threading.Lock()
_stats = {
    "bytes_sent": 0,
    "events_sent": 0,
    "errors": 0,
    "stop": False,
}
# session_id -> {"user_id": int, "event_count": int, "started_at": int}
_sessions: dict = {}


def random_user_id():
    return random.randint(1, 100_000)


def random_session_id():
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=10))


def random_url():
    paths = ["/home", "/products", "/cart", "/checkout", "/about", "/blog", "/search"]
    return random.choice(paths) + "?" + "".join(random.choices(string.ascii_letters, k=6))


def _pick_or_create_session():
    """Pick an existing session, or create a new one.

    Each event has a small chance of starting a fresh session, but most events
    extend an existing one. This creates the realistic pattern where some
    sessions have many events (active users) and many have just one (bouncers).
    """
    with _lock:
        # Decide: continue an existing session or start a new one?
        if _sessions and random.random() >= NEW_SESSION_PROBABILITY:
            # Pick a random existing session
            session_id = random.choice(list(_sessions.keys()))
            session = _sessions[session_id]
            session["event_count"] += 1
            user_id = session["user_id"]
            event_count = session["event_count"]
        else:
            # Start a new session
            session_id = random_session_id()
            user_id = random_user_id()
            event_count = 1
            _sessions[session_id] = {
                "user_id": user_id,
                "event_count": event_count,
                "started_at": int(time.time()),
            }
            # Cap the pool size
            if len(_sessions) > MAX_ACTIVE_SESSIONS:
                # Drop the oldest entry (insertion-ordered dict)
                oldest = next(iter(_sessions))
                _sessions.pop(oldest, None)

    # Purchase probability increases with event_count in the session.
    # The first event in a session almost never purchases; later events do.
    p_purchase = min(
        BASE_PURCHASE_PROBABILITY + PURCHASE_SCALING * event_count,
        MAX_PURCHASE_PROBABILITY,
    )
    is_purchase = random.random() < p_purchase

    return session_id, user_id, event_count, is_purchase


def generate_event():
    """Generate one event dict on-the-fly. ~1 KB of JSON.

    Events are part of a session that may have many events. The probability
    of a purchase event grows with how long the user has been in the session,
    which gives the ML model a real signal to learn.
    """
    session_id, user_id, event_count, is_purchase = _pick_or_create_session()

    event = {
        "user_id": user_id,
        "url": random_url(),
        "action": "purchase" if is_purchase else random.choice(["click", "view", "scroll"]),
        "session_id": session_id,
        "timestamp": int(time.time()),
    }
    if is_purchase:
        event["amount"] = round(random.uniform(5.0, 500.0), 2)
    return event


async def producer_task(task_id: int, worker_id: int):
    """One async producer. Generates and sends events until stopped."""
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks=1,                       # wait for leader ack only (faster)
        linger_ms=10,                 # small batching window
        compression_type="gzip",      # compress for higher throughput
        max_batch_size=64 * 1024,     # 64 KB batches
    )
    await producer.start()
    try:
        while not _stats["stop"]:
            # Build a batch of events
            events = [generate_event() for _ in range(EVENTS_PER_BATCH)]
            # Serialize to JSON bytes
            payloads = [json.dumps(e).encode("utf-8") for e in events]
            batch_bytes = sum(len(p) for p in payloads)

            try:
                # Fire all sends without waiting for ack each time.
                # aiokafka batches them on the wire via linger_ms/max_batch_size.
                futures = [producer.send(TOPIC, p) for p in payloads]
                # Wait for the whole batch to be delivered (one round trip)
                await asyncio.gather(*futures)
                with _lock:
                    _stats["bytes_sent"] += batch_bytes
                    _stats["events_sent"] += len(payloads)
            except Exception as e:
                with _lock:
                    _stats["errors"] += 1
                if _stats["errors"] < 5:
                    print(f"  worker={worker_id} task={task_id} error: {e}")
    finally:
        await producer.stop()


async def worker_loop(worker_id: int):
    """One async event loop running multiple producer tasks."""
    await asyncio.gather(*[
        producer_task(t, worker_id) for t in range(TASKS_PER_WORKER)
    ])


def thread_entry(worker_id: int):
    """Thread entry point: run the async loop in this thread."""
    asyncio.run(worker_loop(worker_id))


def progress_reporter():
    """Background thread printing live throughput stats."""
    start = time.time()
    last_bytes = 0
    last_time = start
    while not _stats["stop"]:
        time.sleep(PROGRESS_INTERVAL_SEC)
        with _lock:
            current = _stats["bytes_sent"]
            events = _stats["events_sent"]
            errors = _stats["errors"]
        now = time.time()
        elapsed_total = now - start
        elapsed_window = now - last_time
        bytes_window = current - last_bytes
        mb_total = current / 1024 / 1024
        gb_total = current / 1024 / 1024 / 1024
        mb_per_sec = (bytes_window / 1024 / 1024) / max(elapsed_window, 0.001)
        eps = int((events / elapsed_total) if elapsed_total > 0 else 0)
        pct = min(100.0, 100.0 * current / TARGET_BYTES)
        print(
            f"  [{elapsed_total:6.1f}s] "
            f"{gb_total:5.2f} / {TARGET_BYTES/1024/1024/1024:.0f} GB "
            f"({pct:5.1f}%)  "
            f"{mb_per_sec:6.1f} MB/s  "
            f"{eps:,} ev/s  "
            f"errors={errors}"
        )
        last_bytes = current
        last_time = now
        if current >= TARGET_BYTES:
            _stats["stop"] = True
            break


def main():
    print(f"Starting load test:")
    print(f"  target:      {TARGET_BYTES/1024/1024/1024:.0f} GB")
    print(f"  workers:     {NUM_WORKERS} threads")
    print(f"  tasks/worker:{TASKS_PER_WORKER} = {NUM_WORKERS * TASKS_PER_WORKER} total")
    print(f"  topic:       {TOPIC}")
    print()

    # Start progress reporter
    reporter = threading.Thread(target=progress_reporter, daemon=True)
    reporter.start()

    # Start worker threads
    start = time.time()
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as pool:
        futures = [pool.submit(thread_entry, i) for i in range(NUM_WORKERS)]
        # Wait for all to complete (they exit when _stats["stop"] is True)
        for f in futures:
            f.result()

    elapsed = time.time() - start
    final_mb = _stats["bytes_sent"] / 1024 / 1024
    final_gb = _stats["bytes_sent"] / 1024 / 1024 / 1024
    eps = int(_stats["events_sent"] / elapsed) if elapsed > 0 else 0
    mbps = final_mb / elapsed

    print()
    print(f"Done!")
    print(f"  Total time:  {elapsed:.1f}s")
    print(f"  Total data:  {final_gb:.2f} GB ({final_mb:.0f} MB)")
    print(f"  Total events:{_stats['events_sent']:,}")
    print(f"  Throughput:  {mbps:.1f} MB/s, {eps:,} events/s")
    print(f"  Errors:      {_stats['errors']}")


if __name__ == "__main__":
    main()