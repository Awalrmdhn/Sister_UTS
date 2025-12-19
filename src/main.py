import asyncio
import logging
import time
import json
from contextlib import asynccontextmanager
from typing import List, Union, Optional
from fastapi import FastAPI, HTTPException, Query
from src.models import Event, PublishResponse, EventView, Stats
from src.dedup_store import DedupStore
from src.consumer import consumer_worker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

QUEUE_SIZE = 10000
queue: Optional[asyncio.Queue] = None
dedup_store: Optional[DedupStore] = None
# Statistik in-memory untuk metrik sementara
stats = {"received": 0, "duplicate_dropped": 0}
workers = []
stop_event = asyncio.Event()
start_time = time.time()

@asynccontextmanager
async def lifespan(app: FastAPI):
    global queue, dedup_store, workers, stop_event
    queue = asyncio.Queue(maxsize=QUEUE_SIZE)
    # Pastikan folder data ada via volume docker atau lokal
    dedup_store = DedupStore(db_path="data/dedup.db")
    stop_event.clear()

    # Worker Count 4 (Untuk uji konkurensi POIN D)
    worker_count = 4
    loop = asyncio.get_event_loop()
    for i in range(worker_count):
        name = f"w{i}"
        task = loop.create_task(consumer_worker(name, queue, dedup_store, stats, stop_event))
        workers.append(task)
    logging.info("Startup complete. Workers: %d", worker_count)
    
    yield
    
    # Graceful shutdown
    stop_event.set()
    for t in workers:
        if not t.done():
            t.cancel()
    logging.info("Shutdown complete.")

app = FastAPI(title="UAS Pub-Sub Aggregator", lifespan=lifespan)

@app.post("/publish", response_model=PublishResponse)
async def publish(payload: Union[Event, List[Event]]):
    """
    Menerima single atau batch event.
    """
    global stats, queue
    events = payload if isinstance(payload, list) else [payload]
    stats["received"] = stats.get("received", 0) + len(events)

    enqueued = 0
    for ev in events:
        try:
            # Put event ke queue internal (in-memory broker)
            await queue.put(ev) 
            enqueued += 1
        except asyncio.QueueFull:
            # Strategi backpressure sederhana
            raise HTTPException(status_code=503, detail="Queue is full")
            
    return PublishResponse(received=len(events), enqueued=enqueued)

@app.get("/events", response_model=List[EventView])
async def get_events(topic: Optional[str] = Query(None)):
    """
    Mengembalikan daftar event unik yang tersimpan di DB.
    """
    rows = dedup_store.get_events(topic)
    results = []
    for r in rows:
        try:
            payload = json.loads(r[4]) if r[4] else {}
        except Exception:
            payload = {}
        results.append(EventView(
            topic=r[0],
            event_id=r[1],
            timestamp=r[2],
            source=r[3],
            payload=payload
        ))
    return results

@app.get("/stats", response_model=Stats)
async def get_stats():
    """
    Mengembalikan statistik performa.
    unique_processed diambil dari DB (Persisten).
    received & duplicate_dropped adalah counter in-memory sejak restart terakhir.
    """
    up_seconds = int(time.time() - start_time)
    ds = dedup_store.get_stats()
    return Stats(
        received=stats.get("received", 0),
        unique_processed=ds.get("unique_processed", 0),
        duplicate_dropped=stats.get("duplicate_dropped", 0),
        topics=ds.get("topics", []),
        uptime_seconds=up_seconds
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.main:app", host="0.0.0.0", port=8080)