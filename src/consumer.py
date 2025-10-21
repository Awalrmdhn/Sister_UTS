import asyncio
import logging
from datetime import datetime, UTC
import json

logger = logging.getLogger("consumer")
logger.setLevel(logging.INFO)

async def consumer_worker(name: str, queue: asyncio.Queue, dedup_store, stats, stop_event: asyncio.Event):
    logger.info("Worker %s started", name)
    stats.setdefault("received", 0)
    stats.setdefault("unique_processed", 0)
    stats.setdefault("duplicate_dropped", 0)

    while not stop_event.is_set():
        try:
            event = await asyncio.wait_for(queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        try:
            processed_at = datetime.now(UTC).isoformat()
            payload_text = json.dumps(event.payload, default=str)
            ok = dedup_store.mark_processed(
                event.topic, event.event_id, processed_at,
                event.timestamp.isoformat(), event.source, payload_text
            )
            if ok:
                stats["unique_processed"] += 1
                logger.info("PROCESSED %s %s", event.topic, event.event_id)
            else:
                stats["duplicate_dropped"] += 1
                logger.info("DUPLICATE %s %s", event.topic, event.event_id)
        except Exception as e:
            logger.exception("Error processing event %s %s: %s", getattr(event,'topic',None), getattr(event,'event_id',None), e)
        finally:
            queue.task_done()
