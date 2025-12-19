# src/publisher.py
import httpx
import uuid
import random
import asyncio
import os
from datetime import datetime
import json

AGG_URL = "http://aggregator:8080/publish"
TOTAL = 20000        
BATCH = 100
DUP_RATE = 0.30      

async def send_batches(events_batches):
    async with httpx.AsyncClient(timeout=60.0) as client:
        for idx, batch in enumerate(events_batches, start=1):
            try:
                resp = await client.post(AGG_URL, json=batch)
                if resp.status_code != 200:
                    print(f"[ERROR {resp.status_code}] Batch {idx}: {resp.text}")
                else:
                    # Print setiap 10 batch agar log tidak terlalu penuh
                    if idx % 10 == 0:
                        print(f"[OK] Batch {idx}/{len(events_batches)} sent.")
            except Exception as e:
                print(f"[EXCEPTION] Batch {idx}: {e}")
            # Sedikit delay agar aggregator tidak overload (flow control sederhana)
            await asyncio.sleep(0.01)  

def make_events():
    print(f"Generating {TOTAL} events with {int(DUP_RATE*100)}% duplication rate...")
    events = []
    # Generate event unik
    for i in range(TOTAL):
        event_id = str(uuid.uuid4())
        ev = {
            "topic": f"topic-{random.randint(1,10)}",
            "event_id": event_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "source": "publisher-service",
            "payload": {"seq": i, "value": random.uniform(20, 35)}
        }
        events.append(ev)

    # Ambil sampel untuk duplikasi
    num_duplicates = int(TOTAL * DUP_RATE)
    if num_duplicates > 0:
        duplicates = random.sample(events, num_duplicates)
        events += duplicates
    
    random.shuffle(events)

    batches = [events[i:i+BATCH] for i in range(0, len(events), BATCH)]
    return batches

async def main():
    batches = make_events()
    print(f"Total to send: {len(batches)} batches (Total Events sent: {len(batches)*BATCH})")
    start = datetime.now()
    await send_batches(batches)
    end = datetime.now()
    print(f"✅ Done sending all events. Duration: {end - start}")

if __name__ == "__main__":
    asyncio.run(main())