import pytest
import asyncio
import uuid
import os
from httpx import AsyncClient
from src.main import app
from asgi_lifespan import LifespanManager

# --- FIXTURE PEMBERSIH DATABASE ---
@pytest.fixture(autouse=True)
def clean_db():
    """Menghapus database sebelum dan sesudah setiap test case"""
    db_path = "data/dedup.db"
    
    # Hapus sebelum tes
    if os.path.exists(db_path):
        os.remove(db_path)
    
    yield
    
    # Hapus setelah tes (opsional, agar bersih)
    if os.path.exists(db_path):
        os.remove(db_path)

# --- Helper untuk generate dummy event ---
def get_event(topic="sensor.test", event_id=None):
    return {
        "topic": topic,
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": "2025-10-20T17:50:00Z",
        "source": "unit-test",
        "payload": {"val": 123}
    }

# --- GROUP 1: Basic Functional Tests ---

@pytest.mark.asyncio
async def test_publish_single_event():
    """1. Test publish satu event valid"""
    async with LifespanManager(app):
        async with AsyncClient(app=app, base_url="http://test") as ac:
            payload = get_event()
            response = await ac.post("/publish", json=payload)
            assert response.status_code == 200
            assert response.json()["received"] == 1
            assert response.json()["enqueued"] == 1

@pytest.mark.asyncio
async def test_publish_batch_events():
    """2. Test publish batch (list of events)"""
    async with LifespanManager(app):
        async with AsyncClient(app=app, base_url="http://test") as ac:
            payload = [get_event(), get_event()]
            response = await ac.post("/publish", json=payload)
            assert response.status_code == 200
            assert response.json()["received"] == 2
            assert response.json()["enqueued"] == 2

@pytest.mark.asyncio
async def test_duplicate_event_dropped():
    """3. Test dedup logic: event yang sama dikirim 2x"""
    async with LifespanManager(app):
        async with AsyncClient(app=app, base_url="http://test") as ac:
            payload = get_event(event_id="unique-id-001")
            
            # Kirim pertama
            await ac.post("/publish", json=payload)
            
            # Kirim kedua (Duplikat)
            await ac.post("/publish", json=payload)
            
            # Tunggu worker memproses
            await asyncio.sleep(0.5)
            
            stats = await ac.get("/stats")
            data = stats.json()
            
            # Harusnya 1 unique, 1 duplicate
            assert data["unique_processed"] == 1
            assert data["duplicate_dropped"] >= 1

@pytest.mark.asyncio
async def test_get_events_all():
    """4. Test ambil semua event"""
    async with LifespanManager(app):
        async with AsyncClient(app=app, base_url="http://test") as ac:
            await ac.post("/publish", json=get_event())
            await asyncio.sleep(0.2)
            
            res = await ac.get("/events")
            assert res.status_code == 200
            assert len(res.json()) >= 1

@pytest.mark.asyncio
async def test_get_stats_structure():
    """5. Test struktur JSON stats"""
    async with LifespanManager(app):
        async with AsyncClient(app=app, base_url="http://test") as ac:
            res = await ac.get("/stats")
            assert res.status_code == 200
            data = res.json()
            keys = ["received", "unique_processed", "duplicate_dropped", "topics", "uptime_seconds"]
            for k in keys:
                assert k in data

# --- GROUP 2: Filtering & Query Params ---

@pytest.mark.asyncio
async def test_get_events_filter_topic_success():
    """6. Test filter event berdasarkan topik yang ada"""
    async with LifespanManager(app):
        async with AsyncClient(app=app, base_url="http://test") as ac:
            await ac.post("/publish", json=get_event(topic="topic.A"))
            await ac.post("/publish", json=get_event(topic="topic.B"))
            await asyncio.sleep(0.5)
            
            res = await ac.get("/events?topic=topic.A")
            events = res.json()
            assert len(events) == 1
            assert events[0]["topic"] == "topic.A"

@pytest.mark.asyncio
async def test_get_events_filter_topic_empty():
    """7. Test filter topik yang tidak ada"""
    async with LifespanManager(app):
        async with AsyncClient(app=app, base_url="http://test") as ac:
            await ac.post("/publish", json=get_event(topic="topic.A"))
            await asyncio.sleep(0.2)
            res = await ac.get("/events?topic=topic.Z")
            assert len(res.json()) == 0

@pytest.mark.asyncio
async def test_stats_topics_list():
    """8. Test apakah list topik di stats terupdate"""
    async with LifespanManager(app):
        async with AsyncClient(app=app, base_url="http://test") as ac:
            await ac.post("/publish", json=get_event(topic="new.topic"))
            await asyncio.sleep(0.5)
            res = await ac.get("/stats")
            assert "new.topic" in res.json()["topics"]

# --- GROUP 3: Validation & Edge Cases ---

@pytest.mark.asyncio
async def test_publish_empty_list():
    """9. Test publish list kosong []"""
    async with LifespanManager(app):
        async with AsyncClient(app=app, base_url="http://test") as ac:
            res = await ac.post("/publish", json=[])
            assert res.status_code == 200
            assert res.json()["received"] == 0

@pytest.mark.asyncio
async def test_invalid_payload_missing_field():
    """10. Test payload tidak valid (kurang field)"""
    async with LifespanManager(app):
        async with AsyncClient(app=app, base_url="http://test") as ac:
            bad_payload = {"topic": "no.id"} 
            res = await ac.post("/publish", json=bad_payload)
            assert res.status_code == 422 

@pytest.mark.asyncio
async def test_invalid_payload_wrong_type():
    """11. Test payload salah tipe data"""
    async with LifespanManager(app):
        async with AsyncClient(app=app, base_url="http://test") as ac:
            bad_payload = get_event()
            bad_payload["timestamp"] = "bukan-tanggal" 
            res = await ac.post("/publish", json=bad_payload)
            assert res.status_code == 422

@pytest.mark.asyncio
async def test_method_not_allowed():
    """12. Test HTTP method salah (GET di endpoint POST)"""
    async with LifespanManager(app):
        async with AsyncClient(app=app, base_url="http://test") as ac:
            res = await ac.get("/publish")
            assert res.status_code == 405

# --- GROUP 4: Concurrency ---

@pytest.mark.asyncio
async def test_concurrent_publish_race_condition():
    """13. Test mengirim banyak request secara bersamaan (Concurrency)"""
    async with LifespanManager(app):
        async with AsyncClient(app=app, base_url="http://test") as ac:
            # Siapkan 20 event identik (potensi race condition)
            evt = get_event(event_id="race-id-999")
            tasks = [ac.post("/publish", json=evt) for _ in range(20)]
            
            # Jalankan concurrent
            await asyncio.gather(*tasks)
            
            # Tunggu worker
            await asyncio.sleep(1.0)
            
            stats = await ac.get("/stats")
            data = stats.json()
            
            # Sistem harus kuat: hanya 1 yang masuk unique, 19 dropped
            assert data["unique_processed"] == 1
            # Pastikan sisanya terhitung sebagai duplicate
            assert data["duplicate_dropped"] >= 19