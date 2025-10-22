import pytest
from httpx import AsyncClient
from src.main import app
from asgi_lifespan import LifespanManager  # type: ignore


@pytest.mark.asyncio
async def test_publish_event():
    async with LifespanManager(app):  
        async with AsyncClient(app=app, base_url="http://test") as ac:
            payload = {
                "topic": "sensor.temp",
                "event_id": "evt-001",
                "timestamp": "2025-10-20T17:50:00Z",
                "source": "device-01",
                "payload": {"value": 30.5}
            }
            response = await ac.post("/publish", json=payload)
            assert response.status_code == 200
            assert response.json()["received"] == 1


@pytest.mark.asyncio
async def test_duplicate_event():
    async with LifespanManager(app):
        async with AsyncClient(app=app, base_url="http://test") as ac:
            payload = {
                "topic": "sensor.temp",
                "event_id": "evt-001",
                "timestamp": "2025-10-20T17:50:00Z",
                "source": "device-01",
                "payload": {"value": 30.5}
            }
            # kirim ulang event dengan event_id yang sama
            await ac.post("/publish", json=payload)
            stats = await ac.get("/stats")
            data = stats.json()
            assert data["duplicate_dropped"] >= 1


@pytest.mark.asyncio
async def test_get_events():
    async with LifespanManager(app):
        async with AsyncClient(app=app, base_url="http://test") as ac:
            res = await ac.get("/events?topic=sensor.temp")
            assert res.status_code == 200
            assert isinstance(res.json(), list)
            assert len(res.json()) >= 1


@pytest.mark.asyncio
async def test_get_stats():
    async with LifespanManager(app):
        async with AsyncClient(app=app, base_url="http://test") as ac:
            res = await ac.get("/stats")
            assert res.status_code == 200
            body = res.json()
            assert "received" in body
            assert "unique_processed" in body
            assert "duplicate_dropped" in body


@pytest.mark.asyncio
async def test_invalid_publish():
    async with LifespanManager(app):
        async with AsyncClient(app=app, base_url="http://test") as ac:
            payload = {"wrong": "data"}
            res = await ac.post("/publish", json=payload)
            assert res.status_code == 422  # validation error dari FastAPI