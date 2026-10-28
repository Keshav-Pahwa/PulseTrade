import pytest
from fastapi.testclient import TestClient
from src.api.main import app
from src.producers.order_simulator import generate_order_lifecycle
from src.pipelines.eod_downloader import run_eod_ingestion

client = TestClient(app)

def test_health_endpoint():
    resp = client.get("/api/v1/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["api_version"] == "v1"
    assert data["status"] == "HEALTHY"

def test_order_lifecycle_determinism():
    events = generate_order_lifecycle("NIFTY", 24500.0)
    assert len(events) == 3
    order_ids = {e["order_id"] for e in events}
    assert len(order_ids) == 1  # Same key guarantees same Kafka partition
    assert [e["sequence_num"] for e in events] == [1, 2, 3]
    assert events[0]["status"] == "PLACED"
    assert events[1]["status"] == "PARTIALLY_FILLED"
    assert events[2]["status"] in ("FILLED", "CANCELLED")

@pytest.mark.asyncio
async def test_async_eod_pipeline():
    records = await run_eod_ingestion(["NIFTY", "RELIANCE"])
    assert len(records) == 2
    symbols = {r["symbol"] for r in records}
    assert symbols == {"NIFTY", "RELIANCE"}
    for r in records:
        assert r["high"] >= r["low"]
        assert r["volume"] > 0
