"""
PulseTrade SEFO — High-Performance FastAPI Backend (/api/v1)
-------------------------------------------------------------
Provides clean, versioned RESTful contracts for Post-Trade Reconciliation,
Real-Time Position & PnL Calculation, Trading Infrastructure Diagnostics,
and On-Demand EOD Market Data Ingestion.
"""
import time
import psycopg2
from psycopg2.extras import RealDictCursor
import redis
from fastapi import FastAPI, Query, HTTPException
from src.config.settings import settings
from src.api.v1.schemas import (
    PaginatedOrdersResponse,
    PositionResponse,
    TickSnapshotResponse,
    DiagnosticsHealthResponse,
    EODTriggerRequest,
    EODTriggerResponse,
)
from src.pipelines.eod_downloader import run_eod_ingestion

app = FastAPI(
    title="PulseTrade SEFO Control Plane API",
    description="Post-Trade Reconciliation, Market Data Ingestion & Trading Infrastructure Diagnostics",
    version="1.0.0",
)

def get_db_conn():
    return psycopg2.connect(settings.postgres_dsn, cursor_factory=RealDictCursor)

def get_redis_client():
    return redis.Redis.from_url(settings.redis_url, decode_responses=True)

@app.get("/api/v1/health")
def health_check():
    return {"api_version": "v1", "service": "pulsetrade-sefo", "status": "HEALTHY"}

@app.get("/api/v1/orders", response_model=PaginatedOrdersResponse)
def list_orders(
    symbol: str | None = Query(None, description="Filter by trading symbol e.g. NIFTY"),
    status: str | None = Query(None, description="Filter by order status e.g. FILLED"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    conn = get_db_conn()
    try:
        filters = []
        params = []
        if symbol:
            filters.append("symbol = %s")
            params.append(symbol.upper())
        if status:
            filters.append("status = %s")
            params.append(status.upper())

        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS cnt FROM orders {where_clause};", params)
            total = cur.fetchone()["cnt"]

            cur.execute(
                f"SELECT * FROM orders {where_clause} ORDER BY updated_at DESC LIMIT %s OFFSET %s;",
                params + [limit, offset],
            )
            rows = cur.fetchall()
        return {
            "api_version": "v1",
            "total": total,
            "limit": limit,
            "offset": offset,
            "items": rows,
        }
    finally:
        conn.close()

@app.get("/api/v1/positions", response_model=list[PositionResponse])
def get_positions():
    conn = get_db_conn()
    r = get_redis_client()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM positions ORDER BY symbol ASC;")
            positions = cur.fetchall()

        enriched = []
        for pos in positions:
            sym = pos["symbol"]
            tick_data = r.hgetall(f"market:tick:{sym}")
            live_ltp = float(tick_data["ltp"]) if tick_data and "ltp" in tick_data else None
            unrealized = None
            if live_ltp is not None:
                unrealized = round(float(pos["net_notional"]) + (pos["net_qty"] * live_ltp), 2)
            enriched.append({
                **pos,
                "live_ltp": live_ltp,
                "unrealized_pnl": unrealized,
            })
        return enriched
    finally:
        conn.close()

@app.get("/api/v1/market-data/{symbol}/quote", response_model=TickSnapshotResponse)
def get_live_quote(symbol: str):
    r = get_redis_client()
    data = r.hgetall(f"market:tick:{symbol.upper()}")
    if not data:
        raise HTTPException(status_code=404, detail=f"No live tick cached for symbol '{symbol.upper()}'")
    return {
        "symbol": data["symbol"],
        "ltp": float(data["ltp"]),
        "bid": float(data["bid"]),
        "ask": float(data["ask"]),
        "volume": int(data["volume"]),
        "timestamp": data["timestamp"],
    }

@app.get("/api/v1/diagnostics/lag", response_model=DiagnosticsHealthResponse)
def get_infrastructure_diagnostics():
    r = get_redis_client()
    hb = r.get("diagnostics:worker:heartbeat")
    offsets_raw = r.hgetall("diagnostics:partition_offsets")
    offsets = {k: int(v) for k, v in offsets_raw.items()}
    age = round(time.time() - float(hb), 3) if hb else None
    status = "HEALTHY" if (age is not None and age < 10.0) else "DEGRADED"
    return {
        "api_version": "v1",
        "status": status,
        "worker_heartbeat_age_sec": age,
        "partition_offsets": offsets,
    }

@app.post("/api/v1/pipelines/eod-trigger", response_model=EODTriggerResponse)
async def trigger_eod_pipeline(req: EODTriggerRequest):
    records = await run_eod_ingestion(req.symbols, req.trade_date)
    return {
        "api_version": "v1",
        "ingested_count": len(records),
        "records": records,
    }
