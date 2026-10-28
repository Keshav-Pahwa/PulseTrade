from datetime import datetime, date
from pydantic import BaseModel, Field

class OrderResponse(BaseModel):
    order_id: str
    symbol: str
    side: str
    order_qty: int
    filled_qty: int
    avg_fill_price: float
    status: str
    sequence_num: int
    kafka_partition: int
    kafka_offset: int
    updated_at: datetime

class PaginatedOrdersResponse(BaseModel):
    api_version: str = "v1"
    total: int
    limit: int
    offset: int
    items: list[OrderResponse]

class PositionResponse(BaseModel):
    symbol: str
    net_qty: int
    gross_buy_qty: int
    gross_sell_qty: int
    net_notional: float
    live_ltp: float | None = None
    unrealized_pnl: float | None = None
    updated_at: datetime

class TickSnapshotResponse(BaseModel):
    symbol: str
    ltp: float
    bid: float
    ask: float
    volume: int
    timestamp: str

class DiagnosticsHealthResponse(BaseModel):
    api_version: str = "v1"
    status: str
    worker_heartbeat_age_sec: float | None
    partition_offsets: dict[str, int]

class EODTriggerRequest(BaseModel):
    symbols: list[str] = Field(default_factory=lambda: ["NIFTY", "BANKNIFTY", "RELIANCE", "TCS"])
    trade_date: date | None = None

class EODTriggerResponse(BaseModel):
    api_version: str = "v1"
    ingested_count: int
    records: list[dict]
