# Architecture Deep-Dive & Flow Documentation

![System Architecture](images/system_architecture.png)

## 1. Dual-Speed Kafka Pipeline Design

High-frequency trading infrastructure requires different durability/latency trade-offs depending on the workflow:

### Pipeline A: Post-Trade Execution Stream (`executed-trades`)
* **Business Criticality:** Every order fill impacts clearing, accounting, and risk exposure. Zero messages can be lost or double-counted.
* **Producer Config:** `acks='all'`, `enable.idempotence=True`, `Key = order_id`.
* **Consumer Config:** `enable.auto.commit=False`, micro-batch size of `500` records or `200ms` window, synchronous offset commit strictly **after** PostgreSQL `COMMIT`.

### Pipeline B: Real-Time Market Tick Stream (`market-data-ticks`)
* **Business Criticality:** Powers live UI charts and mark-to-market diagnostics where ultra-low latency matters more than recovering a dropped millisecond tick.
* **Producer Config:** `acks=0` (fire-and-forget), `Key = symbol`.
* **Consumer Config:** Batches 100 ticks into a single **Redis Pipeline** (`HSET market:tick:{symbol}`) to achieve sub-millisecond cache updates.

![Order Lifecycle Flow](images/order_lifecycle_flow.png)

---

## 2. Database Schema & Indexing Rationale

1. **`orders` Table:**
   * Primary Key: `order_id`
   * Composite Index: `idx_orders_symbol_status ON orders (symbol, status)` supports filtered frontend queries (`GET /api/v1/orders?symbol=NIFTY&status=FILLED`) via Index Scan rather than Sequential Scan.
2. **`trade_executions` Table:**
   * Primary Key: `execution_id`
   * Uses `ON CONFLICT (execution_id) DO NOTHING` so replayed Kafka batches never double-count individual fills when calculating net positions.
3. **`eod_market_data` Table:**
   * Composite Primary Key: `(symbol, trade_date)` ensures re-running the EOD downloader for any historical date cleanly overwrites corrupt or partial vendor data.
