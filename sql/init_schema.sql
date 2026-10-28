-- ============================================================================
-- PulseTrade SEFO - Relational Schema & Idempotent Indexing Strategy
-- ============================================================================

-- 1. Orders Table (Tracks latest deterministic state per order_id)
CREATE TABLE IF NOT EXISTS orders (
    order_id        VARCHAR(64) PRIMARY KEY,
    symbol          VARCHAR(32) NOT NULL,
    side            VARCHAR(8)  NOT NULL CHECK (side IN ('BUY', 'SELL')),
    order_qty       INTEGER     NOT NULL CHECK (order_qty > 0),
    filled_qty      INTEGER     NOT NULL DEFAULT 0 CHECK (filled_qty >= 0),
    avg_fill_price  NUMERIC(14, 4) NOT NULL DEFAULT 0.0000,
    status          VARCHAR(24) NOT NULL CHECK (status IN ('PLACED', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED', 'REJECTED')),
    sequence_num    BIGINT      NOT NULL DEFAULT 1,
    kafka_partition INTEGER     NOT NULL,
    kafka_offset    BIGINT      NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_symbol_status ON orders (symbol, status);
CREATE INDEX IF NOT EXISTS idx_orders_updated_at ON orders (updated_at DESC);

-- 2. Trade Executions Audit Log (Immutable Ledger of Individual Fills)
CREATE TABLE IF NOT EXISTS trade_executions (
    execution_id    VARCHAR(64) PRIMARY KEY,
    order_id        VARCHAR(64) NOT NULL REFERENCES orders(order_id),
    symbol          VARCHAR(32) NOT NULL,
    side            VARCHAR(8)  NOT NULL,
    exec_qty        INTEGER     NOT NULL,
    exec_price      NUMERIC(14, 4) NOT NULL,
    executed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_executions_order_id ON trade_executions (order_id);
CREATE INDEX IF NOT EXISTS idx_executions_symbol_time ON trade_executions (symbol, executed_at DESC);

-- 3. Net Positions Table (Real-Time Symbol Exposure & Cash Flow)
CREATE TABLE IF NOT EXISTS positions (
    symbol          VARCHAR(32) PRIMARY KEY,
    net_qty         BIGINT      NOT NULL DEFAULT 0,
    gross_buy_qty   BIGINT      NOT NULL DEFAULT 0,
    gross_sell_qty  BIGINT      NOT NULL DEFAULT 0,
    net_notional    NUMERIC(18, 4) NOT NULL DEFAULT 0.0000,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 4. End-of-Day (EOD) Market Data OHLCV Table
CREATE TABLE IF NOT EXISTS eod_market_data (
    symbol          VARCHAR(32) NOT NULL,
    trade_date      DATE        NOT NULL,
    open_price      NUMERIC(14, 4) NOT NULL,
    high_price      NUMERIC(14, 4) NOT NULL,
    low_price       NUMERIC(14, 4) NOT NULL,
    close_price     NUMERIC(14, 4) NOT NULL,
    volume          BIGINT      NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, trade_date)
);
