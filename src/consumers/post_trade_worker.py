"""
Fault-Tolerant Post-Trade Micro-Batch Consumer
-----------------------------------------------
Key Engineering Features:
1. Manual Offset Commits (`enable.auto.commit=False`).
2. Micro-Batching: Collects up to `BATCH_SIZE` (500) events or waits `BATCH_TIMEOUT_MS` (200ms).
3. Idempotent SQL Reconciliation: Uses PostgreSQL `ON CONFLICT (order_id) DO UPDATE` with sequence guards
   so replays or out-of-order recovery never corrupt state or double-count positions.
4. Synchronous Offset Commit (`consumer.commit(asynchronous=False)`) strictly AFTER DB transaction commit.
"""
import json
import time
import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka import Consumer, KafkaError
from src.config.settings import settings

UPSERT_ORDERS_SQL = """
INSERT INTO orders (
    order_id, symbol, side, order_qty, filled_qty,
    avg_fill_price, status, sequence_num, kafka_partition, kafka_offset
)
VALUES %s
ON CONFLICT (order_id) DO UPDATE SET
    filled_qty      = EXCLUDED.filled_qty,
    avg_fill_price  = EXCLUDED.avg_fill_price,
    status          = EXCLUDED.status,
    sequence_num    = EXCLUDED.sequence_num,
    kafka_partition = EXCLUDED.kafka_partition,
    kafka_offset    = EXCLUDED.kafka_offset,
    updated_at      = NOW()
WHERE EXCLUDED.sequence_num >= orders.sequence_num;
"""

INSERT_EXECUTIONS_SQL = """
INSERT INTO trade_executions (
    execution_id, order_id, symbol, side, exec_qty, exec_price
)
VALUES %s
ON CONFLICT (execution_id) DO NOTHING;
"""

RECONCILE_POSITIONS_SQL = """
INSERT INTO positions (symbol, net_qty, gross_buy_qty, gross_sell_qty, net_notional, updated_at)
SELECT
    symbol,
    SUM(CASE WHEN side = 'BUY' THEN exec_qty ELSE -exec_qty END) AS net_qty,
    SUM(CASE WHEN side = 'BUY' THEN exec_qty ELSE 0 END) AS gross_buy_qty,
    SUM(CASE WHEN side = 'SELL' THEN exec_qty ELSE 0 END) AS gross_sell_qty,
    SUM(CASE WHEN side = 'BUY' THEN -exec_qty * exec_price ELSE exec_qty * exec_price END) AS net_notional,
    NOW()
FROM trade_executions
GROUP BY symbol
ON CONFLICT (symbol) DO UPDATE SET
    net_qty        = EXCLUDED.net_qty,
    gross_buy_qty  = EXCLUDED.gross_buy_qty,
    gross_sell_qty = EXCLUDED.gross_sell_qty,
    net_notional   = EXCLUDED.net_notional,
    updated_at     = NOW();
"""

def persist_micro_batch(conn, batch_msgs: list) -> None:
    """Deduplicates in-memory batch by order_id and persists atomically to PostgreSQL."""
    latest_orders = {}
    executions = []

    for msg in batch_msgs:
        payload = json.loads(msg.value().decode("utf-8"))
        oid = payload["order_id"]

        # Keep highest sequence number per order_id within the same micro-batch
        if oid not in latest_orders or payload["sequence_num"] >= latest_orders[oid]["sequence_num"]:
            latest_orders[oid] = {
                **payload,
                "partition": msg.partition(),
                "offset": msg.offset(),
            }

        if payload.get("exec_qty", 0) > 0:
            executions.append((
                payload["execution_id"],
                oid,
                payload["symbol"],
                payload["side"],
                payload["exec_qty"],
                payload["avg_fill_price"],
            ))

    order_rows = [
        (
            o["order_id"], o["symbol"], o["side"], o["order_qty"],
            o["filled_qty"], o["avg_fill_price"], o["status"],
            o["sequence_num"], o["partition"], o["offset"]
        )
        for o in latest_orders.values()
    ]

    with conn.cursor() as cur:
        if order_rows:
            execute_values(cur, UPSERT_ORDERS_SQL, order_rows)
        if executions:
            execute_values(cur, INSERT_EXECUTIONS_SQL, executions)
            cur.execute(RECONCILE_POSITIONS_SQL)
    conn.commit()

def run_post_trade_worker():
    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": settings.post_trade_group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([settings.trades_topic])
    conn = psycopg2.connect(settings.postgres_dsn)

    batch = []
    last_flush = time.monotonic()
    timeout_sec = settings.batch_timeout_ms / 1000.0

    print(f"[Post-Trade Worker] Listening on '{settings.trades_topic}' (Batch={settings.batch_size}, Timeout={settings.batch_timeout_ms}ms)...")
    try:
        while True:
            msg = consumer.poll(timeout=0.05)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[Kafka Error] {msg.error()}")
                else:
                    batch.append(msg)

            now = time.monotonic()
            if batch and (len(batch) >= settings.batch_size or (now - last_flush) >= timeout_sec):
                persist_micro_batch(conn, batch)
                # Synchronous commit ONLY after PostgreSQL transaction succeeds
                consumer.commit(asynchronous=False)
                print(f"[Committed] Flushed micro-batch of {len(batch)} trade events to PostgreSQL.")
                batch.clear()
                last_flush = now
    except KeyboardInterrupt:
        print("Shutting down Post-Trade Worker gracefully...")
    finally:
        if batch:
            persist_micro_batch(conn, batch)
            consumer.commit(asynchronous=False)
        conn.close()
        consumer.close()

if __name__ == "__main__":
    run_post_trade_worker()
