"""
Resilient Post-Trade Order Lifecycle Producer
----------------------------------------------
Guarantees:
1. Key-Based Routing: Uses `order_id` as the Kafka message key so PLACED -> PARTIALLY_FILLED -> FILLED/CANCELLED
   events for the same order always land on the exact same partition in strict chronological order.
2. Zero Data Loss: Configured with `acks='all'` and `enable.idempotence=True`.
"""
import json
import time
import uuid
import random
from datetime import datetime, timezone
from confluent_kafka import Producer
from src.config.settings import settings

SYMBOLS = ["NIFTY", "BANKNIFTY", "RELIANCE", "TCS", "INFY", "HDFCBANK"]

def create_order_producer() -> Producer:
    conf = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 5,
        "linger.ms": 5,
        "compression.type": "lz4",
    }
    return Producer(conf)

def delivery_callback(err, msg):
    if err is not None:
        print(f"[ERROR] Order delivery failed for key={msg.key()}: {err}")
    else:
        print(
            f"[ACK] key={msg.key().decode()} -> topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )

def generate_order_lifecycle(symbol: str, base_price: float) -> list[dict]:
    order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
    side = random.choice(["BUY", "SELL"])
    total_qty = random.choice([50, 100, 200, 500])
    now_iso = lambda: datetime.now(timezone.utc).isoformat()

    # Step 1: Order Placed
    events = [{
        "order_id": order_id,
        "execution_id": f"EXEC-{uuid.uuid4().hex[:8].upper()}",
        "symbol": symbol,
        "side": side,
        "order_qty": total_qty,
        "filled_qty": 0,
        "exec_qty": 0,
        "avg_fill_price": 0.0,
        "status": "PLACED",
        "sequence_num": 1,
        "timestamp": now_iso(),
    }]

    # Step 2: Partial Fill
    partial_qty = total_qty // 2
    fill_price_1 = round(base_price * random.uniform(0.999, 1.001), 2)
    events.append({
        "order_id": order_id,
        "execution_id": f"EXEC-{uuid.uuid4().hex[:8].upper()}",
        "symbol": symbol,
        "side": side,
        "order_qty": total_qty,
        "filled_qty": partial_qty,
        "exec_qty": partial_qty,
        "avg_fill_price": fill_price_1,
        "status": "PARTIALLY_FILLED",
        "sequence_num": 2,
        "timestamp": now_iso(),
    })

    # Step 3: Terminal State (Filled or Cancelled)
    terminal_status = random.choices(["FILLED", "CANCELLED"], weights=[0.8, 0.2])[0]
    if terminal_status == "FILLED":
        rem_qty = total_qty - partial_qty
        fill_price_2 = round(base_price * random.uniform(0.999, 1.001), 2)
        avg_price = round((partial_qty * fill_price_1 + rem_qty * fill_price_2) / total_qty, 4)
        events.append({
            "order_id": order_id,
            "execution_id": f"EXEC-{uuid.uuid4().hex[:8].upper()}",
            "symbol": symbol,
            "side": side,
            "order_qty": total_qty,
            "filled_qty": total_qty,
            "exec_qty": rem_qty,
            "avg_fill_price": avg_price,
            "status": "FILLED",
            "sequence_num": 3,
            "timestamp": now_iso(),
        })
    else:
        events.append({
            "order_id": order_id,
            "execution_id": f"EXEC-{uuid.uuid4().hex[:8].upper()}",
            "symbol": symbol,
            "side": side,
            "order_qty": total_qty,
            "filled_qty": partial_qty,
            "exec_qty": 0,
            "avg_fill_price": fill_price_1,
            "status": "CANCELLED",
            "sequence_num": 3,
            "timestamp": now_iso(),
        })

    return events

def run_simulator(cycles: int = 50, delay_sec: float = 0.05):
    producer = create_order_producer()
    base_prices = {
        "NIFTY": 24500.0, "BANKNIFTY": 52100.0, "RELIANCE": 2950.0,
        "TCS": 4100.0, "INFY": 1650.0, "HDFCBANK": 1680.0
    }
    print(f"Starting Order Lifecycle Producer on topic '{settings.trades_topic}'...")
    for _ in range(cycles):
        sym = random.choice(SYMBOLS)
        lifecycle = generate_order_lifecycle(sym, base_prices[sym])
        for event in lifecycle:
            producer.produce(
                topic=settings.trades_topic,
                key=event["order_id"].encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                on_delivery=delivery_callback,
            )
            producer.poll(0)
            time.sleep(delay_sec)
    producer.flush()
    print("Order simulation finished cleanly.")

if __name__ == "__main__":
    run_simulator()
