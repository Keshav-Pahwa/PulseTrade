"""
Ultra-Low-Latency Market Data Tick Producer
--------------------------------------------
Configured with `acks=0` (Fire-and-Forget) for high-frequency L1 price ticks
intended for real-time monitoring dashboards and Redis caching.
"""
import json
import time
import random
from datetime import datetime, timezone
from confluent_kafka import Producer
from src.config.settings import settings

SYMBOLS = {
    "NIFTY": 24500.0,
    "BANKNIFTY": 52100.0,
    "RELIANCE": 2950.0,
    "TCS": 4100.0,
    "INFY": 1650.0,
    "HDFCBANK": 1680.0,
}

def create_tick_producer() -> Producer:
    return Producer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "acks": 0,  # Fire-and-forget for sub-millisecond UI diagnostics
        "linger.ms": 2,
    })

def run_tick_stream(total_ticks: int = 2000):
    producer = create_tick_producer()
    print(f"Streaming {total_ticks} market ticks to '{settings.ticks_topic}' (acks=0)...")
    for i in range(total_ticks):
        sym = random.choice(list(SYMBOLS.keys()))
        SYMBOLS[sym] = round(SYMBOLS[sym] * random.uniform(0.9995, 1.0005), 2)
        tick = {
            "symbol": sym,
            "ltp": SYMBOLS[sym],
            "bid": round(SYMBOLS[sym] - 0.5, 2),
            "ask": round(SYMBOLS[sym] + 0.5, 2),
            "volume": random.randint(10, 500),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        producer.produce(
            topic=settings.ticks_topic,
            key=sym.encode("utf-8"),
            value=json.dumps(tick).encode("utf-8"),
        )
        if i % 200 == 0:
            producer.poll(0)
        time.sleep(0.002)
    producer.flush()
    print("Tick stream completed.")

if __name__ == "__main__":
    run_tick_stream()
