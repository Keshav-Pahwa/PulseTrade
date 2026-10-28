"""
Real-Time Market Data Diagnostics & Redis Cache Worker
-------------------------------------------------------
Consumes high-frequency ticks and pipelines updates into Redis for sub-millisecond
access by the FastAPI monitoring endpoints. Also publishes worker heartbeats.
"""
import json
import time
import redis
from confluent_kafka import Consumer
from src.config.settings import settings

def run_tick_cache_worker():
    r = redis.Redis.from_url(settings.redis_url, decode_responses=True)
    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": settings.tick_cache_group,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 1000,
    })
    consumer.subscribe([settings.ticks_topic])

    buffer = []
    last_pipe = time.monotonic()
    print(f"[Tick Cache Worker] Caching live ticks from '{settings.ticks_topic}' into Redis...")

    try:
        while True:
            msg = consumer.poll(0.02)
            if msg and not msg.error():
                buffer.append((msg.partition(), msg.offset(), json.loads(msg.value().decode("utf-8"))))

            if buffer and (len(buffer) >= 100 or (time.monotonic() - last_pipe) >= 0.1):
                pipe = r.pipeline()
                for part, offset, tick in buffer:
                    sym = tick["symbol"]
                    pipe.hset(f"market:tick:{sym}", mapping={
                        "symbol": sym,
                        "ltp": str(tick["ltp"]),
                        "bid": str(tick["bid"]),
                        "ask": str(tick["ask"]),
                        "volume": str(tick["volume"]),
                        "timestamp": tick["timestamp"],
                    })
                    pipe.hset("diagnostics:partition_offsets", f"p{part}", str(offset))
                pipe.set("diagnostics:worker:heartbeat", str(time.time()), ex=30)
                pipe.execute()
                buffer.clear()
                last_pipe = time.monotonic()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    run_tick_cache_worker()
