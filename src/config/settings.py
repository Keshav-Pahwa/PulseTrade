from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    trades_topic: str = "executed-trades"
    ticks_topic: str = "market-data-ticks"
    post_trade_group: str = "post-trade-reconciler-group"
    tick_cache_group: str = "tick-diagnostics-group"

    postgres_dsn: str = "postgresql://sefo_user:sefo_password@localhost:5432/pulsetrade_db"
    redis_url: str = "redis://localhost:6379/0"

    batch_size: int = 500
    batch_timeout_ms: int = 200

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()
