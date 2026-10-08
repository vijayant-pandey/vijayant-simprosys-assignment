"""Application configuration, loaded from environment variables / a local .env file.

Secrets (WEBHOOK_SECRET) have no default on purpose: the service refuses to start
without one instead of silently running with a well-known value.
"""
from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # --- Database -----------------------------------------------------------
    database_url: str = "sqlite:///./webhook.db"
    db_echo: bool = False

    # --- Webhook authentication --------------------------------------------
    webhook_secret: SecretStr = Field(min_length=16)
    signature_header: str = "X-Webhook-Signature"
    max_body_bytes: int = 64 * 1024

    # --- Queue / worker -----------------------------------------------------
    celery_broker_url: str = "redis://localhost:6379/0"
    # Run tasks inline in the calling process. Only for local debugging/tests.
    celery_task_always_eager: bool = False

    # Retry policy: an event gets 1 initial attempt + max_retries retries.
    max_retries: int = Field(default=5, ge=0)
    retry_backoff_base_seconds: float = Field(default=2.0, gt=0)
    retry_backoff_max_seconds: float = Field(default=300.0, gt=0)

    # A PROCESSING row whose lock is older than this is assumed to belong to a
    # crashed worker and may be reclaimed. Must exceed the slowest downstream call.
    processing_lease_seconds: int = Field(default=300, gt=0)

    # Sweeper: re-enqueue events whose queue message was lost (broker outage,
    # crash between commit and publish, ...).
    sweeper_interval_seconds: int = Field(default=60, gt=0)
    sweeper_grace_seconds: int = Field(default=120, ge=0)
    sweeper_batch_size: int = Field(default=500, gt=0)

    # --- Simulated downstream service ---------------------------------------
    downstream_failure_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    downstream_latency_seconds: float = Field(default=0.05, ge=0.0)

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
