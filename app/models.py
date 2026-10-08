import enum
from datetime import datetime, timezone

from sqlalchemy import (
    JSON,
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    TypeDecorator,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB

from .databases import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class EventStatus(str, enum.Enum):
    RECEIVED = "RECEIVED"      # stored, waiting for a worker
    PROCESSING = "PROCESSING"  # claimed by a worker (see locked_at / lock_token)
    RETRYING = "RETRYING"      # last attempt failed transiently, retry scheduled
    PROCESSED = "PROCESSED"    # terminal: downstream accepted the event
    FAILED = "FAILED"          # terminal: permanent error or retries exhausted (dead letter)


class UTCDateTime(TypeDecorator):
    """Timezone-aware UTC datetimes on every backend.

    Postgres stores TIMESTAMPTZ natively; SQLite has no timezone support and
    returns naive values, so we normalise to UTC on write and re-attach UTC on read.
    """

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            if value.tzinfo is None:
                raise ValueError("naive datetime given; use timezone-aware UTC datetimes")
            value = value.astimezone(timezone.utc)
        return value

    def process_result_value(self, value, dialect):
        if value is not None and value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value


# BIGINT on Postgres/MySQL; SQLite only auto-increments "INTEGER PRIMARY KEY".
BigIntPK = BigInteger().with_variant(Integer, "sqlite")
JSONPayload = JSON().with_variant(JSONB(), "postgresql")


class WebhookEvent(Base):
    __tablename__ = "webhook_events"

    # Surrogate key: compact, monotonic, cheap to index and to reference.
    id = Column(BigIntPK, primary_key=True, autoincrement=True)

    # Natural key from the sender. The UNIQUE constraint (not an application-level
    # "SELECT then INSERT") is what guarantees an event is stored only once,
    # even when duplicates arrive concurrently on different API instances.
    event_id = Column(String(100), nullable=False)
    shop_id = Column(String(100), nullable=False)
    event_type = Column(String(100), nullable=False)
    occurred_at = Column(UTCDateTime(), nullable=False)  # sender's "timestamp"
    payload = Column(JSONPayload, nullable=False)

    status = Column(String(20), nullable=False, default=EventStatus.RECEIVED.value, server_default="RECEIVED")
    # Number of failed processing attempts so far.
    retry_count = Column(Integer, nullable=False, default=0, server_default="0")
    error_message = Column(Text, nullable=True)
    next_retry_at = Column(UTCDateTime(), nullable=True)

    # Worker lease. lock_token acts as a fencing token: a worker can only
    # finalize an event while it still holds the token it claimed it with.
    locked_at = Column(UTCDateTime(), nullable=True)
    lock_token = Column(String(36), nullable=True)

    created_at = Column(UTCDateTime(), nullable=False, default=utcnow)
    updated_at = Column(UTCDateTime(), nullable=False, default=utcnow, onupdate=utcnow)
    processed_at = Column(UTCDateTime(), nullable=True)

    __table_args__ = (
        UniqueConstraint("event_id", name="uq_webhook_events_event_id"),
        CheckConstraint(
            "status IN ('RECEIVED','PROCESSING','RETRYING','PROCESSED','FAILED')",
            name="ck_webhook_events_status",
        ),
        CheckConstraint("retry_count >= 0", name="ck_webhook_events_retry_count"),
        # Sweeper / ops queries: "events in status X older than T".
        Index("ix_webhook_events_status_updated_at", "status", "updated_at"),
        # Per-shop listing / support lookups, newest first.
        Index("ix_webhook_events_shop_id_created_at", "shop_id", "created_at"),
    )

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<WebhookEvent id={self.id} event_id={self.event_id!r} status={self.status}>"
