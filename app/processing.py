"""Idempotent, concurrency-safe processing of stored webhook events.

The queue gives *at-least-once* delivery: the same task can run twice (worker
crash before ack, sweeper re-enqueue, broker redelivery). Correctness therefore
lives in the database, not in the queue:

1. **Claim** - a single conditional UPDATE moves the row to PROCESSING only if it
   is RECEIVED/RETRYING (or PROCESSING with an expired lease). The database
   serialises concurrent UPDATEs on one row, so exactly one worker sees
   ``rowcount == 1``; everyone else skips. PROCESSED/FAILED rows are never claimed
   again, which is what makes processing idempotent.
2. **Work** - call downstream with no DB transaction held open.
3. **Finalize** - another conditional UPDATE guarded by the claim's
   ``lock_token``. If our lease expired and another worker took over, our
   update matches zero rows and we back off instead of overwriting its result.
"""
import logging
import random
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from sqlalchemy import and_, or_, select, update
from sqlalchemy.orm import Session

from .config import Settings, get_settings
from .downstream import PermanentDownstreamError, send_order_event
from .models import EventStatus, WebhookEvent, utcnow

logger = logging.getLogger(__name__)

MAX_ERROR_LENGTH = 2000

Downstream = Callable[[dict[str, Any]], None]


class Outcome(str, Enum):
    PROCESSED = "PROCESSED"
    RETRY = "RETRY"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"  # not claimable: already done, in flight elsewhere, or missing


@dataclass(frozen=True)
class ProcessResult:
    outcome: Outcome
    retry_in_seconds: float | None = None


def compute_backoff(retry_count: int, settings: Settings) -> float:
    """Exponential backoff with full jitter: random(0, min(cap, base * 2^(n-1)))."""
    ceiling = min(settings.retry_backoff_max_seconds, settings.retry_backoff_base_seconds * 2 ** max(retry_count - 1, 0))
    # Floor at 1s so a burst of failures doesn't hammer a struggling dependency.
    return max(1.0, random.uniform(0, ceiling))


def claim_event(db: Session, event_pk: int, settings: Settings) -> str | None:
    """Atomically take ownership of an event. Returns the lock token, or None."""
    now = utcnow()
    token = str(uuid.uuid4())
    lease_expired_before = now - timedelta(seconds=settings.processing_lease_seconds)
    result = db.execute(
        update(WebhookEvent)
        .where(
            WebhookEvent.id == event_pk,
            or_(
                WebhookEvent.status.in_([EventStatus.RECEIVED.value, EventStatus.RETRYING.value]),
                and_(
                    WebhookEvent.status == EventStatus.PROCESSING.value,
                    WebhookEvent.locked_at < lease_expired_before,
                ),
            ),
        )
        .values(status=EventStatus.PROCESSING.value, locked_at=now, lock_token=token, updated_at=now)
        .execution_options(synchronize_session=False)
    )
    db.commit()
    return token if result.rowcount == 1 else None


def _finalize(db: Session, event_pk: int, token: str, **values: Any) -> bool:
    now = utcnow()
    result = db.execute(
        update(WebhookEvent)
        .where(
            WebhookEvent.id == event_pk,
            WebhookEvent.status == EventStatus.PROCESSING.value,
            WebhookEvent.lock_token == token,
        )
        .values(locked_at=None, lock_token=None, updated_at=now, **values)
        .execution_options(synchronize_session=False)
    )
    db.commit()
    if result.rowcount != 1:
        logger.warning("lost lease before finalizing; another worker owns the event", extra={"event_pk": event_pk})
        return False
    return True


def _snapshot(event: WebhookEvent) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "event_type": event.event_type,
        "shop_id": event.shop_id,
        "payload": event.payload,
    }


def process_event(
    db: Session,
    event_pk: int,
    *,
    downstream: Downstream | None = None,
    settings: Settings | None = None,
) -> ProcessResult:
    settings = settings or get_settings()
    downstream = downstream or send_order_event

    token = claim_event(db, event_pk, settings)
    if token is None:
        current = db.scalar(select(WebhookEvent.status).where(WebhookEvent.id == event_pk))
        logger.info("event not claimable, skipping", extra={"event_pk": event_pk, "status": current})
        return ProcessResult(Outcome.SKIPPED)

    event = db.get(WebhookEvent, event_pk, populate_existing=True)
    snapshot = _snapshot(event)
    retry_count = event.retry_count
    db.commit()  # end the read transaction; never hold one across the network call

    log_extra = {"event_id": snapshot["event_id"], "attempt": retry_count + 1}
    try:
        downstream(snapshot)
    except PermanentDownstreamError as exc:
        logger.error("permanent downstream failure, marking FAILED", extra=log_extra)
        _finalize(
            db, event_pk, token,
            status=EventStatus.FAILED.value,
            retry_count=retry_count + 1,
            error_message=_format_error(exc),
            next_retry_at=None,
        )
        return ProcessResult(Outcome.FAILED)
    except Exception as exc:  # transient or unexpected: both are worth retrying
        failures = retry_count + 1
        error = _format_error(exc)
        if failures > settings.max_retries:
            logger.error("retries exhausted, marking FAILED", extra={**log_extra, "error": error})
            _finalize(
                db, event_pk, token,
                status=EventStatus.FAILED.value,
                retry_count=failures,
                error_message=error,
                next_retry_at=None,
            )
            return ProcessResult(Outcome.FAILED)

        delay = compute_backoff(failures, settings)
        logger.warning("processing failed, scheduling retry", extra={**log_extra, "error": error, "retry_in": delay})
        owned = _finalize(
            db, event_pk, token,
            status=EventStatus.RETRYING.value,
            retry_count=failures,
            error_message=error,
            next_retry_at=utcnow() + timedelta(seconds=delay),
        )
        return ProcessResult(Outcome.RETRY, retry_in_seconds=delay) if owned else ProcessResult(Outcome.SKIPPED)

    owned = _finalize(
        db, event_pk, token,
        status=EventStatus.PROCESSED.value,
        processed_at=utcnow(),
        error_message=None,
        next_retry_at=None,
    )
    if owned:
        logger.info("event processed", extra=log_extra)
        return ProcessResult(Outcome.PROCESSED)
    return ProcessResult(Outcome.SKIPPED)


def find_stalled_event_ids(db: Session, settings: Settings | None = None, now: datetime | None = None) -> list[int]:
    """Events whose queue message was probably lost and need re-enqueueing.

    - RECEIVED for longer than the grace period (publish failed / message lost)
    - RETRYING whose retry time passed more than the grace period ago
    - PROCESSING with an expired lease (worker died mid-task)
    """
    settings = settings or get_settings()
    now = now or utcnow()
    grace = timedelta(seconds=settings.sweeper_grace_seconds)
    lease = timedelta(seconds=settings.processing_lease_seconds)
    stmt = (
        select(WebhookEvent.id)
        .where(
            or_(
                and_(WebhookEvent.status == EventStatus.RECEIVED.value, WebhookEvent.updated_at < now - grace),
                and_(WebhookEvent.status == EventStatus.RETRYING.value, WebhookEvent.next_retry_at < now - grace),
                and_(WebhookEvent.status == EventStatus.PROCESSING.value, WebhookEvent.locked_at < now - lease),
            )
        )
        .order_by(WebhookEvent.id)
        .limit(settings.sweeper_batch_size)
    )
    return list(db.scalars(stmt))


def _format_error(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"[:MAX_ERROR_LENGTH]
