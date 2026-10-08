"""Storing incoming webhooks exactly once."""
import logging
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .models import EventStatus, WebhookEvent
from .schemas import WebhookRequest

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IngestResult:
    event: WebhookEvent
    created: bool


def store_event(db: Session, webhook: WebhookRequest, raw_payload: dict[str, Any]) -> IngestResult:
    """Insert the event, or return the existing row if ``event_id`` was seen before.

    We deliberately do *not* "SELECT, then INSERT if missing": two concurrent
    deliveries would both see "missing". Instead we INSERT and let the UNIQUE
    constraint on ``event_id`` arbitrate; the loser gets an IntegrityError and
    reads the winner's row.
    """
    event = WebhookEvent(
        event_id=webhook.event_id,
        shop_id=webhook.shop_id,
        event_type=webhook.event_type,
        occurred_at=webhook.timestamp,
        payload=raw_payload,
        status=EventStatus.RECEIVED.value,
        retry_count=0,
    )
    db.add(event)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        existing = db.scalar(select(WebhookEvent).where(WebhookEvent.event_id == webhook.event_id))
        if existing is None:
            # The violation was not the event_id uniqueness - don't hide it.
            raise
        if existing.payload != raw_payload:
            logger.warning(
                "duplicate event_id with a different payload; keeping the first version",
                extra={"event_id": webhook.event_id},
            )
        return IngestResult(event=existing, created=False)
    return IngestResult(event=event, created=True)
