"""Simulated downstream order service (stands in for an external HTTP API).

Real integrations should map HTTP 5xx / timeouts / connection errors to
TransientDownstreamError and 4xx validation errors to PermanentDownstreamError,
and pass ``event_id`` as an idempotency key so a retried call after an
ambiguous failure (e.g. timeout after the remote committed) is not applied twice.
"""
import logging
import random
import time
from typing import Any

from .config import get_settings

logger = logging.getLogger(__name__)


class DownstreamError(Exception):
    """Base class for downstream failures."""


class TransientDownstreamError(DownstreamError):
    """Temporary failure (timeout, 5xx, rate limit) - safe to retry."""


class PermanentDownstreamError(DownstreamError):
    """Failure that retrying cannot fix (rejected payload, unknown shop)."""


def send_order_event(event: dict[str, Any]) -> None:
    settings = get_settings()
    if settings.downstream_latency_seconds:
        time.sleep(settings.downstream_latency_seconds)

    if random.random() < settings.downstream_failure_rate:
        raise TransientDownstreamError("downstream order service unavailable (simulated)")

    logger.info(
        "downstream accepted event",
        extra={"event_id": event["event_id"], "event_type": event["event_type"], "shop_id": event["shop_id"]},
    )
