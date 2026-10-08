from datetime import datetime, timedelta, timezone

import pytest

from app.downstream import PermanentDownstreamError, TransientDownstreamError
from app.models import EventStatus, WebhookEvent, utcnow
from app.processing import (
    Outcome,
    _finalize,
    claim_event,
    compute_backoff,
    find_stalled_event_ids,
    process_event,
)


def create_event(db, event_id="evt_1", **fields) -> WebhookEvent:
    event = WebhookEvent(
        event_id=event_id,
        shop_id="shop_1",
        event_type="order.created",
        occurred_at=datetime(2026, 8, 18, 10, 30, tzinfo=timezone.utc),
        payload={"event_id": event_id},
        **fields,
    )
    db.add(event)
    db.commit()
    return event


def reload(db, event: WebhookEvent) -> WebhookEvent:
    db.expire_all()
    return db.get(WebhookEvent, event.id)


class FakeDownstream:
    """Fails with the given exceptions in order, then succeeds."""

    def __init__(self, *failures: Exception):
        self.failures = list(failures)
        self.calls: list[dict] = []

    def __call__(self, event: dict) -> None:
        self.calls.append(event)
        if self.failures:
            raise self.failures.pop(0)


def test_successful_processing(db, settings):
    event = create_event(db)
    downstream = FakeDownstream()

    result = process_event(db, event.id, downstream=downstream, settings=settings)

    assert result.outcome is Outcome.PROCESSED
    event = reload(db, event)
    assert event.status == EventStatus.PROCESSED.value
    assert event.processed_at is not None
    assert event.retry_count == 0
    assert event.error_message is None
    assert event.lock_token is None and event.locked_at is None
    assert downstream.calls == [
        {"event_id": "evt_1", "event_type": "order.created", "shop_id": "shop_1", "payload": {"event_id": "evt_1"}}
    ]


def test_transient_failure_is_recorded_then_retried_successfully(db, settings):
    event = create_event(db)
    downstream = FakeDownstream(TransientDownstreamError("timeout"))

    first = process_event(db, event.id, downstream=downstream, settings=settings)

    assert first.outcome is Outcome.RETRY
    assert first.retry_in_seconds >= 1
    event = reload(db, event)
    assert event.status == EventStatus.RETRYING.value
    assert event.retry_count == 1
    assert event.error_message == "TransientDownstreamError: timeout"
    assert event.next_retry_at > utcnow()

    second = process_event(db, event.id, downstream=downstream, settings=settings)

    assert second.outcome is Outcome.PROCESSED
    event = reload(db, event)
    assert event.status == EventStatus.PROCESSED.value
    assert event.retry_count == 1  # history of the failed attempt is kept
    assert event.error_message is None
    assert event.next_retry_at is None
    assert len(downstream.calls) == 2


def test_unexpected_exceptions_are_retried(db, settings):
    event = create_event(db)
    result = process_event(db, event.id, downstream=FakeDownstream(KeyError("boom")), settings=settings)
    assert result.outcome is Outcome.RETRY
    assert reload(db, event).error_message == "KeyError: 'boom'"


def test_marked_failed_after_retry_limit(db, settings):
    assert settings.max_retries == 3
    event = create_event(db)
    downstream = FakeDownstream(*[TransientDownstreamError(f"attempt {i}") for i in range(10)])

    outcomes = [process_event(db, event.id, downstream=downstream, settings=settings).outcome for _ in range(4)]

    assert outcomes == [Outcome.RETRY, Outcome.RETRY, Outcome.RETRY, Outcome.FAILED]
    event = reload(db, event)
    assert event.status == EventStatus.FAILED.value
    assert event.retry_count == 4  # 1 initial attempt + 3 retries
    assert event.error_message == "TransientDownstreamError: attempt 3"
    assert event.next_retry_at is None

    # FAILED is terminal: further deliveries do nothing.
    assert process_event(db, event.id, downstream=downstream, settings=settings).outcome is Outcome.SKIPPED
    assert len(downstream.calls) == 4


def test_permanent_failure_is_not_retried(db, settings):
    event = create_event(db)
    downstream = FakeDownstream(PermanentDownstreamError("unknown shop"))

    result = process_event(db, event.id, downstream=downstream, settings=settings)

    assert result.outcome is Outcome.FAILED
    event = reload(db, event)
    assert event.status == EventStatus.FAILED.value
    assert event.retry_count == 1
    assert event.error_message == "PermanentDownstreamError: unknown shop"


def test_processed_event_is_not_processed_again(db, settings):
    event = create_event(db)
    downstream = FakeDownstream()

    assert process_event(db, event.id, downstream=downstream, settings=settings).outcome is Outcome.PROCESSED
    assert process_event(db, event.id, downstream=downstream, settings=settings).outcome is Outcome.SKIPPED
    assert len(downstream.calls) == 1


def test_event_in_flight_elsewhere_is_skipped(db, settings):
    event = create_event(db)
    assert claim_event(db, event.id, settings) is not None
    downstream = FakeDownstream()

    assert process_event(db, event.id, downstream=downstream, settings=settings).outcome is Outcome.SKIPPED
    assert downstream.calls == []


def test_missing_event_is_skipped(db, settings):
    assert process_event(db, 999, downstream=FakeDownstream(), settings=settings).outcome is Outcome.SKIPPED


def test_expired_lease_is_reclaimed_and_stale_worker_is_fenced_off(db, settings):
    event = create_event(db)
    stale_token = claim_event(db, event.id, settings)

    # Simulate the first worker hanging past its lease.
    event = reload(db, event)
    event.locked_at = utcnow() - timedelta(seconds=settings.processing_lease_seconds + 1)
    db.commit()

    new_token = claim_event(db, event.id, settings)
    assert new_token is not None and new_token != stale_token

    # The stale worker wakes up and tries to record its result: rejected.
    assert _finalize(db, event.id, stale_token, status=EventStatus.FAILED.value) is False
    assert reload(db, event).status == EventStatus.PROCESSING.value

    assert _finalize(db, event.id, new_token, status=EventStatus.PROCESSED.value) is True
    assert reload(db, event).status == EventStatus.PROCESSED.value


def test_find_stalled_events(db, settings):
    now = utcnow()
    long_ago = now - timedelta(seconds=settings.sweeper_grace_seconds + settings.processing_lease_seconds + 60)

    fresh = create_event(db, "fresh")
    stuck_received = create_event(db, "stuck_received", updated_at=long_ago)
    due_retry = create_event(db, "due_retry", status="RETRYING", next_retry_at=long_ago)
    future_retry = create_event(db, "future_retry", status="RETRYING", next_retry_at=now + timedelta(minutes=5))
    dead_worker = create_event(db, "dead_worker", status="PROCESSING", locked_at=long_ago, lock_token="t")
    live_worker = create_event(db, "live_worker", status="PROCESSING", locked_at=now, lock_token="t")
    done = create_event(db, "done", status="PROCESSED", updated_at=long_ago)
    failed = create_event(db, "failed", status="FAILED", updated_at=long_ago)

    stalled = set(find_stalled_event_ids(db, settings, now=now))

    assert stalled == {stuck_received.id, due_retry.id, dead_worker.id}
    assert not stalled & {fresh.id, future_retry.id, live_worker.id, done.id, failed.id}


@pytest.mark.parametrize("retry_count", [1, 2, 5, 20])
def test_backoff_is_bounded(settings, retry_count):
    ceiling = min(settings.retry_backoff_max_seconds, settings.retry_backoff_base_seconds * 2 ** (retry_count - 1))
    for _ in range(50):
        assert 1.0 <= compute_backoff(retry_count, settings) <= max(1.0, ceiling)
