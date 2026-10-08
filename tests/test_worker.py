"""End-to-end through the real Celery task, executed eagerly (in-process)."""
from datetime import timedelta

import pytest
from sqlalchemy import select

from app import processing, worker
from app.downstream import TransientDownstreamError
from app.models import EventStatus, WebhookEvent, utcnow

from .conftest import make_payload, post_webhook
from .test_processing import FakeDownstream, create_event


@pytest.fixture
def eager_celery():
    conf = worker.celery_app.conf
    previous = conf.task_always_eager
    conf.task_always_eager = True
    yield
    conf.task_always_eager = previous


def test_webhook_is_processed_by_worker(client, db, eager_celery, monkeypatch):
    downstream = FakeDownstream()
    monkeypatch.setattr(processing, "send_order_event", downstream)

    response = post_webhook(client, make_payload())

    assert response.status_code == 202
    event = db.scalar(select(WebhookEvent))
    assert event.status == EventStatus.PROCESSED.value
    assert len(downstream.calls) == 1


def test_worker_retries_transient_failures_until_success(client, db, eager_celery, monkeypatch):
    downstream = FakeDownstream(TransientDownstreamError("503"), TransientDownstreamError("503"))
    monkeypatch.setattr(processing, "send_order_event", downstream)

    post_webhook(client, make_payload())

    event = db.scalar(select(WebhookEvent))
    assert event.status == EventStatus.PROCESSED.value
    assert event.retry_count == 2
    assert len(downstream.calls) == 3


def test_worker_gives_up_after_retry_limit(client, db, eager_celery, monkeypatch, settings):
    downstream = FakeDownstream(*[TransientDownstreamError("503")] * 10)
    monkeypatch.setattr(processing, "send_order_event", downstream)

    post_webhook(client, make_payload())

    event = db.scalar(select(WebhookEvent))
    assert event.status == EventStatus.FAILED.value
    assert event.retry_count == settings.max_retries + 1
    assert len(downstream.calls) == settings.max_retries + 1


def test_sweeper_requeues_stalled_events(db, monkeypatch):
    stuck = create_event(db, "stuck", updated_at=utcnow() - timedelta(hours=1))
    create_event(db, "fresh")
    published = []
    monkeypatch.setattr(worker.process_event_task, "delay", published.append)

    assert worker.requeue_stalled_events() == 1
    assert published == [stuck.id]
