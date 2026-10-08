import json

import pytest
from sqlalchemy import func, select

from app import worker
from app.models import EventStatus, WebhookEvent
from app.security import compute_signature

from .conftest import SECRET, make_payload, post_webhook


def count_events(db) -> int:
    return db.scalar(select(func.count()).select_from(WebhookEvent))


def test_valid_webhook_is_stored_and_enqueued(client, db, enqueued):
    response = post_webhook(client, make_payload())

    assert response.status_code == 202
    assert response.json() == {
        "event_id": "evt_10001",
        "status": "RECEIVED",
        "duplicate": False,
        "message": "Webhook accepted",
    }
    event = db.scalar(select(WebhookEvent).where(WebhookEvent.event_id == "evt_10001"))
    assert event.status == EventStatus.RECEIVED.value
    assert event.shop_id == "shop_123"
    assert event.event_type == "order.created"
    assert event.retry_count == 0
    assert event.payload == make_payload()
    assert event.occurred_at.isoformat() == "2026-08-18T10:30:00+00:00"
    assert enqueued == [event.id]


def test_signature_with_sha256_prefix_is_accepted(client, enqueued):
    body = json.dumps(make_payload()).encode()
    response = post_webhook(client, body, signature="sha256=" + compute_signature(body, SECRET))
    assert response.status_code == 202


@pytest.mark.parametrize(
    "signature",
    [None, "", "not-a-signature", "0" * 64, compute_signature(b"other body", SECRET)],
    ids=["missing", "empty", "garbage", "zeros", "signature-of-other-body"],
)
def test_invalid_signature_is_rejected(client, db, enqueued, signature):
    response = post_webhook(client, make_payload(), signature=signature)

    assert response.status_code == 401
    assert count_events(db) == 0
    assert enqueued == []


def test_signature_with_wrong_secret_is_rejected(client, db, enqueued):
    body = json.dumps(make_payload()).encode()
    response = post_webhook(client, body, signature=compute_signature(body, "some-other-secret-value"))
    assert response.status_code == 401
    assert count_events(db) == 0


def test_tampered_body_is_rejected(client, db, enqueued):
    original = json.dumps(make_payload()).encode()
    tampered = original.replace(b"2500", b"1")
    response = post_webhook(client, tampered, signature=compute_signature(original, SECRET))
    assert response.status_code == 401
    assert count_events(db) == 0


def test_signature_is_checked_before_payload_validation(client, enqueued):
    # An unauthenticated caller must not learn anything about our validation rules.
    response = post_webhook(client, {"junk": True}, signature="bad")
    assert response.status_code == 401


@pytest.mark.parametrize(
    "payload",
    [
        make_payload(event_id=""),
        make_payload(event_id="x" * 101),
        make_payload(event_type="refund.created"),
        make_payload(timestamp="not-a-date"),
        make_payload(timestamp="2026-08-18T10:30:00"),  # naive timestamp
        make_payload(data={"order_id": "ORD-1", "customer_id": "CUS-1", "amount": -1}),
        make_payload(data={"order_id": "ORD-1", "customer_id": "CUS-1"}),
        {k: v for k, v in make_payload().items() if k != "shop_id"},
    ],
    ids=["empty-id", "long-id", "wrong-type", "bad-date", "naive-date", "negative-amount", "no-amount", "no-shop"],
)
def test_invalid_payload_with_valid_signature_returns_422(client, db, enqueued, payload):
    response = post_webhook(client, payload)
    assert response.status_code == 422
    assert count_events(db) == 0
    assert enqueued == []


@pytest.mark.parametrize("body", [b"{not json", b"[1, 2, 3]"])
def test_non_object_json_is_rejected(client, db, enqueued, body):
    response = post_webhook(client, body)
    assert response.status_code in (400, 422)
    assert count_events(db) == 0


def test_oversized_body_is_rejected(client, db, enqueued, settings):
    payload = make_payload(data={"order_id": "O", "customer_id": "C", "amount": 1, "note": "x" * settings.max_body_bytes})
    response = post_webhook(client, payload)
    assert response.status_code == 413
    assert count_events(db) == 0


def test_duplicate_event_id_is_stored_and_enqueued_once(client, db, enqueued):
    first = post_webhook(client, make_payload())
    second = post_webhook(client, make_payload())

    assert first.status_code == 202
    assert second.status_code == 200
    assert second.json()["duplicate"] is True
    assert second.json()["message"] == "Event already received"
    assert count_events(db) == 1
    assert len(enqueued) == 1


def test_duplicate_reports_current_status(client, db, enqueued):
    post_webhook(client, make_payload())
    event = db.scalar(select(WebhookEvent))
    event.status = EventStatus.PROCESSED.value
    db.commit()

    response = post_webhook(client, make_payload())
    assert response.status_code == 200
    assert response.json()["status"] == "PROCESSED"


def test_broker_outage_still_accepts_event(client, db, monkeypatch):
    """The event is committed before publishing; a broker failure must not lose
    it or fail the request - the sweeper re-enqueues it later."""

    def broker_down(*args, **kwargs):
        raise ConnectionError("broker unreachable")

    monkeypatch.setattr(worker.process_event_task, "delay", broker_down)

    response = post_webhook(client, make_payload())

    assert response.status_code == 202
    event = db.scalar(select(WebhookEvent))
    assert event.status == EventStatus.RECEIVED.value


def test_legacy_route_still_works(client, enqueued):
    response = post_webhook(client, make_payload(), path="/webhook/orders")
    assert response.status_code == 202


def test_event_status_endpoint(client, enqueued):
    assert client.get("/webhooks/events/evt_10001").status_code == 404

    post_webhook(client, make_payload())
    response = client.get("/webhooks/events/evt_10001")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "RECEIVED"
    assert body["retry_count"] == 0
    assert "payload" not in body


def test_health_and_root(client):
    assert client.get("/health").json() == {"status": "ok", "database": "ok"}
    assert client.get("/").json() == {"message": "Webhook running"}
