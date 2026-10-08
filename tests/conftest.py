import json
import os
import tempfile
from pathlib import Path

# Configure the app *before* it is imported: settings and the engine are
# created at import time. Environment variables take precedence over .env.
_TMP_DIR = Path(tempfile.mkdtemp(prefix="webhook-tests-"))
os.environ.update(
    {
        "DATABASE_URL": f"sqlite:///{(_TMP_DIR / 'test.db').as_posix()}",
        "WEBHOOK_SECRET": "test-secret-0123456789abcdef",
        "CELERY_BROKER_URL": "memory://",
        "CELERY_TASK_ALWAYS_EAGER": "false",
        "DOWNSTREAM_FAILURE_RATE": "0",
        "DOWNSTREAM_LATENCY_SECONDS": "0",
        "MAX_RETRIES": "3",
        "LOG_LEVEL": "WARNING",
    }
)

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from app import main  # noqa: E402
from app.config import get_settings  # noqa: E402
from app.databases import Base, SessionLocal, engine  # noqa: E402
from app.security import compute_signature  # noqa: E402

SECRET = os.environ["WEBHOOK_SECRET"]


@pytest.fixture(autouse=True)
def fresh_schema():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    yield
    engine.dispose()


@pytest.fixture
def settings():
    return get_settings()


@pytest.fixture
def db():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def enqueued(monkeypatch):
    """Capture what the API publishes instead of talking to a broker."""
    published: list[int] = []

    def fake_enqueue(event_pk: int) -> bool:
        published.append(event_pk)
        return True

    monkeypatch.setattr(main, "enqueue_event", fake_enqueue)
    return published


@pytest.fixture
def client():
    with TestClient(main.app) as test_client:
        yield test_client


def make_payload(**overrides) -> dict:
    payload = {
        "event_id": "evt_10001",
        "event_type": "order.created",
        "shop_id": "shop_123",
        "timestamp": "2026-08-18T10:30:00Z",
        "data": {"order_id": "ORD-1001", "customer_id": "CUS-1001", "amount": 2500},
    }
    payload.update(overrides)
    return payload


def post_webhook(client, payload: dict | bytes, signature: str | None = "auto", path: str = "/webhooks/order"):
    body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}
    if signature == "auto":
        signature = compute_signature(body, SECRET)
    if signature is not None:
        headers["X-Webhook-Signature"] = signature
    return client.post(path, content=body, headers=headers)
