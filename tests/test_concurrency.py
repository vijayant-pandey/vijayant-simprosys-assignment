"""Concurrency / idempotency: many threads racing on the same event, each with
its own DB session (= its own connection), like separate API instances or workers."""
import threading
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import func, select

from app.databases import SessionLocal
from app.ingest import store_event
from app.models import EventStatus, WebhookEvent
from app.processing import Outcome, claim_event, process_event
from app.schemas import WebhookRequest

from .conftest import make_payload
from .test_processing import create_event

THREADS = 8


def run_concurrently(fn, n=THREADS):
    barrier = threading.Barrier(n)

    def task(i):
        barrier.wait()  # release all threads at the same moment
        return fn(i)

    with ThreadPoolExecutor(max_workers=n) as pool:
        return list(pool.map(task, range(n)))


def test_concurrent_duplicate_deliveries_store_exactly_one_row(db):
    payload = make_payload()
    webhook = WebhookRequest.model_validate(payload)

    def deliver(_):
        session = SessionLocal()
        try:
            return store_event(session, webhook, payload).created
        finally:
            session.close()

    created = run_concurrently(deliver)

    assert created.count(True) == 1
    assert db.scalar(select(func.count()).select_from(WebhookEvent)) == 1


def test_only_one_worker_can_claim_an_event(db, settings):
    event = create_event(db)

    def claim(_):
        session = SessionLocal()
        try:
            return claim_event(session, event.id, settings)
        finally:
            session.close()

    tokens = run_concurrently(claim)

    assert sum(token is not None for token in tokens) == 1


def test_duplicate_task_deliveries_call_downstream_once(db, settings):
    event = create_event(db)
    calls = []
    lock = threading.Lock()

    def downstream(snapshot):
        with lock:
            calls.append(snapshot["event_id"])

    def work(_):
        session = SessionLocal()
        try:
            return process_event(session, event.id, downstream=downstream, settings=settings).outcome
        finally:
            session.close()

    outcomes = run_concurrently(work)

    assert outcomes.count(Outcome.PROCESSED) == 1
    assert outcomes.count(Outcome.SKIPPED) == THREADS - 1
    assert calls == ["evt_1"]
    db.expire_all()
    assert db.get(WebhookEvent, event.id).status == EventStatus.PROCESSED.value
