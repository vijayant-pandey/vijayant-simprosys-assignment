"""Celery application and tasks.

Run a worker:   celery -A app.worker worker --loglevel=INFO   (add --pool=solo on Windows)
Run the beat:   celery -A app.worker beat --loglevel=INFO      (schedules the sweeper)
"""
import logging

from celery import Celery, signals

from .config import get_settings
from .databases import session_scope
from .logging_config import configure_logging
from .processing import Outcome, find_stalled_event_ids, process_event

logger = logging.getLogger(__name__)
settings = get_settings()

PROCESS_EVENT_TASK = "webhooks.process_event"
SWEEP_TASK = "webhooks.requeue_stalled_events"

celery_app = Celery("webhook_service", broker=settings.celery_broker_url)
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    # We track results in our own table; a result backend would be redundant.
    task_ignore_result=True,
    # Ack only after the task finishes, so a worker crash causes redelivery
    # instead of a lost event. Safe because processing is idempotent.
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    # Don't let one worker hoard messages it can't start yet.
    worker_prefetch_multiplier=1,
    task_always_eager=settings.celery_task_always_eager,
    task_eager_propagates=True,
    broker_connection_retry_on_startup=True,
    # Fail fast when the broker is down so the API isn't blocked; the sweeper
    # picks up anything that couldn't be published.
    broker_transport_options={"max_retries": 2, "interval_start": 0, "interval_step": 0.5, "interval_max": 1},
    beat_schedule={
        "requeue-stalled-events": {
            "task": SWEEP_TASK,
            "schedule": float(settings.sweeper_interval_seconds),
        }
    },
    timezone="UTC",
)


@signals.setup_logging.connect
def _setup_worker_logging(**_kwargs) -> None:
    # Same JSON log format as the API instead of Celery's default handlers.
    configure_logging(settings.log_level)


@celery_app.task(name=PROCESS_EVENT_TASK)
def process_event_task(event_pk: int) -> str:
    with session_scope() as db:
        result = process_event(db, event_pk)
    if result.outcome is Outcome.RETRY:
        # The retry limit and attempt history live in the database (retry_count),
        # so they survive redeliveries and worker restarts. Celery only schedules
        # the delayed re-run; if that publish fails, the sweeper picks the event
        # up once next_retry_at has passed.
        try:
            process_event_task.apply_async((event_pk,), countdown=result.retry_in_seconds)
        except Exception:
            logger.exception("failed to schedule retry; sweeper will retry", extra={"event_pk": event_pk})
    return result.outcome.value


@celery_app.task(name=SWEEP_TASK)
def requeue_stalled_events() -> int:
    with session_scope() as db:
        ids = find_stalled_event_ids(db)
    for event_pk in ids:
        process_event_task.delay(event_pk)
    if ids:
        logger.warning("re-enqueued stalled events", extra={"count": len(ids)})
    return len(ids)


def enqueue_event(event_pk: int) -> bool:
    """Publish an event for processing. Never raises: the row is already
    durably stored, so a publish failure is recovered by the sweeper."""
    try:
        process_event_task.delay(event_pk)
        return True
    except Exception:
        logger.exception("failed to enqueue event; sweeper will retry", extra={"event_pk": event_pk})
        return False
