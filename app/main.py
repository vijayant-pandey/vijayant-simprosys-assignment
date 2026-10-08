import json
import logging
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from .config import get_settings
from .databases import get_db
from .ingest import store_event
from .logging_config import configure_logging
from .models import WebhookEvent
from .schemas import EventStatusResponse, HealthResponse, WebhookAccepted, WebhookRequest
from .security import verify_signature
from .worker import enqueue_event

settings = get_settings()
configure_logging(settings.log_level)
logger = logging.getLogger(__name__)

# Schema is managed by Alembic migrations (`alembic upgrade head`), not create_all().
app = FastAPI(title="Webhook Ingestion Service", version="1.0.0")


@app.get("/")
def home():
    return {"message": "Webhook running"}


@app.get("/health", response_model=HealthResponse)
def health(response: Response, db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        return HealthResponse(status="ok", database="ok")
    except Exception:
        logger.exception("health check: database unavailable")
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return HealthResponse(status="degraded", database="unavailable")


async def verified_order_webhook(request: Request) -> tuple[WebhookRequest, dict[str, Any]]:
    """Authenticate first, then parse.

    The HMAC is checked against the raw bytes *before* any JSON parsing or
    validation, so unauthenticated callers can't probe our validation rules or
    make us do parsing work.
    """
    declared_length = request.headers.get("content-length")
    if declared_length and declared_length.isdigit() and int(declared_length) > settings.max_body_bytes:
        raise HTTPException(status.HTTP_413_CONTENT_TOO_LARGE, "Payload too large")

    body = await request.body()
    if len(body) > settings.max_body_bytes:
        raise HTTPException(status.HTTP_413_CONTENT_TOO_LARGE, "Payload too large")

    if not verify_signature(body, request.headers.get(settings.signature_header)):
        logger.warning("rejected webhook with invalid signature", extra={"client": getattr(request.client, "host", None)})
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid webhook signature")

    try:
        raw = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Body is not valid JSON")
    if not isinstance(raw, dict):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "Body must be a JSON object")

    try:
        webhook = WebhookRequest.model_validate(raw)
    except ValidationError as exc:
        raise RequestValidationError(exc.errors(include_url=False))
    return webhook, raw


@app.post(
    "/webhooks/order",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=WebhookAccepted,
    responses={
        200: {"model": WebhookAccepted, "description": "Duplicate event_id; already accepted earlier"},
        401: {"description": "Missing or invalid signature"},
    },
)
def receive_order_webhook(
    response: Response,
    verified: tuple[WebhookRequest, dict[str, Any]] = Depends(verified_order_webhook),
    db: Session = Depends(get_db),
) -> WebhookAccepted:
    webhook, raw = verified
    result = store_event(db, webhook, raw)
    event = result.event

    if not result.created:
        # 2xx so the sender stops redelivering; nothing is enqueued again.
        logger.info("duplicate webhook ignored", extra={"event_id": event.event_id, "status": event.status})
        response.status_code = status.HTTP_200_OK
        return WebhookAccepted(
            event_id=event.event_id, status=event.status, duplicate=True, message="Event already received"
        )

    # The row is committed before we publish, so the event can never be lost:
    # if publishing fails the sweeper re-enqueues it.
    enqueue_event(event.id)
    logger.info("webhook accepted", extra={"event_id": event.event_id, "shop_id": event.shop_id})
    return WebhookAccepted(event_id=event.event_id, status=event.status, duplicate=False, message="Webhook accepted")


# Backwards-compatible alias for the route this service originally exposed.
app.add_api_route(
    "/webhook/orders",
    receive_order_webhook,
    methods=["POST"],
    status_code=status.HTTP_202_ACCEPTED,
    response_model=WebhookAccepted,
    include_in_schema=False,
)


@app.get("/webhooks/events/{event_id}", response_model=EventStatusResponse)
def get_event_status(event_id: str, db: Session = Depends(get_db)):
    """Operational lookup of an event's processing state (payload is not exposed).
    In production this belongs behind internal auth / the admin network."""
    event = db.scalar(select(WebhookEvent).where(WebhookEvent.event_id == event_id))
    if event is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Event not found")
    return EventStatusResponse.model_validate(event, from_attributes=True)
