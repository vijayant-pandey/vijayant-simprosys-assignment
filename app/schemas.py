from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints

# Identifiers are opaque strings from the sender; bound their size to match the
# DB columns and keep them to a safe character set.
Identifier = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=100, pattern=r"^[A-Za-z0-9_.:\-]+$"),
]


class OrderData(BaseModel):
    # Unknown fields inside "data" are kept (stored as-is in the payload column)
    # so new optional attributes from the platform don't cause rejections.
    model_config = ConfigDict(extra="allow")

    order_id: Identifier
    customer_id: Identifier
    # Decimal, not float: money must not suffer binary rounding.
    amount: Decimal = Field(ge=0, max_digits=18, decimal_places=4)


class WebhookRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    event_id: Identifier
    event_type: Annotated[str, StringConstraints(pattern=r"^order\.[a-z_]+$", max_length=100)]
    shop_id: Identifier
    # Must carry a timezone (e.g. "...Z"); naive timestamps are ambiguous.
    timestamp: AwareDatetime
    data: OrderData


class WebhookAccepted(BaseModel):
    event_id: str
    status: str
    duplicate: bool
    message: str


class EventStatusResponse(BaseModel):
    event_id: str
    shop_id: str
    event_type: str
    status: str
    retry_count: int
    error_message: str | None
    next_retry_at: datetime | None
    created_at: datetime
    updated_at: datetime
    processed_at: datetime | None


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    database: Literal["ok", "unavailable"]
