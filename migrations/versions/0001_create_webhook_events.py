"""create webhook_events

Revision ID: 0001
Revises:
Create Date: 2026-10-08
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

utc_datetime = sa.DateTime(timezone=True)


def upgrade() -> None:
    op.create_table(
        "webhook_events",
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), primary_key=True, autoincrement=True),
        sa.Column("event_id", sa.String(100), nullable=False),
        sa.Column("shop_id", sa.String(100), nullable=False),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("occurred_at", utc_datetime, nullable=False),
        sa.Column("payload", sa.JSON().with_variant(postgresql.JSONB(), "postgresql"), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="RECEIVED"),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("next_retry_at", utc_datetime, nullable=True),
        sa.Column("locked_at", utc_datetime, nullable=True),
        sa.Column("lock_token", sa.String(36), nullable=True),
        sa.Column("created_at", utc_datetime, nullable=False),
        sa.Column("updated_at", utc_datetime, nullable=False),
        sa.Column("processed_at", utc_datetime, nullable=True),
        sa.UniqueConstraint("event_id", name="uq_webhook_events_event_id"),
        sa.CheckConstraint(
            "status IN ('RECEIVED','PROCESSING','RETRYING','PROCESSED','FAILED')",
            name="ck_webhook_events_status",
        ),
        sa.CheckConstraint("retry_count >= 0", name="ck_webhook_events_retry_count"),
    )
    op.create_index("ix_webhook_events_status_updated_at", "webhook_events", ["status", "updated_at"])
    op.create_index("ix_webhook_events_shop_id_created_at", "webhook_events", ["shop_id", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_webhook_events_shop_id_created_at", table_name="webhook_events")
    op.drop_index("ix_webhook_events_status_updated_at", table_name="webhook_events")
    op.drop_table("webhook_events")
