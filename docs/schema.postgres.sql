BEGIN;

CREATE TABLE alembic_version (
    version_num VARCHAR(32) NOT NULL, 
    CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);

-- Running upgrade  -> 0001

CREATE TABLE webhook_events (
    id BIGSERIAL NOT NULL, 
    event_id VARCHAR(100) NOT NULL, 
    shop_id VARCHAR(100) NOT NULL, 
    event_type VARCHAR(100) NOT NULL, 
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload JSONB NOT NULL, 
    status VARCHAR(20) DEFAULT 'RECEIVED' NOT NULL, 
    retry_count INTEGER DEFAULT '0' NOT NULL, 
    error_message TEXT, 
    next_retry_at TIMESTAMP WITH TIME ZONE, 
    locked_at TIMESTAMP WITH TIME ZONE, 
    lock_token VARCHAR(36), 
    created_at TIMESTAMP WITH TIME ZONE NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL, 
    processed_at TIMESTAMP WITH TIME ZONE, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_webhook_events_event_id UNIQUE (event_id), 
    CONSTRAINT ck_webhook_events_status CHECK (status IN ('RECEIVED','PROCESSING','RETRYING','PROCESSED','FAILED')), 
    CONSTRAINT ck_webhook_events_retry_count CHECK (retry_count >= 0)
);

CREATE INDEX ix_webhook_events_status_updated_at ON webhook_events (status, updated_at);

CREATE INDEX ix_webhook_events_shop_id_created_at ON webhook_events (shop_id, created_at);

INSERT INTO alembic_version (version_num) VALUES ('0001') RETURNING alembic_version.version_num;

COMMIT;

