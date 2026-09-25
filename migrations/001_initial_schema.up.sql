-- PulseFlow Phase 2 schema. PostgreSQL is the source of truth for all
-- accepted events, durable routing work, retries, and terminal outcomes.

CREATE TABLE tenants (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        VARCHAR(255) NOT NULL,
    status      VARCHAR(20)  NOT NULL DEFAULT 'active',
    config      JSONB        NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CHECK (status IN ('active', 'suspended'))
);

CREATE TABLE api_keys (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   UUID         NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    key_hash    VARCHAR(64)  NOT NULL UNIQUE,
    key_prefix  VARCHAR(16)  NOT NULL,
    name        VARCHAR(255) NOT NULL,
    status      VARCHAR(20)  NOT NULL DEFAULT 'active',
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ,
    CHECK (status IN ('active', 'revoked'))
);
CREATE INDEX idx_api_keys_tenant_id ON api_keys(tenant_id);

CREATE TABLE events (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        UUID         NOT NULL REFERENCES tenants(id),
    type             VARCHAR(255) NOT NULL,
    idempotency_key  VARCHAR(255) NOT NULL,
    data             JSONB        NOT NULL,
    status           VARCHAR(20)  NOT NULL DEFAULT 'accepted',
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    processed_at     TIMESTAMPTZ,
    UNIQUE(tenant_id, idempotency_key),
    CHECK (status IN ('accepted', 'processing', 'completed', 'partially_failed', 'failed'))
);
CREATE INDEX idx_events_tenant_type ON events(tenant_id, type);
CREATE INDEX idx_events_tenant_created ON events(tenant_id, created_at DESC);

CREATE TABLE notification_rules (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   UUID         NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    event_type  VARCHAR(255) NOT NULL,
    channel     VARCHAR(50)  NOT NULL,
    config      JSONB        NOT NULL,
    enabled     BOOLEAN      NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX idx_rules_lookup ON notification_rules(tenant_id, event_type)
    WHERE enabled = true;

-- One row represents one logical delivery. attempt_number is incremented as
-- that delivery is retried; the row is not an individual HTTP-attempt log.
CREATE TABLE delivery_attempts (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id             UUID         NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    notification_rule_id UUID         NOT NULL REFERENCES notification_rules(id),
    tenant_id            UUID         NOT NULL REFERENCES tenants(id),
    channel              VARCHAR(50)  NOT NULL,
    status               VARCHAR(20)  NOT NULL DEFAULT 'pending',
    attempt_number       INT          NOT NULL DEFAULT 0,
    max_attempts         INT          NOT NULL DEFAULT 5,
    next_retry_at        TIMESTAMPTZ,
    locked_by            VARCHAR(255),
    locked_until         TIMESTAMPTZ,
    request_payload      JSONB,
    response_status      INT,
    response_body        TEXT,
    error_message        TEXT,
    duration_ms          INT,
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    completed_at         TIMESTAMPTZ,
    UNIQUE(event_id, notification_rule_id),
    CHECK (status IN ('pending', 'delivering', 'delivered', 'failed', 'retrying', 'dead_letter')),
    CHECK (attempt_number >= 0 AND max_attempts > 0)
);
CREATE INDEX idx_delivery_event ON delivery_attempts(event_id);
CREATE INDEX idx_delivery_tenant ON delivery_attempts(tenant_id);
CREATE INDEX idx_delivery_available ON delivery_attempts(status, next_retry_at, locked_until)
    WHERE status IN ('pending', 'retrying', 'delivering');

CREATE TABLE outbox (
    id               BIGSERIAL PRIMARY KEY,
    aggregate_id     UUID         NOT NULL,
    topic            VARCHAR(255) NOT NULL,
    partition_key    VARCHAR(255) NOT NULL,
    payload          JSONB        NOT NULL,
    status           VARCHAR(20)  NOT NULL DEFAULT 'pending',
    publish_attempts INT          NOT NULL DEFAULT 0,
    available_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    locked_by        VARCHAR(255),
    locked_until     TIMESTAMPTZ,
    last_error       TEXT,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at     TIMESTAMPTZ,
    CHECK (status IN ('pending', 'publishing', 'published')),
    CHECK (publish_attempts >= 0)
);
CREATE INDEX idx_outbox_available ON outbox(available_at, created_at)
    WHERE status <> 'published';
