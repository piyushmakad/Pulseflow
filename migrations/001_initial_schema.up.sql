-- Tenants
CREATE TABLE tenants (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        VARCHAR(255) NOT NULL,
    status      VARCHAR(20)  NOT NULL DEFAULT 'active',
    config      JSONB        NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- API Keys
CREATE TABLE api_keys (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   UUID         NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    key_hash    VARCHAR(64)  NOT NULL UNIQUE,
    key_prefix  VARCHAR(8)   NOT NULL,
    name        VARCHAR(255) NOT NULL,
    status      VARCHAR(20)  NOT NULL DEFAULT 'active',
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ
);
CREATE INDEX idx_api_keys_hash ON api_keys(key_hash);
CREATE INDEX idx_api_keys_tenant_id ON api_keys(tenant_id);

-- Events
CREATE TABLE events (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        UUID         NOT NULL REFERENCES tenants(id),
    type             VARCHAR(255) NOT NULL,
    idempotency_key  VARCHAR(255) NOT NULL,
    data             JSONB        NOT NULL,
    status           VARCHAR(20)  NOT NULL DEFAULT 'accepted',
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    processed_at     TIMESTAMPTZ,
    UNIQUE(tenant_id, idempotency_key)
);
CREATE INDEX idx_events_tenant_type ON events(tenant_id, type);
CREATE INDEX idx_events_tenant_created ON events(tenant_id, created_at DESC);

-- Notification Rules
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

-- Delivery Attempts
CREATE TABLE delivery_attempts (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id             UUID         NOT NULL REFERENCES events(id),
    notification_rule_id UUID         NOT NULL REFERENCES notification_rules(id),
    tenant_id            UUID         NOT NULL REFERENCES tenants(id),
    channel              VARCHAR(50)  NOT NULL,
    status               VARCHAR(20)  NOT NULL DEFAULT 'pending',
    attempt_number       INT          NOT NULL DEFAULT 0,
    max_attempts         INT          NOT NULL DEFAULT 5,
    next_retry_at        TIMESTAMPTZ,
    request_payload      JSONB,
    response_status      INT,
    response_body        TEXT,
    error_message        TEXT,
    duration_ms          INT,
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    completed_at         TIMESTAMPTZ
);
CREATE INDEX idx_delivery_event ON delivery_attempts(event_id);
CREATE INDEX idx_delivery_tenant ON delivery_attempts(tenant_id);
CREATE INDEX idx_delivery_retry ON delivery_attempts(status, next_retry_at)
    WHERE status = 'retrying';

-- Transactional Outbox
CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    aggregate_id UUID         NOT NULL,
    topic        VARCHAR(255) NOT NULL,
    payload      JSONB        NOT NULL,
    published    BOOLEAN      NOT NULL DEFAULT false,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX idx_outbox_unpublished ON outbox(created_at)
    WHERE published = false;
