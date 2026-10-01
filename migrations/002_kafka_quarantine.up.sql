-- Invalid Kafka messages must become durable before their source offsets are
-- committed. The matching dead-letter publication is written to outbox in the
-- same transaction by QuarantineMessage.
CREATE TABLE quarantined_messages (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_topic     VARCHAR(255) NOT NULL,
    source_partition INT          NOT NULL,
    source_offset    BIGINT       NOT NULL,
    message_key      BYTEA,
    payload          BYTEA        NOT NULL,
    error_message    TEXT         NOT NULL,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE(source_topic, source_partition, source_offset),
    CHECK (source_partition >= 0),
    CHECK (source_offset >= 0)
);

CREATE INDEX idx_quarantined_messages_created
    ON quarantined_messages(created_at DESC);
