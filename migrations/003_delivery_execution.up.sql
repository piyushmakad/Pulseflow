-- Snapshot the destination configuration when routing creates delivery work.
-- Pending work must not change destination if an administrator later edits a
-- notification rule.
ALTER TABLE delivery_attempts
    ADD COLUMN destination_config JSONB;

UPDATE delivery_attempts AS d
SET destination_config = r.config
FROM notification_rules AS r
WHERE r.id = d.notification_rule_id;

ALTER TABLE delivery_attempts
    ALTER COLUMN destination_config SET NOT NULL;

ALTER TABLE notification_rules
    ADD CONSTRAINT notification_rules_channel_check
    CHECK (channel IN ('webhook', 'email'));

ALTER TABLE delivery_attempts
    ADD CONSTRAINT delivery_attempts_channel_check
    CHECK (channel IN ('webhook', 'email'));
