ALTER TABLE delivery_attempts
    DROP CONSTRAINT IF EXISTS delivery_attempts_channel_check;

ALTER TABLE notification_rules
    DROP CONSTRAINT IF EXISTS notification_rules_channel_check;

ALTER TABLE delivery_attempts
    DROP COLUMN IF EXISTS destination_config;
