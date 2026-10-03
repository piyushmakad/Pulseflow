package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all application configuration.
// Loaded from environment variables with sensible defaults.
type Config struct {
	// HTTP server
	HTTPPort         int           `json:"http_port"`
	HTTPReadTimeout  time.Duration `json:"http_read_timeout"`
	HTTPWriteTimeout time.Duration `json:"http_write_timeout"`
	HTTPIdleTimeout  time.Duration `json:"http_idle_timeout"`
	HTTPMaxBodyBytes int           `json:"http_max_body_bytes"`

	// PostgreSQL
	DatabaseURL     string `json:"database_url"`
	DatabaseMaxConn int    `json:"database_max_conn"`

	// Redis
	RedisURL       string        `json:"redis_url"`
	APIKeyCacheTTL time.Duration `json:"api_key_cache_ttl"`

	// Kafka
	KafkaBrokers         string        `json:"kafka_brokers"`
	KafkaEventsTopic     string        `json:"kafka_events_topic"`
	KafkaDeadLetterTopic string        `json:"kafka_deadletter_topic"`
	KafkaConsumerGroup   string        `json:"kafka_consumer_group"`
	KafkaConsumerRetry   time.Duration `json:"kafka_consumer_retry"`

	// Outbox relay
	OutboxPollInterval time.Duration `json:"outbox_poll_interval"`
	OutboxBatchSize    int           `json:"outbox_batch_size"`
	OutboxLease        time.Duration `json:"outbox_lease"`
	OutboxRetryDelay   time.Duration `json:"outbox_retry_delay"`

	// Worker pools
	WebhookWorkerCount         int           `json:"webhook_worker_count"`
	WebhookBufferSize          int           `json:"webhook_buffer_size"`
	WebhookTimeout             time.Duration `json:"webhook_timeout"`
	EmailWorkerCount           int           `json:"email_worker_count"`
	EmailBufferSize            int           `json:"email_buffer_size"`
	EmailTimeout               time.Duration `json:"email_timeout"`
	DeliveryPollInterval       time.Duration `json:"delivery_poll_interval"`
	DeliveryBatchSize          int           `json:"delivery_batch_size"`
	DeliveryLease              time.Duration `json:"delivery_lease"`
	DeliveryRecoveryInterval   time.Duration `json:"delivery_recovery_interval"`
	DeliveryFinalizeInterval   time.Duration `json:"delivery_finalize_interval"`
	DeliveryPersistenceTimeout time.Duration `json:"delivery_persistence_timeout"`

	// Rate limiting
	RateLimitRequests int           `json:"rate_limit_requests"`
	RateLimitWindow   time.Duration `json:"rate_limit_window"`

	// Retry policy
	RetryMaxAttempts int           `json:"retry_max_attempts"`
	RetryBaseDelay   time.Duration `json:"retry_base_delay"`
	RetryMaxDelay    time.Duration `json:"retry_max_delay"`

	// Operational visibility
	ObservabilityInterval        time.Duration `json:"observability_interval"`
	ObservabilityQueryTimeout    time.Duration `json:"observability_query_timeout"`
	AlertOutboxPending           int64         `json:"alert_outbox_pending"`
	AlertOutboxOldestAge         time.Duration `json:"alert_outbox_oldest_age"`
	AlertExpiredOutboxLeases     int64         `json:"alert_expired_outbox_leases"`
	AlertDueDeliveries           int64         `json:"alert_due_deliveries"`
	AlertExpiredDeliveryLeases   int64         `json:"alert_expired_delivery_leases"`
	AlertPostgresPoolUtilization float64       `json:"alert_postgres_pool_utilization"`

	// Logging
	LogLevel  string `json:"log_level"`
	LogFormat string `json:"log_format"`

	// Graceful shutdown
	ShutdownTimeout time.Duration `json:"shutdown_timeout"`
}

// Load reads configuration from environment variables with defaults.
func Load() (*Config, error) {
	cfg := &Config{
		// HTTP defaults
		HTTPPort:         envInt("HTTP_PORT", 8080),
		HTTPReadTimeout:  envDuration("HTTP_READ_TIMEOUT", 10*time.Second),
		HTTPWriteTimeout: envDuration("HTTP_WRITE_TIMEOUT", 30*time.Second),
		HTTPIdleTimeout:  envDuration("HTTP_IDLE_TIMEOUT", 60*time.Second),
		HTTPMaxBodyBytes: envInt("HTTP_MAX_BODY_BYTES", 1<<20),

		// PostgreSQL defaults
		DatabaseURL:     envStr("DATABASE_URL", "postgres://pulseflow:pulseflow@localhost:5432/pulseflow?sslmode=disable"),
		DatabaseMaxConn: envInt("DATABASE_MAX_CONN", 25),

		// Redis defaults
		RedisURL:       envStr("REDIS_URL", "redis://localhost:6379/0"),
		APIKeyCacheTTL: envDuration("API_KEY_CACHE_TTL", 60*time.Second),

		// Kafka defaults
		KafkaBrokers:         envStr("KAFKA_BROKERS", "localhost:9092"),
		KafkaEventsTopic:     envStr("KAFKA_EVENTS_TOPIC", "events.ingested"),
		KafkaDeadLetterTopic: envStr("KAFKA_DEADLETTER_TOPIC", "events.deadletter"),
		KafkaConsumerGroup:   envStr("KAFKA_CONSUMER_GROUP", "pulseflow-workers"),
		KafkaConsumerRetry:   envDuration("KAFKA_CONSUMER_RETRY", time.Second),

		// Outbox relay defaults
		OutboxPollInterval: envDuration("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
		OutboxBatchSize:    envInt("OUTBOX_BATCH_SIZE", 100),
		OutboxLease:        envDuration("OUTBOX_LEASE", 30*time.Second),
		OutboxRetryDelay:   envDuration("OUTBOX_RETRY_DELAY", 5*time.Second),

		// Worker pool defaults
		WebhookWorkerCount:         envInt("WEBHOOK_WORKER_COUNT", 20),
		WebhookBufferSize:          envInt("WEBHOOK_BUFFER_SIZE", 200),
		WebhookTimeout:             envDuration("WEBHOOK_TIMEOUT", 30*time.Second),
		EmailWorkerCount:           envInt("EMAIL_WORKER_COUNT", 5),
		EmailBufferSize:            envInt("EMAIL_BUFFER_SIZE", 50),
		EmailTimeout:               envDuration("EMAIL_TIMEOUT", 10*time.Second),
		DeliveryPollInterval:       envDuration("DELIVERY_POLL_INTERVAL", 250*time.Millisecond),
		DeliveryBatchSize:          envInt("DELIVERY_BATCH_SIZE", 50),
		DeliveryLease:              envDuration("DELIVERY_LEASE", 10*time.Minute),
		DeliveryRecoveryInterval:   envDuration("DELIVERY_RECOVERY_INTERVAL", 5*time.Second),
		DeliveryFinalizeInterval:   envDuration("DELIVERY_FINALIZE_INTERVAL", 5*time.Second),
		DeliveryPersistenceTimeout: envDuration("DELIVERY_PERSISTENCE_TIMEOUT", 5*time.Second),

		// Rate limiting defaults
		RateLimitRequests: envInt("RATE_LIMIT_REQUESTS", 100),
		RateLimitWindow:   envDuration("RATE_LIMIT_WINDOW", 60*time.Second),

		// Retry defaults
		RetryMaxAttempts: envInt("RETRY_MAX_ATTEMPTS", 5),
		RetryBaseDelay:   envDuration("RETRY_BASE_DELAY", 1*time.Second),
		RetryMaxDelay:    envDuration("RETRY_MAX_DELAY", 15*time.Minute),

		// Operational visibility defaults
		ObservabilityInterval:        envDuration("OBSERVABILITY_INTERVAL", 15*time.Second),
		ObservabilityQueryTimeout:    envDuration("OBSERVABILITY_QUERY_TIMEOUT", 2*time.Second),
		AlertOutboxPending:           envInt64("ALERT_OUTBOX_PENDING", 1000),
		AlertOutboxOldestAge:         envDuration("ALERT_OUTBOX_OLDEST_AGE", 5*time.Minute),
		AlertExpiredOutboxLeases:     envInt64("ALERT_EXPIRED_OUTBOX_LEASES", 1),
		AlertDueDeliveries:           envInt64("ALERT_DUE_DELIVERIES", 1000),
		AlertExpiredDeliveryLeases:   envInt64("ALERT_EXPIRED_DELIVERY_LEASES", 1),
		AlertPostgresPoolUtilization: envFloat("ALERT_POSTGRES_POOL_UTILIZATION", 0.90),

		// Logging defaults
		LogLevel:  envStr("LOG_LEVEL", "info"),
		LogFormat: envStr("LOG_FORMAT", "json"),

		// Shutdown defaults
		ShutdownTimeout: envDuration("SHUTDOWN_TIMEOUT", 30*time.Second),
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) validate() error {
	if c.HTTPPort < 1 || c.HTTPPort > 65535 {
		return fmt.Errorf("invalid HTTP_PORT: %d", c.HTTPPort)
	}
	if c.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	if c.HTTPMaxBodyBytes < 1 {
		return fmt.Errorf("HTTP_MAX_BODY_BYTES must be >= 1")
	}
	if c.APIKeyCacheTTL <= 0 {
		return fmt.Errorf("API_KEY_CACHE_TTL must be > 0")
	}
	if c.RateLimitRequests < 1 || c.RateLimitWindow <= 0 {
		return fmt.Errorf("rate limit requests and window must be positive")
	}
	if len(c.KafkaBrokerAddresses()) == 0 {
		return fmt.Errorf("KAFKA_BROKERS must contain at least one broker")
	}
	if strings.TrimSpace(c.KafkaEventsTopic) == "" || strings.TrimSpace(c.KafkaDeadLetterTopic) == "" || strings.TrimSpace(c.KafkaConsumerGroup) == "" {
		return fmt.Errorf("Kafka topics and consumer group are required")
	}
	if c.KafkaConsumerRetry <= 0 {
		return fmt.Errorf("KAFKA_CONSUMER_RETRY must be > 0")
	}
	if c.OutboxPollInterval <= 0 || c.OutboxBatchSize < 1 || c.OutboxLease <= 0 || c.OutboxRetryDelay <= 0 {
		return fmt.Errorf("outbox poll interval, batch size, lease, and retry delay must be positive")
	}
	if c.WebhookWorkerCount < 1 || c.WebhookBufferSize < 1 || c.WebhookTimeout <= 0 {
		return fmt.Errorf("webhook worker count, buffer size, and timeout must be positive")
	}
	if c.EmailWorkerCount < 1 || c.EmailBufferSize < 1 || c.EmailTimeout <= 0 {
		return fmt.Errorf("email worker count, buffer size, and timeout must be positive")
	}
	if c.DeliveryPollInterval <= 0 || c.DeliveryBatchSize < 1 || c.DeliveryLease <= 0 ||
		c.DeliveryRecoveryInterval <= 0 || c.DeliveryFinalizeInterval <= 0 || c.DeliveryPersistenceTimeout <= 0 {
		return fmt.Errorf("delivery intervals, batch size, lease, and persistence timeout must be positive")
	}
	webhookQueueWait := time.Duration(1+(c.WebhookBufferSize+c.WebhookWorkerCount-1)/c.WebhookWorkerCount) * c.WebhookTimeout
	emailQueueWait := time.Duration(1+(c.EmailBufferSize+c.EmailWorkerCount-1)/c.EmailWorkerCount) * c.EmailTimeout
	if c.DeliveryLease <= webhookQueueWait || c.DeliveryLease <= emailQueueWait {
		return fmt.Errorf("DELIVERY_LEASE must exceed the maximum configured queue wait and delivery timeout")
	}
	if c.RetryMaxAttempts < 1 || c.RetryBaseDelay <= 0 || c.RetryMaxDelay < c.RetryBaseDelay {
		return fmt.Errorf("retry attempts and delays must be positive, and RETRY_MAX_DELAY must be >= RETRY_BASE_DELAY")
	}
	if c.ObservabilityInterval <= 0 || c.ObservabilityQueryTimeout <= 0 {
		return fmt.Errorf("observability interval and query timeout must be positive")
	}
	if c.AlertOutboxPending < 0 || c.AlertOutboxOldestAge < 0 || c.AlertExpiredOutboxLeases < 0 || c.AlertDueDeliveries < 0 || c.AlertExpiredDeliveryLeases < 0 {
		return fmt.Errorf("operational alert thresholds cannot be negative")
	}
	if c.AlertPostgresPoolUtilization < 0 || c.AlertPostgresPoolUtilization > 1 {
		return fmt.Errorf("ALERT_POSTGRES_POOL_UTILIZATION must be within [0, 1]")
	}
	return nil
}

func (c *Config) KafkaBrokerAddresses() []string {
	parts := strings.Split(c.KafkaBrokers, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}

// Environment variable helpers

func envStr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		i, err := strconv.Atoi(v)
		if err != nil {
			return fallback
		}
		return i
	}
	return fallback
}

func envInt64(key string, fallback int64) int64 {
	if v := os.Getenv(key); v != "" {
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fallback
		}
		return i
	}
	return fallback
}

func envFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		value, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return fallback
		}
		return value
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fallback
		}
		return d
	}
	return fallback
}
