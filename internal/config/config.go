package config

import (
	"fmt"
	"os"
	"strconv"
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

	// PostgreSQL
	DatabaseURL     string `json:"database_url"`
	DatabaseMaxConn int    `json:"database_max_conn"`

	// Redis
	RedisURL string `json:"redis_url"`

	// Kafka
	KafkaBrokers        string `json:"kafka_brokers"`
	KafkaEventsTopic    string `json:"kafka_events_topic"`
	KafkaDeadLetterTopic string `json:"kafka_deadletter_topic"`
	KafkaConsumerGroup  string `json:"kafka_consumer_group"`

	// Outbox relay
	OutboxPollInterval time.Duration `json:"outbox_poll_interval"`
	OutboxBatchSize    int           `json:"outbox_batch_size"`

	// Worker pools
	WebhookWorkerCount  int           `json:"webhook_worker_count"`
	WebhookBufferSize   int           `json:"webhook_buffer_size"`
	WebhookTimeout      time.Duration `json:"webhook_timeout"`
	EmailWorkerCount    int           `json:"email_worker_count"`
	EmailBufferSize     int           `json:"email_buffer_size"`
	EmailTimeout        time.Duration `json:"email_timeout"`

	// Rate limiting
	RateLimitRequests int           `json:"rate_limit_requests"`
	RateLimitWindow   time.Duration `json:"rate_limit_window"`

	// Retry policy
	RetryMaxAttempts int           `json:"retry_max_attempts"`
	RetryBaseDelay   time.Duration `json:"retry_base_delay"`

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

		// PostgreSQL defaults
		DatabaseURL:     envStr("DATABASE_URL", "postgres://pulseflow:pulseflow@localhost:5432/pulseflow?sslmode=disable"),
		DatabaseMaxConn: envInt("DATABASE_MAX_CONN", 25),

		// Redis defaults
		RedisURL: envStr("REDIS_URL", "redis://localhost:6379/0"),

		// Kafka defaults
		KafkaBrokers:         envStr("KAFKA_BROKERS", "localhost:9092"),
		KafkaEventsTopic:     envStr("KAFKA_EVENTS_TOPIC", "events.ingested"),
		KafkaDeadLetterTopic: envStr("KAFKA_DEADLETTER_TOPIC", "events.deadletter"),
		KafkaConsumerGroup:   envStr("KAFKA_CONSUMER_GROUP", "pulseflow-workers"),

		// Outbox relay defaults
		OutboxPollInterval: envDuration("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
		OutboxBatchSize:    envInt("OUTBOX_BATCH_SIZE", 100),

		// Worker pool defaults
		WebhookWorkerCount: envInt("WEBHOOK_WORKER_COUNT", 20),
		WebhookBufferSize:  envInt("WEBHOOK_BUFFER_SIZE", 200),
		WebhookTimeout:     envDuration("WEBHOOK_TIMEOUT", 30*time.Second),
		EmailWorkerCount:   envInt("EMAIL_WORKER_COUNT", 5),
		EmailBufferSize:    envInt("EMAIL_BUFFER_SIZE", 50),
		EmailTimeout:       envDuration("EMAIL_TIMEOUT", 10*time.Second),

		// Rate limiting defaults
		RateLimitRequests: envInt("RATE_LIMIT_REQUESTS", 100),
		RateLimitWindow:   envDuration("RATE_LIMIT_WINDOW", 60*time.Second),

		// Retry defaults
		RetryMaxAttempts: envInt("RETRY_MAX_ATTEMPTS", 5),
		RetryBaseDelay:   envDuration("RETRY_BASE_DELAY", 1*time.Second),

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
	if c.KafkaBrokers == "" {
		return fmt.Errorf("KAFKA_BROKERS is required")
	}
	if c.WebhookWorkerCount < 1 {
		return fmt.Errorf("WEBHOOK_WORKER_COUNT must be >= 1")
	}
	if c.EmailWorkerCount < 1 {
		return fmt.Errorf("EMAIL_WORKER_COUNT must be >= 1")
	}
	return nil
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
