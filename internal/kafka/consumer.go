package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"pulseflow/internal/domain"
	"pulseflow/internal/platform/logger"
)

type RoutingStore interface {
	RouteEventMessage(ctx context.Context, message domain.EventMessage, maxAttempts int) ([]domain.DeliveryAttempt, error)
	QuarantineMessage(ctx context.Context, params domain.QuarantineMessageParams) (domain.QuarantinedMessage, bool, error)
}

type ConsumerConfig struct {
	DeadLetterTopic string
	MaxAttempts     int
	RetryDelay      time.Duration
}

type Consumer struct {
	reader Reader
	store  RoutingStore
	config ConsumerConfig
	logger *logger.Logger
}

func NewConsumer(reader Reader, store RoutingStore, cfg ConsumerConfig, log *logger.Logger) (*Consumer, error) {
	if reader == nil || store == nil || log == nil {
		return nil, fmt.Errorf("Kafka reader, routing store, and logger are required")
	}
	if cfg.DeadLetterTopic == "" || cfg.MaxAttempts < 1 || cfg.RetryDelay <= 0 {
		return nil, fmt.Errorf("dead-letter topic, positive max attempts, and retry delay are required")
	}
	return &Consumer{reader: reader, store: store, config: cfg, logger: log}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		message, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.logger.Error("Kafka fetch failed", "error", err)
			if !waitFor(ctx, c.config.RetryDelay) {
				return nil
			}
			continue
		}

		for {
			err = c.processMessage(ctx, message)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return nil
			}
			c.logger.Error("Kafka message processing failed; retaining offset", "topic", message.Topic, "partition", message.Partition, "offset", message.Offset, "error", err)
			if !waitFor(ctx, c.config.RetryDelay) {
				return nil
			}
		}

		for {
			err = c.reader.CommitMessages(ctx, message)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return nil
			}
			// We retry this commit before fetching another message. Committing a
			// later offset from the same partition could otherwise skip this one.
			c.logger.Error("Kafka offset commit failed", "topic", message.Topic, "partition", message.Partition, "offset", message.Offset, "error", err)
			if !waitFor(ctx, c.config.RetryDelay) {
				return nil
			}
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, message Message) error {
	var event domain.EventMessage
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return c.quarantine(ctx, message, fmt.Errorf("decode event message: %w", err))
	}
	if err := event.Validate(); err != nil {
		return c.quarantine(ctx, message, fmt.Errorf("validate event message: %w", err))
	}

	_, err := c.store.RouteEventMessage(ctx, event, c.config.MaxAttempts)
	if err == nil {
		return nil
	}
	if errors.Is(err, domain.ErrValidation) || errors.Is(err, domain.ErrNotFound) {
		return c.quarantine(ctx, message, fmt.Errorf("route event message: %w", err))
	}
	return err
}

func (c *Consumer) quarantine(ctx context.Context, message Message, cause error) error {
	quarantined, created, err := c.store.QuarantineMessage(ctx, domain.QuarantineMessageParams{
		SourceTopic:     message.Topic,
		SourcePartition: message.Partition,
		SourceOffset:    message.Offset,
		MessageKey:      message.Key,
		Payload:         message.Value,
		ErrorMessage:    cause.Error(),
		DeadLetterTopic: c.config.DeadLetterTopic,
	})
	if err != nil {
		return fmt.Errorf("persist quarantined message: %w", err)
	}
	c.logger.Warn("Kafka message quarantined", "quarantine_id", quarantined.ID, "created", created, "topic", message.Topic, "partition", message.Partition, "offset", message.Offset, "error", cause)
	return nil
}
