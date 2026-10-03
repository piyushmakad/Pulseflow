package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"pulseflow/internal/domain"
	"pulseflow/internal/platform/logger"
)

type OutboxStore interface {
	ClaimOutbox(ctx context.Context, owner string, batchSize int, lease time.Duration) ([]domain.OutboxEntry, error)
	MarkOutboxPublished(ctx context.Context, owner string, ids []int64) (int64, error)
	ReleaseOutboxForRetry(ctx context.Context, owner string, ids []int64, availableAt time.Time, cause string) (int64, error)
}

type OutboxRelayConfig struct {
	Owner        string
	BatchSize    int
	Lease        time.Duration
	PollInterval time.Duration
	RetryDelay   time.Duration
}

type OutboxRelay struct {
	store     OutboxStore
	publisher Publisher
	config    OutboxRelayConfig
	logger    *logger.Logger
	metrics   OutboxMetrics
}

type OutboxMetrics interface {
	RecordOutboxPublish(published, failed int)
}

func NewOutboxRelay(store OutboxStore, publisher Publisher, cfg OutboxRelayConfig, log *logger.Logger, metrics ...OutboxMetrics) (*OutboxRelay, error) {
	if store == nil || publisher == nil || log == nil {
		return nil, fmt.Errorf("outbox store, publisher, and logger are required")
	}
	if strings.TrimSpace(cfg.Owner) == "" || cfg.BatchSize < 1 || cfg.Lease <= 0 || cfg.PollInterval <= 0 || cfg.RetryDelay <= 0 {
		return nil, fmt.Errorf("outbox owner, positive batch size, lease, poll interval, and retry delay are required")
	}
	relay := &OutboxRelay{store: store, publisher: publisher, config: cfg, logger: log}
	if len(metrics) > 0 {
		relay.metrics = metrics[0]
	}
	return relay, nil
}

func (r *OutboxRelay) Run(ctx context.Context) error {
	for {
		count, err := r.processBatch(ctx)
		if err != nil && ctx.Err() == nil {
			r.logger.Error("outbox relay batch failed", "error", err)
		}
		if ctx.Err() != nil {
			return nil
		}
		if count == r.config.BatchSize {
			continue
		}
		if !waitFor(ctx, r.config.PollInterval) {
			return nil
		}
	}
}

func (r *OutboxRelay) processBatch(ctx context.Context) (int, error) {
	entries, err := r.store.ClaimOutbox(ctx, r.config.Owner, r.config.BatchSize, r.config.Lease)
	if err != nil || len(entries) == 0 {
		return len(entries), err
	}

	messages := make([]Message, len(entries))
	for i, entry := range entries {
		messages[i] = Message{
			Topic: entry.Topic,
			Key:   []byte(entry.PartitionKey),
			Value: entry.Payload,
			Time:  entry.CreatedAt,
		}
	}

	results := r.publisher.Publish(ctx, messages)
	if len(results) != len(entries) {
		results = make([]error, len(entries))
		for i := range results {
			results[i] = fmt.Errorf("publisher returned an invalid result count")
		}
	}

	var publishedIDs []int64
	var failedIDs []int64
	var publishErrors []error
	for i, result := range results {
		if result == nil {
			publishedIDs = append(publishedIDs, entries[i].ID)
			continue
		}
		failedIDs = append(failedIDs, entries[i].ID)
		publishErrors = append(publishErrors, result)
	}
	if r.metrics != nil {
		r.metrics.RecordOutboxPublish(len(publishedIDs), len(failedIDs))
	}

	var stateErrors []error
	if len(publishedIDs) > 0 {
		updated, err := r.store.MarkOutboxPublished(ctx, r.config.Owner, publishedIDs)
		if err != nil {
			stateErrors = append(stateErrors, fmt.Errorf("mark outbox published: %w", err))
		} else if updated != int64(len(publishedIDs)) {
			stateErrors = append(stateErrors, fmt.Errorf("mark outbox published: updated %d of %d rows", updated, len(publishedIDs)))
		}
	}
	if len(failedIDs) > 0 {
		cause := compactErrors(publishErrors)
		updated, err := r.store.ReleaseOutboxForRetry(ctx, r.config.Owner, failedIDs, time.Now().Add(r.config.RetryDelay), cause)
		if err != nil {
			stateErrors = append(stateErrors, fmt.Errorf("release outbox for retry: %w", err))
		} else if updated != int64(len(failedIDs)) {
			stateErrors = append(stateErrors, fmt.Errorf("release outbox for retry: updated %d of %d rows", updated, len(failedIDs)))
		}
	}

	if len(publishedIDs) > 0 {
		r.logger.Info("outbox messages published", "count", len(publishedIDs), "relay_owner", r.config.Owner)
	}
	if len(failedIDs) > 0 {
		r.logger.Warn("outbox messages scheduled for retry", "count", len(failedIDs),
			"relay_owner", r.config.Owner, "error", compactErrors(publishErrors))
	}
	return len(entries), errors.Join(stateErrors...)
}

func compactErrors(errs []error) string {
	if len(errs) == 0 {
		return "Kafka publish failed"
	}
	parts := make([]string, 0, len(errs))
	seen := make(map[string]struct{}, len(errs))
	for _, err := range errs {
		if err == nil {
			continue
		}
		message := err.Error()
		if _, exists := seen[message]; exists {
			continue
		}
		seen[message] = struct{}{}
		parts = append(parts, message)
	}
	joined := strings.Join(parts, "; ")
	if len(joined) > 2000 {
		return joined[:2000]
	}
	return joined
}

func waitFor(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
