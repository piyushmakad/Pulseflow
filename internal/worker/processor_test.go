package worker

import (
	"context"
	"testing"
	"time"

	"pulseflow/internal/domain"
	"pulseflow/internal/observability"
	"pulseflow/internal/platform/logger"
)

type processorStore struct {
	updated domain.DeliveryAttempt
}

func (s *processorStore) RecordDeliveryResult(context.Context, string, string, domain.DeliveryResult, string) (domain.DeliveryAttempt, error) {
	return s.updated, nil
}

func (s *processorStore) FinalizeEvent(context.Context, string) (domain.EventStatus, bool, error) {
	return domain.EventStatusCompleted, true, nil
}

type successfulDeliverer struct{}

func (successfulDeliverer) Deliver(context.Context, domain.DeliveryAttempt) Outcome {
	return Outcome{Kind: OutcomeDelivered}
}

func TestProcessorRecordsMetricsAfterDurableResult(t *testing.T) {
	metrics := observability.NewRegistry()
	processor, err := NewProcessor(
		&processorStore{updated: domain.DeliveryAttempt{Channel: domain.NotificationChannelWebhook, Status: domain.DeliveryStatusDelivered}},
		successfulDeliverer{}, NewRetryPolicy(time.Millisecond, time.Second), "owner", "events.deadletter",
		time.Second, logger.New("error", "text"), metrics,
	)
	if err != nil {
		t.Fatal(err)
	}
	processor.Handle(context.Background(), domain.DeliveryAttempt{ID: "delivery-1", EventID: "event-1", TenantID: "tenant-1"})
	if got := metrics.Snapshot().Deliveries[domain.NotificationChannelWebhook][string(domain.DeliveryStatusDelivered)].Count; got != 1 {
		t.Fatalf("delivery metric = %d, want 1", got)
	}
}
