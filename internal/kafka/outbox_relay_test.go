package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"pulseflow/internal/domain"
	"pulseflow/internal/observability"
	"pulseflow/internal/platform/logger"
)

func TestOutboxRelayMarksSuccessfulMessagesAndReleasesFailures(t *testing.T) {
	store := &fakeOutboxStore{entries: []domain.OutboxEntry{
		{ID: 1, Topic: "events.ingested", PartitionKey: "tenant-1", Payload: []byte(`{"event_id":"1"}`)},
		{ID: 2, Topic: "events.ingested", PartitionKey: "tenant-2", Payload: []byte(`{"event_id":"2"}`)},
	}}
	publisher := &fakePublisher{results: []error{nil, errors.New("Kafka unavailable")}}
	metrics := observability.NewRegistry()
	relay := newTestRelay(t, store, publisher, metrics)

	count, err := relay.processBatch(context.Background())
	if err != nil {
		t.Fatalf("process batch: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 processed rows, got %d", count)
	}
	if len(store.marked) != 1 || store.marked[0] != 1 {
		t.Fatalf("expected row 1 marked published, got %v", store.marked)
	}
	if len(store.released) != 1 || store.released[0] != 2 {
		t.Fatalf("expected row 2 released, got %v", store.released)
	}
	if store.releaseCause != "Kafka unavailable" {
		t.Fatalf("unexpected retry cause %q", store.releaseCause)
	}
	if snapshot := metrics.Snapshot(); snapshot.OutboxPublished != 1 || snapshot.OutboxPublishFailed != 1 {
		t.Fatalf("unexpected outbox metrics: %+v", snapshot)
	}
}

func TestOutboxRelayLeavesLeaseForRecoveryWhenMarkFails(t *testing.T) {
	store := &fakeOutboxStore{
		entries: []domain.OutboxEntry{{ID: 7, Topic: "events.ingested", PartitionKey: "tenant", Payload: []byte(`{}`)}},
		markErr: errors.New("PostgreSQL unavailable"),
	}
	relay := newTestRelay(t, store, &fakePublisher{results: []error{nil}})

	_, err := relay.processBatch(context.Background())
	if err == nil {
		t.Fatal("expected mark failure")
	}
	if len(store.released) != 0 {
		t.Fatalf("published row must remain leased for later recovery, released=%v", store.released)
	}
}

func TestOutboxRelayReleasesWholeBatchWhenKafkaIsUnavailable(t *testing.T) {
	store := &fakeOutboxStore{entries: []domain.OutboxEntry{
		{ID: 1, Topic: "events.ingested", PartitionKey: "tenant", Payload: []byte(`{}`)},
		{ID: 2, Topic: "events.ingested", PartitionKey: "tenant", Payload: []byte(`{}`)},
	}}
	publishErr := errors.New("dial tcp: broker unavailable")
	relay := newTestRelay(t, store, &fakePublisher{results: []error{publishErr, publishErr}})

	_, err := relay.processBatch(context.Background())
	if err != nil {
		t.Fatalf("release after Kafka failure: %v", err)
	}
	if len(store.marked) != 0 || len(store.released) != 2 {
		t.Fatalf("expected all rows released and none marked; marked=%v released=%v", store.marked, store.released)
	}
}

func newTestRelay(t *testing.T, store OutboxStore, publisher Publisher, metrics ...OutboxMetrics) *OutboxRelay {
	t.Helper()
	relay, err := NewOutboxRelay(store, publisher, OutboxRelayConfig{
		Owner:        "relay-test",
		BatchSize:    10,
		Lease:        time.Minute,
		PollInterval: time.Millisecond,
		RetryDelay:   time.Second,
	}, logger.New("error", "text"), metrics...)
	if err != nil {
		t.Fatalf("new relay: %v", err)
	}
	return relay
}

type fakeOutboxStore struct {
	entries      []domain.OutboxEntry
	claimErr     error
	markErr      error
	releaseErr   error
	marked       []int64
	released     []int64
	releaseCause string
}

func (s *fakeOutboxStore) ClaimOutbox(context.Context, string, int, time.Duration) ([]domain.OutboxEntry, error) {
	return s.entries, s.claimErr
}

func (s *fakeOutboxStore) MarkOutboxPublished(_ context.Context, _ string, ids []int64) (int64, error) {
	s.marked = append(s.marked, ids...)
	if s.markErr != nil {
		return 0, s.markErr
	}
	return int64(len(ids)), nil
}

func (s *fakeOutboxStore) ReleaseOutboxForRetry(_ context.Context, _ string, ids []int64, _ time.Time, cause string) (int64, error) {
	s.released = append(s.released, ids...)
	s.releaseCause = cause
	if s.releaseErr != nil {
		return 0, s.releaseErr
	}
	return int64(len(ids)), nil
}

type fakePublisher struct {
	results []error
}

func (p *fakePublisher) Publish(context.Context, []Message) []error { return p.results }
func (p *fakePublisher) Close() error                               { return nil }
