package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"pulseflow/internal/domain"
	"pulseflow/internal/observability"
	"pulseflow/internal/platform/logger"
)

func TestConsumerCommitsOnlyAfterDurableRouting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	order := make([]string, 0, 2)
	message := validKafkaMessage(t)
	reader := &fakeReader{message: message, onCommit: func() {
		order = append(order, "commit")
		cancel()
	}}
	store := &fakeRoutingStore{route: func(domain.EventMessage) error {
		order = append(order, "route")
		return nil
	}}
	metrics := observability.NewRegistry()
	consumer := newTestConsumer(t, reader, store, metrics)

	if err := consumer.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	if !reflect.DeepEqual(order, []string{"route", "commit"}) {
		t.Fatalf("expected route before commit, got %v", order)
	}
	snapshot := metrics.Snapshot()
	if snapshot.KafkaRouted != 1 || snapshot.KafkaLag["events.ingested/1"] != 3 {
		t.Fatalf("unexpected Kafka metrics: %+v", snapshot)
	}
}

func TestConsumerQuarantinesPoisonMessageBeforeCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	order := make([]string, 0, 2)
	reader := &fakeReader{message: Message{
		Topic: "events.ingested", Partition: 2, Offset: 19, Key: []byte("tenant"), Value: []byte(`not-json`),
	}, onCommit: func() {
		order = append(order, "commit")
		cancel()
	}}
	store := &fakeRoutingStore{quarantine: func(params domain.QuarantineMessageParams) error {
		order = append(order, "quarantine")
		if params.SourceOffset != 19 || params.DeadLetterTopic != "events.deadletter" {
			t.Fatalf("unexpected quarantine params: %+v", params)
		}
		return nil
	}}
	metrics := observability.NewRegistry()
	consumer := newTestConsumer(t, reader, store, metrics)

	if err := consumer.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	if !reflect.DeepEqual(order, []string{"quarantine", "commit"}) {
		t.Fatalf("expected quarantine before commit, got %v", order)
	}
	if store.routeCalls != 0 {
		t.Fatalf("poison message should not route, calls=%d", store.routeCalls)
	}
	if metrics.Snapshot().KafkaQuarantined != 1 {
		t.Fatalf("quarantine metric = %d, want 1", metrics.Snapshot().KafkaQuarantined)
	}
}

func TestConsumerRetriesDatabaseFailureWithoutFetchingNextMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := &fakeReader{message: validKafkaMessage(t), onCommit: cancel}
	store := &fakeRoutingStore{}
	store.route = func(domain.EventMessage) error {
		if store.routeCalls == 1 {
			return domain.ErrUnavailable
		}
		return nil
	}
	consumer := newTestConsumer(t, reader, store)

	if err := consumer.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	if store.routeCalls != 2 {
		t.Fatalf("expected same message routed twice, calls=%d", store.routeCalls)
	}
	if reader.fetchedBeforeCommit {
		t.Fatal("consumer fetched another message before routing and commit recovered")
	}
}

func TestConsumerRetriesOffsetCommitBeforeFetchingAgain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := &fakeReader{
		message:      validKafkaMessage(t),
		commitErrors: []error{errors.New("coordinator unavailable"), nil},
		onCommit:     cancel,
	}
	consumer := newTestConsumer(t, reader, &fakeRoutingStore{})

	if err := consumer.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	if reader.commitCalls != 2 {
		t.Fatalf("expected commit retry, calls=%d", reader.commitCalls)
	}
	if reader.fetchedBeforeCommit {
		t.Fatal("consumer fetched another message before commit recovered")
	}
}

func TestConsumerRecordsCommitErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := &fakeReader{message: validKafkaMessage(t), commitErrors: []error{errors.New("commit failed"), nil}, onCommit: cancel}
	metrics := observability.NewRegistry()
	consumer := newTestConsumer(t, reader, &fakeRoutingStore{}, metrics)
	if err := consumer.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if metrics.Snapshot().KafkaCommitErrors != 1 {
		t.Fatalf("commit error metric = %d, want 1", metrics.Snapshot().KafkaCommitErrors)
	}
}

func newTestConsumer(t *testing.T, reader Reader, store RoutingStore, metrics ...ConsumerMetrics) *Consumer {
	t.Helper()
	consumer, err := NewConsumer(reader, store, ConsumerConfig{
		DeadLetterTopic: "events.deadletter",
		MaxAttempts:     5,
		RetryDelay:      time.Millisecond,
	}, logger.New("error", "text"), metrics...)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	return consumer
}

func validKafkaMessage(t *testing.T) Message {
	t.Helper()
	payload, err := json.Marshal(domain.EventMessage{
		Version: domain.EventMessageVersion, EventID: "event-1", TenantID: "tenant-1",
		Type: "order.created", Data: json.RawMessage(`{"order_id":"123"}`), OccurredAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return Message{Topic: "events.ingested", Partition: 1, Offset: 8, HighWaterMark: 12, Key: []byte("tenant-1"), Value: payload}
}

type fakeReader struct {
	message             Message
	fetchCalls          int
	commitCalls         int
	commitErrors        []error
	onCommit            func()
	committed           bool
	fetchedBeforeCommit bool
}

func (r *fakeReader) FetchMessage(ctx context.Context) (Message, error) {
	r.fetchCalls++
	if r.fetchCalls == 1 {
		return r.message, nil
	}
	if !r.committed {
		r.fetchedBeforeCommit = true
	}
	<-ctx.Done()
	return Message{}, ctx.Err()
}

func (r *fakeReader) CommitMessages(_ context.Context, _ ...Message) error {
	r.commitCalls++
	if r.commitCalls <= len(r.commitErrors) && r.commitErrors[r.commitCalls-1] != nil {
		return r.commitErrors[r.commitCalls-1]
	}
	r.committed = true
	if r.onCommit != nil {
		r.onCommit()
	}
	return nil
}

func (r *fakeReader) Close() error { return nil }

type fakeRoutingStore struct {
	route           func(domain.EventMessage) error
	quarantine      func(domain.QuarantineMessageParams) error
	routeCalls      int
	quarantineCalls int
}

func (s *fakeRoutingStore) RouteEventMessage(_ context.Context, message domain.EventMessage, _ int) ([]domain.DeliveryAttempt, error) {
	s.routeCalls++
	if s.route != nil {
		return nil, s.route(message)
	}
	return nil, nil
}

func (s *fakeRoutingStore) QuarantineMessage(_ context.Context, params domain.QuarantineMessageParams) (domain.QuarantinedMessage, bool, error) {
	s.quarantineCalls++
	if s.quarantine != nil {
		if err := s.quarantine(params); err != nil {
			return domain.QuarantinedMessage{}, false, err
		}
	}
	return domain.QuarantinedMessage{ID: "quarantine-1"}, true, nil
}
