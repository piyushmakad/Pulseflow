package observability

import (
	"sync"
	"testing"
	"time"
)

func TestRegistryIsSafeForConcurrentRecorders(t *testing.T) {
	registry := NewRegistry()
	const goroutines = 20
	const recordsPerGoroutine = 100
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(partition int) {
			defer wg.Done()
			for j := 0; j < recordsPerGoroutine; j++ {
				registry.RecordDelivery("webhook", "delivered", 5*time.Millisecond)
				registry.RecordOutboxPublish(1, 1)
				registry.RecordKafkaRouted()
				registry.RecordRedisFallback("rate_limit")
				registry.RecordKafkaLag("events.ingested", partition, int64(j))
			}
		}(i)
	}
	wg.Wait()

	snapshot := registry.Snapshot()
	want := uint64(goroutines * recordsPerGoroutine)
	if got := snapshot.Deliveries["webhook"]["delivered"].Count; got != want {
		t.Fatalf("delivery count = %d, want %d", got, want)
	}
	if snapshot.OutboxPublished != want || snapshot.OutboxPublishFailed != want || snapshot.KafkaRouted != want {
		t.Fatalf("unexpected counters: %+v", snapshot)
	}
	if snapshot.RedisFallbacks["rate_limit"] != want {
		t.Fatalf("Redis fallback count = %d, want %d", snapshot.RedisFallbacks["rate_limit"], want)
	}
	if len(snapshot.KafkaLag) != goroutines {
		t.Fatalf("Kafka lag partitions = %d, want %d", len(snapshot.KafkaLag), goroutines)
	}
}

func TestSnapshotDoesNotExposeMutableRegistryMaps(t *testing.T) {
	registry := NewRegistry()
	registry.RecordRedisFallback("rate_limit")
	first := registry.Snapshot()
	first.RedisFallbacks["rate_limit"] = 999
	second := registry.Snapshot()
	if second.RedisFallbacks["rate_limit"] != 1 {
		t.Fatalf("snapshot mutation changed registry: %+v", second.RedisFallbacks)
	}
}
