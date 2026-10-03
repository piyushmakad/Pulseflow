package observability

import (
	"fmt"
	"sync"
	"time"
)

type DeliveryCounters struct {
	Count           uint64  `json:"count"`
	TotalDurationMs uint64  `json:"total_duration_ms"`
	AverageMs       float64 `json:"average_ms"`
	MaxDurationMs   uint64  `json:"max_duration_ms"`
}

type RuntimeSnapshot struct {
	Deliveries          map[string]map[string]DeliveryCounters `json:"deliveries"`
	OutboxPublished     uint64                                 `json:"outbox_published"`
	OutboxPublishFailed uint64                                 `json:"outbox_publish_failed"`
	KafkaRouted         uint64                                 `json:"kafka_routed"`
	KafkaQuarantined    uint64                                 `json:"kafka_quarantined"`
	KafkaFetchErrors    uint64                                 `json:"kafka_fetch_errors"`
	KafkaCommitErrors   uint64                                 `json:"kafka_commit_errors"`
	KafkaLag            map[string]int64                       `json:"kafka_lag"`
	RedisFallbacks      map[string]uint64                      `json:"redis_fallbacks"`
}

// Registry contains process-local counters. PostgreSQL remains authoritative
// for durable backlog; these counters describe work observed by this process.
type Registry struct {
	mu sync.RWMutex

	deliveries          map[string]map[string]DeliveryCounters
	outboxPublished     uint64
	outboxPublishFailed uint64
	kafkaRouted         uint64
	kafkaQuarantined    uint64
	kafkaFetchErrors    uint64
	kafkaCommitErrors   uint64
	kafkaLag            map[string]int64
	redisFallbacks      map[string]uint64
}

func NewRegistry() *Registry {
	return &Registry{
		deliveries:     make(map[string]map[string]DeliveryCounters),
		kafkaLag:       make(map[string]int64),
		redisFallbacks: make(map[string]uint64),
	}
}

func (r *Registry) RecordDelivery(channel, status string, duration time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	byStatus := r.deliveries[channel]
	if byStatus == nil {
		byStatus = make(map[string]DeliveryCounters)
		r.deliveries[channel] = byStatus
	}
	counters := byStatus[status]
	counters.Count++
	durationMs := uint64(max(duration.Milliseconds(), 0))
	counters.TotalDurationMs += durationMs
	if durationMs > counters.MaxDurationMs {
		counters.MaxDurationMs = durationMs
	}
	byStatus[status] = counters
}

func (r *Registry) RecordOutboxPublish(published, failed int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if published > 0 {
		r.outboxPublished += uint64(published)
	}
	if failed > 0 {
		r.outboxPublishFailed += uint64(failed)
	}
}

func (r *Registry) RecordKafkaRouted() {
	r.mu.Lock()
	r.kafkaRouted++
	r.mu.Unlock()
}

func (r *Registry) RecordKafkaQuarantined() {
	r.mu.Lock()
	r.kafkaQuarantined++
	r.mu.Unlock()
}

func (r *Registry) RecordKafkaFetchError() {
	r.mu.Lock()
	r.kafkaFetchErrors++
	r.mu.Unlock()
}

func (r *Registry) RecordKafkaCommitError() {
	r.mu.Lock()
	r.kafkaCommitErrors++
	r.mu.Unlock()
}

func (r *Registry) RecordKafkaLag(topic string, partition int, lag int64) {
	if lag < 0 {
		lag = 0
	}
	r.mu.Lock()
	r.kafkaLag[fmt.Sprintf("%s/%d", topic, partition)] = lag
	r.mu.Unlock()
}

func (r *Registry) RecordRedisFallback(operation string) {
	r.mu.Lock()
	r.redisFallbacks[operation]++
	r.mu.Unlock()
}

func (r *Registry) Snapshot() RuntimeSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	snapshot := RuntimeSnapshot{
		Deliveries:          make(map[string]map[string]DeliveryCounters, len(r.deliveries)),
		OutboxPublished:     r.outboxPublished,
		OutboxPublishFailed: r.outboxPublishFailed,
		KafkaRouted:         r.kafkaRouted,
		KafkaQuarantined:    r.kafkaQuarantined,
		KafkaFetchErrors:    r.kafkaFetchErrors,
		KafkaCommitErrors:   r.kafkaCommitErrors,
		KafkaLag:            make(map[string]int64, len(r.kafkaLag)),
		RedisFallbacks:      make(map[string]uint64, len(r.redisFallbacks)),
	}
	for channel, statuses := range r.deliveries {
		copyStatuses := make(map[string]DeliveryCounters, len(statuses))
		for status, counters := range statuses {
			if counters.Count > 0 {
				counters.AverageMs = float64(counters.TotalDurationMs) / float64(counters.Count)
			}
			copyStatuses[status] = counters
		}
		snapshot.Deliveries[channel] = copyStatuses
	}
	for partition, lag := range r.kafkaLag {
		snapshot.KafkaLag[partition] = lag
	}
	for operation, count := range r.redisFallbacks {
		snapshot.RedisFallbacks[operation] = count
	}
	return snapshot
}
