package domain

import (
	"encoding/json"
	"time"
)

// EventStatus defines the allowed states for an event (our State Machine).
// In TypeScript, this would be an enum or string literal type:
// type EventStatus = "accepted" | "processing" | "completed" | "partially_failed" | "failed";
type EventStatus string

const (
	EventStatusAccepted        EventStatus = "accepted"
	EventStatusProcessing      EventStatus = "processing"
	EventStatusCompleted       EventStatus = "completed"
	EventStatusPartiallyFailed EventStatus = "partially_failed"
	EventStatusFailed          EventStatus = "failed"
)

// Event represents an event deeply ingested from a tenant.
type Event struct {
	ID             string          `json:"id"`
	TenantID       string          `json:"tenant_id"`
	Type           string          `json:"type"`            // e.g., "order.created"
	IdempotencyKey string          `json:"idempotency_key"` // Prevents duplicate processing
	Data           json.RawMessage `json:"data"`            // json.RawMessage is like any[] or Record<string, any> in TS
	Status         EventStatus     `json:"status"`
	CreatedAt      time.Time       `json:"created_at"`
	ProcessedAt    *time.Time      `json:"processed_at,omitempty"`
}

// OutboxEntry represents a message waiting to be published to Kafka.
// This is the implementation of our Transactional Outbox pattern.
type OutboxEntry struct {
	ID          int64           `json:"id"`
	AggregateID string          `json:"aggregate_id"` // Matches Event.ID
	Topic       string          `json:"topic"`
	Payload     json.RawMessage `json:"payload"`
	Published   bool            `json:"published"`
	CreatedAt   time.Time       `json:"created_at"`
	PublishedAt *time.Time      `json:"published_at,omitempty"`
}
