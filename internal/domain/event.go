package domain

import (
	"encoding/json"
	"strings"
	"time"
)

const EventMessageVersion = 1

type EventStatus string

const (
	EventStatusAccepted        EventStatus = "accepted"
	EventStatusProcessing      EventStatus = "processing"
	EventStatusCompleted       EventStatus = "completed"
	EventStatusPartiallyFailed EventStatus = "partially_failed"
	EventStatusFailed          EventStatus = "failed"
)

func (s EventStatus) IsTerminal() bool {
	return s == EventStatusCompleted || s == EventStatusPartiallyFailed || s == EventStatusFailed
}

func (s EventStatus) CanTransitionTo(next EventStatus) bool {
	switch s {
	case EventStatusAccepted:
		return next == EventStatusProcessing
	case EventStatusProcessing:
		return next == EventStatusCompleted || next == EventStatusPartiallyFailed || next == EventStatusFailed
	default:
		return false
	}
}

func (s EventStatus) ValidateTransition(next EventStatus) error {
	if !s.CanTransitionTo(next) {
		return ErrInvalidStateTransition
	}
	return nil
}

type Event struct {
	ID             string          `json:"id"`
	TenantID       string          `json:"tenant_id"`
	Type           string          `json:"type"`
	IdempotencyKey string          `json:"idempotency_key"`
	Data           json.RawMessage `json:"data"`
	Status         EventStatus     `json:"status"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	ProcessedAt    *time.Time      `json:"processed_at,omitempty"`
}

// EventMessage is the versioned payload written to the transactional outbox.
type EventMessage struct {
	Version    int             `json:"version"`
	EventID    string          `json:"event_id"`
	TenantID   string          `json:"tenant_id"`
	Type       string          `json:"type"`
	Data       json.RawMessage `json:"data"`
	OccurredAt time.Time       `json:"occurred_at"`
}

func (m EventMessage) Validate() error {
	if m.Version != EventMessageVersion {
		return NewValidationError("version", "unsupported event message version")
	}
	if strings.TrimSpace(m.EventID) == "" || strings.TrimSpace(m.TenantID) == "" || strings.TrimSpace(m.Type) == "" {
		return NewValidationError("event_message", "event_id, tenant_id, and type are required")
	}
	if !json.Valid(m.Data) {
		return NewValidationError("data", "must be valid JSON")
	}
	if m.OccurredAt.IsZero() {
		return NewValidationError("occurred_at", "is required")
	}
	return nil
}

type OutboxStatus string

const (
	OutboxStatusPending    OutboxStatus = "pending"
	OutboxStatusPublishing OutboxStatus = "publishing"
	OutboxStatusPublished  OutboxStatus = "published"
)

type OutboxEntry struct {
	ID              int64           `json:"id"`
	AggregateID     string          `json:"aggregate_id"`
	Topic           string          `json:"topic"`
	PartitionKey    string          `json:"partition_key"`
	Payload         json.RawMessage `json:"payload"`
	Status          OutboxStatus    `json:"status"`
	PublishAttempts int             `json:"publish_attempts"`
	AvailableAt     time.Time       `json:"available_at"`
	LockedBy        *string         `json:"locked_by,omitempty"`
	LockedUntil     *time.Time      `json:"locked_until,omitempty"`
	LastError       *string         `json:"last_error,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	PublishedAt     *time.Time      `json:"published_at,omitempty"`
}
