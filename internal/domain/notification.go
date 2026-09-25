package domain

import (
	"encoding/json"
	"time"
)

type DeliveryAttemptStatus string

const (
	DeliveryStatusPending    DeliveryAttemptStatus = "pending"
	DeliveryStatusDelivering DeliveryAttemptStatus = "delivering"
	DeliveryStatusDelivered  DeliveryAttemptStatus = "delivered"
	DeliveryStatusFailed     DeliveryAttemptStatus = "failed"
	DeliveryStatusRetrying   DeliveryAttemptStatus = "retrying"
	DeliveryStatusDeadLetter DeliveryAttemptStatus = "dead_letter"
)

func (s DeliveryAttemptStatus) IsTerminal() bool {
	return s == DeliveryStatusDelivered || s == DeliveryStatusFailed || s == DeliveryStatusDeadLetter
}

func (s DeliveryAttemptStatus) CanTransitionTo(next DeliveryAttemptStatus) bool {
	switch s {
	case DeliveryStatusPending:
		return next == DeliveryStatusDelivering
	case DeliveryStatusDelivering:
		return next == DeliveryStatusDelivered || next == DeliveryStatusFailed || next == DeliveryStatusRetrying
	case DeliveryStatusRetrying:
		return next == DeliveryStatusDelivering || next == DeliveryStatusDeadLetter
	default:
		return false
	}
}

func (s DeliveryAttemptStatus) ValidateTransition(next DeliveryAttemptStatus) error {
	if !s.CanTransitionTo(next) {
		return ErrInvalidStateTransition
	}
	return nil
}

type NotificationRule struct {
	ID        string          `json:"id"`
	TenantID  string          `json:"tenant_id"`
	EventType string          `json:"event_type"`
	Channel   string          `json:"channel"`
	Config    json.RawMessage `json:"config"`
	Enabled   bool            `json:"enabled"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

type DeliveryAttempt struct {
	ID                 string                `json:"id"`
	EventID            string                `json:"event_id"`
	NotificationRuleID string                `json:"notification_rule_id"`
	TenantID           string                `json:"tenant_id"`
	Channel            string                `json:"channel"`
	Status             DeliveryAttemptStatus `json:"status"`
	AttemptNumber      int                   `json:"attempt_number"`
	MaxAttempts        int                   `json:"max_attempts"`
	NextRetryAt        *time.Time            `json:"next_retry_at,omitempty"`
	LockedBy           *string               `json:"locked_by,omitempty"`
	LockedUntil        *time.Time            `json:"locked_until,omitempty"`
	RequestPayload     json.RawMessage       `json:"request_payload,omitempty"`
	ResponseStatus     *int                  `json:"response_status,omitempty"`
	ResponseBody       *string               `json:"response_body,omitempty"`
	ErrorMessage       *string               `json:"error_message,omitempty"`
	DurationMs         *int                  `json:"duration_ms,omitempty"`
	CreatedAt          time.Time             `json:"created_at"`
	UpdatedAt          time.Time             `json:"updated_at"`
	CompletedAt        *time.Time            `json:"completed_at,omitempty"`
}

type DeliveryResult struct {
	Status         DeliveryAttemptStatus
	ResponseStatus *int
	ResponseBody   *string
	ErrorMessage   *string
	DurationMs     *int
	NextRetryAt    *time.Time
}
