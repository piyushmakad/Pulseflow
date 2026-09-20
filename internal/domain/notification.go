package domain

import (
	"encoding/json"
	"time"
)

// DeliveryAttemptStatus represents the state machine for a specific delivery.
type DeliveryAttemptStatus string

const (
	DeliveryStatusPending    DeliveryAttemptStatus = "pending"
	DeliveryStatusDelivering DeliveryAttemptStatus = "delivering"
	DeliveryStatusDelivered  DeliveryAttemptStatus = "delivered"
	DeliveryStatusFailed     DeliveryAttemptStatus = "failed"
	DeliveryStatusRetrying   DeliveryAttemptStatus = "retrying"
	DeliveryStatusDeadLetter DeliveryAttemptStatus = "dead_letter"
)

// NotificationRule defines what happens when a specific event occurs.
// E.g., "When order.created happens, send a webhook to https://api.myclient.com/webhook"
type NotificationRule struct {
	ID        string          `json:"id"`
	TenantID  string          `json:"tenant_id"`
	EventType string          `json:"event_type"` // Action trigger (or "*" for all)
	Channel   string          `json:"channel"`    // "webhook", "email", etc.
	Config    json.RawMessage `json:"config"`     // Channel-specific config (e.g., URL, headers)
	Enabled   bool            `json:"enabled"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// DeliveryAttempt represents a single attempt to deliver a notification.
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
	RequestPayload     json.RawMessage       `json:"request_payload,omitempty"`
	ResponseStatus     *int                  `json:"response_status,omitempty"` // Pointer because it might not have an HTTP status if it failed before connecting
	ResponseBody       *string               `json:"response_body,omitempty"`
	ErrorMessage       *string               `json:"error_message,omitempty"`
	DurationMs         *int                  `json:"duration_ms,omitempty"`
	CreatedAt          time.Time             `json:"created_at"`
	CompletedAt        *time.Time            `json:"completed_at,omitempty"`
}
