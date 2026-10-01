package domain

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestEventStatusTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		from    EventStatus
		to      EventStatus
		allowed bool
	}{
		{"accepted to processing", EventStatusAccepted, EventStatusProcessing, true},
		{"processing to completed", EventStatusProcessing, EventStatusCompleted, true},
		{"processing to partially failed", EventStatusProcessing, EventStatusPartiallyFailed, true},
		{"processing to failed", EventStatusProcessing, EventStatusFailed, true},
		{"accepted cannot skip processing", EventStatusAccepted, EventStatusCompleted, false},
		{"terminal cannot restart", EventStatusCompleted, EventStatusProcessing, false},
		{"processing cannot move backwards", EventStatusProcessing, EventStatusAccepted, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.from.ValidateTransition(tt.to)
			if tt.allowed && err != nil {
				t.Fatalf("expected transition to be allowed, got %v", err)
			}
			if !tt.allowed && !errors.Is(err, ErrInvalidStateTransition) {
				t.Fatalf("expected ErrInvalidStateTransition, got %v", err)
			}
		})
	}
}

func TestEventMessageValidation(t *testing.T) {
	t.Parallel()
	valid := EventMessage{
		Version: EventMessageVersion, EventID: "event-1", TenantID: "tenant-1",
		Type: "order.created", Data: json.RawMessage(`{"order_id":"123"}`), OccurredAt: time.Now(),
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid message rejected: %v", err)
	}

	invalidVersion := valid
	invalidVersion.Version++
	if err := invalidVersion.Validate(); !errors.Is(err, ErrValidation) {
		t.Fatalf("expected version validation error, got %v", err)
	}

	invalidData := valid
	invalidData.Data = json.RawMessage(`not-json`)
	if err := invalidData.Validate(); !errors.Is(err, ErrValidation) {
		t.Fatalf("expected data validation error, got %v", err)
	}
}

func TestDeliveryStatusTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		from    DeliveryAttemptStatus
		to      DeliveryAttemptStatus
		allowed bool
	}{
		{"pending to delivering", DeliveryStatusPending, DeliveryStatusDelivering, true},
		{"delivering to delivered", DeliveryStatusDelivering, DeliveryStatusDelivered, true},
		{"delivering to failed", DeliveryStatusDelivering, DeliveryStatusFailed, true},
		{"delivering to retrying", DeliveryStatusDelivering, DeliveryStatusRetrying, true},
		{"retrying to delivering", DeliveryStatusRetrying, DeliveryStatusDelivering, true},
		{"retrying to dead letter", DeliveryStatusRetrying, DeliveryStatusDeadLetter, true},
		{"pending cannot complete", DeliveryStatusPending, DeliveryStatusDelivered, false},
		{"delivered is terminal", DeliveryStatusDelivered, DeliveryStatusDelivering, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.from.ValidateTransition(tt.to)
			if tt.allowed && err != nil {
				t.Fatalf("expected transition to be allowed, got %v", err)
			}
			if !tt.allowed && !errors.Is(err, ErrInvalidStateTransition) {
				t.Fatalf("expected ErrInvalidStateTransition, got %v", err)
			}
		})
	}
}
