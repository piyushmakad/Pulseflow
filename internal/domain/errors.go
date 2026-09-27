package domain

import "errors"

// Sentinel errors used across the domain layer.
var (
	// ErrNotFound indicates the requested resource does not exist.
	ErrNotFound = errors.New("resource not found")

	// ErrAlreadyExists indicates a uniqueness constraint violation (e.g., idempotency key).
	ErrAlreadyExists = errors.New("resource already exists")

	// ErrInvalidStateTransition indicates an illegal state machine transition.
	ErrInvalidStateTransition = errors.New("invalid state transition")

	// ErrUnauthorized indicates missing or invalid authentication.
	ErrUnauthorized = errors.New("unauthorized")

	// ErrForbidden indicates the tenant does not have access to the resource.
	ErrForbidden = errors.New("forbidden")

	// ErrRateLimited indicates the tenant has exceeded their rate limit.
	ErrRateLimited = errors.New("rate limited")

	// ErrUnavailable indicates a required durable dependency is temporarily unavailable.
	ErrUnavailable = errors.New("service unavailable")

	// ErrValidation indicates the request failed validation.
	ErrValidation = errors.New("validation error")
)

// ValidationError wraps ErrValidation with a specific field and message.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return "validation error: " + e.Field + ": " + e.Message
}

func (e *ValidationError) Unwrap() error {
	return ErrValidation
}

// NewValidationError creates a new ValidationError.
func NewValidationError(field, message string) *ValidationError {
	return &ValidationError{Field: field, Message: message}
}
