package worker

import (
	"math/rand/v2"
	"time"

	"pulseflow/internal/domain"
)

type OutcomeKind string

const (
	OutcomeDelivered OutcomeKind = "delivered"
	OutcomePermanent OutcomeKind = "permanent_failure"
	OutcomeRetryable OutcomeKind = "retryable_failure"
)

// Outcome describes what happened outside PulseFlow. RetryPolicy translates
// it into the durable delivery state stored in PostgreSQL.
type Outcome struct {
	Kind           OutcomeKind
	ResponseStatus *int
	ResponseBody   *string
	ErrorMessage   *string
	RetryAfter     *time.Duration
}

type RetryPolicy struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
	Jitter    func(time.Duration) time.Duration
}

func NewRetryPolicy(baseDelay, maxDelay time.Duration) RetryPolicy {
	return RetryPolicy{
		BaseDelay: baseDelay,
		MaxDelay:  maxDelay,
		Jitter: func(delay time.Duration) time.Duration {
			// A 50%-150% range prevents many failing deliveries retrying together.
			return time.Duration(float64(delay) * (0.5 + rand.Float64()))
		},
	}
}

func (p RetryPolicy) Result(delivery domain.DeliveryAttempt, outcome Outcome, now time.Time, duration time.Duration) domain.DeliveryResult {
	durationMs := int(duration.Milliseconds())
	result := domain.DeliveryResult{
		ResponseStatus: outcome.ResponseStatus,
		ResponseBody:   outcome.ResponseBody,
		ErrorMessage:   outcome.ErrorMessage,
		DurationMs:     &durationMs,
	}

	switch outcome.Kind {
	case OutcomeDelivered:
		result.Status = domain.DeliveryStatusDelivered
	case OutcomePermanent:
		result.Status = domain.DeliveryStatusFailed
	default:
		result.Status = domain.DeliveryStatusRetrying
		delay := p.exponentialDelay(delivery.AttemptNumber + 1)
		if outcome.RetryAfter != nil && *outcome.RetryAfter >= 0 {
			delay = *outcome.RetryAfter
		} else if p.Jitter != nil {
			delay = p.Jitter(delay)
		}
		if delay > p.MaxDelay {
			delay = p.MaxDelay
		}
		nextRetry := now.Add(delay)
		result.NextRetryAt = &nextRetry
	}
	return result
}

func (p RetryPolicy) exponentialDelay(attemptNumber int) time.Duration {
	if attemptNumber < 1 {
		attemptNumber = 1
	}
	delay := p.BaseDelay
	for i := 1; i < attemptNumber && delay < p.MaxDelay; i++ {
		if delay > p.MaxDelay/2 {
			return p.MaxDelay
		}
		delay *= 2
	}
	if delay > p.MaxDelay {
		return p.MaxDelay
	}
	return delay
}
