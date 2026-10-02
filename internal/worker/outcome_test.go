package worker

import (
	"testing"
	"time"

	"pulseflow/internal/domain"
)

func TestRetryPolicyUsesExponentialDelayAndCap(t *testing.T) {
	policy := RetryPolicy{BaseDelay: time.Second, MaxDelay: 5 * time.Second, Jitter: func(d time.Duration) time.Duration { return d }}
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)

	first := policy.Result(domain.DeliveryAttempt{AttemptNumber: 0}, Outcome{Kind: OutcomeRetryable}, now, time.Millisecond)
	if first.NextRetryAt == nil || first.NextRetryAt.Sub(now) != time.Second {
		t.Fatalf("first retry delay = %v, want 1s", first.NextRetryAt)
	}
	capped := policy.Result(domain.DeliveryAttempt{AttemptNumber: 10}, Outcome{Kind: OutcomeRetryable}, now, time.Millisecond)
	if capped.NextRetryAt == nil || capped.NextRetryAt.Sub(now) != 5*time.Second {
		t.Fatalf("capped retry delay = %v, want 5s", capped.NextRetryAt)
	}
}

func TestRetryPolicyHonorsRetryAfter(t *testing.T) {
	policy := RetryPolicy{BaseDelay: time.Second, MaxDelay: time.Minute, Jitter: func(d time.Duration) time.Duration { return d }}
	now := time.Now()
	retryAfter := 12 * time.Second
	result := policy.Result(domain.DeliveryAttempt{}, Outcome{Kind: OutcomeRetryable, RetryAfter: &retryAfter}, now, 0)
	if result.NextRetryAt == nil || result.NextRetryAt.Sub(now) != retryAfter {
		t.Fatalf("retry delay = %v, want %v", result.NextRetryAt, retryAfter)
	}
}
