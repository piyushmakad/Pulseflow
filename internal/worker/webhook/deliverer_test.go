package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"pulseflow/internal/domain"
	"pulseflow/internal/worker"
)

func TestDeliverClassifiesResponsesAndSendsStableHeaders(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		retryAfter string
		want       worker.OutcomeKind
	}{
		{name: "success", status: http.StatusNoContent, want: worker.OutcomeDelivered},
		{name: "bad request", status: http.StatusBadRequest, want: worker.OutcomePermanent},
		{name: "timeout", status: http.StatusRequestTimeout, want: worker.OutcomeRetryable},
		{name: "rate limited", status: http.StatusTooManyRequests, retryAfter: "7", want: worker.OutcomeRetryable},
		{name: "provider unavailable", status: http.StatusServiceUnavailable, want: worker.OutcomeRetryable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.Header.Get("Idempotency-Key"); got != "delivery-1" {
					t.Errorf("Idempotency-Key = %q", got)
				}
				if got := r.Header.Get("X-PulseFlow-Attempt"); got != "3" {
					t.Errorf("X-PulseFlow-Attempt = %q", got)
				}
				if tt.retryAfter != "" {
					w.Header().Set("Retry-After", tt.retryAfter)
				}
				w.WriteHeader(tt.status)
			}))
			defer server.Close()
			config, _ := json.Marshal(Config{URL: server.URL, Headers: map[string]string{"Idempotency-Key": "cannot-override"}})
			outcome := New(server.Client()).Deliver(context.Background(), domain.DeliveryAttempt{
				ID: "delivery-1", AttemptNumber: 2, RequestPayload: json.RawMessage(`{"ok":true}`), DestinationConfig: config,
			})
			if outcome.Kind != tt.want {
				t.Fatalf("kind = %s, want %s", outcome.Kind, tt.want)
			}
			if tt.retryAfter != "" && (outcome.RetryAfter == nil || *outcome.RetryAfter != 7*time.Second) {
				t.Fatalf("Retry-After = %v, want 7s", outcome.RetryAfter)
			}
		})
	}
}

func TestDeliverTreatsNetworkTimeoutAsRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	config, _ := json.Marshal(Config{URL: server.URL})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	outcome := New(server.Client()).Deliver(ctx, domain.DeliveryAttempt{DestinationConfig: config})
	if outcome.Kind != worker.OutcomeRetryable {
		t.Fatalf("kind = %s, want retryable", outcome.Kind)
	}
}
