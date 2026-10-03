package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"pulseflow/internal/platform/logger"
)

type fakeRateLimiter struct {
	allowed   bool
	remaining int64
	err       error
}

func (f fakeRateLimiter) Allow(context.Context, string, int, time.Duration) (bool, int64, error) {
	return f.allowed, f.remaining, f.err
}

func TestRateLimitRejectsExhaustedTenant(t *testing.T) {
	middleware := NewRateLimit(fakeRateLimiter{allowed: false}, 100, time.Minute, logger.New("error", "text"))
	request := httptest.NewRequest(http.MethodPost, "/v1/events", nil)
	request = request.WithContext(WithPrincipal(request.Context(), Principal{TenantID: "tenant-1"}))
	recorder := httptest.NewRecorder()

	middleware.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("next handler should not be called")
	})).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", recorder.Code)
	}
	if recorder.Header().Get("Retry-After") == "" {
		t.Fatal("expected Retry-After header")
	}
}

func TestRateLimitFailsOpen(t *testing.T) {
	metrics := &fakeRedisMetrics{}
	middleware := NewRateLimit(fakeRateLimiter{err: errors.New("redis unavailable")}, 100, time.Minute, logger.New("error", "text"), metrics)
	request := httptest.NewRequest(http.MethodPost, "/v1/events", nil)
	request = request.WithContext(WithPrincipal(request.Context(), Principal{TenantID: "tenant-1"}))
	recorder := httptest.NewRecorder()

	middleware.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("expected fail-open 204, got %d", recorder.Code)
	}
	if len(metrics.operations) != 1 || metrics.operations[0] != "rate_limit_fail_open" {
		t.Fatalf("fallback metrics = %v", metrics.operations)
	}
}
