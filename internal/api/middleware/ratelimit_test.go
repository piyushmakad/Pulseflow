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

type countingRateLimiter struct{ calls int }

func (f *countingRateLimiter) Allow(context.Context, string, int, time.Duration) (bool, int64, error) {
	f.calls++
	return false, 0, nil
}

func TestRateLimitTenantBypass(t *testing.T) {
	for _, tc := range []struct {
		name          string
		bypass        string
		tenant        string
		authenticated bool
		status        int
		calls         int
	}{
		{"test tenant", "test-tenant", "test-tenant", true, 204, 0},
		{"other tenant cannot spoof bypass", "test-tenant", "other-tenant", true, 429, 1},
		{"disabled by default", "", "test-tenant", true, 429, 1},
		{"missing authentication", "test-tenant", "", false, 401, 0},
		{"empty tenant does not bypass", "", "", true, 429, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			limiter := &countingRateLimiter{}
			m := NewRateLimit(limiter, 100, time.Minute, tc.bypass, logger.New("error", "text"))
			r := httptest.NewRequest(http.MethodPost, "/v1/events", nil)
			r.Header.Set("X-Tenant-ID", "test-tenant")
			r.Header.Set("X-RateLimit-Bypassed", "true")
			if tc.authenticated {
				r = r.WithContext(WithPrincipal(r.Context(), Principal{TenantID: tc.tenant}))
			}
			w := httptest.NewRecorder()
			m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			})).ServeHTTP(w, r)
			if w.Code != tc.status || limiter.calls != tc.calls {
				t.Fatalf("status=%d calls=%d; want status=%d calls=%d", w.Code, limiter.calls, tc.status, tc.calls)
			}
			if got := w.Header().Get("X-RateLimit-Bypassed"); (got == "true") != (tc.status == 204) {
				t.Fatalf("unexpected bypass header %q", got)
			}
			if tc.status == 204 && w.Header().Get("X-RateLimit-Limit") != "" {
				t.Fatal("bypassed request must not advertise an enforced quota")
			}
		})
	}
}

func (f fakeRateLimiter) Allow(context.Context, string, int, time.Duration) (bool, int64, error) {
	return f.allowed, f.remaining, f.err
}

func TestRateLimitRejectsExhaustedTenant(t *testing.T) {
	middleware := NewRateLimit(fakeRateLimiter{allowed: false}, 100, time.Minute, "", logger.New("error", "text"))
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
	middleware := NewRateLimit(fakeRateLimiter{err: errors.New("redis unavailable")}, 100, time.Minute, "", logger.New("error", "text"), metrics)
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
