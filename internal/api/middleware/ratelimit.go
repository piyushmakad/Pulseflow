package middleware

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"pulseflow/internal/api/response"
	"pulseflow/internal/platform/logger"
)

type TenantRateLimiter interface {
	Allow(ctx context.Context, tenantID string, capacity int, window time.Duration) (allowed bool, remaining int64, err error)
}

type RateLimit struct {
	limiter  TenantRateLimiter
	capacity int
	window   time.Duration
	logger   *logger.Logger
	metrics  RedisFallbackRecorder
}

func NewRateLimit(limiter TenantRateLimiter, capacity int, window time.Duration, log *logger.Logger, metrics ...RedisFallbackRecorder) *RateLimit {
	rateLimit := &RateLimit{limiter: limiter, capacity: capacity, window: window, logger: log}
	if len(metrics) > 0 {
		rateLimit.metrics = metrics[0]
	}
	return rateLimit
}

func (m *RateLimit) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := PrincipalFromContext(r.Context())
		if !ok {
			response.Error(w, http.StatusUnauthorized, "unauthorized", "authenticated tenant is missing")
			return
		}

		allowed, remaining, err := m.limiter.Allow(r.Context(), principal.TenantID, m.capacity, m.window)
		if err != nil {
			if m.metrics != nil {
				m.metrics.RecordRedisFallback("rate_limit_fail_open")
			}
			m.logger.Warn("rate limiter unavailable; allowing request", "tenant_id", principal.TenantID, "error", err)
			next.ServeHTTP(w, r)
			return
		}

		w.Header().Set("X-RateLimit-Limit", strconv.Itoa(m.capacity))
		w.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(remaining, 10))
		if !allowed {
			retryAfter := int64((m.window + time.Second - 1) / time.Second)
			w.Header().Set("Retry-After", strconv.FormatInt(retryAfter, 10))
			response.Error(w, http.StatusTooManyRequests, "rate_limited", "rate limit exceeded")
			return
		}
		next.ServeHTTP(w, r)
	})
}
