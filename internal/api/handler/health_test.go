package handler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"pulseflow/internal/platform/logger"
)

type healthPingerFunc func(context.Context) error

func (fn healthPingerFunc) Ping(ctx context.Context) error {
	return fn(ctx)
}

func TestHealthReadyWithoutRedis(t *testing.T) {
	health := NewHealth(healthPingerFunc(func(context.Context) error { return nil }), nil, logger.New("error", "json"))
	recorder := httptest.NewRecorder()
	health.Ready(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if strings.Contains(recorder.Body.String(), "redis") {
		t.Fatalf("worker-only readiness reported a Redis check: %s", recorder.Body.String())
	}
}

func TestHealthReadyRequiresPostgres(t *testing.T) {
	health := NewHealth(healthPingerFunc(func(context.Context) error { return errors.New("database unavailable") }), nil, logger.New("error", "json"))
	recorder := httptest.NewRecorder()
	health.Ready(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
}

func TestHealthReadyTreatsRedisAsDegraded(t *testing.T) {
	postgres := healthPingerFunc(func(context.Context) error { return nil })
	redis := healthPingerFunc(func(context.Context) error { return errors.New("redis unavailable") })
	health := NewHealth(postgres, redis, logger.New("error", "json"))
	recorder := httptest.NewRecorder()
	health.Ready(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if !strings.Contains(recorder.Body.String(), `"status":"degraded"`) {
		t.Fatalf("response did not report degraded Redis: %s", recorder.Body.String())
	}
}
