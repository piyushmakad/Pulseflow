package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"pulseflow/internal/domain"
	"pulseflow/internal/platform/logger"
)

type fakeAPIKeyRepository struct {
	key   domain.APIKey
	err   error
	calls int
}

func (f *fakeAPIKeyRepository) FindActiveAPIKeyByHash(context.Context, string) (domain.APIKey, error) {
	f.calls++
	return f.key, f.err
}

type fakeAPIKeyCache struct {
	tenantID string
	apiKeyID string
	found    bool
	getErr   error
	putCalls int
}

type fakeRedisMetrics struct {
	operations []string
}

func (m *fakeRedisMetrics) RecordRedisFallback(operation string) {
	m.operations = append(m.operations, operation)
}

func (f *fakeAPIKeyCache) GetAPIKey(context.Context, string) (string, string, bool, error) {
	return f.tenantID, f.apiKeyID, f.found, f.getErr
}

func (f *fakeAPIKeyCache) PutAPIKey(context.Context, string, string, string, time.Duration) error {
	f.putCalls++
	return nil
}

func TestAuthUsesCacheHit(t *testing.T) {
	repository := &fakeAPIKeyRepository{}
	cache := &fakeAPIKeyCache{tenantID: "tenant-1", apiKeyID: "key-1", found: true}
	auth := NewAuth(repository, cache, time.Minute, logger.New("error", "text"))

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := PrincipalFromContext(r.Context())
		if !ok || principal.TenantID != "tenant-1" || principal.APIKeyID != "key-1" {
			t.Fatalf("unexpected principal: %+v found=%v", principal, ok)
		}
		w.WriteHeader(http.StatusNoContent)
	})

	request := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	request.Header.Set("Authorization", "Bearer pf_live_test")
	recorder := httptest.NewRecorder()
	auth.Middleware(next).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", recorder.Code)
	}
	if repository.calls != 0 {
		t.Fatalf("expected PostgreSQL not to be queried, got %d calls", repository.calls)
	}
}

func TestAuthFallsBackToPostgresWhenCacheFails(t *testing.T) {
	repository := &fakeAPIKeyRepository{key: domain.APIKey{ID: "key-2", TenantID: "tenant-2"}}
	cache := &fakeAPIKeyCache{getErr: errors.New("redis unavailable")}
	metrics := &fakeRedisMetrics{}
	auth := NewAuth(repository, cache, time.Minute, logger.New("error", "text"), metrics)

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, _ := PrincipalFromContext(r.Context())
		if principal.TenantID != "tenant-2" {
			t.Fatalf("expected PostgreSQL principal, got %+v", principal)
		}
		w.WriteHeader(http.StatusNoContent)
	})

	request := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	request.Header.Set("Authorization", "Bearer pf_live_test")
	recorder := httptest.NewRecorder()
	auth.Middleware(next).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNoContent || repository.calls != 1 || cache.putCalls != 1 {
		t.Fatalf("unexpected result: status=%d repository_calls=%d cache_puts=%d", recorder.Code, repository.calls, cache.putCalls)
	}
	if len(metrics.operations) != 1 || metrics.operations[0] != "api_key_lookup_error" {
		t.Fatalf("fallback metrics = %v", metrics.operations)
	}
}

func TestAuthRejectsInvalidKey(t *testing.T) {
	repository := &fakeAPIKeyRepository{err: domain.ErrNotFound}
	auth := NewAuth(repository, nil, time.Minute, logger.New("error", "text"))

	request := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	request.Header.Set("Authorization", "Bearer not-a-pulseflow-key")
	recorder := httptest.NewRecorder()
	auth.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("next handler should not be called")
	})).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", recorder.Code)
	}
}
