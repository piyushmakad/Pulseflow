package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"pulseflow/internal/admin"
	"pulseflow/internal/api"
	"pulseflow/internal/api/handler"
	"pulseflow/internal/api/middleware"
	"pulseflow/internal/domain"
	"pulseflow/internal/platform/logger"
	"pulseflow/internal/store/postgres"
)

type allowAllRateLimiter struct{}

func (allowAllRateLimiter) Allow(context.Context, string, int, time.Duration) (bool, int64, error) {
	return true, 99, nil
}

func TestEventAPIIntegration(t *testing.T) {
	store := apiIntegrationStore(t)
	log := logger.New("error", "text")
	provisioned, err := admin.Provision(context.Background(), store, "API integration tenant", "integration")
	if err != nil {
		t.Fatalf("provision tenant: %v", err)
	}

	auth := middleware.NewAuth(store, nil, time.Minute, log)
	rateLimit := middleware.NewRateLimit(allowAllRateLimiter{}, 100, time.Minute, log)
	events := handler.NewEvent(store, "events.ingested", 1<<20, log)
	health := handler.NewHealth(store, nil, log)
	server := httptest.NewServer(api.NewRouter(events, health, auth, rateLimit))
	t.Cleanup(server.Close)

	requestBody := []byte(`{"type":"order.created","data":{"order_id":"123"}}`)
	createdResponse := doEventRequest(t, server.Client(), http.MethodPost, server.URL+"/v1/events", provisioned.RawKey, "order-123", requestBody)
	if createdResponse.StatusCode != http.StatusAccepted {
		t.Fatalf("expected create 202, got %d", createdResponse.StatusCode)
	}
	var envelope struct {
		Data domain.Event `json:"data"`
	}
	if err := json.NewDecoder(createdResponse.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	_ = createdResponse.Body.Close()
	if envelope.Data.ID == "" || envelope.Data.TenantID != provisioned.Tenant.ID {
		t.Fatalf("unexpected created event: %+v", envelope.Data)
	}

	duplicateResponse := doEventRequest(t, server.Client(), http.MethodPost, server.URL+"/v1/events", provisioned.RawKey, "order-123", requestBody)
	if duplicateResponse.StatusCode != http.StatusOK {
		t.Fatalf("expected duplicate 200, got %d", duplicateResponse.StatusCode)
	}
	_ = duplicateResponse.Body.Close()

	conflictResponse := doEventRequest(t, server.Client(), http.MethodPost, server.URL+"/v1/events", provisioned.RawKey, "order-123", []byte(`{"type":"order.created","data":{"order_id":"different"}}`))
	if conflictResponse.StatusCode != http.StatusConflict {
		t.Fatalf("expected conflict 409, got %d", conflictResponse.StatusCode)
	}
	_ = conflictResponse.Body.Close()

	getResponse := doEventRequest(t, server.Client(), http.MethodGet, server.URL+"/v1/events/"+envelope.Data.ID, provisioned.RawKey, "", nil)
	if getResponse.StatusCode != http.StatusOK {
		t.Fatalf("expected get 200, got %d", getResponse.StatusCode)
	}
	_ = getResponse.Body.Close()

	unauthorizedResponse := doEventRequest(t, server.Client(), http.MethodGet, server.URL+"/v1/events/"+envelope.Data.ID, "pf_live_invalid", "", nil)
	if unauthorizedResponse.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected invalid key 401, got %d", unauthorizedResponse.StatusCode)
	}
	_ = unauthorizedResponse.Body.Close()
}

func doEventRequest(t *testing.T, client *http.Client, method, url, apiKey, idempotencyKey string, body []byte) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create HTTP request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+apiKey)
	request.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("perform HTTP request: %v", err)
	}
	return response
}

func apiIntegrationStore(t *testing.T) *postgres.Store {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set TEST_DATABASE_URL to run API integration tests")
	}

	ctx := context.Background()
	adminPool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect admin pool: %v", err)
	}
	schema := fmt.Sprintf("pulseflow_api_test_%d", time.Now().UnixNano())
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		adminPool.Close()
		t.Fatalf("create test schema: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse test database URL: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("create test pool: %v", err)
	}
	migrationPath := filepath.Join("..", "..", "migrations", "001_initial_schema.up.sql")
	migration, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		_, _ = adminPool.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		adminPool.Close()
	})
	return postgres.NewWithPool(pool)
}
