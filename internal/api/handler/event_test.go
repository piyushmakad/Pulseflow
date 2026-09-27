package handler

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"

	"pulseflow/internal/api/middleware"
	"pulseflow/internal/domain"
	"pulseflow/internal/platform/logger"
	"pulseflow/internal/store/postgres"
)

type fakeEventStore struct {
	createdEvent domain.Event
	created      bool
	createErr    error
	createParams postgres.CreateEventParams
	getEvent     domain.Event
	getErr       error
	getTenantID  string
	getEventID   string
}

func (f *fakeEventStore) CreateEvent(_ context.Context, params postgres.CreateEventParams) (domain.Event, bool, error) {
	f.createParams = params
	return f.createdEvent, f.created, f.createErr
}

func (f *fakeEventStore) GetEvent(_ context.Context, tenantID, eventID string) (domain.Event, error) {
	f.getTenantID = tenantID
	f.getEventID = eventID
	return f.getEvent, f.getErr
}

func TestCreateEventAccepted(t *testing.T) {
	store := &fakeEventStore{
		created:      true,
		createdEvent: domain.Event{ID: "event-1", TenantID: "tenant-1", Type: "order.created"},
	}
	handler := NewEvent(store, "events.ingested", 1024, logger.New("error", "text"))
	request := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewBufferString(`{"type":"order.created","data":{"order_id":"123"}}`))
	request.Header.Set("Idempotency-Key", "order-123")
	request = request.WithContext(middleware.WithPrincipal(request.Context(), middleware.Principal{TenantID: "tenant-1"}))
	recorder := httptest.NewRecorder()

	handler.Create(recorder, request)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", recorder.Code, recorder.Body.String())
	}
	if store.createParams.TenantID != "tenant-1" || store.createParams.IdempotencyKey != "order-123" || store.createParams.Topic != "events.ingested" {
		t.Fatalf("unexpected repository params: %+v", store.createParams)
	}
}

func TestCreateEventDuplicateReturnsOK(t *testing.T) {
	store := &fakeEventStore{createdEvent: domain.Event{ID: "event-1"}, created: false}
	handler := NewEvent(store, "events.ingested", 1024, logger.New("error", "text"))
	request := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewBufferString(`{"type":"order.created","data":{}}`))
	request.Header.Set("Idempotency-Key", "order-123")
	request = request.WithContext(middleware.WithPrincipal(request.Context(), middleware.Principal{TenantID: "tenant-1"}))
	recorder := httptest.NewRecorder()

	handler.Create(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}
}

func TestCreateEventConflict(t *testing.T) {
	store := &fakeEventStore{createErr: domain.ErrAlreadyExists}
	handler := NewEvent(store, "events.ingested", 1024, logger.New("error", "text"))
	request := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewBufferString(`{"type":"order.created","data":{}}`))
	request.Header.Set("Idempotency-Key", "order-123")
	request = request.WithContext(middleware.WithPrincipal(request.Context(), middleware.Principal{TenantID: "tenant-1"}))
	recorder := httptest.NewRecorder()

	handler.Create(recorder, request)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", recorder.Code)
	}
}

func TestGetEventUsesAuthenticatedTenant(t *testing.T) {
	store := &fakeEventStore{getEvent: domain.Event{ID: "event-1", TenantID: "tenant-1"}}
	handler := NewEvent(store, "events.ingested", 1024, logger.New("error", "text"))
	request := httptest.NewRequest(http.MethodGet, "/v1/events/event-1", nil)
	routeContext := chi.NewRouteContext()
	routeContext.URLParams.Add("eventID", "event-1")
	ctx := context.WithValue(request.Context(), chi.RouteCtxKey, routeContext)
	ctx = middleware.WithPrincipal(ctx, middleware.Principal{TenantID: "tenant-1"})
	request = request.WithContext(ctx)
	recorder := httptest.NewRecorder()

	handler.Get(recorder, request)

	if recorder.Code != http.StatusOK || store.getTenantID != "tenant-1" || store.getEventID != "event-1" {
		t.Fatalf("unexpected get result: status=%d tenant=%q event=%q", recorder.Code, store.getTenantID, store.getEventID)
	}
}

func TestGetEventNotFound(t *testing.T) {
	store := &fakeEventStore{getErr: domain.ErrNotFound}
	handler := NewEvent(store, "events.ingested", 1024, logger.New("error", "text"))
	request := httptest.NewRequest(http.MethodGet, "/v1/events/missing", nil)
	routeContext := chi.NewRouteContext()
	routeContext.URLParams.Add("eventID", "missing")
	ctx := context.WithValue(request.Context(), chi.RouteCtxKey, routeContext)
	ctx = middleware.WithPrincipal(ctx, middleware.Principal{TenantID: "tenant-1"})
	request = request.WithContext(ctx)
	recorder := httptest.NewRecorder()

	handler.Get(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", recorder.Code)
	}
}
