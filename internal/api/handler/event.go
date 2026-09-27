package handler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"pulseflow/internal/api/middleware"
	"pulseflow/internal/api/response"
	"pulseflow/internal/domain"
	"pulseflow/internal/platform/logger"
	"pulseflow/internal/store/postgres"
)

type EventStore interface {
	CreateEvent(ctx context.Context, params postgres.CreateEventParams) (domain.Event, bool, error)
	GetEvent(ctx context.Context, tenantID, eventID string) (domain.Event, error)
}

type Event struct {
	store        EventStore
	topic        string
	maxBodyBytes int64
	logger       *logger.Logger
}

func NewEvent(store EventStore, topic string, maxBodyBytes int64, log *logger.Logger) *Event {
	return &Event{store: store, topic: topic, maxBodyBytes: maxBodyBytes, logger: log}
}

type createEventRequest struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type eventEnvelope struct {
	Data    domain.Event `json:"data"`
	Created bool         `json:"created"`
}

func (h *Event) Create(w http.ResponseWriter, r *http.Request) {
	principal, ok := middleware.PrincipalFromContext(r.Context())
	if !ok {
		response.Error(w, http.StatusUnauthorized, "unauthorized", "authenticated tenant is missing")
		return
	}

	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if idempotencyKey == "" {
		response.Error(w, http.StatusBadRequest, "validation_error", "Idempotency-Key header is required")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, h.maxBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var request createEventRequest
	if err := decoder.Decode(&request); err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			response.Error(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body is too large")
			return
		}
		response.Error(w, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		response.Error(w, http.StatusBadRequest, "invalid_json", "request body must contain one JSON object")
		return
	}

	event, created, err := h.store.CreateEvent(r.Context(), postgres.CreateEventParams{
		TenantID:       principal.TenantID,
		Type:           request.Type,
		IdempotencyKey: idempotencyKey,
		Data:           request.Data,
		Topic:          h.topic,
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}

	status := http.StatusOK
	if created {
		status = http.StatusAccepted
	}
	response.JSON(w, status, eventEnvelope{Data: event, Created: created})
}

func (h *Event) Get(w http.ResponseWriter, r *http.Request) {
	principal, ok := middleware.PrincipalFromContext(r.Context())
	if !ok {
		response.Error(w, http.StatusUnauthorized, "unauthorized", "authenticated tenant is missing")
		return
	}

	event, err := h.store.GetEvent(r.Context(), principal.TenantID, chi.URLParam(r, "eventID"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	response.JSON(w, http.StatusOK, eventEnvelope{Data: event})
}

func (h *Event) writeError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, domain.ErrValidation):
		response.Error(w, http.StatusBadRequest, "validation_error", err.Error())
	case errors.Is(err, domain.ErrAlreadyExists):
		response.Error(w, http.StatusConflict, "idempotency_conflict", err.Error())
	case errors.Is(err, domain.ErrNotFound):
		response.Error(w, http.StatusNotFound, "not_found", "event not found")
	case errors.Is(err, domain.ErrUnavailable):
		response.Error(w, http.StatusServiceUnavailable, "service_unavailable", "event storage is temporarily unavailable")
	default:
		h.logger.Error("event API operation failed",
			"request_id", middleware.RequestIDFromContext(r.Context()),
			"error", err,
		)
		response.Error(w, http.StatusInternalServerError, "internal_error", "an internal error occurred")
	}
}
