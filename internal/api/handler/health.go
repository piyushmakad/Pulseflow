package handler

import (
	"context"
	"net/http"
	"time"

	"pulseflow/internal/api/response"
	"pulseflow/internal/platform/logger"
)

type HealthPinger interface {
	Ping(ctx context.Context) error
}

type Health struct {
	postgres HealthPinger
	redis    HealthPinger
	logger   *logger.Logger
}

func NewHealth(postgres, redis HealthPinger, log *logger.Logger) *Health {
	return &Health{postgres: postgres, redis: redis, logger: log}
}

func (h *Health) Live(w http.ResponseWriter, _ *http.Request) {
	response.JSON(w, http.StatusOK, map[string]any{"status": "alive"})
}

func (h *Health) Ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	checks := map[string]string{"postgres": "ok"}
	if err := h.postgres.Ping(ctx); err != nil {
		checks["postgres"] = "unavailable"
		h.logger.Error("readiness PostgreSQL check failed", "error", err)
		response.JSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "not_ready",
			"checks": checks,
		})
		return
	}

	status := "ready"
	if h.redis != nil {
		checks["redis"] = "ok"
		if err := h.redis.Ping(ctx); err != nil {
			checks["redis"] = "degraded"
			status = "degraded"
			h.logger.Warn("readiness Redis check degraded", "error", err)
		}
	}
	response.JSON(w, http.StatusOK, map[string]any{"status": status, "checks": checks})
}
