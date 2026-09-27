package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"

	"pulseflow/internal/api/handler"
	appmiddleware "pulseflow/internal/api/middleware"
)

func NewRouter(events *handler.Event, health *handler.Health, auth *appmiddleware.Auth, rateLimit *appmiddleware.RateLimit) http.Handler {
	router := chi.NewRouter()
	router.Use(appmiddleware.RequestID)
	router.Use(chimiddleware.Recoverer)

	router.Get("/healthz", health.Live)
	router.Get("/readyz", health.Ready)

	router.Route("/v1", func(r chi.Router) {
		r.Use(auth.Middleware)
		r.Use(rateLimit.Middleware)
		r.Post("/events", events.Create)
		r.Get("/events/{eventID}", events.Get)
	})

	return router
}
