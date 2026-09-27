package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"pulseflow/internal/platform/logger"
)

type ServerConfig struct {
	Port            int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration
}

type Server struct {
	httpServer      *http.Server
	shutdownTimeout time.Duration
	logger          *logger.Logger
}

func NewServer(cfg ServerConfig, router http.Handler, log *logger.Logger) *Server {
	return &Server{
		httpServer: &http.Server{
			Addr:         fmt.Sprintf(":%d", cfg.Port),
			Handler:      router,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
			IdleTimeout:  cfg.IdleTimeout,
		},
		shutdownTimeout: cfg.ShutdownTimeout,
		logger:          log,
	}
}

func (s *Server) Run(ctx context.Context) error {
	serveErrors := make(chan error, 1)
	go func() {
		s.logger.Info("HTTP server listening", "address", s.httpServer.Addr)
		serveErrors <- s.httpServer.ListenAndServe()
	}()

	select {
	case err := <-serveErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve HTTP: %w", err)
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
	defer cancel()
	if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
		_ = s.httpServer.Close()
		return fmt.Errorf("shutdown HTTP server: %w", err)
	}

	err := <-serveErrors
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve HTTP during shutdown: %w", err)
	}
	return nil
}
