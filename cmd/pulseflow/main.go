package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"pulseflow/internal/config"
	"pulseflow/internal/platform/logger"
)

func main() {
	mode := flag.String("mode", "all", "Run mode: all, api, worker")
	flag.Parse()

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	log := logger.New(cfg.LogLevel, cfg.LogFormat)
	log.Info("starting pulseflow",
		"mode", *mode,
		"version", version(),
	)

	// Root context cancelled on SIGTERM/SIGINT
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	var wg sync.WaitGroup

	switch *mode {
	case "all":
		startAPI(ctx, &wg, cfg, log)
		startWorker(ctx, &wg, cfg, log)
	case "api":
		startAPI(ctx, &wg, cfg, log)
	case "worker":
		startWorker(ctx, &wg, cfg, log)
	default:
		log.Error("unknown mode", "mode", *mode)
		os.Exit(1)
	}

	// Block until context is cancelled (signal received)
	<-ctx.Done()
	log.Info("shutdown signal received, draining...")

	// Wait for all goroutine groups to finish
	wg.Wait()
	log.Info("shutdown complete")
}

func startAPI(ctx context.Context, wg *sync.WaitGroup, cfg *config.Config, log *logger.Logger) {
	// Will be implemented in Phase 3
	log.Info("api server starting", "port", cfg.HTTPPort)
}

func startWorker(ctx context.Context, wg *sync.WaitGroup, cfg *config.Config, log *logger.Logger) {
	// Will be implemented in Phase 4-5
	log.Info("worker engine starting")
}

func version() string {
	return "0.1.0"
}
