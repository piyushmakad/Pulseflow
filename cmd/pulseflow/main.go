package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"pulseflow/internal/admin"
	"pulseflow/internal/api"
	"pulseflow/internal/api/handler"
	"pulseflow/internal/api/middleware"
	"pulseflow/internal/config"
	"pulseflow/internal/platform/logger"
	postgresstore "pulseflow/internal/store/postgres"
	redisstore "pulseflow/internal/store/redis"
)

func main() {
	command := flag.String("command", "run", "Command: run or provision")
	mode := flag.String("mode", "all", "Run mode: all, api, worker")
	tenantName := flag.String("tenant-name", "Local Development", "Tenant name used by the provision command")
	keyName := flag.String("key-name", "default", "API key name used by the provision command")
	flag.Parse()

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	log := logger.New(cfg.LogLevel, cfg.LogFormat)
	log.Info("starting pulseflow",
		"command", *command,
		"mode", *mode,
		"version", version(),
	)
	if *command == "provision" {
		if err := provision(context.Background(), cfg, *tenantName, *keyName); err != nil {
			log.Error("provision failed", "error", err)
			os.Exit(1)
		}
		return
	}
	if *command != "run" {
		log.Error("unknown command", "command", *command)
		os.Exit(1)
	}

	// Root context cancelled on SIGTERM/SIGINT
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	var wg sync.WaitGroup

	switch *mode {
	case "all":
		if err := startAPI(ctx, cancel, &wg, cfg, log); err != nil {
			log.Error("failed to start API", "error", err)
			os.Exit(1)
		}
		startWorker(ctx, &wg, cfg, log)
	case "api":
		if err := startAPI(ctx, cancel, &wg, cfg, log); err != nil {
			log.Error("failed to start API", "error", err)
			os.Exit(1)
		}
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

func startAPI(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, cfg *config.Config, log *logger.Logger) error {
	postgres, err := postgresstore.New(ctx, cfg.DatabaseURL, cfg.DatabaseMaxConn)
	if err != nil {
		return err
	}

	redis, err := redisstore.New(cfg.RedisURL)
	if err != nil {
		postgres.Close()
		return err
	}

	auth := middleware.NewAuth(postgres, redis, cfg.APIKeyCacheTTL, log)
	rateLimit := middleware.NewRateLimit(redis, cfg.RateLimitRequests, cfg.RateLimitWindow, log)
	events := handler.NewEvent(postgres, cfg.KafkaEventsTopic, int64(cfg.HTTPMaxBodyBytes), log)
	health := handler.NewHealth(postgres, redis, log)
	router := api.NewRouter(events, health, auth, rateLimit)
	server := api.NewServer(api.ServerConfig{
		Port:            cfg.HTTPPort,
		ReadTimeout:     cfg.HTTPReadTimeout,
		WriteTimeout:    cfg.HTTPWriteTimeout,
		IdleTimeout:     cfg.HTTPIdleTimeout,
		ShutdownTimeout: cfg.ShutdownTimeout,
	}, router, log)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer postgres.Close()
		defer func() {
			if err := redis.Close(); err != nil {
				log.Warn("failed to close Redis client", "error", err)
			}
		}()

		if err := server.Run(ctx); err != nil {
			log.Error("HTTP server stopped with error", "error", err)
			cancel()
		}
	}()
	return nil
}

func startWorker(ctx context.Context, wg *sync.WaitGroup, cfg *config.Config, log *logger.Logger) {
	// Will be implemented in Phase 4-5
	log.Info("worker engine starting")
}

func provision(ctx context.Context, cfg *config.Config, tenantName, keyName string) error {
	store, err := postgresstore.New(ctx, cfg.DatabaseURL, cfg.DatabaseMaxConn)
	if err != nil {
		return err
	}
	defer store.Close()

	result, err := admin.Provision(ctx, store, tenantName, keyName)
	if err != nil {
		return err
	}
	fmt.Printf("Tenant ID: %s\nAPI Key ID: %s\nAPI Key (shown once): %s\n", result.Tenant.ID, result.APIKey.ID, result.RawKey)
	return nil
}

func version() string {
	return "0.1.0"
}
