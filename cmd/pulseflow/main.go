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
	kafkaruntime "pulseflow/internal/kafka"
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

	if err := run(ctx, cancel, *mode, cfg, log); err != nil {
		log.Error("pulseflow stopped with error", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cancel context.CancelFunc, mode string, cfg *config.Config, log *logger.Logger) error {
	if mode != "all" && mode != "api" && mode != "worker" {
		return fmt.Errorf("unknown mode %q", mode)
	}
	postgres, err := postgresstore.New(ctx, cfg.DatabaseURL, cfg.DatabaseMaxConn)
	if err != nil {
		return fmt.Errorf("connect PostgreSQL: %w", err)
	}
	defer postgres.Close()

	var wg sync.WaitGroup

	switch mode {
	case "all":
		if err := startAPI(ctx, cancel, &wg, cfg, log, postgres); err != nil {
			return fmt.Errorf("start API: %w", err)
		}
		if err := startWorker(ctx, cancel, &wg, cfg, log, postgres); err != nil {
			cancel()
			wg.Wait()
			return fmt.Errorf("start worker: %w", err)
		}
	case "api":
		if err := startAPI(ctx, cancel, &wg, cfg, log, postgres); err != nil {
			return fmt.Errorf("start API: %w", err)
		}
	case "worker":
		if err := startWorker(ctx, cancel, &wg, cfg, log, postgres); err != nil {
			return fmt.Errorf("start worker: %w", err)
		}
	default:
		return fmt.Errorf("unknown mode %q", mode)
	}

	// Block until context is cancelled (signal received)
	<-ctx.Done()
	log.Info("shutdown signal received, draining...")

	// Wait for all goroutine groups to finish
	wg.Wait()
	log.Info("shutdown complete")
	return nil
}

func startAPI(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, cfg *config.Config, log *logger.Logger, postgres *postgresstore.Store) error {
	redis, err := redisstore.New(cfg.RedisURL)
	if err != nil {
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

func startWorker(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, cfg *config.Config, log *logger.Logger, postgres *postgresstore.Store) error {
	producer, err := kafkaruntime.NewProducer(cfg.KafkaBrokerAddresses())
	if err != nil {
		return err
	}
	reader, err := kafkaruntime.NewReader(kafkaruntime.ReaderConfig{
		Brokers: cfg.KafkaBrokerAddresses(),
		GroupID: cfg.KafkaConsumerGroup,
		Topic:   cfg.KafkaEventsTopic,
	})
	if err != nil {
		_ = producer.Close()
		return err
	}

	owner := outboxOwner()
	relay, err := kafkaruntime.NewOutboxRelay(postgres, producer, kafkaruntime.OutboxRelayConfig{
		Owner:        owner,
		BatchSize:    cfg.OutboxBatchSize,
		Lease:        cfg.OutboxLease,
		PollInterval: cfg.OutboxPollInterval,
		RetryDelay:   cfg.OutboxRetryDelay,
	}, log)
	if err != nil {
		_ = reader.Close()
		_ = producer.Close()
		return err
	}
	consumer, err := kafkaruntime.NewConsumer(reader, postgres, kafkaruntime.ConsumerConfig{
		DeadLetterTopic: cfg.KafkaDeadLetterTopic,
		MaxAttempts:     cfg.RetryMaxAttempts,
		RetryDelay:      cfg.KafkaConsumerRetry,
	}, log)
	if err != nil {
		_ = reader.Close()
		_ = producer.Close()
		return err
	}

	workerCtx, stopWorker := context.WithCancel(ctx)
	componentErrors := make(chan error, 2)
	go func() { componentErrors <- relay.Run(workerCtx) }()
	go func() { componentErrors <- consumer.Run(workerCtx) }()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if err := producer.Close(); err != nil {
				log.Warn("failed to close Kafka producer", "error", err)
			}
			if err := reader.Close(); err != nil {
				log.Warn("failed to close Kafka reader", "error", err)
			}
		}()
		defer stopWorker()

		for completed := 0; completed < 2; completed++ {
			err := <-componentErrors
			if err != nil {
				log.Error("worker component stopped with error", "error", err)
				stopWorker()
				cancel()
				continue
			}
			if ctx.Err() == nil {
				log.Error("worker component stopped unexpectedly")
				stopWorker()
				cancel()
			}
		}
	}()

	log.Info("worker engine started", "outbox_owner", owner, "consumer_group", cfg.KafkaConsumerGroup)
	return nil
}

func outboxOwner() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
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
