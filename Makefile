.PHONY: help dev infra-up infra-down migrate-up migrate-down build run test lint clean

# Default
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Infrastructure
infra-up: ## Start infrastructure (PostgreSQL, Redis, Kafka)
	docker compose up -d

infra-down: ## Stop infrastructure
	docker compose down

infra-reset: ## Reset infrastructure (destroy volumes)
	docker compose down -v

# Database migrations (requires golang-migrate CLI)
migrate-up: ## Run database migrations
	migrate -path ./migrations -database "$(DATABASE_URL)" up

migrate-down: ## Rollback database migrations
	migrate -path ./migrations -database "$(DATABASE_URL)" down

# Application
build: ## Build the binary
	go build -o bin/pulseflow ./cmd/pulseflow

run: ## Run with default mode (all)
	go run ./cmd/pulseflow

run-api: ## Run in API-only mode
	go run ./cmd/pulseflow --mode=api

run-worker: ## Run in worker-only mode
	go run ./cmd/pulseflow --mode=worker

# Development
dev: infra-up run ## Start infra and run app

# Testing
test: ## Run all tests
	go test ./... -v -race

test-short: ## Run unit tests only
	go test ./... -v -short

# Quality
lint: ## Run linter
	golangci-lint run ./...

# Cleanup
clean: ## Remove build artifacts
	rm -rf bin/
