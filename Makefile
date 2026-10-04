.PHONY: help dev infra-up infra-down migrate-up migrate-down build docker-build docker-build-multiarch docker-push-ocir run test lint clean k8s-check k8s-render k8s-namespace k8s-infra k8s-migrate k8s-app k8s-status

OCIR_IMAGE ?= bom.ocir.io/bm7wpbkcaaqu/pulseflow
IMAGE_TAG ?= 0.1.0

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
	go run ./cmd/pulseflow --command=migrate

migrate-down: ## Rollback database migrations
	migrate -path ./migrations -database "$(DATABASE_URL)" down

# Application
build: ## Build the binary
	go build -o bin/pulseflow ./cmd/pulseflow

docker-build: ## Build the local PulseFlow container image
	docker build -t pulseflow:dev .

docker-build-multiarch: ## Verify amd64 and arm64 images build (does not push)
	docker buildx build --platform linux/amd64,linux/arm64 --output type=cacheonly .

docker-push-ocir: ## Build and push the release image to OCI Container Registry
	docker buildx build --platform linux/amd64,linux/arm64 -t $(OCIR_IMAGE):$(IMAGE_TAG) --push .

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

# Oracle OKE deployment
k8s-check: ## Verify cluster access, nodes, system pods, and storage classes
	kubectl get nodes -o wide
	kubectl get pods -A
	kubectl get storageclass

k8s-render: ## Render all Oracle manifests locally without applying them
	kubectl kustomize deploy/kubernetes/oracle-free/infra
	kubectl kustomize deploy/kubernetes/oracle-free/migration
	kubectl kustomize deploy/kubernetes/oracle-free/app

k8s-namespace: ## Create the PulseFlow Kubernetes namespace
	kubectl apply -f deploy/kubernetes/oracle-free/namespace.yaml

k8s-infra: ## Deploy PostgreSQL, Redis, Kafka, and Kafka topics
	kubectl apply -k deploy/kubernetes/oracle-free/infra
	kubectl rollout status statefulset/postgres -n pulseflow --timeout=5m
	kubectl rollout status statefulset/kafka -n pulseflow --timeout=10m
	kubectl rollout status deployment/redis -n pulseflow --timeout=5m
	kubectl wait --for=condition=complete job/kafka-topics -n pulseflow --timeout=10m

k8s-migrate: ## Run the release database migration as a one-shot Job
	kubectl delete job pulseflow-migrate -n pulseflow --ignore-not-found
	kubectl apply -k deploy/kubernetes/oracle-free/migration
	kubectl wait --for=condition=complete job/pulseflow-migrate -n pulseflow --timeout=5m
	kubectl logs job/pulseflow-migrate -n pulseflow

k8s-app: ## Deploy the PulseFlow API and worker after migration succeeds
	kubectl apply -k deploy/kubernetes/oracle-free/app
	kubectl rollout status deployment/pulseflow-api -n pulseflow --timeout=5m
	kubectl rollout status deployment/pulseflow-worker -n pulseflow --timeout=5m

k8s-status: ## Show PulseFlow workloads, storage, and external service address
	kubectl get all,pvc -n pulseflow

# Cleanup
clean: ## Remove build artifacts
	rm -rf bin/
