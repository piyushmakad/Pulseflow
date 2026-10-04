# PulseFlow

[![CI](https://github.com/piyushmakad/Pulseflow/actions/workflows/ci.yml/badge.svg)](https://github.com/piyushmakad/Pulseflow/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/Go-1.24.5-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![Architecture](https://img.shields.io/badge/architecture-modular%20monolith-4B5563)](#architecture)

PulseFlow is a multi-tenant event ingestion and notification delivery system written in Go. It accepts authenticated events, persists them with idempotency guarantees, publishes them through a PostgreSQL transactional outbox, and delivers matching notifications with bounded worker pools and durable retries.

The project is intentionally built as a **modular monolith**: one repository, one Go module, and one container image. The API, background worker, and migration job run as separate processes in deployment, but share the same domain model and implementation.

> PulseFlow is a learning and portfolio project built around production-oriented reliability patterns. The included single-node Kubernetes profile is not a highly available production environment.

## Why this project exists

Event-driven systems often look reliable in the happy path but lose work when a database transaction succeeds and Kafka publishing fails, or create uncontrolled concurrency when downstream providers slow down. PulseFlow focuses on those failure boundaries.

Its main design goals are:

- PostgreSQL remains the source of truth.
- Accepting an event and scheduling its publication is one atomic transaction.
- Repeated client requests are safe through tenant-scoped idempotency keys.
- Delivery work has explicit, validated state transitions.
- Worker concurrency and in-memory queues are bounded.
- Failed work is retried durably and can be recovered after a process crash.
- Redis improves performance and protection without owning durable business state.
- The same codebase runs locally, as split processes, or on Kubernetes.

## Architecture

```mermaid
flowchart LR
    Client[API client] -->|Bearer API key| API[PulseFlow API]
    API -->|event + outbox row\none PostgreSQL transaction| PG[(PostgreSQL)]
    API -.->|API-key cache\nrate limiting| Redis[(Redis)]

    Relay[Outbox relay] -->|claim pending rows| PG
    Relay -->|publish| Kafka[(Kafka)]
    Kafka --> Consumer[Kafka consumer]
    Consumer -->|create delivery work| PG

    PG --> Dispatcher[Delivery dispatchers]
    Dispatcher --> WebhookPool[Bounded webhook pool]
    Dispatcher --> EmailPool[Bounded email pool]
    WebhookPool --> Provider[Webhook providers]
    EmailPool --> SimulatedEmail[Simulated email adapter]
    WebhookPool -->|persist outcome / retry| PG
    EmailPool -->|persist outcome / retry| PG

    PG --> Monitor[Operational monitor]
    Monitor --> Logs[Structured logs and alerts]
```

### Runtime roles

One compiled binary exposes three roles:

| Command | Responsibility |
|---|---|
| `pulseflow --mode=api` | HTTP API, authentication, rate limiting, health probes |
| `pulseflow --mode=worker` | Outbox relay, Kafka consumer, dispatchers, delivery pools, recovery |
| `pulseflow --command=migrate` | Apply embedded, versioned PostgreSQL migrations |

`--mode=all` runs the API and workers together for local development. Kubernetes runs the API and worker separately so they can be operated and scaled independently without creating separate services or repositories.

## Reliability model

### Transactional outbox

The ingestion request does **not** write to PostgreSQL and Kafka independently. It commits the event and its outbox entry together:

```text
BEGIN
  INSERT event
  INSERT outbox message
COMMIT
```

If the transaction fails, neither record exists. If Kafka is unavailable after the transaction commits, the event remains accepted and the outbox relay retries later. Publishing is at-least-once, so downstream routing is designed to be idempotent.

### Explicit state machines

Event lifecycle:

```text
accepted -> processing -> completed
                       -> partially_failed
                       -> failed
```

Delivery lifecycle:

```text
pending -> delivering -> delivered
                      -> failed
                      -> retrying -> delivering
                                  -> dead_letter
```

Transitions are checked in Go and guarded by expected-state updates in PostgreSQL. Terminal states cannot transition again.

### Bounded concurrency and backpressure

Webhook and email delivery use separate pools with configurable worker counts and bounded channel buffers. When a provider slows down, its queue fills and the dispatcher stops claiming unlimited additional work. Context cancellation, delivery deadlines, lease recovery, and graceful shutdown are part of the worker lifecycle.

### Redis has two limited jobs

Redis is used only for:

1. Caching validated API-key lookups.
2. Enforcing per-tenant API rate limits.

It does not store events, outbox entries, deliveries, or final outcomes. Authentication falls back to PostgreSQL when the cache is unavailable, while rate limiting fails open and records the degraded operation.

## Technology choices

| Technology | Purpose | Architectural reason |
|---|---|---|
| Go | API and background processing | Simple deployment, explicit concurrency and cancellation |
| PostgreSQL | Durable source of truth | Transactions, constraints, row locking, lease-based work claiming |
| Kafka | Event transport | Partitioned asynchronous routing and consumer groups |
| Redis | Disposable cache and rate-limit state | Low-latency API protection; never authoritative |
| Docker Compose | Local dependencies | Reproducible development environment |
| Kubernetes + Kustomize | Deployment | Separate runtime roles from one image without duplicating manifests |
| GitHub Actions | Continuous integration | Formatting, vetting, race tests, and multi-architecture image builds |

No extra datastore, queue, service mesh, or telemetry backend is included without a concrete responsibility.

## Repository layout

```text
cmd/pulseflow/                 application entry point and runtime composition
internal/
  admin/                       tenant and API-key provisioning
  api/                         HTTP server, handlers, middleware, responses
  config/                      environment configuration and validation
  domain/                      entities, errors, and state machines
  kafka/                       producer, consumer, reader, and outbox relay
  observability/               process metrics and durable backlog monitoring
  platform/                    logging and migration runner
  store/postgres/              transactional repositories and work leases
  store/redis/                 API-key cache and rate limiter
  worker/                      dispatchers, bounded pools, retries, maintenance
migrations/                    embedded PostgreSQL schema migrations
deploy/kubernetes/             provider-neutral base and Oracle OKE overlays
docs/learning/                 phase-by-phase Node.js-to-Go learning guide
implementation_plan.md         architecture, invariants, and transaction design
```

## Local quick start

### Prerequisites

- Go 1.24 or newer
- Docker with Docker Compose
- GNU Make is optional; equivalent commands are shown below

### 1. Start PostgreSQL, Redis, and Kafka

```bash
docker compose up -d
```

Or:

```bash
make infra-up
```

The application defaults match the local Compose ports and credentials. Kafka runs in KRaft mode, so ZooKeeper is not required.

### 2. Apply database migrations

```bash
go run ./cmd/pulseflow --command=migrate
```

### 3. Create a tenant and API key

```bash
go run ./cmd/pulseflow --command=provision \
  --tenant-name="Local Development" \
  --key-name="local"
```

The raw `pf_live_...` key is displayed once. Store it securely; PostgreSQL keeps only its SHA-256 hash.

### 4. Run PulseFlow

```bash
go run ./cmd/pulseflow --mode=all
```

The API listens on `http://localhost:8080`.

### 5. Submit and inspect an event

```bash
curl -i http://localhost:8080/v1/events \
  -X POST \
  -H "Authorization: Bearer $PULSEFLOW_API_KEY" \
  -H "Idempotency-Key: order-1001" \
  -H "Content-Type: application/json" \
  -d '{"type":"order.created","data":{"order_id":"1001","total":4999}}'
```

A new event returns `202 Accepted`. Sending the same payload with the same tenant and idempotency key returns the existing event with `200 OK`. Reusing the key for different event content returns `409 Conflict`.

Retrieve it with the ID from the response:

```bash
curl http://localhost:8080/v1/events/EVENT_ID \
  -H "Authorization: Bearer $PULSEFLOW_API_KEY"
```

Health endpoints do not require authentication:

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
```

## Configuration

PulseFlow reads configuration from environment variables and validates relationships such as delivery lease duration versus maximum queue wait. See [`.env.example`](.env.example) for the complete list.

Common settings include:

| Variable | Default | Meaning |
|---|---:|---|
| `DATABASE_URL` | local PostgreSQL | Durable application state |
| `REDIS_URL` | `redis://localhost:6379/0` | Cache and rate limiter |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated brokers |
| `WEBHOOK_WORKER_COUNT` | `20` | Maximum concurrent webhook handlers per process |
| `WEBHOOK_BUFFER_SIZE` | `200` | Bounded webhook queue capacity |
| `RETRY_MAX_ATTEMPTS` | `5` | Maximum logical delivery attempts |
| `SHUTDOWN_TIMEOUT` | `30s` | Graceful drain deadline |

Do not commit `.env`, Kubernetes `secret.yaml`, OCI auth tokens, kubeconfig files, or private keys.

## API surface

| Method | Path | Authentication | Purpose |
|---|---|---|---|
| `GET` | `/healthz` | None | Process liveness |
| `GET` | `/readyz` | None | Dependency-aware readiness |
| `POST` | `/v1/events` | Bearer API key | Persist an idempotent event and outbox entry |
| `GET` | `/v1/events/{eventID}` | Bearer API key | Read a tenant-scoped event |

The intentionally small API keeps administration separate from the ingestion hot path. Tenant/API-key provisioning currently uses the CLI, and notification rules are managed through the repository layer rather than a public admin endpoint.

## Testing and verification

Run the full Go suite with the race detector:

```bash
go test -race ./...
```

Or:

```bash
make test
```

PostgreSQL integration tests are opt-in:

```bash
TEST_DATABASE_URL='postgres://pulseflow:pulseflow@localhost:5432/pulseflow?sslmode=disable' \
  go test ./internal/store/postgres -v
```

CI verifies:

- `gofmt`
- `go vet`
- all tests with the race detector
- Linux AMD64 and ARM64 container builds

The test suite covers state transitions, idempotency, transaction rollback, concurrent work claiming, Kafka publication and quarantine behavior, bounded worker pools, retry classification, graceful shutdown, Redis fallbacks, and alert transitions.

## Kubernetes and Oracle OKE

The Kubernetes resources preserve the same modular-monolith boundary:

```text
one repository -> one multi-architecture image
                         |-- API Deployment
                         |-- Worker Deployment
                         `-- Migration Job
```

The Oracle learning profile includes PostgreSQL, Redis, and a single Kafka broker so the entire system can be studied in one cluster. Deploy in this order:

```bash
make k8s-check
make k8s-namespace
make k8s-infra
make k8s-migrate
make k8s-app
make k8s-status
```

Image publishing and complete OKE instructions are in [`deploy/kubernetes/oracle-free/README.md`](deploy/kubernetes/oracle-free/README.md).

The supplied profile is deliberately cost-conscious and single-node. A production environment would need multiple worker nodes, managed or highly available PostgreSQL, replicated Kafka, backups and restore testing, TLS/DNS, centralized metrics, and load/failure testing.

## Failure behavior

| Failure | PulseFlow behavior |
|---|---|
| PostgreSQL unavailable | Ingestion stops because the source of truth cannot commit safely |
| Kafka unavailable | Accepted events remain in the outbox and publishing retries |
| Redis unavailable | API-key lookup falls back to PostgreSQL; rate limiting fails open and is logged |
| Webhook returns `408`, `429`, or `5xx` | Delivery is retried with bounded exponential backoff and `Retry-After` support |
| Webhook returns another `4xx` | Delivery is marked permanently failed |
| Worker exits during a delivery | Expired PostgreSQL leases make work recoverable |
| Process receives `SIGTERM` | New work stops and active requests/pools drain until the configured deadline |

Webhook delivery is at-least-once. Receivers should deduplicate using the stable `Idempotency-Key` or `X-PulseFlow-Delivery-ID` header.

## Learning path and design documentation

This repository includes documentation written for developers moving from Node.js/TypeScript to Go:

1. [Foundation](docs/learning/01-foundation.md)
2. [Domain and PostgreSQL](docs/learning/02-domain-and-postgresql.md)
3. [HTTP API](docs/learning/03-http-api.md)
4. [Transactional outbox and Kafka](docs/learning/04-outbox-and-kafka.md)
5. [Worker pools and delivery](docs/learning/05-worker-pools.md)
6. [Operational visibility](docs/learning/06-observability.md)
7. [Packaging and Kubernetes](docs/learning/07-deployment.md)

For transaction boundaries, failure semantics, schema rationale, and technology decisions, read the full [`implementation_plan.md`](implementation_plan.md).

## Current limitations

- Email delivery is simulated; the durable workflow is implemented, but no external email provider is configured.
- The public API supports event ingestion and lookup, not notification-rule administration.
- Runtime counters are process-local and emitted through structured logs; there is no Prometheus/OpenTelemetry backend yet.
- The Oracle learning overlay is single-node and single-broker, so it is not highly available.

These are explicit scope boundaries rather than hidden production claims.
