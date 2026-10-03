# Phase 6 — Operational Visibility

## Status and goal

Implemented. Phase 6 makes backlog, delivery outcomes, dependency fallbacks, Kafka progress, and PostgreSQL pressure visible without adding Prometheus, OpenTelemetry, or another service before the deployment environment needs one.

PulseFlow now has three kinds of operational evidence:

```text
Durable PostgreSQL state   Process-local counters   Structured event logs
        |                           |                         |
        +-------------+-------------+-------------------------+
                      |
             periodic JSON snapshot
                      |
       threshold crossing / recovery alerts
```

## Node.js mental model

The closest Node design would combine:

- Pino or Winston structured logs.
- An in-memory metrics object protected from inconsistent updates.
- A scheduled database query for durable queue depth.
- A small alert state machine that logs only when a threshold is crossed or recovered.

Go differs because many goroutines update metrics concurrently. A `sync.RWMutex` protects maps and grouped counters in one `Registry`.

## Why there are two metric sources

### PostgreSQL gauges

Backlog and lease health come directly from PostgreSQL:

- Pending and publishing outbox rows.
- Expired outbox publishing leases.
- Age of the oldest unpublished outbox row.
- Deliveries currently due.
- Expired delivery leases.
- PostgreSQL connection-pool usage.

These values survive restarts and describe the whole database, not one process.

### Process-local counters

The registry counts events observed by one running process:

- Delivery outcomes and latency by channel/status.
- Kafka publish acknowledgements/failures.
- Kafka routed/quarantined messages and client errors.
- Last observed Kafka lag by topic/partition.
- Redis fallback operations.

These counters reset when the process restarts. With multiple replicas, every replica has its own counters. They are operational evidence, not a replacement for PostgreSQL.

## File-by-file explanation

### `internal/observability/registry.go`

`Registry` is the concurrency-safe in-memory counter store.

```go
type Registry struct {
    mu sync.RWMutex
    deliveries map[string]map[string]DeliveryCounters
    // other counters...
}
```

`mu.Lock()` is used while changing counters. `mu.RLock()` allows concurrent readers while building a snapshot.

Node code normally does not need a mutex because JavaScript executes synchronous map changes on one event-loop thread. Go goroutines can truly update the map concurrently; writing without synchronization can race or panic.

`Snapshot` deep-copies maps. A caller therefore cannot accidentally mutate the registry:

```text
Registry map -> copied snapshot -> logger
```

Delivery latency stores count, total, average, and maximum. This gives useful initial visibility without choosing histogram buckets for a metrics backend that does not exist yet.

### `internal/observability/monitor.go`

`Monitor` runs immediately at startup and then on `OBSERVABILITY_INTERVAL`.

Each cycle:

1. Creates a short PostgreSQL query context.
2. Reads durable backlog and pool statistics.
3. copies process counters from `Registry`.
4. Writes one structured `operational metrics` log.
5. Evaluates alert thresholds.

Alerts are edge-triggered:

```text
normal -> threshold crossed -> one "alert firing" log
firing -> still above threshold -> no repeated alert log
firing -> below threshold -> one "alert recovered" log
```

This prevents a 15-second monitor from writing the same warning every 15 seconds for a two-hour outage.

If the snapshot query itself fails, `operational_metrics_collection` fires. It recovers after the first successful query.

### `internal/store/postgres/operational_repo.go`

`OperationalSnapshot` is the database source for the monitor.

It uses two CTEs so outbox and active-delivery indexes are each scanned once per snapshot. Published outbox rows and terminal deliveries are excluded from backlog scans.

`pgxpool.Stat()` supplies:

- Acquired connections.
- Idle connections.
- Total and maximum connections.
- Cumulative time spent waiting to acquire connections.

No correctness decision depends on this query. If monitoring fails, event ingestion and delivery continue.

### `internal/worker/processor.go`

A delivery metric is recorded only after `RecordDeliveryResult` succeeds.

That ordering matters:

```text
provider call -> PostgreSQL result commits -> increment process metric
```

If PostgreSQL rejects the update, PulseFlow does not claim that a durable delivery result exists.

Statuses such as `retrying` and `dead_letter` are separate series, so retry and dead-letter counts are visible without extra counters.

Delivery logs now include delivery ID, event ID, tenant ID, channel, attempt, status, and worker owner. Raw payloads, API keys, and webhook headers are not logged.

### `internal/kafka/outbox_relay.go`

Records Kafka publisher acknowledgements and failed publish attempts. These are attempt counters; PostgreSQL outbox backlog remains the authoritative measure of unpublished work.

Relay logs include `relay_owner`, making it possible to identify which process held a lease.

### `internal/kafka/consumer.go`, `message.go`, and `reader.go`

Kafka messages now retain `HighWaterMark`. After a successful offset commit, observed partition lag is calculated as:

```text
high water mark - committed offset - 1
```

The consumer also counts routed messages, quarantined messages, fetch errors, and commit errors.

Lag is deliberately called "last observed" lag. A partition that sends no new message does not refresh its value, and this is not a cluster-wide consumer-group monitoring system. If exact fleet-wide lag becomes an operational requirement, Phase 7 can select a Kafka-aware backend/tool with a concrete operator.

### `internal/api/middleware/auth.go` and `ratelimit.go`

Redis fallback counters distinguish:

- `api_key_lookup_error`: Redis failed, so auth queried PostgreSQL.
- `api_key_cache_miss`: Redis was healthy but did not contain the key, so auth queried PostgreSQL.
- `rate_limit_fail_open`: Redis rate limiting failed and the request was allowed.

Redis remains disposable acceleration. These counters reveal degraded performance or protection without making Redis a source of truth.

### `internal/api/handler/event.go`

After the event/outbox transaction commits, the handler logs `event persisted` with event ID, tenant ID, type, state, idempotent-created result, and request ID.

The log happens after persistence, so it describes a durable boundary rather than an attempted operation.

### `internal/config/config.go` and `.env.example`

New settings:

```env
OBSERVABILITY_INTERVAL=15s
OBSERVABILITY_QUERY_TIMEOUT=2s
ALERT_OUTBOX_PENDING=1000
ALERT_OUTBOX_OLDEST_AGE=5m
ALERT_EXPIRED_OUTBOX_LEASES=1
ALERT_DUE_DELIVERIES=1000
ALERT_EXPIRED_DELIVERY_LEASES=1
ALERT_POSTGRES_POOL_UTILIZATION=0.90
```

Count/age thresholds can be set to `0` to disable their alert. PostgreSQL utilization can also be `0` to disable it; otherwise it is a ratio from `0` to `1`.

Thresholds are starting defaults, not universal production values. Tune them from expected traffic, provider latency, database capacity, and acceptable recovery time.

### `cmd/pulseflow/main.go`

Creates exactly one registry and monitor per process. The same registry is injected into API middleware, Kafka components, and delivery processors.

The monitor runs in `all`, `api`, and `worker` modes and stops through the same root context and wait group as the rest of the application.

## Example structured snapshot

A JSON log contains fields conceptually like:

```json
{
  "msg": "operational metrics",
  "database": {
    "outbox_pending": 12,
    "outbox_publishing": 2,
    "expired_outbox_leases": 0,
    "oldest_outbox_age_seconds": 8.4,
    "due_deliveries": 31,
    "expired_delivery_leases": 0,
    "postgres_acquired": 6,
    "postgres_max": 25
  },
  "runtime": {
    "outbox_published": 840,
    "kafka_routed": 820,
    "redis_fallbacks": {
      "api_key_cache_miss": 14
    }
  }
}
```

## Why there is no `/metrics` endpoint yet

An unauthenticated endpoint would expose global tenant workload and dependency information. PulseFlow currently has tenant API keys but no operator/admin authentication model.

Periodic structured logs provide visibility without opening that data over HTTP. A future deployment can route these logs to its existing platform, or add an authenticated Prometheus/OpenTelemetry exporter after ownership and retention are defined.

## Why distributed tracing was not added

The current questions can be answered with:

- Request ID for synchronous API work.
- Event ID through ingestion and Kafka routing.
- Delivery ID through retries and providers.
- Owner IDs for outbox/delivery leases.
- Structured state-transition logs.

Adding OpenTelemetry now would require decisions about sampling, propagation, collector deployment, storage, retention, and sensitive attributes. There is no concrete unresolved trace question that justifies those technologies yet.

Tracing becomes justified when operators need to follow one request across independently deployed systems and logs plus stable IDs are insufficient.

## Failure behavior

| Failure | Observability behavior |
|---|---|
| PostgreSQL snapshot query fails | Collection alert fires; event processing remains independent. |
| Kafka publish fails | Publish-failure counter grows and durable outbox backlog/age rises. |
| Worker/provider slows | Due-delivery backlog and delivery latency rise. |
| Worker crashes | Expired-lease gauge triggers before/while maintenance recovers it. |
| Redis fails | Auth lookup-error and rate-limit fail-open counters grow. |
| PostgreSQL pool saturates | Utilization alert fires at the configured ratio. |
| Process restarts | Runtime counters reset; PostgreSQL backlog gauges remain accurate. |

## Tests

Phase 6 tests cover:

- Concurrent counter updates.
- Snapshot map isolation.
- Alert firing and recovery transitions.
- Metrics-collection failure and recovery.
- Kafka routed/quarantined/commit/lag metrics.
- Outbox publish result metrics.
- Redis fallback metrics.
- Delivery metrics after durable result persistence.
- PostgreSQL backlog and expired-lease snapshots.

Run unit tests:

```powershell
go test ./...
```

Run the PostgreSQL integration tests:

```powershell
$env:TEST_DATABASE_URL="postgres://postgres:password@localhost:5432/pulseflow?sslmode=disable"
go test ./internal/store/postgres -v
```
