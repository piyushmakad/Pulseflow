# Phase 5 — Worker Pools and Delivery

## Status and goal

Implemented. Phase 5 turns the durable delivery rows created in Phase 4 into real webhook calls and simulated email deliveries.

PulseFlow is still one modular Go codebase. `--mode=api`, `--mode=worker`, and `--mode=all` only choose which modules run in a process; they are not separate services or repositories.

```text
PostgreSQL delivery_attempts (durable source of truth)
                    |
                    | claim only when a pool has space
                    v
           channel-specific dispatcher
                    |
                    v
        bounded Go channel (memory only)
                    |
          +---------+---------+
          |         |         |
       worker 1  worker 2  worker N
          |         |         |
          +---- webhook/email-+
                    |
                    v
        save outcome in PostgreSQL
                    |
          +---------+---------+
          |                   |
       retry later        finalize event
          |
       dead letter after max attempts
          |
     PostgreSQL outbox -> Kafka
```

The PostgreSQL table is the queue that survives a crash. The Go channel is only a small waiting room that limits how much one process can hold.

## Node.js mental model for channels

In Node, you might use `p-limit`, BullMQ, or a semaphore:

```js
const limit = pLimit(20);
await Promise.all(jobs.map(job => limit(() => deliver(job))));
```

The Go version starts a fixed number of goroutines that all read from one bounded channel:

```go
jobs := make(chan DeliveryAttempt, 200)

for i := 0; i < 20; i++ {
    go func() {
        for delivery := range jobs {
            deliver(delivery)
        }
    }()
}
```

- A goroutine is comparable to a lightweight async task.
- Sending `jobs <- delivery` adds to the in-memory waiting queue.
- `for delivery := range jobs` waits for values until the channel closes.
- A buffer of 200 means at most 200 jobs wait in memory; it does not mean 200 run concurrently.
- The worker count of 20 is the concurrency limit.
- A full channel makes the dispatcher stop claiming rows. That is backpressure.

## File-by-file guide and naming

### `migrations/003_delivery_execution.up.sql`

Adds `destination_config` to `delivery_attempts` and limits channel names to `webhook` or `email`.

The destination is copied from the notification rule when routing occurs. Editing a rule later cannot redirect already-pending work. The migration is named `delivery_execution` because migration names describe their data change, not a project phase number.

### `internal/domain/notification.go`

Defines the vocabulary shared by PostgreSQL and workers:

- Channel constants avoid repeated string literals.
- `DestinationConfig` holds the rule snapshot.
- `DeliveryDeadLetterMessage` defines the Kafka message emitted after attempts are exhausted.

This file owns data and state names, not HTTP or SQL behavior, so it belongs in `domain`.

### `internal/store/postgres/notification_repo.go`

This is the PostgreSQL implementation of delivery state changes.

- `RouteEvent` creates durable rows and snapshots rule config.
- `ClaimDueDeliveriesByChannel` uses `FOR UPDATE SKIP LOCKED`, preventing two processes from claiming the same row.
- `RecordDeliveryResult` verifies `locked_by`, increments attempts, and stores the outcome.
- `RecoverExpiredDeliveries` handles workers that died while holding leases.
- `FinalizeEvent` calculates one event’s result.
- `FinalizeReadyEvents` repairs events left in `processing` if immediate finalization failed.

`repo` means repository: code that loads and saves domain data. It does not mean another Git repository.

### `internal/worker/outcome.go`

Separates external-provider outcomes from database states. A deliverer returns `OutcomeDelivered`, `OutcomePermanent`, or `OutcomeRetryable`. `RetryPolicy.Result` converts that into a `domain.DeliveryResult`.

Retry delay is exponential with jitter and capped by `RETRY_MAX_DELAY`. A valid `Retry-After` takes priority, but is also capped. This resembles a Node function mapping an Axios response/error to `{ status, nextRetryAt }`.

### `internal/worker/pool.go`

Owns the bounded channel and fixed goroutines.

- `NewPool` allocates the channel and starts exactly the configured worker count.
- `Available` reports remaining waiting slots.
- `Submit` puts one claimed delivery into the channel.
- `CloseAndDrain` closes input, waits for queued work, and cancels only after the shutdown deadline.

The generic name `Pool` is intentional: it knows nothing about URLs, email addresses, or SQL.

### `internal/worker/dispatcher.go`

Moves work from PostgreSQL into one pool. It asks `pool.Available()` before claiming. A database lease begins at claim time, so claiming thousands of rows into an unbounded queue could let leases expire before execution.

Webhook and email have separate dispatchers and pools. A slow webhook provider therefore cannot consume all email capacity.

### `internal/worker/processor.go`

Connects three actions for one job:

1. Call a `Deliverer`.
2. Apply `RetryPolicy`.
3. Persist the result and try to finalize the event.

`Deliverer` and `ResultStore` are small Go interfaces, similar to dependency injection with TypeScript interfaces. The provider uses the worker timeout context. Result persistence gets a fresh short context because an HTTP timeout must still be recorded in PostgreSQL.

### `internal/worker/webhook/deliverer.go`

Sends an HTTP `POST` containing event JSON. Stable `Idempotency-Key`, `X-PulseFlow-Delivery-ID`, and `X-PulseFlow-Attempt` headers let the receiver deduplicate retries.

| Result | Action |
|---|---|
| HTTP 2xx | Delivered |
| Network error or timeout | Retry |
| HTTP 408 or 429 | Retry |
| HTTP 5xx | Retry |
| Other HTTP 4xx | Permanent failure |

Only 64 KiB of response body is saved, preventing a provider from filling memory or PostgreSQL with a huge response.

### `internal/worker/email/deliverer.go`

Validates `to` and `subject`, logs a simulated delivery, and returns success. Email is deliberately simulated: adding SendGrid, SES, or another SDK and credentials is not necessary to prove this architecture.

### `internal/worker/maintenance.go`

Periodically recovers expired leases and finalizes events whose deliveries are terminal. It is a module inside the same worker process, not another service.

### `internal/worker/engine.go`

Coordinates Kafka components, dispatchers, maintenance, and pools. Shutdown order is:

1. Stop Kafka/dispatcher intake.
2. Wait for those components to exit.
3. Close pool channels.
4. Drain queued and active work until `SHUTDOWN_TIMEOUT`.
5. Cancel remaining work after the deadline.

`Engine` describes coordination; it contains no provider or SQL implementation.

### `internal/config/config.go` and `.env.example`

Add configurable pool counts/buffers/timeouts, claim interval/batch/lease, maintenance intervals, retry limits, and persistence timeout. Validation ensures the lease exceeds the worst configured queue wait plus execution time, so healthy queued work is not mistaken for abandoned work.

### `cmd/pulseflow/main.go`

This is the composition root: the place concrete modules are constructed and connected. It creates one PostgreSQL store, Kafka relay/consumer, two deliverers, processors, pools, dispatchers, maintenance, and the engine.

## Transaction boundaries

The provider call is never inside a PostgreSQL transaction. A DB transaction must not stay open during network I/O.

```text
TX3: claim due delivery + assign lease -> COMMIT
     call provider (no DB transaction)
TX4: verify lease owner + save outcome
     + insert dead-letter outbox row when exhausted -> COMMIT
TX5: finalize parent event when every delivery is terminal -> COMMIT
```

TX4 and TX5 are separate so concurrent deliveries can finish independently. The finalization sweep repairs the gap if TX5 temporarily fails.

## State machines

```text
pending -----> delivering -----> delivered
                   |
                   +-----------> failed
                   |
                   +-----------> retrying -----> delivering
                                         |
                                         +-----> dead_letter
```

The parent event is `completed` when all deliveries succeed, `partially_failed` when only some succeed, and `failed` when none succeed.

## Failure behavior

| Failure | Behavior |
|---|---|
| PostgreSQL unavailable before claim | Nothing is claimed; durable rows remain unchanged. |
| PostgreSQL unavailable after provider response | Result write fails. The lease expires and the delivery is retried, possibly duplicating the external call. |
| Redis unavailable | Workers are unaffected. Redis is only the API-key cache and distributed API rate limiter; PostgreSQL remains authoritative. |
| Kafka unavailable | Outbox rows remain in PostgreSQL and publishing retries. Existing delivery rows continue. New Kafka events cannot route until Kafka returns. |
| Webhook network error, timeout, 408, 429, or 5xx | Retry with `Retry-After` or exponential backoff plus jitter. |
| Webhook ordinary 4xx | Permanent failure. |
| Worker crashes | Its lease expires; recovery consumes an attempt and retries or dead-letters. |

Redis is not a delivery queue, scheduler, lease store, or source of truth. PostgreSQL already gives these operations durability and transactional updates.

## At-least-once delivery

A provider can accept a request and the connection can fail before PulseFlow sees its response. PulseFlow cannot prove what the provider did, so webhooks are at-least-once rather than exactly-once. Receivers should deduplicate using the stable delivery ID.

## Tests and commands

Phase 5 tests cover concurrency bounds, backpressure, normal/forced shutdown, retry timing, `Retry-After`, webhook classification, destination snapshots, lease recovery, dead lettering, and event finalization.

```bash
go test ./...
```

Run PostgreSQL integration tests in PowerShell:

```powershell
$env:TEST_DATABASE_URL="postgres://postgres:password@localhost:5432/pulseflow?sslmode=disable"
go test ./internal/store/postgres -v
```

Apply migration 003 to an existing local database before starting the application:

```bash
make migrate-up
```
