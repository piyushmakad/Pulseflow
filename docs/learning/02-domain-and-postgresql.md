# Phase 2 — Domain and PostgreSQL

## Goal

Make business state explicit and build the durable PostgreSQL operations needed by later API, Kafka, and worker phases.

Phase 2 does not start an HTTP server or Kafka consumer. It creates the correctness layer those components will call.

## What is implemented

### Domain models

- `internal/domain/event.go` defines events, event statuses, versioned Kafka messages, and outbox rows.
- `internal/domain/notification.go` defines rules, logical delivery records, delivery results, and delivery statuses.
- `CanTransitionTo`, `ValidateTransition`, and `IsTerminal` make state rules readable and unit-testable.

Node analogy:

```ts
type EventStatus = "accepted" | "processing" | "completed" | "failed";
```

Go uses a named string type and constants:

```go
type EventStatus string

const EventStatusAccepted EventStatus = "accepted"
```

The named type prevents accidentally passing an arbitrary string where an `EventStatus` is expected.

### PostgreSQL store

`internal/store/postgres/store.go` owns a `pgxpool.Pool`.

Node analogy: `pgxpool.Pool` is comparable to a `pg.Pool`, but repository functions block until completion and return errors explicitly instead of returning promises.

The store also centralizes:

- Transaction begin/commit/rollback behavior.
- PostgreSQL error translation.
- Connection health checks.

### Atomic event ingestion

`CreateEvent` performs this transaction:

```text
BEGIN
  INSERT event
  INSERT outbox message
COMMIT
```

If either insert fails, neither remains. This eliminates the database/Kafka dual-write problem before Kafka code even exists.

The method returns `(event, created, error)`:

- `created=true`: this idempotency key produced a new event.
- `created=false`: the same logical event already exists.
- error wrapping `ErrAlreadyExists`: the key was reused with different data.

Go callers inspect wrapped sentinel errors with `errors.Is`, similar in intent to checking a custom error class with `instanceof`.

### Idempotent routing

`RouteEvent` locks an accepted event, moves it to `processing`, loads matching rules, and creates delivery rows in one transaction.

The database constraint:

```sql
UNIQUE(event_id, notification_rule_id)
```

is the real concurrency guarantee. An in-memory `if` check would not protect multiple processes.

If no rules match, the event becomes `completed` in the same transaction.

### Leased claims

Both outbox messages and deliveries use leases:

```text
available row → claimed by instance → work outside transaction → result persisted
```

`FOR UPDATE SKIP LOCKED` lets multiple processes claim different rows without waiting on one another. The transaction ends before Kafka or HTTP network I/O begins.

The lease fields are:

- `locked_by`: which process owns the claim.
- `locked_until`: when another process may recover it.

This is analogous to a visibility timeout in SQS or a job lock in BullMQ, except PostgreSQL is implementing the durable queue semantics.

### Delivery results and finalization

`RecordDeliveryResult` verifies ownership of a `delivering` row before changing it. It increments the attempt count and clears the lease.

`FinalizeEvent` locks delivery statuses and computes:

- All delivered, or no deliveries: `completed`.
- Some delivered and some permanently failed: `partially_failed`.
- No successful delivery: `failed`.

## Transaction pattern in Go

The helper accepts a function:

```go
err := store.withTx(ctx, func(tx pgx.Tx) error {
    // database operations
    return nil
})
```

This resembles a callback-based transaction in Prisma or Sequelize. The important difference is that Go's `defer` guarantees the rollback attempt when the function exits early.

## Pointers and nullable columns

Fields such as `*time.Time`, `*int`, and `*string` represent nullable SQL columns.

```go
CompletedAt *time.Time
```

- `nil`: SQL `NULL`; the delivery is not completed.
- non-nil: a real timestamp.

This is the Go equivalent of `Date | null`, not an object reference used for mutation.

## Tests

`internal/domain/state_machine_test.go` is a table-driven unit test. Table-driven tests are the common Go replacement for Jest's `test.each`.

`internal/store/postgres/store_integration_test.go` creates an isolated PostgreSQL schema and exercises:

1. Tenant and rule creation.
2. Event plus outbox insertion.
3. Duplicate and conflicting idempotency keys.
4. Idempotent routing.
5. Delivery claiming and completion.
6. Event finalization.
7. Outbox claiming and publication state.

It skips automatically when `TEST_DATABASE_URL` is absent.

## Suggested reading order

1. `internal/domain/event.go`
2. `internal/domain/notification.go`
3. `migrations/001_initial_schema.up.sql`
4. `internal/store/postgres/store.go`
5. `internal/store/postgres/event_repo.go`
6. `internal/store/postgres/notification_repo.go`
7. `internal/store/postgres/outbox_repo.go`
8. The two test files

## Exercises

1. Add a forbidden transition to the unit-test table and confirm the test passes.
2. Temporarily remove the delivery uniqueness constraint in a disposable database and reason about duplicate Kafka messages.
3. Add a test for an event with no matching rules.
4. Add a retrying delivery test and verify `next_retry_at` is required.
