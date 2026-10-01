# Phase 4 — Outbox Relay and Kafka Routing

## Status

Implemented.

## Goal

Move accepted events from PostgreSQL to Kafka without a database/Kafka dual
write, then turn each consumed event into durable delivery rows in PostgreSQL.

This phase deliberately stops before calling webhook or email providers. Phase
5 will claim and execute the delivery rows created here.

## The complete flow

```text
HTTP request
  -> PostgreSQL TX 1: INSERT event + INSERT outbox
  -> Outbox relay claims a leased batch
  -> Kafka producer publishes with tenant_id as the message key
  -> Kafka consumer reads EventMessage
  -> PostgreSQL TX 2: verify event + create delivery rows
  -> Consumer commits the Kafka offset
```

PostgreSQL appears twice because the writes mean different things:

- TX 1 says, "PulseFlow accepted this event."
- TX 2 says, "PulseFlow created the durable delivery work for this event."

Kafka transports the routing signal between them. It is not the source of
truth for event or delivery state.

## Files added in this phase

### `internal/kafka/message.go`

Defines the small `Message`, `Publisher`, and `Reader` abstractions used by the
relay and consumer.

The Kafka library is hidden behind these interfaces. This is similar to using
a small TypeScript interface around a third-party SDK: business orchestration
can be tested without starting a real broker.

### `internal/kafka/producer.go`

Adapts `segmentio/kafka-go` to the local `Publisher` interface.

Important settings:

- `Async: false` waits for a broker result before PostgreSQL is updated.
- `RequiredAcks: RequireAll` asks Kafka for acknowledgement from all in-sync
  replicas.
- `Hash` assigns the same message key to the same partition.
- `AllowAutoTopicCreation: false` prevents misspelled topic names from silently
  creating new topics.

`Publish` returns one error slot per input message. That lets the relay mark
successful rows as published while releasing failed rows for retry.

Node comparison: this resembles adapting a KafkaJS producer behind your own
`Publisher` interface instead of importing KafkaJS in every module.

### `internal/kafka/reader.go`

Adapts the Kafka consumer-group reader to the local `Reader` interface.

`FetchMessage` does not automatically commit. `CommitMessages` is called
explicitly only after routing or quarantine is durable in PostgreSQL.

### `internal/kafka/outbox_relay.go`

Runs this loop:

```text
claim leased rows in a short transaction
  -> publish outside the transaction
  -> mark acknowledged rows published
  -> release failed rows with a future available_at
```

The PostgreSQL transaction ends before Kafka network I/O starts. A slow or
unavailable broker therefore cannot keep database row locks open.

If publishing succeeds but `MarkOutboxPublished` fails, the row remains leased.
After the lease expires, it is published again. This intentional duplicate is
why the system promises at-least-once delivery rather than exactly once.

Node comparison: the lease is like a BullMQ visibility timeout. A crashed
worker does not permanently own the job.

### `internal/kafka/consumer.go`

For each Kafka message, the consumer:

1. Decodes `EventMessage`.
2. Validates its version and required fields.
3. Calls `RouteEventMessage`.
4. Waits until PostgreSQL commits delivery rows.
5. Commits that Kafka message's offset.

The consumer does not fetch a later message after a routing or commit failure.
It retries the current operation first. This matters because committing a
later offset also acknowledges earlier offsets in the same partition.

### `internal/domain/quarantine.go`

Defines two different records:

- `QuarantinedMessage` is the PostgreSQL source-of-truth record.
- `DeadLetterMessage` is the versioned JSON payload published later.

Keeping those names separate avoids treating a database row and a Kafka
message as if they were the same object.

### `internal/store/postgres/quarantine_repo.go`

`QuarantineMessage` performs one transaction:

```text
INSERT quarantined_messages
INSERT outbox(topic = events.deadletter)
COMMIT
```

The unique source `(topic, partition, offset)` makes the operation idempotent.
The consumer commits the source offset only after this transaction succeeds.

### `migrations/002_kafka_quarantine.*.sql`

Adds `quarantined_messages`. Raw keys and payloads use PostgreSQL `BYTEA`
because a poison Kafka message is not guaranteed to contain valid UTF-8 or
JSON.

### `cmd/pulseflow/main.go`

Worker mode now starts the relay and routing consumer from the same Go binary:

```powershell
go run ./cmd/pulseflow --mode=worker
```

`--mode=all` runs the API and worker modules in one process. `--mode=api` and
`--mode=worker` allow the same codebase to be deployed as separately scaled
processes without creating microservices or separate repositories. In `all`
mode, both modules share one PostgreSQL connection pool; separate processes
naturally own one pool each.

## Event message validation

The consumer does not route directly from arbitrary message fields. Inside the
routing transaction, `RouteEventMessage` loads the event with `FOR UPDATE` and
checks that these immutable values match:

- event ID;
- tenant ID;
- event type;
- JSON data;
- occurrence time.

A mismatched message is quarantined instead of being allowed to route work for
the wrong tenant.

## Failure behavior

### PostgreSQL unavailable

- The relay cannot claim new rows.
- The consumer retains its current Kafka offset and retries.
- No delivery work is acknowledged as durable.

### Kafka unavailable

- API ingestion continues because TX 1 only requires PostgreSQL.
- Outbox rows remain pending or are released for retry.
- The relay catches up after Kafka recovers.

### Relay crash

- Before publish: the lease expires and another relay claims the row.
- After publish but before mark: the message is published again after lease
  expiry.

### Consumer crash

- Before TX 2 commits: no offset was committed, so Kafka redelivers.
- After TX 2 but before offset commit: Kafka redelivers, but
  `UNIQUE(event_id, notification_rule_id)` prevents duplicate logical delivery
  rows.

### Invalid message

- The raw message and error are stored in `quarantined_messages`.
- A dead-letter outbox row is stored in the same transaction.
- Only then is the source Kafka offset committed.

## Local setup

Apply both migrations to an existing local database. If migration 001 was
already applied, only apply migration 002:

```powershell
psql "$env:DATABASE_URL" -f migrations/002_kafka_quarantine.up.sql
```

Docker Compose creates the two Kafka topics through the `kafka-init` container:

```powershell
docker compose up -d zookeeper kafka kafka-init
```

Run the complete process:

```powershell
go run ./cmd/pulseflow --mode=all
```

## Tests

Normal unit tests use fake publishers/readers and do not require Kafka:

```powershell
go test ./internal/kafka -v
go test ./...
```

PostgreSQL integration tests also cover idempotent quarantine and concurrent
outbox claims:

```powershell
$env:TEST_DATABASE_URL = "postgres://USER:PASSWORD@localhost:5432/pulseflow?sslmode=disable"
go test ./internal/store/postgres -v
```

## What to learn

- An outbox solves atomic creation of database state and publish intent; it does
  not produce exactly-once delivery.
- Kafka offsets are acknowledgements, not database transactions.
- Leases recover abandoned work without holding transactions across network
  calls.
- Stable message versions decouple transport contracts from internal structs.
- Idempotency is the normal way to make at-least-once systems correct.
