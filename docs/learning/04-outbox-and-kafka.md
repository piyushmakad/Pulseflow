# Phase 4 — Outbox Relay and Kafka

## Status

Planned. Phase 2 provides the outbox repository operations this phase will use.

## Goal

Publish accepted events to Kafka without introducing a PostgreSQL/Kafka dual write, then route consumed messages into durable delivery rows.

## Outbox relay flow

```text
claim leased batch in PostgreSQL
commit claim transaction
publish batch to Kafka
mark rows published
```

Kafka publication happens outside the SQL transaction. If publishing succeeds but marking fails, the relay may publish the same message again. This is why the consumer must be idempotent.

Node comparison: this resembles a BullMQ worker with a visibility timeout, but the outbox table and Kafka producer are separate pieces and the failure boundary is explicit.

## Kafka consumer flow

1. Fetch a Kafka message.
2. Decode the versioned `EventMessage`.
3. Call `RouteEvent`.
4. Commit the Kafka offset only after delivery rows are durable.

The consumer does not wait for webhooks. Kafka transports the routing signal; PostgreSQL tracks delivery work.

## Poison messages

A message that cannot be decoded must be persisted to a quarantine/dead-letter outbox record before its source offset is committed. Logging and skipping would silently lose evidence.

## What to learn

- At-least-once delivery is normal, not an exceptional edge case.
- Kafka offsets are acknowledgements, not database transactions.
- Partition ordering means broker arrival order, not necessarily database creation order across concurrent relays.
- Stable message versions are safer than decoding directly into internal database models.

## What to test

- Kafka unavailable while API ingestion continues.
- Relay crash before and after publishing.
- Duplicate messages.
- Multiple relay instances claiming concurrently.
- Poison-message quarantine.
- Consumer crash before offset commit.
