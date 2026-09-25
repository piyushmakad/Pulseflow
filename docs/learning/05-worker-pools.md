# Phase 5 — Worker Pools and Delivery

## Status

Planned. Phase 2 already provides durable delivery claiming, result recording, and event finalization.

## Goal

Deliver webhooks and simulated email with bounded concurrency, durable retry scheduling, backpressure, and graceful shutdown.

## Data flow

```text
PostgreSQL dispatcher
  → claim due rows with leases
  → bounded channel
  → fixed number of workers
  → webhook/email provider
  → persist result
  → finalize event when all deliveries are terminal
```

## Node comparison

In Node, concurrency is often limited with `p-limit`, a queue library, or a semaphore. In Go, a buffered channel plus a fixed number of goroutines is a natural bounded worker pool.

The channel is not the durable queue. It only controls in-process concurrency. PostgreSQL remains the durable copy, so a process crash does not erase pending work.

## Retry classification

Retry:

- Network failures and timeouts.
- HTTP 408 and 429.
- HTTP 5xx.

Normally do not retry other HTTP 4xx responses. Honor `Retry-After` when present; otherwise use exponential backoff with jitter.

## At-least-once webhooks

The provider might accept a request just before PulseFlow loses the connection. PulseFlow cannot know whether it succeeded, so it retries.

Every attempt sends the same logical delivery ID in an idempotency header. Providers can deduplicate it. PulseFlow must not claim exactly-once HTTP delivery.

## Graceful shutdown

1. Stop claiming new database work.
2. Close worker input channels after dispatchers stop.
3. Drain buffered and in-flight work.
4. Cancel execution contexts only after the shutdown deadline.
5. Let leases recover anything still unfinished.

## What to test

- Concurrency never exceeds the worker count.
- A full channel stops the dispatcher from claiming more rows.
- Retry timing and max-attempt dead lettering.
- Worker crash and expired-lease recovery.
- Normal drain and forced-timeout shutdown.
- Completed, partially failed, and failed event outcomes.
