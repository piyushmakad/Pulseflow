# Phase 6 — Operational Visibility

## Status

Planned. Structured logging exists, but production metrics and tracing are not selected yet.

## Goal

Make correctness and backlog visible without adding technology merely because it is fashionable.

## Start with signals the architecture requires

- Oldest pending outbox age.
- Pending/publishing outbox counts.
- Due delivery count.
- Expired delivery lease count.
- Delivery latency and result counts by channel.
- Retry and dead-letter counts.
- Kafka consumer lag.
- PostgreSQL pool saturation.
- Redis fallback count.

## Node comparison

The concepts are the same as Pino/Winston logs plus Prometheus or OpenTelemetry instrumentation. The Go difference is mostly library shape: context is passed explicitly, and metrics are usually package-level instruments or injected interfaces rather than JavaScript singletons.

## Logging rules

- Use structured attributes, not interpolated prose.
- Include event ID, delivery ID, tenant ID, and worker/relay instance ID where relevant.
- Never log raw API keys or sensitive webhook headers.
- Log state transitions and recovery actions at the boundary where they become durable.

## Technology decision

Do not add a metrics backend or tracing collector until the deployment environment defines who operates it and how data is retained. The code can expose stable counters and tracing hooks without committing prematurely to extra infrastructure.
