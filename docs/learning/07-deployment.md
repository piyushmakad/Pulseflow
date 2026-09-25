# Phase 7 — Packaging, Deployment, and Resilience

## Status

Planned.

## Goal

Turn the same PulseFlow codebase and image into reliable API and worker deployments.

## Same image, different roles

```text
pulseflow --mode=api
pulseflow --mode=worker
pulseflow --mode=all
```

This is still a modular monolith. Separate containers do not automatically mean microservices; the ownership, source tree, domain model, and release artifact remain unified.

## Build requirements

- Match the Go version in `go.mod`, Docker, and CI.
- Commit `go.sum`.
- Use a multi-stage build and a minimal runtime image.
- Run as a non-root user where the environment permits.
- Keep configuration in environment variables or the deployment secret mechanism.

## Health behavior

- Liveness asks whether the process is functioning.
- Readiness asks whether this instance should receive work.
- A temporary dependency outage should generally make readiness fail, not force an endless liveness restart loop.

## Deployment tests

- PostgreSQL outage and recovery.
- Redis outage with API-key fallback and rate-limit fail-open behavior.
- Kafka outage with growing and later draining outbox backlog.
- Rolling worker shutdown with lease recovery.
- Multiple API and worker replicas.
- Migration compatibility during a rolling deployment.

## Node comparison

This is similar to deploying one Node repository as separate `web` and `worker` process types. Go packages make the internal boundaries explicit, while one compiled binary removes runtime dependency installation from the production container.
