# Phase 3 — HTTP API, Authentication, and Rate Limiting

## Status

Planned. This guide describes what will be built; it is not implemented yet.

## Goal

Expose event ingestion and lookup over HTTP while keeping handlers thin. Handlers should validate transport input and call Phase 2 repository/application operations rather than embedding SQL.

## Intended packages

```text
internal/api/server.go
internal/api/router.go
internal/api/middleware/auth.go
internal/api/middleware/ratelimit.go
internal/api/handler/event.go
internal/api/handler/health.go
```

## Node comparison

Chi middleware is conceptually similar to Express middleware:

```ts
app.post("/v1/events", auth, rateLimit, createEvent);
```

In Go, `http.Handler` is an interface with one method. Middleware is normally a function that accepts and returns an `http.Handler`.

Unlike Express, request-scoped values and cancellation travel through `request.Context()`. Database calls must use that context so work stops when the client disconnects or the server shuts down.

## API-key flow

1. Read a bearer token.
2. Hash it with SHA-256 because the generated key has high entropy.
3. Check the short-lived Redis cache.
4. Fall back to PostgreSQL when Redis misses or is unavailable.
5. Attach the authenticated tenant ID to the request context.

Redis is a cache, not the authority. PostgreSQL stores key status and expiry.

## Rate limiting

A Redis Lua token bucket performs the atomic read/refill/consume operation. If Redis is unavailable, rate limiting fails open while authentication falls back to PostgreSQL.

This is an intentional split:

- Rate limiting protects capacity but is not correctness-critical.
- Authentication is correctness-critical and must retain a PostgreSQL path.

## What to test

- Missing/malformed bearer tokens.
- Expired and revoked keys.
- Redis cache hit, miss, and outage.
- Rate-limit exhaustion and fail-open behavior.
- Same and conflicting idempotency-key requests.
- Tenant isolation on event lookup.
- Graceful HTTP shutdown.
