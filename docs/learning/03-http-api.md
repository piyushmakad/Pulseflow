# Phase 3 — HTTP API, Authentication, and Rate Limiting

## Status

Implemented.

## Goal

Expose the Phase 2 event operations through HTTP while keeping transport, authentication, rate limiting, and persistence separate.

## Package structure

```text
internal/
├── admin/
│   └── provision.go             # One-shot tenant and API-key provisioning
├── api/
│   ├── server.go                # net/http lifecycle and graceful shutdown
│   ├── router.go                # Chi routes and middleware order
│   ├── handler/
│   │   ├── event.go             # POST/GET /v1/events
│   │   └── health.go            # /healthz and /readyz
│   ├── middleware/
│   │   ├── auth.go              # API-key authentication
│   │   ├── ratelimit.go         # Per-tenant token bucket middleware
│   │   └── requestid.go         # Request correlation ID
│   └── response/
│       └── response.go          # Consistent JSON responses
└── store/redis/
    ├── store.go                 # Redis connection
    ├── cache.go                 # API-key cache
    └── ratelimiter.go           # Atomic Lua token bucket
```

The folder is the Go package boundary. Files within a folder divide responsibilities for readers; they are compiled together.

## Node/Express comparison

The router is conceptually similar to:

```ts
app.get("/healthz", health.live);
app.get("/readyz", health.ready);
app.use("/v1", requestId);
app.use("/v1", auth);
app.use("/v1", rateLimit);
app.post("/v1/events", eventHandler.create);
app.get("/v1/events/:eventID", eventHandler.get);
```

Chi composes standard `http.Handler` values. A Go middleware has this shape:

```go
func Middleware(next http.Handler) http.Handler
```

That is comparable to Express calling `next()`, but the authenticated principal is carried in `request.Context()` rather than by mutating `req.user`.

## API-key authentication

The client sends:

```http
Authorization: Bearer pf_live_...
```

Authentication:

1. Parses the bearer header and requires the `pf_live_` prefix.
2. Calculates SHA-256 of the complete raw key.
3. Checks Redis for the hash.
4. Falls back to PostgreSQL on a miss or Redis failure.
5. Caches a successful PostgreSQL result for a short TTL.
6. Adds `{tenantID, apiKeyID}` to the request context.

Only the hash is stored in PostgreSQL. Redis is not authoritative; losing Redis cannot prevent valid authentication while PostgreSQL is available.

## Provision a local tenant and key

Apply the migration, set `DATABASE_URL`, and run:

```powershell
go run ./cmd/pulseflow `
  --command=provision `
  --tenant-name="My Local Tenant" `
  --key-name="development"
```

The command prints the tenant ID, API-key ID, and raw `pf_live_...` key. Store the raw key locally because it cannot be recovered from PostgreSQL.

We use a one-shot command instead of a public tenant-creation endpoint because public administration requires a separate authorization model.

## Event endpoints

### Create an event

```http
POST /v1/events
Authorization: Bearer pf_live_...
Idempotency-Key: order-123
Content-Type: application/json

{
  "type": "order.created",
  "data": {"order_id": "123"}
}
```

Results:

- `202 Accepted`: a new event and outbox row were committed.
- `200 OK`: the same idempotent request already existed.
- `409 Conflict`: the key was reused with different type or data.
- `400 Bad Request`: invalid JSON, unknown fields, or missing input.
- `401 Unauthorized`: invalid API key.
- `429 Too Many Requests`: token bucket exhausted.

The handler gets `tenant_id` from authentication. It never trusts a tenant ID supplied in the request body.

### Get an event

```http
GET /v1/events/{eventID}
Authorization: Bearer pf_live_...
```

The repository query includes both event ID and authenticated tenant ID, preserving tenant isolation.

## Token-bucket rate limiting

The Redis Lua script atomically loads the bucket, refills tokens based on elapsed time, consumes one token, and stores the result with a TTL. Lua prevents separate `GET` and `SET` operations from racing across API processes.

If Redis fails, middleware logs a warning and allows the request. Rate limiting protects capacity; it is not part of event correctness.

## Request context

Node commonly attaches authentication directly:

```ts
req.user = { tenantId, apiKeyId };
```

Go derives a context:

```go
ctx := middleware.WithPrincipal(r.Context(), principal)
next.ServeHTTP(w, r.WithContext(ctx))
```

Handlers retrieve it with `PrincipalFromContext`. The same context reaches PostgreSQL and Redis, so request cancellation propagates.

## Health and shutdown

- `/healthz` reports process liveness.
- `/readyz` requires PostgreSQL.
- Redis failure reports `degraded` while remaining ready because both Redis uses have fallbacks.
- `api.Server.Run` stops accepting requests on root-context cancellation and drains active requests until `SHUTDOWN_TIMEOUT`.

## Tests

```powershell
go test ./internal/api/... -v
```

Tests cover cache hits, PostgreSQL fallback, invalid keys, rate-limit rejection/fail-open, event creation, safe duplicates, conflicts, and tenant scoping.

Run the full HTTP/PostgreSQL test with:

```powershell
$env:TEST_DATABASE_URL = "your PostgreSQL test URL"
go test ./internal/api -run TestEventAPIIntegration -v
```

Run the real Redis Lua/cache test with:

```powershell
$env:TEST_REDIS_URL = "redis://localhost:6379/1"
go test ./internal/store/redis -run TestRedisCacheAndTokenBucketIntegration -v
```

Use a disposable Redis database number for integration testing.

## Suggested reading order

1. `internal/api/response/response.go`
2. `internal/api/middleware/requestid.go`
3. `internal/api/middleware/auth.go`
4. `internal/api/middleware/ratelimit.go`
5. `internal/api/handler/event.go`
6. `internal/api/router.go`
7. `internal/api/server.go`
8. `cmd/pulseflow/main.go`
