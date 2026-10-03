# PulseFlow Learning Path

This folder explains PulseFlow one implementation phase at a time for someone coming from Node.js/TypeScript.

The guides separate two things:

- **What exists now** — code you can open, run, and change.
- **What comes later** — the design direction, without pretending it is implemented.

## Phase map

| Phase | Topic | Status |
|---|---|---|
| 1 | Go project foundation and local infrastructure | Implemented |
| 2 | Domain state machines and PostgreSQL repositories | Implemented |
| 3 | HTTP API, API-key authentication, and rate limiting | Implemented |
| 4 | Transactional outbox relay and Kafka routing | Implemented |
| 5 | Bounded workers, webhook delivery, retries, and finalization | Implemented |
| 6 | Operational visibility | Implemented |
| 7 | Packaging, deployment, CI, and resilience | Implemented |

Start with Phase 1 even if you already know Node project structure. Go's package visibility, explicit error handling, and use of `context.Context` affect every later phase.

## Useful commands

```powershell
go test ./...
go test -race ./...
go run ./cmd/pulseflow --mode=all
```

The PostgreSQL integration test is opt-in:

```powershell
$env:TEST_DATABASE_URL = "postgres://pulseflow:pulseflow@localhost:5432/pulseflow?sslmode=disable"
go test ./internal/store/postgres -v
```

## Node-to-Go mental model

| Node/TypeScript | Go |
|---|---|
| `package.json` | `go.mod` |
| `node_modules` | Module cache plus `go.sum` checksums |
| `src/index.ts` | `cmd/pulseflow/main.go` |
| ES module | Go package |
| `class` used for data | Usually a `struct` |
| TypeScript interface | Go `interface`, normally behavior-focused |
| `Promise<T>` | A blocking function returning `(T, error)`; concurrency is explicit |
| `AbortSignal` | `context.Context` |
| `undefined` / nullable value | Pointer such as `*time.Time` when absence matters |
| Throw/catch | Return and inspect `error` values |
| Worker/event-loop task | Goroutine, usually coordinated by channels and contexts |

Do not try to translate Node syntax line by line. Learn the invariants and data flow first, then see how Go expresses them.
