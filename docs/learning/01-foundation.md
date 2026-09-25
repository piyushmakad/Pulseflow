# Phase 1 — Foundation

## Goal

Create one Go module and one executable that can later run different parts of PulseFlow from the same codebase.

## What is implemented

- `go.mod` declares the module name and Go version.
- `cmd/pulseflow/main.go` is the executable entrypoint.
- `internal/config` reads environment variables into a typed struct.
- `internal/platform/logger` wraps Go's standard `log/slog` logger.
- Docker Compose describes PostgreSQL, Redis, Kafka, and Zookeeper for local development.
- SQL migrations define the initial database.

The `--mode` flag supports `all`, `api`, and `worker`. The API and worker startup functions are still placeholders until later phases.

## Node comparison

In Node you might have:

```text
package.json
src/index.ts
src/config.ts
```

The Go equivalent is:

```text
go.mod
cmd/pulseflow/main.go
internal/config/config.go
```

`cmd/pulseflow` is not a special compiler keyword. It is a community convention: every folder below `cmd` represents a buildable executable.

The `internal` directory is enforced by the Go toolchain. Packages outside this module cannot import it. Think of it as stronger than an unexported JavaScript module.

## Important Go ideas

### Capitalization controls visibility

`Config` and `Load` are exported. `envStr` is private to the package. Go uses capitalization instead of `export` keywords.

### Explicit error returns

```go
cfg, err := config.Load()
if err != nil {
    // handle failure
}
```

There is no hidden promise rejection or exception path here. The function returns two values, and the caller decides what to do with the error.

### Context-driven shutdown

`signal.NotifyContext` creates a context cancelled by SIGINT or SIGTERM. Later, the API, Kafka consumer, dispatcher, and worker pools will all observe contexts derived from it.

Node analogy: this is similar to passing one `AbortSignal` through every async subsystem, but cancellation is a standard convention across Go libraries.

## Try it

```powershell
go test ./...
go run ./cmd/pulseflow --mode=all
```

Stop the process with Ctrl+C and observe the structured shutdown logs.
