# Phase 7 — Packaging, Kubernetes, Oracle, and CI

## Status

Implemented.

## Goal

Package the existing modular monolith once and run that image in different
roles without creating separate Go codebases:

```text
pulseflow --mode=api
pulseflow --mode=worker
pulseflow --command=migrate
```

For a Node developer, this resembles one repository exposing three npm scripts:

```json
{
  "scripts": {
    "start:api": "node app.js --mode=api",
    "start:worker": "node app.js --mode=worker",
    "migrate": "node app.js --command=migrate"
  }
}
```

Go produces one compiled executable instead of installing Node modules in each
runtime container.

## Environment split

```text
Local development
└── Docker Compose
    ├── PostgreSQL
    ├── Redis
    ├── Kafka in KRaft mode
    └── PulseFlow started from the terminal

Oracle learning deployment
└── OKE Basic
    └── single Flex worker node
        ├── API Deployment
        ├── Worker Deployment
        ├── PostgreSQL StatefulSet + volume
        ├── Kafka StatefulSet + volume
        └── Redis Deployment without durable storage
```

OKE is Oracle Kubernetes Engine. EKS is the AWS-specific name and is not used
on Oracle.

## Three different meanings of "worker"

| Term | Meaning |
|---|---|
| OKE worker node | Oracle VM on which Kubernetes schedules pods |
| PulseFlow worker pod | One `pulseflow --mode=worker` operating-system process |
| Go worker | One goroutine inside that process that handles deliveries |

If Kubernetes runs two worker pods and each has
`WEBHOOK_WORKER_COUNT=4`, the theoretical webhook concurrency is:

```text
2 pods × 4 goroutines = 8 concurrent webhook deliveries
```

The real throughput can still be lower because of PostgreSQL capacity, network
latency, provider latency, Kafka partition ownership, and backpressure.

## `migrations/embed.go`

`//go:embed *.sql` copies the migration files into the compiled Go binary.
The runtime image therefore does not need a separate `migrations` directory.
The SQL and executable cannot accidentally come from different commits.

Node analogy:

```javascript
// Conceptually similar to bundling SQL files into the production artifact.
import migration001 from "./001_initial_schema.up.sql";
```

The Go `embed.FS` is read-only. It cannot modify the repository or migration
files at runtime.

## `internal/platform/migration/runner.go`

This package connects the embedded SQL files to `golang-migrate`.

```go
func Up(databaseURL string) error
```

Read that declaration as: `Up` accepts a PostgreSQL URL and returns an error. A
nil error means every required migration is applied.

The migration library records the current version in PostgreSQL and obtains a
PostgreSQL advisory lock. The lock prevents two deployment jobs from changing
the schema simultaneously. `migrate.ErrNoChange` is considered success because
an already-current schema is the desired result.

The URL scheme is changed from `postgres://` to `pgx5://` only to select the
golang-migrate pgx v5 driver. Host, credentials, database and query parameters
are preserved.

## `cmd/pulseflow/main.go`

The executable now understands:

```bash
pulseflow --command=migrate
```

The Kubernetes migration Job uses this before application Deployments roll out.

Worker-only mode also starts a small health server on `HEALTH_PORT`, default
9090. It exposes only:

```text
GET /healthz
GET /readyz
```

It does not expose event ingestion routes.

PostgreSQL controls worker readiness because it contains the durable outbox,
delivery leases and delivery results. Kafka failure does not kill the worker:
the relay and consumer retry while PostgreSQL preserves pending work.

## `internal/api/handler/health.go`

The same health handler now supports:

```text
API mode:    PostgreSQL + Redis checks
Worker mode: PostgreSQL check only
```

Redis failure reports `degraded` with HTTP 200. Redis contains disposable
API-key cache and rate-limit state. PostgreSQL failure reports HTTP 503 because
the source of truth is unavailable.

Liveness deliberately does not query dependencies. Restarting every pod during
a PostgreSQL or Kafka outage would create a restart storm without repairing the
dependency.

## `Dockerfile`

```text
golang builder image
        │ compiles static binary
        ▼
small Alpine runtime image
```

Important decisions:

- `TARGETOS` and `TARGETARCH` support `linux/amd64` and `linux/arm64`.
- BuildKit caches dependencies and compilation output.
- `-trimpath` removes local build paths from the binary.
- The runtime uses UID/GID 10001 instead of root.
- `SIGTERM` activates the existing graceful-shutdown path.
- SQL migrations are embedded, so only the executable is copied.

The application and migration Job use the same immutable image.

## `.dockerignore`

This controls the Docker build context. Git history, local environment files,
docs, test output and binaries are not uploaded to the builder. Excluding
`.env` also prevents local credentials from entering an image layer.

## `docker-compose.yml`

Kafka was upgraded to an ARM64-capable Confluent release and changed from
ZooKeeper mode to KRaft mode:

```text
Before: Kafka + ZooKeeper
After:  Kafka with an internal KRaft controller
```

KRaft is still Kafka; it removes a separate coordination process. This reduces
memory use and keeps local development closer to Oracle ARM64. Kafka also has a
named volume so ordinary container restarts do not erase published records.

## Kubernetes base files

`deploy/kubernetes/base` contains provider-neutral application resources.

### `configmap.yaml`

A ConfigMap holds non-secret settings such as topics, ports, worker counts and
buffer sizes. Changing it does not automatically restart existing pods.

### `api.yaml`

Creates an API Deployment and internal Service. It adds readiness and liveness
probes, resource limits, a read-only filesystem and a non-root security context.
The Kubernetes termination window is 45 seconds so the Go process receives its
30-second graceful-shutdown period.

`maxUnavailable: 0` keeps an old API pod until its replacement is ready.

### `worker.yaml`

Runs `--mode=worker`. It has no public Service because users never send HTTP
traffic to it. The kubelet calls the pod health port directly.

On SIGTERM, dispatchers stop claiming work and bounded pools drain before exit.

### `migration-job.yaml`

A Job is a run-to-completion workload. It is deliberately applied separately:

```text
infrastructure ready
      ↓
migration Job succeeds
      ↓
API and worker rollout
```

This preserves an explicit schema/deployment boundary.

The image supports both `linux/arm64` for Always Free A1 capacity and
`linux/amd64` for x86 Flex workers used with trial credits or paid accounts.

## Oracle infrastructure files

`deploy/kubernetes/oracle-free/infra` is separate so a future paid environment
can replace these single-instance dependencies without changing the app.

- PostgreSQL gets a retained 50 GiB volume and remains the source of truth.
- Redis uses `emptyDir`; its disposable cache/rate-limit data may be lost.
- Kafka runs one KRaft broker/controller with a retained 50 GiB volume.
- A bootstrap Job idempotently creates ingestion and dead-letter topics.

Kafka replication is one because this cost-constrained profile has one broker.
That is not high availability.

## Oracle application overlay

`oracle-free/app/kustomization.yaml` changes the generic image to an OCI
Registry path and configures two API pods and one worker pod. Two API processes
survive one process crash, but they still share one node.

The API Service becomes an OCI flexible Load Balancer capped at 10 Mbps.
PostgreSQL, Redis, Kafka and the worker health port remain private.

Kustomize is built into `kubectl`; it avoids duplicating nearly identical YAML.

## Secret handling

`secret.example.yaml` documents required keys but contains no usable password.
Copy it to `secret.yaml`; that filename is ignored by Git.

Kubernetes Secret values are encoded for transport but are not safe to commit.
The OCI Registry login is stored separately in `ocir-pull-secret`.

## CI

`.github/workflows/ci.yml` runs:

```text
test job                  container job
├── gofmt                 ├── QEMU
├── go vet                ├── Docker Buildx
└── go test -race         └── amd64 + arm64 builds
```

CI verifies images but does not publish or deploy. Registry and Oracle
credentials are not assumed or added without a release decision.

## Failure behavior

| Failure | Result |
|---|---|
| PostgreSQL unavailable at startup | Process exits; Kubernetes retries it |
| PostgreSQL fails while running | API is not-ready; worker cannot make durable progress |
| Redis unavailable | API remains ready but degraded; fallbacks apply |
| Kafka unavailable | PostgreSQL outbox grows; worker stays alive and retries |
| Webhook provider slow | Bounded pool and channel buffer apply backpressure |
| API gets SIGTERM | Server stops accepting traffic and drains requests |
| Worker gets SIGTERM | Dispatch stops and pools drain to the deadline |
| Kafka or PostgreSQL pod restarts | Persistent volume preserves ordinary restart data |
| Entire free node fails | Environment is unavailable until node/volumes recover |

## Free profile versus production-grade Oracle

Oracle is the chosen provider, but the cost-conscious profile is not highly available.
Real production also requires multiple nodes, HA PostgreSQL, three replicated
Kafka brokers, tested backups/restores, TLS/DNS, operator alert delivery and
load/failure testing. These improve infrastructure availability without
splitting the Go codebase or changing PostgreSQL's source-of-truth role.
