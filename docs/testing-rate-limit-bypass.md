# Dedicated load-test tenant

Set `RATE_LIMIT_BYPASS_TENANT_ID` in the **API process environment** to the
tenant ID returned when provisioning your dedicated test tenant. This is a
tenant UUID, not a username or raw API key. Empty (the default) disables bypass.

All valid keys for that tenant bypass the Redis rate limiter. Authentication,
tenant isolation, validation, idempotency, and database persistence still run.
Other tenants keep their configured limits. No database migration is needed.
Requests cannot opt in through a header, body field, or query parameter.

## Local PowerShell

```powershell
$env:RATE_LIMIT_BYPASS_TENANT_ID = "YOUR_TEST_TENANT_UUID"
go run ./cmd/pulseflow --command=run --mode=all
```

PulseFlow reads process environment variables; `.env` files are not automatically
loaded. In Postman/k6, use the test tenant's API key as the bearer token. Setting
this variable only in Postman/k6 does not change the server configuration.

## Oracle Cloud Shell

First build, push, and deploy an image containing this change using the existing
deployment workflow and a new image tag. The currently deployed older image does
not support this variable. Check that kubectl targets your OKE cluster.

```bash
kubectl set env -n pulseflow deployment/pulseflow-api \
  RATE_LIMIT_BYPASS_TENANT_ID="YOUR_TEST_TENANT_UUID"
kubectl rollout status -n pulseflow deployment/pulseflow-api
```

This changes the API Deployment and rolls out its replicas. It is not stored in
your checked-in manifests; a later deployment can change the setting. Keep the
test tenant ID in your deployment environment and keep its API key private.

Send an authenticated event request. Its response should include
`X-RateLimit-Bypassed: true`. Normal tenants do not receive this header.
Startup also logs a warning identifying the configured bypass tenant.

After testing, disable the bypass and wait for the rollout:

```bash
kubectl set env -n pulseflow deployment/pulseflow-api RATE_LIMIT_BYPASS_TENANT_ID-
kubectl rollout status -n pulseflow deployment/pulseflow-api
```

In Node.js terms, configuration reads `process.env`, authentication attaches a
trusted tenant to the request, and middleware calls `next()` immediately only
when that tenant matches the configured test tenant. Go uses the request context
for the trusted principal. Runtime configuration is read at startup, so changing
the variable requires restarting the process (Kubernetes handles this rollout).

Record the bypass in benchmark results: this measures authenticated ingestion
with the rate-limit Redis operation excluded, not the normal limited path.
