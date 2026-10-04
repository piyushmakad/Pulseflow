# PulseFlow Postman verification

The collection verifies the deployed HTTP boundary against the same behaviors
covered by Go integration tests:

- liveness and dependency readiness;
- invalid API-key rejection;
- event ingestion and outbox persistence;
- tenant-scoped event lookup;
- idempotent replay;
- conflicting idempotency-key reuse.

## Import

Import both files into Postman:

```text
PulseFlow.postman_collection.json
PulseFlow.postman_environment.json
```

Select the **PulseFlow - Oracle OKE** environment and set:

```text
base_url = http://YOUR_LOAD_BALANCER_IP
api_key  = pf_live_...
```

Set `api_key` as a secret in Postman. Do not export an environment containing a
real key back into this repository.

## Run

Run the complete collection in its defined order. `Create Event` generates a
new order ID and idempotency key, then stores the returned event ID for the
following requests.

Expected results:

| Request | Expected status |
|---|---:|
| Liveness | 200 |
| Readiness | 200 |
| Reject Invalid API Key | 401 |
| Create Event | 202 |
| Get Event | 200 |
| Replay Same Event | 200 |
| Reject Idempotency Conflict | 409 |

The environment template intentionally contains no credential. The load
balancer URL uses HTTP in the learning environment; do not send real customer
data until TLS is configured.
