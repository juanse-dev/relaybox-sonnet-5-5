# Spec 002 — Durable delivery worker and retries

## Goal

Deliver pending Relaybox records to their target URLs in the background.

Build on Spec 001 without changing its enqueue/idempotency contract.

Relaybox provides at-least-once delivery.

## Outbound request

For each due delivery, send an HTTP `POST` to its `target_url`.

Headers:

```text
Content-Type: application/json
X-Relaybox-Delivery-Id: <delivery UUID>
X-Relaybox-Attempt: <1-based attempt number>
```

The request body is exactly the stored JSON payload serialized as JSON.

A response with status `200..=299` is success.

The following are failures and must be retried:

- network/connect errors;
- request timeout;
- every non-2xx HTTP status.

## Delivery state

Extend persistent delivery state to support:

```text
pending
in_flight
delivered
```

Persist enough metadata to support durable retry scheduling and lease recovery.

The public delivery representation returned by the existing POST/GET endpoints now also
includes when known:

```json
{
  "next_attempt_at": "RFC3339 timestamp or null",
  "last_error": "string or null",
  "delivered_at": "RFC3339 timestamp or null"
}
```

Existing Spec 001 fields remain.

## Attempts

`attempts` counts outbound HTTP attempts.

The first outbound request uses attempt `1`.

The attempt number must be persisted durably.

## Retry schedule

After a failed attempt, return the delivery to `pending` and set:

```text
next_attempt_at = failure_time + backoff
```

Backoff is:

```text
attempt 1 failure -> 1 second
attempt 2 failure -> 2 seconds
attempt 3 failure -> 4 seconds
attempt 4 failure -> 8 seconds
...
```

Cap the backoff at 60 seconds.

For this specification there is no maximum attempt count. A delivery keeps retrying
until it succeeds.

## Leasing and crash recovery

Workers must claim work durably before sending it.

A claimed delivery becomes `in_flight` and has a lease expiry timestamp.

A worker must not normally send a delivery that currently has an unexpired lease owned
by another worker.

If Relaybox crashes or a worker disappears while a delivery is `in_flight`, the
delivery must become eligible again after the lease expires.

This is at-least-once behavior: if the remote accepted the request but Relaybox crashed
before recording success, a later retry may send it again.

Do not try to promise exactly-once delivery.

## Concurrency

Add configuration:

```text
RELAYBOX_WORKER_CONCURRENCY
RELAYBOX_POLL_INTERVAL_MS
RELAYBOX_REQUEST_TIMEOUT_MS
RELAYBOX_LEASE_SECONDS
```

Defaults:

```text
RELAYBOX_WORKER_CONCURRENCY=4
RELAYBOX_POLL_INTERVAL_MS=200
RELAYBOX_REQUEST_TIMEOUT_MS=5000
RELAYBOX_LEASE_SECONDS=30
```

`RELAYBOX_WORKER_CONCURRENCY` is the maximum number of outbound HTTP requests that one
Relaybox process may have active at once.

Configuration values must be validated at startup. Concurrency, poll interval, request
timeout, and lease seconds must all be greater than zero.

The request timeout must be strictly less than the lease duration. Invalid combinations
must fail startup clearly.

## Startup

The HTTP API and worker run in the same process.

Starting multiple Relaybox processes against the same SQLite database is allowed.
The durable claim/lease mechanism must prevent them from intentionally processing the
same unexpired claim concurrently.

SQLite contention should be handled without panicking the process.

## Success behavior

On 2xx:

- transition the delivery to `delivered`;
- persist `delivered_at`;
- clear lease metadata;
- leave `last_error` null or clear it;
- never schedule the delivery again.

## Failure behavior

On failure:

- transition back to `pending`;
- clear lease ownership;
- persist a useful `last_error`;
- calculate and persist `next_attempt_at`;
- retry only when that timestamp is due.

Do not expose raw Reqwest/SQLx debug dumps through the public API.

## Architecture

The application worker must depend on an outbound-delivery port.

Reqwest-specific sending belongs in `infrastructure/`.

Claiming, state transitions, and retry decisions belong in application/domain code, not
in Axum handlers.

## Tests

Add meaningful tests for at least:

- successful delivery;
- body and Relaybox headers sent correctly;
- non-2xx retry;
- network failure retry;
- exponential backoff;
- delivered records are not sent again;
- worker concurrency limit;
- expired lease recovery;
- two workers/process-like instances cannot both own the same unexpired claim.

Tests may use an in-process local HTTP test server.

Keep test durations short and deterministic.

## Not in scope

Do not implement:

- maximum attempt/dead-letter behavior;
- manual cancellation;
- manual requeue;
- `Retry-After`;
- per-host concurrency.

## Acceptance criteria

The spec is complete when:

- pending deliveries are delivered durably;
- retries follow the specified schedule;
- attempts and state survive restart;
- expired claims are recoverable;
- active global concurrency is bounded;
- multiple workers do not intentionally share an unexpired claim;
- Spec 001 behavior remains compatible;
- automated tests cover the worker behavior;
- all checks in `AGENTS.md` pass.
