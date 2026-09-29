# Spec 001 — Durable enqueue and query API

## Goal

Create the first usable Relaybox service.

This specification introduces an HTTP API that accepts a webhook delivery request and
stores it durably in SQLite. It does **not** send outbound webhooks yet.

## Required API

### `POST /v1/deliveries`

Required header:

```text
Idempotency-Key: <value>
```

Request body:

```json
{
  "target_url": "https://example.test/webhooks",
  "payload": {
    "event": "invoice.created",
    "invoice_id": "inv_123"
  }
}
```

`payload` may be any valid JSON value, including arrays, strings, numbers, booleans,
objects, or `null`.

### Validation

`Idempotency-Key`:

- is required;
- after trimming surrounding ASCII whitespace, must not be empty;
- maximum length: 128 bytes.

`target_url`:

- must be an absolute URL;
- scheme must be `http` or `https`;
- must contain a host.

Invalid JSON or invalid input returns a 4xx response using the repository error shape.

Use:

- `400` for malformed JSON or missing/invalid `Idempotency-Key`;
- `422` for a syntactically valid JSON request whose `target_url` is invalid.

### New request

For a previously unseen idempotency key:

- create one delivery with a UUID;
- persist the original target URL and JSON payload;
- set status to `pending`;
- set `attempts` to `0`;
- persist `created_at`;
- return `201 Created`.

Response:

```json
{
  "id": "UUID",
  "status": "pending",
  "attempts": 0,
  "target_url": "https://example.test/webhooks",
  "payload": {
    "event": "invoice.created",
    "invoice_id": "inv_123"
  },
  "created_at": "RFC3339 timestamp"
}
```

### Idempotent replay

If the same normalized `Idempotency-Key` is submitted again with the same
`target_url` and JSON payload:

- do not create another row;
- return the existing delivery;
- return `200 OK`.

JSON object key ordering must not cause a conflict. For example, these payloads are the
same for idempotency purposes:

```json
{"a":1,"b":2}
```

```json
{"b":2,"a":1}
```

The idempotency guarantee must be enforced by durable persistence, not only by an
in-memory map.

Concurrent requests using the same idempotency key must not create multiple deliveries.

### Idempotency conflict

If the same normalized idempotency key is reused with a different target URL or a
different JSON payload:

- create nothing;
- leave the original delivery unchanged;
- return `409 Conflict`.

Error code:

```text
idempotency_conflict
```

## `GET /v1/deliveries/{id}`

For an existing delivery:

- return `200 OK`;
- return the same public representation used by POST.

For an unknown or malformed UUID:

- return `404 Not Found`;
- error code: `delivery_not_found`.

## `GET /health`

Return:

```json
{"status":"ok"}
```

with `200 OK`.

This endpoint does not need to run a database query.

## Persistence

Use SQLite via SQLx.

The schema must be created through migrations.

A delivery created before process shutdown must still be queryable after Relaybox is
restarted against the same database.

The database must enforce uniqueness of the normalized idempotency key.

## Architecture

Respect the architecture in `README.md`.

At minimum the implementation should make the following responsibilities separable:

- API transport;
- enqueue/query use cases;
- delivery repository contract;
- SQLite repository implementation.

Do not place SQL in Axum handlers.

## Not in scope

Do not implement:

- outbound webhook delivery;
- retry/backoff;
- background workers;
- dead-letter behavior;
- cancellation;
- `Retry-After`;
- per-host concurrency.

Those belong to later specifications.

## Acceptance criteria

The spec is complete when:

- the required endpoints behave as described;
- idempotency survives restart;
- concurrent same-key enqueue cannot create multiple rows;
- same-key/same-content replays are stable;
- same-key/different-content returns 409;
- invalid input returns the specified 4xx responses;
- migrations work on a new database;
- behavior is covered by meaningful automated tests;
- all checks in `AGENTS.md` pass.
