# Spec 003 — Dead-letter lifecycle and cancellation

## Goal

Bound automatic retries and add explicit operator-controlled lifecycle actions.

Build on Specs 001 and 002.

## Maximum attempts

Add configuration:

```text
RELAYBOX_MAX_ATTEMPTS
```

Default:

```text
RELAYBOX_MAX_ATTEMPTS=5
```

It must be an integer greater than zero.

An outbound request still increments `attempts` exactly as defined in Spec 002.

When an attempt fails:

- if `attempts < RELAYBOX_MAX_ATTEMPTS`, schedule the normal retry;
- if `attempts >= RELAYBOX_MAX_ATTEMPTS`, transition to `dead_letter` instead.

A dead-letter delivery:

- has no active lease;
- has no `next_attempt_at`;
- keeps its final `last_error`;
- is never picked up automatically by the worker.

Add `dead_lettered_at`.

## Status model

The persisted/public status set is now:

```text
pending
in_flight
delivered
dead_letter
canceled
```

Terminal states:

```text
delivered
dead_letter
canceled
```

## Cancel API

Add:

```text
POST /v1/deliveries/{id}/cancel
```

Behavior:

### pending

Transition atomically to `canceled`.

Return `200 OK` with the updated delivery.

Set `canceled_at`.

Clear:

- `next_attempt_at`;
- lease metadata.

### canceled

Cancellation is idempotent.

Return `200 OK` with the existing canceled delivery.

### in_flight

Return `409 Conflict`.

Error code:

```text
delivery_in_flight
```

Do not attempt to abort an already-running outbound HTTP request.

### delivered

Return `409 Conflict`.

Error code:

```text
delivery_already_delivered
```

### dead_letter

Return `409 Conflict`.

Error code:

```text
delivery_dead_lettered
```

### unknown/malformed id

Return the same `404 delivery_not_found` behavior as the existing GET endpoint.

A successful cancellation must be durable and must prevent future automatic delivery.

## Dead-letter query API

Add:

```text
GET /v1/dead-letters
```

Query parameters:

```text
limit
```

Rules:

- default `limit=50`;
- minimum `1`;
- maximum `100`;
- invalid values return `400`.

Return newest dead-lettered deliveries first.

Response:

```json
{
  "items": [
    { "...delivery view..." }
  ]
}
```

Only `dead_letter` deliveries appear.

## Manual requeue

Add:

```text
POST /v1/dead-letters/{id}/requeue
```

For a `dead_letter` delivery:

- transition to `pending`;
- reset `attempts` to `0`;
- clear `last_error`;
- clear `dead_lettered_at`;
- set `next_attempt_at` so it is immediately eligible;
- return `200 OK` with the updated delivery.

For any existing delivery not currently in `dead_letter`:

- return `409 Conflict`;
- error code: `delivery_not_dead_lettered`.

Unknown/malformed ID:

- return `404 delivery_not_found`.

Concurrent requeue requests must not create duplicate delivery rows.

## Public representation

Expose these fields when applicable:

```json
{
  "dead_lettered_at": "RFC3339 timestamp or null",
  "canceled_at": "RFC3339 timestamp or null"
}
```

Existing fields remain compatible.

## Architecture

State-transition rules belong in domain/application code.

Handlers must not implement transitions with direct SQL.

Repository operations used for cancel/requeue must be atomic enough that concurrent
worker activity cannot silently violate the transition contract.

## Tests

Add meaningful tests for at least:

- transition to dead-letter exactly at max attempts;
- no automatic sends after dead-letter;
- successful pending cancellation;
- repeated cancellation is idempotent;
- in-flight cancellation conflict;
- delivered/dead-letter cancellation conflicts;
- dead-letter listing filter and ordering;
- limit validation;
- manual requeue resets the required fields;
- requeued delivery can be processed again;
- cancellation/requeue persist across restart;
- relevant worker race cases.

## Acceptance criteria

The spec is complete when:

- retry count is bounded by configuration;
- dead-letter state is durable and not automatically processed;
- cancellation follows the state-specific contract;
- dead-letter listing is correct;
- manual requeue returns a delivery to a valid pending state;
- previous API/worker behavior remains compatible;
- tests assert actual state and HTTP behavior;
- all checks in `AGENTS.md` pass.
