# Spec 004 — Retry-After and per-origin concurrency

## Goal

Adapt the existing worker to two production constraints:

1. remote services may explicitly tell Relaybox how long to wait before retrying;
2. one remote origin must not consume all available worker concurrency.

This is intentionally a change to an existing worker. Preserve the architecture and
public behavior from earlier specifications unless this spec changes it.

## Retry-After

When an outbound response is `429 Too Many Requests` or
`503 Service Unavailable`, inspect the `Retry-After` response header.

For this specification support the standard **delta-seconds** form:

```text
Retry-After: 12
```

The value:

- is a base-10 non-negative integer number of seconds;
- may be zero;
- must fit in an unsigned 32-bit integer.

HTTP-date form is not required by this specification.

Malformed, negative, overflowing, or otherwise invalid `Retry-After` values are ignored.

### Scheduling rule

A 429/503 still counts as a failed outbound attempt.

Calculate the ordinary exponential-backoff retry time from Spec 002.

If a valid `Retry-After` is present, choose the later of:

```text
failure_time + exponential_backoff
failure_time + Retry-After
```

Persist that timestamp in `next_attempt_at`.

Examples:

```text
attempt 1 backoff = 1s, Retry-After: 10  -> retry in 10s
attempt 4 backoff = 8s, Retry-After: 2   -> retry in 8s
```

For statuses other than 429/503, ignore `Retry-After`.

If the failed attempt reaches `RELAYBOX_MAX_ATTEMPTS`, the delivery becomes
`dead_letter` as defined by Spec 003; `Retry-After` does not override dead-lettering.

## Per-origin concurrency

Add:

```text
RELAYBOX_PER_ORIGIN_CONCURRENCY
```

Default:

```text
RELAYBOX_PER_ORIGIN_CONCURRENCY=2
```

It must be greater than zero.

An **origin** is identified by normalized:

```text
scheme + host + effective port
```

Examples:

```text
https://example.com/a
https://example.com/b
```

are the same origin.

```text
https://example.com
http://example.com
```

are different origins.

```text
https://example.com
https://example.com:443
```

are the same origin.

The per-origin limit applies to active outbound HTTP requests within one Relaybox
process.

It does not need to coordinate a per-origin semaphore across multiple separate Relaybox
processes.

The existing `RELAYBOX_WORKER_CONCURRENCY` remains the global maximum active outbound
request count.

Therefore at all times in one process:

```text
active_requests_total <= RELAYBOX_WORKER_CONCURRENCY
```

and for every origin:

```text
active_requests_for_origin <= RELAYBOX_PER_ORIGIN_CONCURRENCY
```

## Scheduling / starvation requirement

Deliveries waiting for an origin-specific permit must not monopolize all global worker
capacity.

Example:

```text
global concurrency = 4
per-origin concurrency = 1

100 due deliveries -> https://slow.example/...
1 due delivery     -> https://fast.example/...
```

While one request to `slow.example` is active, the other slow deliveries must not fill
the remaining three global execution slots merely waiting for the same origin permit.
The `fast.example` delivery must remain able to run.

No strict fairness algorithm is required, but the implementation must avoid this
head-of-line/starvation behavior.

## URL/origin behavior

Use the already validated target URL.

Origin-key calculation belongs outside HTTP handlers.

Do not alter the target URL sent to the remote service.

## Tests

Add deterministic tests for at least:

- valid `Retry-After` on 429;
- valid `Retry-After` on 503;
- smaller Retry-After does not shorten exponential backoff;
- invalid Retry-After is ignored;
- Retry-After on an unrelated status is ignored;
- dead-letter at max attempts wins over Retry-After;
- global concurrency still holds;
- one origin never exceeds the configured per-origin concurrency;
- different origins can make progress concurrently;
- a saturated origin does not starve another origin;
- default-port normalization (`https://x` and `https://x:443`) shares a limit.

Avoid timing-fragile tests. Prefer controlled test servers, barriers/channels, and
bounded deadlines.

## Architecture

Do not rewrite the worker wholesale unless necessary.

Preserve:

- durable claim/lease semantics;
- existing repository abstraction;
- outbound transport abstraction;
- state-transition ownership in domain/application layers.

Per-origin scheduling is application behavior. Reqwest remains an infrastructure
detail.

## Acceptance criteria

The spec is complete when:

- Retry-After affects only the specified responses and is durably scheduled;
- dead-letter behavior remains correct;
- global and per-origin concurrency limits are both enforced;
- saturated origins do not monopolize worker capacity;
- previous specs remain compatible;
- tests exercise the concurrency behavior rather than merely checking completion;
- all checks in `AGENTS.md` pass.
