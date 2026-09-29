# Relaybox

Relaybox is a small durable webhook-delivery service used as an engineering benchmark.

It accepts delivery requests over HTTP, stores them durably in SQLite, and—through
incremental specifications under `specs/`—evolves into a reliable background delivery
system.

The repository is intentionally small, but it is expected to be treated like production
software: clear boundaries, durable state, explicit error handling, deterministic tests,
and minimal unrelated change.

## Technology

Use stable Rust.

The intended stack is:

- Tokio for async runtime
- Axum for HTTP
- SQLite with SQLx for persistence and migrations
- Reqwest for outbound HTTP
- Serde / serde_json for serialization
- tracing / tracing-subscriber for structured logging
- thiserror for library/domain errors
- anyhow may be used only at process/bootstrap boundaries
- uuid for delivery identifiers
- url for URL parsing and validation

Additional mature crates are allowed when a current specification genuinely requires
them. Do not add dependencies merely for convenience when the standard library or an
existing dependency is sufficient.

## Architecture

Use the following dependency direction:

```text
api  ───────────────┐
                    ▼
              application
               ▲        ▲
               │        │
            domain   application ports
                         ▲
                         │
                 infrastructure

main/config → composition only
```

The physical layout should remain recognizably close to:

```text
src/
├── api/
│   ├── mod.rs
│   ├── handlers.rs
│   └── routes.rs
├── application/
│   ├── mod.rs
│   ├── ports.rs
│   └── ...
├── domain/
│   ├── mod.rs
│   ├── delivery.rs
│   └── ...
├── infrastructure/
│   ├── mod.rs
│   ├── sqlite.rs
│   ├── http.rs
│   └── ...
├── config.rs
├── lib.rs
└── main.rs

migrations/
tests/
specs/
```

The exact file split may evolve when justified, but the architectural boundaries below
are invariants.

### Domain

`domain/` contains business concepts, state, and invariants.

It must not depend on:

- Axum
- SQLx
- Reqwest
- transport-specific HTTP request/response types

Domain types should not know how they are stored or transported.

### Application

`application/` owns use cases and orchestration.

Ports needed by application logic—repositories, outbound delivery transport, clocks,
or similar interfaces—belong here or in a closely related application module.

Application code may depend on `domain/`, but must not contain SQL queries or Axum
handlers.

### Infrastructure

`infrastructure/` implements application ports.

This is where SQLite/SQLx and Reqwest-specific behavior belongs.

Infrastructure may depend on application/domain contracts. Domain must never depend on
infrastructure.

### API

`api/` is an HTTP adapter.

Handlers are responsible for:

- extracting and validating transport-level input;
- invoking application use cases;
- mapping application/domain outcomes to HTTP responses.

Handlers must not issue SQL queries directly and must not implement delivery-worker
business logic.

### Composition root

`main.rs` and configuration/bootstrap code wire concrete infrastructure into the
application.

Do not hide business behavior in `main.rs`.

## Persistence

SQLite is the source of truth for delivery state.

All schema changes must be represented by SQLx migrations under `migrations/`.

Tests must use isolated databases. They must not depend on a developer's existing
database file or on test execution order.

Durability matters: behavior that is specified as persistent must survive process
restart.

## Delivery semantics

Relaybox provides **at-least-once delivery**, not exactly-once delivery.

Once background delivery exists, a process can fail after an external server has
accepted a webhook but before Relaybox durably records the success. A later retry is
therefore allowed to deliver the same logical delivery again.

Do not claim exactly-once semantics.

## HTTP and JSON conventions

Unless a specification says otherwise:

- request and response bodies use JSON;
- success responses use JSON when a resource/state is returned;
- errors use this shape:

```json
{
  "error": {
    "code": "machine_readable_code",
    "message": "Human-readable explanation"
  }
}
```

- `Content-Type: application/json` should be used for JSON responses;
- timestamps are UTC RFC 3339 strings;
- delivery IDs are UUID strings.

Do not expose internal SQL errors, filesystem paths, or Rust debug representations to
API clients.

## Configuration

Configuration is read from environment variables at startup.

Stable names used by the project:

```text
RELAYBOX_DATABASE_URL
RELAYBOX_BIND
```

Specifications may add more variables later.

Defaults:

```text
RELAYBOX_DATABASE_URL=sqlite://relaybox.db
RELAYBOX_BIND=127.0.0.1:3000
```

Invalid configuration must fail startup with a useful error.

## Testing philosophy

Prefer behavior tests over implementation tests.

Tests should prove observable behavior: HTTP contracts, persistence, state transitions,
retry scheduling, and architectural invariants where practical.

A test named after a behavior must assert that behavior; merely asserting exit status or
"no panic" is not sufficient.

Tests should be deterministic. Avoid arbitrary sleeps when a controllable dependency,
short configured duration, or explicit polling with a deadline is more reliable.

## Logging

Use `tracing`.

Logs are diagnostic and are not part of the public API. Never require exact log strings
for functional tests.

## Specifications

The product is implemented incrementally.

Each benchmark task names exactly one specification under `specs/`. Implement only the
named specification on top of the current repository state.

Future specifications may be present in the repository. They are not permission to
implement future behavior early.

`AGENTS.md` defines the required engineering and pull-request workflow.
