# AGENTS.md

These instructions apply to the entire repository.

## Role

You are the implementation engineer for Relaybox.

Architecture and product requirements are already defined by `README.md` and the
current specification. Your job is to implement them faithfully, not redesign the
product.

## Before changing code

For every task:

1. Read this file completely.
2. Read `README.md` completely.
3. Read only the specification explicitly named in the task.
4. Inspect the current source, migrations, tests, and `Cargo.toml`.
5. Check the current Git branch and working tree.

Do **not** inspect future specifications unless the task explicitly asks you to do so.

## Scope discipline

Implement only the current specification.

Do not:

- implement requirements from later specs;
- perform unrelated refactors;
- rename public APIs without a requirement;
- replace established dependencies or architecture without necessity;
- add speculative abstractions for hypothetical future work;
- modify `README.md`, `AGENTS.md`, or specification files unless the current task
  explicitly requires documentation changes.

If the current specification conflicts with `README.md` or these instructions, stop and
explain the conflict instead of silently choosing one.

If a requirement is genuinely ambiguous and materially affects behavior, ask rather
than inventing a product decision.

## Architecture invariants

These rules are mandatory:

- `domain/` must not depend on Axum, SQLx, or Reqwest.
- HTTP handlers must not access SQLite/SQLx directly.
- SQL belongs in infrastructure.
- Outbound HTTP belongs in infrastructure behind an application-facing port.
- Business/state-transition decisions belong in domain/application code, not handlers.
- `main.rs` is a composition root, not a business-logic module.
- SQLite migrations are required for schema changes.
- Durable behavior must use persistent state, not only in-memory state.
- Do not use `unsafe` unless the specification makes it unavoidable and the PR explains
  why. The benchmark is designed so `unsafe` should not be necessary.

## Implementation style

Use stable Rust and idiomatic ownership/error handling.

Prefer:

- small cohesive modules;
- explicit domain/application errors;
- dependency injection through ordinary Rust types/traits where it improves testability;
- established crates already present in the repository.

Avoid:

- unnecessary generic frameworks;
- service-locator/global mutable state;
- panics for expected runtime errors;
- `.unwrap()` / `.expect()` in production paths where failure is possible and should be
  handled;
- copying large payloads unnecessarily;
- blocking filesystem/network/database work on async executor threads.

Adding a dependency is allowed when justified by the current spec. Keep additions
minimal.

## Tests

Every behavioral change requires tests.

Tests must assert the behavior they claim to test.

Include regression tests for bugs fixed during Codex review.

Where relevant, cover:

- happy path;
- invalid input;
- persistence/restart behavior;
- state transitions;
- concurrency/races;
- failure/retry behavior.

Do not weaken, delete, or bypass an existing test merely to make the suite pass unless
the current specification explicitly changes that behavior.

## Required checks

Before declaring implementation complete, run:

```text
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

If the repository defines additional CI checks, run the relevant ones as well.

Fix failures before opening or updating the pull request.

## Git workflow for each specification

The benchmark task will start from a clean repository state.

For the current specification:

1. Create a focused branch if the task has not already placed you on one.
   Use a descriptive name beginning with `spec/`, for example:
   `spec/001-enqueue`.
2. Implement only the current spec.
3. Run all required checks.
4. Commit all intended source/test/migration changes.
5. Push the branch.
6. Open a GitHub pull request against `main` using `gh`.
7. The PR title must begin with the spec number, for example:
   `001: durable enqueue API`.
8. The PR body must briefly include:
   - what changed;
   - important implementation decisions;
   - tests/checks run.
9. Add a PR comment containing exactly:

```text
@codex review
```

10. Stop. Do not poll GitHub. The benchmark runner will resume the same session when
    the review is ready.

Do not merge your own PR unless the benchmark task explicitly instructs you to do so.

## Responding to Codex review

When the benchmark runner tells you a Codex review is ready:

1. Use `gh` to read the complete latest review, including inline comments.
2. Evaluate every actionable finding against the current spec, README, and this file.
3. Fix every valid finding with the smallest correct change.
4. Add or update regression tests where appropriate.
5. If a finding is not valid, do not blindly change the code; reply with a concise
   technical explanation.
6. Run all required checks.
7. Commit and push the correction.
8. Reply to relevant review threads/comments when useful.
9. Add a new PR comment containing exactly:

```text
@codex review
```

10. Stop again. Do not poll GitHub.

Repeat until the benchmark runner reports that the review is clean.

## Repository hygiene

Do not commit:

- benchmark-runner output;
- local databases;
- editor/IDE state;
- temporary test files;
- build artifacts;
- copied source archives;
- secrets or tokens.

Keep the working tree clean when you finish a round.
