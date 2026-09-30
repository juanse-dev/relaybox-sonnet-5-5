CREATE TABLE deliveries (
    id              TEXT    PRIMARY KEY NOT NULL,
    idempotency_key BLOB    NOT NULL UNIQUE,
    target_url      TEXT    NOT NULL,
    payload         TEXT    NOT NULL,
    status          TEXT    NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    created_at      TEXT    NOT NULL
);
