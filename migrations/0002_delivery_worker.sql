ALTER TABLE deliveries ADD COLUMN next_attempt_at  TEXT;
ALTER TABLE deliveries ADD COLUMN last_error       TEXT;
ALTER TABLE deliveries ADD COLUMN delivered_at     TEXT;
ALTER TABLE deliveries ADD COLUMN lease_token      TEXT;
ALTER TABLE deliveries ADD COLUMN lease_expires_at TEXT;

CREATE INDEX idx_deliveries_claimable
    ON deliveries (status, next_attempt_at, lease_expires_at);
