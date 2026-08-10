-- +goose Up
-- Client-supplied Idempotency-Key. A client that retries a submission after a
-- timeout must not create a second job for the same payload.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS idempotency_key TEXT NOT NULL DEFAULT '';

-- Partial unique index: the empty string means "no key supplied", and those
-- rows must stay unconstrained. The index is what makes the check race-free —
-- two concurrent requests with the same key cannot both insert.
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency_key
    ON jobs (idempotency_key) WHERE idempotency_key <> '';

-- +goose Down
DROP INDEX IF EXISTS idx_jobs_idempotency_key;
ALTER TABLE jobs DROP COLUMN IF EXISTS idempotency_key;
