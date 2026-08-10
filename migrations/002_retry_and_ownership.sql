-- +goose Up
-- Retry scheduling and claim ownership.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS locked_by       TEXT NOT NULL DEFAULT '';
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;

-- Claim reads only pending rows; a partial index keeps it small and hot
-- regardless of how many completed rows accumulate.
DROP INDEX IF EXISTS idx_jobs_status_created;
CREATE INDEX IF NOT EXISTS idx_jobs_pending_claim ON jobs (created_at) WHERE status = 'pending';

-- +goose Down
DROP INDEX IF EXISTS idx_jobs_pending_claim;
CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs (status, created_at);
ALTER TABLE jobs DROP COLUMN IF EXISTS next_attempt_at;
ALTER TABLE jobs DROP COLUMN IF EXISTS locked_by;
