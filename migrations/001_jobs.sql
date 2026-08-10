-- +goose Up
CREATE TABLE IF NOT EXISTS jobs (
    id              UUID PRIMARY KEY,
    status          TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    text_content    TEXT NOT NULL DEFAULT '',
    object_key      TEXT NOT NULL DEFAULT '',
    content_type    TEXT NOT NULL DEFAULT '',
    file_name       TEXT NOT NULL DEFAULT '',
    size_bytes      BIGINT NOT NULL DEFAULT 0,
    result_summary  TEXT NOT NULL DEFAULT '',
    error_message   TEXT NOT NULL DEFAULT '',
    attempts        INT NOT NULL DEFAULT 0,
    trace_context   TEXT NOT NULL DEFAULT '',
    lease_until     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs (status, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_lease ON jobs (status, lease_until) WHERE status = 'processing';

-- +goose Down
DROP TABLE IF EXISTS jobs;
