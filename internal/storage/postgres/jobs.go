package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dimeken95/test_task/internal/domain"
)

// jobColumns is the single source of truth for the SELECT/RETURNING projection
// consumed by scanJob.
const jobColumns = `id, status, text_content, object_key, content_type, file_name,
	size_bytes, result_summary, error_message, attempts, trace_context,
	idempotency_key, locked_by, lease_until, next_attempt_at,
	created_at, updated_at, completed_at`

// uniqueViolation is the SQLSTATE Postgres raises when a unique index rejects
// a row; for this schema that only ever means a duplicate Idempotency-Key.
const uniqueViolation = "23505"

type JobRepo struct {
	pool *pgxpool.Pool
}

func NewPool(ctx context.Context, databaseURL string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

func NewJobRepo(pool *pgxpool.Pool) *JobRepo {
	return &JobRepo{pool: pool}
}

func (r *JobRepo) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

func (r *JobRepo) Create(ctx context.Context, job *domain.Job) error {
	if job.ID == "" {
		job.ID = uuid.NewString()
	}
	now := time.Now().UTC()
	job.CreatedAt = now
	job.UpdatedAt = now
	if job.Status == "" {
		job.Status = domain.StatusPending
	}

	_, err := r.pool.Exec(ctx, `
		INSERT INTO jobs (
			id, status, text_content, object_key, content_type, file_name,
			size_bytes, result_summary, error_message, attempts, trace_context,
			idempotency_key, locked_by, lease_until, next_attempt_at,
			created_at, updated_at, completed_at
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18
		)`,
		job.ID, job.Status, job.Text, job.ObjectKey, job.ContentType, job.FileName,
		job.SizeBytes, job.ResultSummary, job.ErrorMessage, job.Attempts, job.TraceContext,
		job.IdempotencyKey, job.LockedBy, job.LeaseUntil, job.NextAttemptAt,
		job.CreatedAt, job.UpdatedAt, job.CompletedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			return domain.ErrDuplicateKey
		}
		return fmt.Errorf("insert job: %w", err)
	}
	return nil
}

// GetByIdempotencyKey is the fast path: it lets a retried submission be
// answered before the payload is streamed anywhere. The unique index is what
// makes it correct under a race; this is only an optimisation.
func (r *JobRepo) GetByIdempotencyKey(ctx context.Context, key string) (*domain.Job, error) {
	if key == "" {
		return nil, domain.ErrNotFound
	}
	row := r.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE idempotency_key = $1`, key)

	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job by idempotency key: %w", err)
	}
	return job, nil
}

func (r *JobRepo) Get(ctx context.Context, id string) (*domain.Job, error) {
	if _, err := uuid.Parse(id); err != nil {
		// Reject malformed ids before they reach Postgres, which would otherwise
		// raise a type error instead of a clean 404.
		return nil, domain.ErrNotFound
	}
	row := r.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id)

	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	return job, nil
}

// ClaimBatch takes up to limit due jobs in a single round trip. The inner
// SELECT ... FOR UPDATE SKIP LOCKED lets every worker replica claim disjoint
// rows without blocking each other.
func (r *JobRepo) ClaimBatch(ctx context.Context, workerID string, lease time.Duration, limit int) ([]*domain.Job, error) {
	leaseUntil := time.Now().UTC().Add(lease)

	rows, err := r.pool.Query(ctx, `
		UPDATE jobs SET
			status          = 'processing',
			attempts        = attempts + 1,
			lease_until     = $1,
			locked_by       = $2,
			next_attempt_at = NULL,
			updated_at      = NOW()
		WHERE id IN (
			SELECT id FROM jobs
			WHERE status = 'pending'
			  AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
			ORDER BY created_at
			FOR UPDATE SKIP LOCKED
			LIMIT $3
		)
		RETURNING `+jobColumns, leaseUntil, workerID, limit)
	if err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*domain.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan claimed: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim rows: %w", err)
	}
	return jobs, nil
}

func (r *JobRepo) ExtendLease(ctx context.Context, id string, lease time.Duration) error {
	leaseUntil := time.Now().UTC().Add(lease)
	tag, err := r.pool.Exec(ctx, `
		UPDATE jobs
		SET lease_until = $2,
		    updated_at = NOW()
		WHERE id = $1 AND status = 'processing'`, id, leaseUntil)
	if err != nil {
		return fmt.Errorf("extend lease: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	return nil
}

func (r *JobRepo) Complete(ctx context.Context, id, summary string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'completed',
		    result_summary = $2,
		    error_message = '',
		    lease_until = NULL,
		    locked_by = '',
		    completed_at = NOW(),
		    updated_at = NOW()
		WHERE id = $1 AND status = 'processing'`, id, summary)
	if err != nil {
		return fmt.Errorf("complete job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	return nil
}

// Retry either reschedules the job with exponential backoff or, once attempts
// are exhausted, moves it to failed. Both outcomes are one statement so a
// crashing worker can never leave the row half-updated.
func (r *JobRepo) Retry(ctx context.Context, id, message string, maxAttempts int, base, max time.Duration) (bool, error) {
	row := r.pool.QueryRow(ctx, `
		UPDATE jobs SET
			status = CASE WHEN attempts >= $3 THEN 'failed' ELSE 'pending' END,
			error_message = $2,
			lease_until = NULL,
			locked_by = '',
			next_attempt_at = CASE
				WHEN attempts >= $3 THEN NULL
				ELSE NOW() + make_interval(secs =>
					LEAST($4 * POWER(2, GREATEST(attempts - 1, 0)), $5))
			END,
			completed_at = CASE WHEN attempts >= $3 THEN NOW() ELSE NULL END,
			updated_at = NOW()
		WHERE id = $1 AND status = 'processing'
		RETURNING status`,
		id, message, maxAttempts, base.Seconds(), max.Seconds())

	var status string
	if err := row.Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, domain.ErrNotFound
		}
		return false, fmt.Errorf("retry job: %w", err)
	}
	return status == string(domain.StatusPending), nil
}

func (r *JobRepo) Fail(ctx context.Context, id, message string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'failed',
		    error_message = $2,
		    lease_until = NULL,
		    locked_by = '',
		    next_attempt_at = NULL,
		    completed_at = NOW(),
		    updated_at = NOW()
		WHERE id = $1 AND status = 'processing'`, id, message)
	if err != nil {
		return fmt.Errorf("fail job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	return nil
}

// ReapExpired returns jobs whose worker died mid-flight back to the queue.
// Attempts are already incremented by the claim, so a pod that keeps crashing
// on the same payload still hits MAX_ATTEMPTS instead of looping forever.
func (r *JobRepo) ReapExpired(ctx context.Context) (int64, error) {
	tag, err := r.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'pending',
		    lease_until = NULL,
		    locked_by = '',
		    error_message = 'lease expired, requeued',
		    updated_at = NOW()
		WHERE status = 'processing'
		  AND lease_until IS NOT NULL
		  AND lease_until < NOW()`)
	if err != nil {
		return 0, fmt.Errorf("reap expired: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (r *JobRepo) CountByStatus(ctx context.Context, status domain.Status) (int64, error) {
	var n int64
	err := r.pool.QueryRow(ctx, `SELECT COUNT(*) FROM jobs WHERE status = $1`, status).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count by status: %w", err)
	}
	return n, nil
}

type scannable interface {
	Scan(dest ...any) error
}

func scanJob(row scannable) (*domain.Job, error) {
	var j domain.Job
	var status string
	err := row.Scan(
		&j.ID, &status, &j.Text, &j.ObjectKey, &j.ContentType, &j.FileName,
		&j.SizeBytes, &j.ResultSummary, &j.ErrorMessage, &j.Attempts, &j.TraceContext,
		&j.IdempotencyKey, &j.LockedBy, &j.LeaseUntil, &j.NextAttemptAt,
		&j.CreatedAt, &j.UpdatedAt, &j.CompletedAt,
	)
	if err != nil {
		return nil, err
	}
	j.Status = domain.Status(status)
	return &j, nil
}
