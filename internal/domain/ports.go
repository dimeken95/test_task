package domain

import (
	"context"
	"io"
	"time"
)

type JobRepository interface {
	// Create returns ErrDuplicateKey when the job's IdempotencyKey is already
	// taken, which is how a retried submission is detected.
	Create(ctx context.Context, job *Job) error
	Get(ctx context.Context, id string) (*Job, error)
	GetByIdempotencyKey(ctx context.Context, key string) (*Job, error)

	// ClaimBatch atomically moves up to limit due pending jobs to processing
	// and stamps them with workerID and a lease deadline.
	ClaimBatch(ctx context.Context, workerID string, lease time.Duration, limit int) ([]*Job, error)
	ExtendLease(ctx context.Context, id string, lease time.Duration) error
	Complete(ctx context.Context, id, summary string) error

	// Retry reschedules a job for a later attempt with exponential backoff.
	// It returns false once attempts reach maxAttempts, in which case the job
	// is moved to failed instead.
	Retry(ctx context.Context, id, message string, maxAttempts int, base, max time.Duration) (bool, error)
	Fail(ctx context.Context, id, message string) error

	ReapExpired(ctx context.Context) (int64, error)
	CountByStatus(ctx context.Context, status Status) (int64, error)
	Ping(ctx context.Context) error
}

type ObjectStore interface {
	// Put streams body to key and returns the number of bytes actually written.
	// size is a hint: <= 0 means unknown, which is the normal case for
	// multipart/form-data ingress.
	Put(ctx context.Context, key string, body io.Reader, size int64, contentType string) (int64, error)
	Delete(ctx context.Context, key string) error
	PresignGet(ctx context.Context, key string, expiry time.Duration) (string, error)
	Ping(ctx context.Context) error
}

type ProcessInput struct {
	JobID        string
	Text         string
	ContentType  string
	SizeBytes    int64
	DownloadURL  string
	TraceContext string
}

type ProcessResult struct {
	Summary     string
	ProcessedAt time.Time
}

type Processor interface {
	Process(ctx context.Context, in ProcessInput) (ProcessResult, error)
}
