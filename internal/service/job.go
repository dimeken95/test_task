package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/dimeken95/test_task/internal/domain"
)

var tracer = otel.Tracer("github.com/dimeken95/test_task/service")

type JobService struct {
	repo   domain.JobRepository
	store  domain.ObjectStore
	logger *slog.Logger
}

func NewJobService(repo domain.JobRepository, store domain.ObjectStore, logger *slog.Logger) *JobService {
	if logger == nil {
		logger = slog.Default()
	}
	return &JobService{repo: repo, store: store, logger: logger}
}

// Draft accumulates a job across a multipart stream. The id is allocated up
// front so the file can be streamed to its final object key before the request
// has been fully parsed — that is what lets the handler keep scanning for
// fields that arrive after the file part.
type Draft struct {
	svc       *JobService
	job       *domain.Job
	uploaded  bool
	committed bool
}

func (s *JobService) NewDraft(traceContext string) *Draft {
	return &Draft{
		svc: s,
		job: &domain.Job{
			ID:           uuid.NewString(),
			Status:       domain.StatusPending,
			TraceContext: traceContext,
		},
	}
}

// SetIdempotencyKey ties the draft to a client-supplied Idempotency-Key.
func (d *Draft) SetIdempotencyKey(key string) { d.job.IdempotencyKey = key }

// FindByIdempotencyKey answers a retried submission before anything is
// streamed. Returns ErrNotFound when the key is new.
func (s *JobService) FindByIdempotencyKey(ctx context.Context, key string) (*domain.Job, error) {
	if key == "" {
		return nil, domain.ErrNotFound
	}
	return s.repo.GetByIdempotencyKey(ctx, key)
}

func (d *Draft) ID() string       { return d.job.ID }
func (d *Draft) HasFile() bool    { return d.uploaded }
func (d *Draft) SetText(t string) { d.job.Text = strings.TrimSpace(t) }

// AttachFile streams body into object storage. It records the byte count the
// store actually wrote, which is the only trustworthy size: multipart parts
// carry no Content-Length.
func (d *Draft) AttachFile(ctx context.Context, fileName, contentType string, sizeHint int64, body io.Reader) error {
	if d.uploaded {
		return fmt.Errorf("%w: only one file part is supported", domain.ErrInvalidInput)
	}

	ctx, span := tracer.Start(ctx, "JobService.AttachFile")
	defer span.End()

	key := path.Join("jobs", d.job.ID, sanitizeFileName(fileName))
	span.SetAttributes(
		attribute.String("job.id", d.job.ID),
		attribute.String("object.key", key),
		attribute.String("content.type", contentType),
	)

	written, err := d.svc.store.Put(ctx, key, body, sizeHint, contentType)
	if err != nil {
		// The store aborts its own multipart upload; a failed PutObject leaves
		// nothing behind. Delete defensively in case a partial object exists.
		d.svc.deleteQuietly(ctx, key)
		return fmt.Errorf("upload object: %w", err)
	}

	d.uploaded = true
	d.job.ObjectKey = key
	d.job.FileName = fileName
	d.job.ContentType = contentType
	d.job.SizeBytes = written
	span.SetAttributes(attribute.Int64("size.bytes", written))
	return nil
}

// Commit persists the job. Object storage is written first, so a DB failure
// leaves an orphan that we remove on a best-effort basis.
//
// The second return value reports that an existing job was returned instead of
// a new one, because the Idempotency-Key had already been used. That case is
// decided by the unique index rather than a prior read, so two concurrent
// retries of the same submission cannot both create a job.
func (d *Draft) Commit(ctx context.Context) (*domain.Job, bool, error) {
	ctx, span := tracer.Start(ctx, "JobService.Commit",
		trace.WithAttributes(attribute.String("job.id", d.job.ID)))
	defer span.End()

	if d.job.Text == "" && !d.uploaded {
		return nil, false, domain.ErrNoPayload
	}

	err := d.svc.repo.Create(ctx, d.job)
	switch {
	case err == nil:
		d.committed = true
		return d.job, false, nil

	case errors.Is(err, domain.ErrDuplicateKey):
		// Lost the race, or the client retried after our fast-path lookup.
		// The payload we just uploaded belongs to a job that will never exist.
		if d.uploaded {
			d.svc.deleteQuietly(ctx, d.job.ObjectKey)
		}
		existing, getErr := d.svc.repo.GetByIdempotencyKey(ctx, d.job.IdempotencyKey)
		if getErr != nil {
			return nil, false, fmt.Errorf("resolve idempotent replay: %w", getErr)
		}
		span.SetAttributes(attribute.Bool("job.idempotent_replay", true))
		return existing, true, nil

	default:
		if d.uploaded {
			d.svc.deleteQuietly(ctx, d.job.ObjectKey)
		}
		return nil, false, fmt.Errorf("persist job: %w", err)
	}
}

// Discard removes an uploaded object when the request fails before Commit.
// Safe to call unconditionally via defer.
func (d *Draft) Discard(ctx context.Context) {
	if d.committed || !d.uploaded {
		return
	}
	d.svc.deleteQuietly(ctx, d.job.ObjectKey)
}

func (s *JobService) deleteQuietly(ctx context.Context, key string) {
	// The request context is typically already cancelled on the failure path.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := s.store.Delete(ctx, key); err != nil {
		s.logger.ErrorContext(ctx, "orphan object cleanup failed", "object_key", key, "err", err)
	}
}

func (s *JobService) Get(ctx context.Context, id string) (*domain.Job, error) {
	ctx, span := tracer.Start(ctx, "JobService.Get", trace.WithAttributes(attribute.String("job.id", id)))
	defer span.End()
	return s.repo.Get(ctx, id)
}

func (s *JobService) PingDeps(ctx context.Context) error {
	if err := s.repo.Ping(ctx); err != nil {
		return err
	}
	return s.store.Ping(ctx)
}

// unsafeFileChars keeps object keys predictable and shell/URL friendly.
var unsafeFileChars = regexp.MustCompile(`[^\w.\-]+`)

func sanitizeFileName(name string) string {
	name = path.Base(strings.ReplaceAll(name, `\`, "/"))
	name = unsafeFileChars.ReplaceAllString(name, "_")
	name = strings.Trim(name, ".")
	if len(name) > 128 {
		name = name[len(name)-128:]
	}
	if name == "" {
		return fmt.Sprintf("file-%d", time.Now().UnixNano())
	}
	return name
}
