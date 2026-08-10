package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/dimeken95/test_task/internal/config"
	"github.com/dimeken95/test_task/internal/domain"
	"github.com/dimeken95/test_task/internal/observability"
)

var tracer = otel.Tracer("github.com/dimeken95/test_task/worker")

type Worker struct {
	cfg       config.Config
	repo      domain.JobRepository
	store     domain.ObjectStore
	processor domain.Processor
	logger    *slog.Logger

	jobsCh   chan *domain.Job
	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

func New(cfg config.Config, repo domain.JobRepository, store domain.ObjectStore, proc domain.Processor, logger *slog.Logger) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{
		cfg:       cfg,
		repo:      repo,
		store:     store,
		processor: proc,
		logger:    logger,
		jobsCh:    make(chan *domain.Job, cfg.WorkerConcurrency),
		stopCh:    make(chan struct{}),
	}
}

func (w *Worker) Start(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.pollerLoop(ctx)
	}()

	for i := range w.cfg.WorkerConcurrency {
		w.wg.Add(1)
		go func(id int) {
			defer w.wg.Done()
			w.workerLoop(ctx, id)
		}(i)
	}

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.reaperLoop(ctx)
	}()
}

// Stop signals the poller to stop claiming and waits for in-flight jobs.
// Queued-but-unstarted jobs are drained back to pending so they are picked up
// immediately by another replica instead of waiting out their lease.
//
// ctx bounds that final drain and must still be live: the caller's request
// context is already cancelled by the time shutdown runs.
func (w *Worker) Stop(ctx context.Context) {
	w.stopOnce.Do(func() { close(w.stopCh) })
	w.wg.Wait()
	w.releaseQueued(ctx)
}

func (w *Worker) releaseQueued(ctx context.Context) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	for {
		select {
		case job := <-w.jobsCh:
			if _, err := w.repo.Retry(ctx, job.ID, "worker shutting down", w.cfg.MaxAttempts, 0, 0); err != nil {
				w.logger.WarnContext(ctx, "release queued job failed", "job_id", job.ID, "err", err)
			}
		default:
			return
		}
	}
}

func (w *Worker) pollerLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.WorkerPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.pollOnce(ctx)
		}
	}
}

// pollOnce claims in batches sized to the free capacity of the queue, so a
// single round trip feeds all idle workers instead of one job per tick.
func (w *Worker) pollOnce(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		default:
		}

		free := cap(w.jobsCh) - len(w.jobsCh)
		if free <= 0 {
			return
		}
		if free > w.cfg.WorkerClaimBatch {
			free = w.cfg.WorkerClaimBatch
		}

		jobs, err := w.repo.ClaimBatch(ctx, w.cfg.WorkerID, w.cfg.WorkerLease, free)
		if err != nil {
			if ctx.Err() == nil {
				w.logger.ErrorContext(ctx, "claim failed", "err", err)
			}
			return
		}
		if len(jobs) == 0 {
			return
		}
		observability.JobsClaimed.Add(float64(len(jobs)))

		for _, job := range jobs {
			select {
			case <-ctx.Done():
				return
			case w.jobsCh <- job:
			}
		}
	}
}

func (w *Worker) workerLoop(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		case job := <-w.jobsCh:
			if err := w.process(ctx, job); err != nil {
				w.logger.ErrorContext(ctx, "process failed", "worker", id, "job_id", job.ID, "err", err)
			}
		}
	}
}

func (w *Worker) reaperLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.ReaperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		case <-ticker.C:
			n, err := w.repo.ReapExpired(ctx)
			if err != nil {
				if ctx.Err() == nil {
					w.logger.ErrorContext(ctx, "reaper failed", "err", err)
				}
				continue
			}
			if n > 0 {
				observability.JobsReaped.Add(float64(n))
				w.logger.InfoContext(ctx, "reaped expired leases", "count", n)
			}
		}
	}
}

func (w *Worker) process(parent context.Context, job *domain.Job) error {
	ctx := restoreTrace(parent, job.TraceContext)
	ctx, span := tracer.Start(ctx, "Worker.ProcessJob",
		trace.WithAttributes(
			attribute.String("job.id", job.ID),
			attribute.Int("job.attempt", job.Attempts),
		),
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	defer span.End()

	// Job outcome must be recorded even when the pod is shutting down,
	// otherwise the row sits in `processing` until the lease expires.
	finalCtx, cancelFinal := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancelFinal()

	hbCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()
	go w.heartbeat(hbCtx, job.ID)

	start := time.Now()
	w.logger.InfoContext(ctx, "processing job", "job_id", job.ID, "attempt", job.Attempts)

	res, err := w.runJob(ctx, job)
	stopHeartbeat()

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		observability.ObserveJobDuration("failed", time.Since(start))
		observability.ProcessorErrors.Inc()
		return w.recordFailure(finalCtx, job, err)
	}

	if err := w.repo.Complete(finalCtx, job.ID, res.Summary); err != nil {
		observability.ObserveJobDuration("failed", time.Since(start))
		return fmt.Errorf("complete job: %w", err)
	}
	observability.ObserveJobDuration("completed", time.Since(start))
	w.logger.InfoContext(ctx, "job completed", "job_id", job.ID, "duration_ms", time.Since(start).Milliseconds())
	return nil
}

func (w *Worker) runJob(ctx context.Context, job *domain.Job) (domain.ProcessResult, error) {
	var downloadURL string
	if job.ObjectKey != "" {
		// The processor is an external service: it gets a short-lived presigned
		// URL rather than the bytes or any credentials of ours.
		url, err := w.store.PresignGet(ctx, job.ObjectKey, w.cfg.S3PresignTTL)
		if err != nil {
			return domain.ProcessResult{}, fmt.Errorf("presign object: %w", err)
		}
		downloadURL = url
	}

	return w.processor.Process(ctx, domain.ProcessInput{
		JobID:        job.ID,
		Text:         job.Text,
		ContentType:  job.ContentType,
		SizeBytes:    job.SizeBytes,
		DownloadURL:  downloadURL,
		TraceContext: job.TraceContext,
	})
}

// recordFailure retries transient faults with backoff and gives up on
// permanent ones. Attempts are already counted by the claim, so a job cannot
// loop forever even if the worker keeps crashing.
func (w *Worker) recordFailure(ctx context.Context, job *domain.Job, cause error) error {
	if errors.Is(cause, domain.ErrPermanent) {
		if err := w.repo.Fail(ctx, job.ID, cause.Error()); err != nil {
			return fmt.Errorf("mark failed (%w): %w", cause, err)
		}
		w.logger.WarnContext(ctx, "job failed permanently", "job_id", job.ID, "err", cause)
		return cause
	}

	rescheduled, err := w.repo.Retry(ctx, job.ID, cause.Error(),
		w.cfg.MaxAttempts, w.cfg.RetryBackoffBase, w.cfg.RetryBackoffMax)
	if err != nil {
		return fmt.Errorf("reschedule (%w): %w", cause, err)
	}
	if rescheduled {
		observability.JobsRetried.Inc()
		w.logger.WarnContext(ctx, "job rescheduled", "job_id", job.ID, "attempt", job.Attempts, "err", cause)
	} else {
		w.logger.WarnContext(ctx, "job failed, attempts exhausted", "job_id", job.ID, "attempts", job.Attempts, "err", cause)
	}
	return cause
}

func (w *Worker) heartbeat(ctx context.Context, jobID string) {
	// Renew at half the lease so a slow video job is never reaped from under us.
	// The floor only guards against a pathologically small lease; with the
	// default 2m lease this is a 60s heartbeat.
	interval := max(w.cfg.WorkerLease/2, 100*time.Millisecond)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := w.repo.ExtendLease(ctx, jobID, w.cfg.WorkerLease); err != nil {
				w.logger.WarnContext(ctx, "extend lease failed", "job_id", jobID, "err", err)
			}
		}
	}
}

// restoreTrace rebuilds the span context captured at ingress so the async
// processing shows up in the same distributed trace as the HTTP request.
func restoreTrace(ctx context.Context, stored string) context.Context {
	if stored == "" {
		return ctx
	}
	carrier := propagation.MapCarrier{}
	for _, pair := range strings.Split(stored, ";") {
		if k, v, ok := strings.Cut(pair, "="); ok {
			carrier[k] = v
		}
	}
	return propagation.TraceContext{}.Extract(ctx, carrier)
}
