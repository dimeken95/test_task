package worker_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dimeken95/test_task/internal/config"
	"github.com/dimeken95/test_task/internal/domain"
	"github.com/dimeken95/test_task/internal/worker"
)

type outcome struct {
	completed   []string
	retried     []string
	failed      []string
	maxBatchAsk int
	claimCalls  int
}

type fakeRepo struct {
	mu      sync.Mutex
	pending []*domain.Job
	out     outcome
	leases  atomic.Int32
	// retryReschedules controls whether Retry reports the job as rescheduled.
	retryReschedules bool
}

func (r *fakeRepo) Create(context.Context, *domain.Job) error { return nil }
func (r *fakeRepo) Get(context.Context, string) (*domain.Job, error) {
	return nil, domain.ErrNotFound
}
func (r *fakeRepo) GetByIdempotencyKey(context.Context, string) (*domain.Job, error) {
	return nil, domain.ErrNotFound
}

func (r *fakeRepo) ClaimBatch(_ context.Context, workerID string, _ time.Duration, limit int) ([]*domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.out.claimCalls++
	if limit > r.out.maxBatchAsk {
		r.out.maxBatchAsk = limit
	}
	if len(r.pending) == 0 {
		return nil, nil
	}
	n := min(limit, len(r.pending))
	batch := r.pending[:n]
	r.pending = r.pending[n:]
	for _, j := range batch {
		j.Status = domain.StatusProcessing
		j.LockedBy = workerID
		j.Attempts++
	}
	return batch, nil
}

func (r *fakeRepo) ExtendLease(context.Context, string, time.Duration) error {
	r.leases.Add(1)
	return nil
}

func (r *fakeRepo) Complete(_ context.Context, id, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.out.completed = append(r.out.completed, id)
	return nil
}

func (r *fakeRepo) Retry(_ context.Context, id, _ string, _ int, _, _ time.Duration) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.out.retried = append(r.out.retried, id)
	return r.retryReschedules, nil
}

func (r *fakeRepo) Fail(_ context.Context, id, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.out.failed = append(r.out.failed, id)
	return nil
}

func (r *fakeRepo) ReapExpired(context.Context) (int64, error) { return 0, nil }
func (r *fakeRepo) CountByStatus(context.Context, domain.Status) (int64, error) {
	return 0, nil
}
func (r *fakeRepo) Ping(context.Context) error { return nil }

func (r *fakeRepo) snapshot() outcome {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.out
}

type fakeStore struct{ presignCalls atomic.Int32 }

func (*fakeStore) Put(context.Context, string, io.Reader, int64, string) (int64, error) {
	return 0, nil
}
func (*fakeStore) Delete(context.Context, string) error { return nil }
func (s *fakeStore) PresignGet(_ context.Context, key string, _ time.Duration) (string, error) {
	s.presignCalls.Add(1)
	return "https://storage.test/" + key + "?X-Amz-Signature=abc", nil
}
func (*fakeStore) Ping(context.Context) error { return nil }

type fakeProc struct {
	seen  atomic.Int32
	urls  chan string
	err   error
	delay time.Duration
}

func (p *fakeProc) Process(ctx context.Context, in domain.ProcessInput) (domain.ProcessResult, error) {
	p.seen.Add(1)
	if p.urls != nil {
		select {
		case p.urls <- in.DownloadURL:
		default:
		}
	}
	if p.delay > 0 {
		select {
		case <-time.After(p.delay):
		case <-ctx.Done():
			return domain.ProcessResult{}, ctx.Err()
		}
	}
	if p.err != nil {
		return domain.ProcessResult{}, p.err
	}
	return domain.ProcessResult{Summary: "ok", ProcessedAt: time.Now()}, nil
}

func testCfg() config.Config {
	return config.Config{
		WorkerID:           "worker-test",
		WorkerConcurrency:  3,
		WorkerClaimBatch:   3,
		WorkerPollInterval: 10 * time.Millisecond,
		WorkerLease:        time.Minute,
		ReaperInterval:     time.Hour,
		MaxAttempts:        5,
		RetryBackoffBase:   time.Second,
		RetryBackoffMax:    time.Minute,
		S3PresignTTL:       15 * time.Minute,
	}
}

func jobs(n int) []*domain.Job {
	out := make([]*domain.Job, n)
	for i := range out {
		out[i] = &domain.Job{ID: fmt.Sprintf("job-%d", i), Status: domain.StatusPending, Text: "payload"}
	}
	return out
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", msg)
}

func TestWorkerProcessesClaimedJobs(t *testing.T) {
	repo := &fakeRepo{pending: jobs(6)}
	proc := &fakeProc{}
	w := worker.New(testCfg(), repo, &fakeStore{}, proc, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, func() bool { return len(repo.snapshot().completed) == 6 }, "6 completions")
	cancel()
	w.Stop(context.Background())

	got := repo.snapshot()
	if len(got.completed) != 6 {
		t.Fatalf("completed=%d want 6", len(got.completed))
	}
	if len(got.retried) != 0 || len(got.failed) != 0 {
		t.Fatalf("unexpected failures: %+v", got)
	}
}

// The poller must ask for a batch rather than one job per round trip;
// otherwise throughput is capped by the poll interval.
func TestWorkerClaimsInBatches(t *testing.T) {
	repo := &fakeRepo{pending: jobs(6)}
	w := worker.New(testCfg(), repo, &fakeStore{}, &fakeProc{}, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, func() bool { return len(repo.snapshot().completed) == 6 }, "6 completions")
	cancel()
	w.Stop(context.Background())

	if got := repo.snapshot().maxBatchAsk; got < 2 {
		t.Fatalf("largest batch requested was %d; poller is not batching", got)
	}
}

// A transient processor error must be rescheduled, not buried as failed.
func TestWorkerReschedulesTransientFailure(t *testing.T) {
	repo := &fakeRepo{pending: jobs(1), retryReschedules: true}
	proc := &fakeProc{err: errors.New("processor unreachable")}
	w := worker.New(testCfg(), repo, &fakeStore{}, proc, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, func() bool { return len(repo.snapshot().retried) == 1 }, "retry")
	cancel()
	w.Stop(context.Background())

	got := repo.snapshot()
	if len(got.failed) != 0 {
		t.Fatalf("transient error must not fail the job: %+v", got)
	}
}

// A permanent error (upstream 4xx) must terminate the job immediately instead
// of burning the whole retry budget on a payload that will never be accepted.
func TestWorkerFailsPermanentError(t *testing.T) {
	repo := &fakeRepo{pending: jobs(1)}
	proc := &fakeProc{err: fmt.Errorf("%w: processor rejected job with 400", domain.ErrPermanent)}
	w := worker.New(testCfg(), repo, &fakeStore{}, proc, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, func() bool { return len(repo.snapshot().failed) == 1 }, "permanent failure")
	cancel()
	w.Stop(context.Background())

	if got := repo.snapshot(); len(got.retried) != 0 {
		t.Fatalf("permanent error must not be retried: %+v", got)
	}
}

// The processor receives a presigned URL, never the bytes or our credentials.
func TestWorkerPassesPresignedURL(t *testing.T) {
	repo := &fakeRepo{pending: []*domain.Job{
		{ID: "job-file", Status: domain.StatusPending, ObjectKey: "jobs/job-file/a.png"},
	}}
	proc := &fakeProc{urls: make(chan string, 1)}
	store := &fakeStore{}
	w := worker.New(testCfg(), repo, store, proc, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	var url string
	select {
	case url = <-proc.urls:
	case <-time.After(3 * time.Second):
		t.Fatal("processor was never called")
	}
	cancel()
	w.Stop(context.Background())

	if url == "" || store.presignCalls.Load() == 0 {
		t.Fatalf("expected presigned url, got %q", url)
	}
}

// Text-only jobs have no object, so no presign call should happen.
func TestWorkerSkipsPresignForTextOnly(t *testing.T) {
	repo := &fakeRepo{pending: jobs(1)}
	store := &fakeStore{}
	w := worker.New(testCfg(), repo, store, &fakeProc{}, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, func() bool { return len(repo.snapshot().completed) == 1 }, "completion")
	cancel()
	w.Stop(context.Background())

	if store.presignCalls.Load() != 0 {
		t.Fatalf("presign called %d times for a text-only job", store.presignCalls.Load())
	}
}

// A long job must keep renewing its lease so the reaper does not steal it
// out from under the worker that is actively processing it.
func TestWorkerHeartbeatsLongJob(t *testing.T) {
	cfg := testCfg()
	cfg.WorkerLease = 200 * time.Millisecond // heartbeat every 100ms
	cfg.WorkerPollInterval = 5 * time.Millisecond

	repo := &fakeRepo{pending: jobs(1)}
	proc := &fakeProc{delay: 450 * time.Millisecond}
	w := worker.New(cfg, repo, &fakeStore{}, proc, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, func() bool { return len(repo.snapshot().completed) == 1 }, "completion")
	cancel()
	w.Stop(context.Background())

	if repo.leases.Load() < 2 {
		t.Fatalf("lease renewals=%d, expected the heartbeat to fire repeatedly", repo.leases.Load())
	}
}

// Jobs already claimed but not yet started must be released on shutdown so
// another replica picks them up immediately instead of after the lease expires.
func TestWorkerReleasesQueuedJobsOnStop(t *testing.T) {
	cfg := testCfg()
	cfg.WorkerConcurrency = 1
	cfg.WorkerClaimBatch = 4

	repo := &fakeRepo{pending: jobs(4), retryReschedules: true}
	proc := &fakeProc{delay: 400 * time.Millisecond}
	w := worker.New(cfg, repo, &fakeStore{}, proc, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, func() bool { return proc.seen.Load() >= 1 }, "first job to start")
	cancel()
	w.Stop(context.Background())

	if got := repo.snapshot(); len(got.retried) == 0 {
		t.Fatalf("queued jobs were dropped instead of released: %+v", got)
	}
}
