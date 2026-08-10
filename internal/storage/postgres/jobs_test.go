package postgres_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dimeken95/test_task/internal/domain"
	"github.com/dimeken95/test_task/internal/storage/postgres"
)

// These tests exercise the parts that cannot be faked: SKIP LOCKED semantics,
// the atomic claim, and the retry/reap state machine. They need a real
// Postgres; CI provides one via a service container.
// testSchema isolates this package's rows. `go test ./...` runs packages in
// parallel against the same TEST_DATABASE_URL, so without a private schema the
// end-to-end package's TRUNCATE would delete rows out from under these tests.
const testSchema = "test_storage_postgres"

func testPool(t *testing.T) (*pgxpool.Pool, context.Context) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; see `make test-integration`")
	}

	ctx := context.Background()
	if err := ensureSchema(ctx, dsn, testSchema); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	pool, err := postgres.NewPool(ctx, withSearchPath(dsn, testSchema), 10)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	dir, err := filepath.Abs(filepath.Join("..", "..", "..", "migrations"))
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.Migrate(ctx, pool, dir); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool, ctx
}

func ensureSchema(ctx context.Context, dsn, schema string) error {
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer admin.Close()
	_, err = admin.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+schema)
	return err
}

// withSearchPath pins every connection from the pool to the given schema, so
// unqualified table names — including goose's version table — resolve there.
func withSearchPath(dsn, schema string) string {
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "search_path=" + schema
}

func setupRepo(t *testing.T) (*postgres.JobRepo, context.Context) {
	t.Helper()
	pool, ctx := testPool(t)
	// Isolate tests from each other's rows.
	if _, err := pool.Exec(ctx, `TRUNCATE jobs`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return postgres.NewJobRepo(pool), ctx
}

func seed(t *testing.T, repo *postgres.JobRepo, ctx context.Context, n int) []string {
	t.Helper()
	ids := make([]string, n)
	for i := range ids {
		job := &domain.Job{ID: uuid.NewString(), Status: domain.StatusPending, Text: "payload"}
		if err := repo.Create(ctx, job); err != nil {
			t.Fatalf("create: %v", err)
		}
		ids[i] = job.ID
		// Keep created_at ordering deterministic for the FIFO assertion.
		time.Sleep(time.Millisecond)
	}
	return ids
}

// The whole point of FOR UPDATE SKIP LOCKED: concurrent workers must partition
// the queue, never hand the same job to two of them.
func TestClaimIsExactlyOnceUnderConcurrency(t *testing.T) {
	repo, ctx := setupRepo(t)
	const total = 40
	seed(t, repo, ctx, total)

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		seen = map[string]int{}
	)
	for i := range 8 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for range 10 {
				batch, err := repo.ClaimBatch(ctx, "worker-"+uuid.NewString(), time.Minute, 3)
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				mu.Lock()
				for _, j := range batch {
					seen[j.ID]++
				}
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	if len(seen) != total {
		t.Fatalf("claimed %d distinct jobs, want %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %s claimed %d times", id, n)
		}
	}
}

// A claim must stamp ownership and consume an attempt in the same statement.
func TestClaimRecordsOwnershipAndAttempt(t *testing.T) {
	repo, ctx := setupRepo(t)
	ids := seed(t, repo, ctx, 1)

	batch, err := repo.ClaimBatch(ctx, "worker-7", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 1 {
		t.Fatalf("claimed %d jobs, want 1", len(batch))
	}
	job := batch[0]
	if job.Status != domain.StatusProcessing || job.LockedBy != "worker-7" || job.Attempts != 1 {
		t.Fatalf("unexpected claimed job: %+v", job)
	}
	if job.LeaseUntil == nil || job.LeaseUntil.Before(time.Now()) {
		t.Fatalf("lease not set: %+v", job.LeaseUntil)
	}

	stored, err := repo.Get(ctx, ids[0])
	if err != nil {
		t.Fatal(err)
	}
	if stored.LockedBy != "worker-7" {
		t.Fatalf("ownership not persisted: %+v", stored)
	}
}

func TestClaimRespectsBatchLimitAndFIFO(t *testing.T) {
	repo, ctx := setupRepo(t)
	ids := seed(t, repo, ctx, 5)

	batch, err := repo.ClaimBatch(ctx, "w", time.Minute, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 2 {
		t.Fatalf("batch=%d want 2", len(batch))
	}
	got := map[string]bool{batch[0].ID: true, batch[1].ID: true}
	if !got[ids[0]] || !got[ids[1]] {
		t.Fatalf("expected the two oldest jobs, got %v", got)
	}
}

// Retry must reschedule into the future, and a rescheduled job must not be
// claimable until its backoff has elapsed.
func TestRetrySchedulesBackoff(t *testing.T) {
	repo, ctx := setupRepo(t)
	seed(t, repo, ctx, 1)

	batch, err := repo.ClaimBatch(ctx, "w", time.Minute, 1)
	if err != nil || len(batch) != 1 {
		t.Fatalf("claim: %v %d", err, len(batch))
	}
	id := batch[0].ID

	rescheduled, err := repo.Retry(ctx, id, "upstream timeout", 5, 30*time.Second, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !rescheduled {
		t.Fatal("expected the job to be rescheduled")
	}

	stored, err := repo.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != domain.StatusPending {
		t.Fatalf("status=%s want pending", stored.Status)
	}
	if stored.NextAttemptAt == nil || !stored.NextAttemptAt.After(time.Now()) {
		t.Fatalf("next_attempt_at not in the future: %v", stored.NextAttemptAt)
	}

	again, err := repo.ClaimBatch(ctx, "w", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Fatal("a backed-off job must not be claimable yet")
	}
}

// Once attempts are exhausted, Retry must terminate the job instead of
// looping it back into the queue forever.
func TestRetryGivesUpAtMaxAttempts(t *testing.T) {
	repo, ctx := setupRepo(t)
	seed(t, repo, ctx, 1)

	const maxAttempts = 3
	var id string
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		batch, err := repo.ClaimBatch(ctx, "w", time.Minute, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(batch) != 1 {
			t.Fatalf("attempt %d: claimed %d jobs", attempt, len(batch))
		}
		id = batch[0].ID

		// Zero backoff so the next claim is immediately eligible.
		rescheduled, err := repo.Retry(ctx, id, "boom", maxAttempts, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		wantRescheduled := attempt < maxAttempts
		if rescheduled != wantRescheduled {
			t.Fatalf("attempt %d: rescheduled=%v want %v", attempt, rescheduled, wantRescheduled)
		}
	}

	stored, err := repo.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != domain.StatusFailed {
		t.Fatalf("status=%s want failed", stored.Status)
	}
	if stored.CompletedAt == nil {
		t.Fatal("terminal job should record completed_at")
	}
}

// A worker that dies mid-flight leaves the row in processing; the reaper must
// return it to the queue once the lease lapses.
func TestReapExpiredRequeuesAbandonedJobs(t *testing.T) {
	repo, ctx := setupRepo(t)
	seed(t, repo, ctx, 1)

	batch, err := repo.ClaimBatch(ctx, "doomed-worker", 50*time.Millisecond, 1)
	if err != nil || len(batch) != 1 {
		t.Fatalf("claim: %v", err)
	}
	id := batch[0].ID

	time.Sleep(120 * time.Millisecond)

	n, err := repo.ReapExpired(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("reaped=%d want 1", n)
	}

	stored, err := repo.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != domain.StatusPending || stored.LockedBy != "" {
		t.Fatalf("job not released: %+v", stored)
	}

	// And it must be immediately claimable again.
	again, err := repo.ClaimBatch(ctx, "w2", time.Minute, 1)
	if err != nil || len(again) != 1 {
		t.Fatalf("requeued job not claimable: %v %d", err, len(again))
	}
	if again[0].Attempts != 2 {
		t.Fatalf("attempts=%d want 2", again[0].Attempts)
	}
}

// A live lease must be renewable, and renewing must keep the reaper away.
func TestExtendLeaseKeepsJobOwned(t *testing.T) {
	repo, ctx := setupRepo(t)
	seed(t, repo, ctx, 1)

	batch, _ := repo.ClaimBatch(ctx, "w", 50*time.Millisecond, 1)
	if len(batch) != 1 {
		t.Fatal("claim failed")
	}
	id := batch[0].ID

	if err := repo.ExtendLease(ctx, id, time.Minute); err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond)

	n, err := repo.ReapExpired(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("reaped %d jobs despite an extended lease", n)
	}
}

// State transitions are guarded: only a processing job may complete, which is
// what stops a reaped-and-reclaimed job from being finished twice.
func TestCompleteOnlyAppliesToProcessingJobs(t *testing.T) {
	repo, ctx := setupRepo(t)
	ids := seed(t, repo, ctx, 1)

	if err := repo.Complete(ctx, ids[0], "summary"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("completing a pending job should fail, got %v", err)
	}

	batch, _ := repo.ClaimBatch(ctx, "w", time.Minute, 1)
	if err := repo.Complete(ctx, batch[0].ID, "done"); err != nil {
		t.Fatal(err)
	}
	if err := repo.Complete(ctx, batch[0].ID, "done again"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("double completion should fail, got %v", err)
	}

	stored, _ := repo.Get(ctx, ids[0])
	if stored.Status != domain.StatusCompleted || stored.ResultSummary != "done" {
		t.Fatalf("unexpected job: %+v", stored)
	}
}

func TestGetUnknownAndMalformedIDs(t *testing.T) {
	repo, ctx := setupRepo(t)

	if _, err := repo.Get(ctx, uuid.NewString()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("unknown id: got %v", err)
	}
	if _, err := repo.Get(ctx, "not-a-uuid"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("malformed id should be a clean 404, got %v", err)
	}
}

// The partial unique index is what makes idempotent submission race-free, so
// a duplicate key must be reported as such rather than as a generic failure.
func TestIdempotencyKeyIsUnique(t *testing.T) {
	repo, ctx := setupRepo(t)

	first := &domain.Job{ID: uuid.NewString(), Status: domain.StatusPending, Text: "one", IdempotencyKey: "order-1"}
	if err := repo.Create(ctx, first); err != nil {
		t.Fatal(err)
	}

	second := &domain.Job{ID: uuid.NewString(), Status: domain.StatusPending, Text: "two", IdempotencyKey: "order-1"}
	if err := repo.Create(ctx, second); !errors.Is(err, domain.ErrDuplicateKey) {
		t.Fatalf("got %v want ErrDuplicateKey", err)
	}

	found, err := repo.GetByIdempotencyKey(ctx, "order-1")
	if err != nil {
		t.Fatal(err)
	}
	if found.ID != first.ID {
		t.Fatalf("resolved to %s want %s", found.ID, first.ID)
	}
}

// The index is partial: jobs without a key must not collide with each other.
func TestJobsWithoutIdempotencyKeyDoNotCollide(t *testing.T) {
	repo, ctx := setupRepo(t)

	for range 3 {
		job := &domain.Job{ID: uuid.NewString(), Status: domain.StatusPending, Text: "no key"}
		if err := repo.Create(ctx, job); err != nil {
			t.Fatalf("keyless insert rejected: %v", err)
		}
	}

	if _, err := repo.GetByIdempotencyKey(ctx, ""); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("empty key must never resolve, got %v", err)
	}
}

// Concurrent inserts of the same key: exactly one wins, the rest are told it
// is a duplicate.
func TestConcurrentIdempotentInsertsElectOneWinner(t *testing.T) {
	repo, ctx := setupRepo(t)

	const n = 8
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		succeeded int
		duplicate int
	)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job := &domain.Job{ID: uuid.NewString(), Status: domain.StatusPending, IdempotencyKey: "racing"}
			err := repo.Create(ctx, job)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				succeeded++
			case errors.Is(err, domain.ErrDuplicateKey):
				duplicate++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if succeeded != 1 || duplicate != n-1 {
		t.Fatalf("succeeded=%d duplicate=%d, want 1 and %d", succeeded, duplicate, n-1)
	}
}

func TestCountByStatus(t *testing.T) {
	repo, ctx := setupRepo(t)
	seed(t, repo, ctx, 3)

	n, err := repo.CountByStatus(ctx, domain.StatusPending)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("pending=%d want 3", n)
	}
}

// Migrations run from every replica at once in Kubernetes; the advisory lock
// must make that safe rather than racing inside goose's version table.
func TestConcurrentMigrationsAreSerialised(t *testing.T) {
	pool, ctx := testPool(t)
	dir, _ := filepath.Abs(filepath.Join("..", "..", "..", "migrations"))

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := postgres.Migrate(ctx, pool, dir); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent migration failed: %v", err)
	}
}
