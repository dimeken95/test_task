package service_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dimeken95/test_task/internal/domain"
	"github.com/dimeken95/test_task/internal/service"
)

type stubRepo struct {
	mu      sync.Mutex
	created []*domain.Job
	err     error
}

func (r *stubRepo) Create(_ context.Context, job *domain.Job) error {
	if r.err != nil {
		return r.err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// Mirrors the partial unique index on idempotency_key.
	if job.IdempotencyKey != "" {
		for _, existing := range r.created {
			if existing.IdempotencyKey == job.IdempotencyKey {
				return domain.ErrDuplicateKey
			}
		}
	}
	cp := *job
	r.created = append(r.created, &cp)
	return nil
}
func (r *stubRepo) Get(context.Context, string) (*domain.Job, error) {
	return nil, domain.ErrNotFound
}
func (r *stubRepo) GetByIdempotencyKey(_ context.Context, key string) (*domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, j := range r.created {
		if key != "" && j.IdempotencyKey == key {
			cp := *j
			return &cp, nil
		}
	}
	return nil, domain.ErrNotFound
}
func (r *stubRepo) ClaimBatch(context.Context, string, time.Duration, int) ([]*domain.Job, error) {
	return nil, nil
}
func (r *stubRepo) ExtendLease(context.Context, string, time.Duration) error { return nil }
func (r *stubRepo) Complete(context.Context, string, string) error           { return nil }
func (r *stubRepo) Retry(context.Context, string, string, int, time.Duration, time.Duration) (bool, error) {
	return false, nil
}
func (r *stubRepo) Fail(context.Context, string, string) error { return nil }
func (r *stubRepo) ReapExpired(context.Context) (int64, error) { return 0, nil }
func (r *stubRepo) CountByStatus(context.Context, domain.Status) (int64, error) {
	return 0, nil
}
func (r *stubRepo) Ping(context.Context) error { return nil }

type stubStore struct {
	mu      sync.Mutex
	puts    map[string]int64
	deletes []string
	putErr  error
}

func newStubStore() *stubStore { return &stubStore{puts: map[string]int64{}} }

func (s *stubStore) Put(_ context.Context, key string, body io.Reader, _ int64, _ string) (int64, error) {
	if s.putErr != nil {
		return 0, s.putErr
	}
	n, err := io.Copy(io.Discard, body)
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts[key] = n
	return n, nil
}

func (s *stubStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletes = append(s.deletes, key)
	return nil
}
func (s *stubStore) PresignGet(context.Context, string, time.Duration) (string, error) {
	return "", nil
}
func (s *stubStore) Ping(context.Context) error { return nil }

func (s *stubStore) deleted() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.deletes...)
}

func newSvc(t *testing.T) (*service.JobService, *stubRepo, *stubStore) {
	t.Helper()
	repo, store := &stubRepo{}, newStubStore()
	return service.NewJobService(repo, store, slog.Default()), repo, store
}

func TestDraftRecordsStoredSize(t *testing.T) {
	svc, repo, store := newSvc(t)
	body := strings.NewReader(strings.Repeat("x", 1234))

	d := svc.NewDraft("")
	if err := d.AttachFile(context.Background(), "a.png", "image/png", 0, body); err != nil {
		t.Fatal(err)
	}
	job, _, err := d.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if job.SizeBytes != 1234 {
		t.Fatalf("size=%d want 1234", job.SizeBytes)
	}
	if store.puts[job.ObjectKey] != 1234 {
		t.Fatalf("stored %d bytes", store.puts[job.ObjectKey])
	}
	if len(repo.created) != 1 {
		t.Fatalf("created=%d", len(repo.created))
	}
}

func TestDraftRejectsEmptyPayload(t *testing.T) {
	svc, _, _ := newSvc(t)
	if _, _, err := svc.NewDraft("").Commit(context.Background()); !errors.Is(err, domain.ErrNoPayload) {
		t.Fatalf("got %v want ErrNoPayload", err)
	}
}

func TestDraftRejectsSecondFile(t *testing.T) {
	svc, _, _ := newSvc(t)
	d := svc.NewDraft("")
	ctx := context.Background()

	if err := d.AttachFile(ctx, "a.png", "image/png", 0, strings.NewReader("one")); err != nil {
		t.Fatal(err)
	}
	err := d.AttachFile(ctx, "b.png", "image/png", 0, strings.NewReader("two"))
	if !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("got %v want ErrInvalidInput", err)
	}
}

// Object storage is written before the row, so a failed insert has to clean up
// or the bucket slowly fills with objects no job references.
func TestCommitRemovesOrphanObjectOnDBFailure(t *testing.T) {
	repo, store := &stubRepo{err: errors.New("db down")}, newStubStore()
	svc := service.NewJobService(repo, store, slog.Default())
	ctx := context.Background()

	d := svc.NewDraft("")
	if err := d.AttachFile(ctx, "a.png", "image/png", 0, strings.NewReader("data")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.Commit(ctx); err == nil {
		t.Fatal("expected commit to fail")
	}

	if got := store.deleted(); len(got) != 1 {
		t.Fatalf("orphan not cleaned up, deletes=%v", got)
	}
}

// Discard is deferred on every request; after a successful commit it must be
// a no-op, otherwise it would delete a live job's payload.
func TestDiscardIsNoOpAfterCommit(t *testing.T) {
	svc, _, store := newSvc(t)
	ctx := context.Background()

	d := svc.NewDraft("")
	if err := d.AttachFile(ctx, "a.png", "image/png", 0, strings.NewReader("data")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	d.Discard(ctx)

	if got := store.deleted(); len(got) != 0 {
		t.Fatalf("committed object was deleted: %v", got)
	}
}

func TestDiscardCleansUpAbandonedUpload(t *testing.T) {
	svc, _, store := newSvc(t)
	ctx := context.Background()

	d := svc.NewDraft("")
	if err := d.AttachFile(ctx, "a.png", "image/png", 0, strings.NewReader("data")); err != nil {
		t.Fatal(err)
	}
	d.Discard(ctx) // request aborted before commit

	if got := store.deleted(); len(got) != 1 {
		t.Fatalf("abandoned object not cleaned up: %v", got)
	}
}

// Object keys are derived from client-supplied filenames, so traversal and
// separators must not survive into the key.
func TestObjectKeysAreSafe(t *testing.T) {
	cases := []struct{ name, mustNotContain string }{
		{"../../etc/passwd", ".."},
		{`..\..\windows\system32`, `\`},
		{"a/b/c.png", "/b/"},
		{"file name with spaces.png", " "},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, _, _ := newSvc(t)
			d := svc.NewDraft("")
			if err := d.AttachFile(context.Background(), tc.name, "image/png", 0, strings.NewReader("x")); err != nil {
				t.Fatal(err)
			}
			job, _, err := d.Commit(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(job.ObjectKey, tc.mustNotContain) {
				t.Fatalf("unsafe object key %q", job.ObjectKey)
			}
			if !strings.HasPrefix(job.ObjectKey, "jobs/"+job.ID+"/") {
				t.Fatalf("key escaped its job prefix: %q", job.ObjectKey)
			}
		})
	}
}

func TestUploadFailureIsReported(t *testing.T) {
	repo, store := &stubRepo{}, newStubStore()
	store.putErr = errors.New("s3 unavailable")
	svc := service.NewJobService(repo, store, slog.Default())

	d := svc.NewDraft("")
	err := d.AttachFile(context.Background(), "a.png", "image/png", 0, strings.NewReader("x"))
	if err == nil || !strings.Contains(err.Error(), "s3 unavailable") {
		t.Fatalf("got %v", err)
	}
}
