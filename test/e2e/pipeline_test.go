// Package e2e wires the real handler, service, Postgres repository and worker
// together and drives them through the public HTTP surface. Only the object
// store and the external processor are stand-ins, and both are real HTTP
// servers rather than in-process fakes, so the presigned-URL handshake between
// the worker, the store and the processor is genuinely exercised.
package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	httpapi "github.com/dimeken95/test_task/internal/api/http"
	"github.com/dimeken95/test_task/internal/config"
	"github.com/dimeken95/test_task/internal/domain"
	"github.com/dimeken95/test_task/internal/processor"
	"github.com/dimeken95/test_task/internal/service"
	"github.com/dimeken95/test_task/internal/storage/postgres"
	"github.com/dimeken95/test_task/internal/worker"
)

// objectStore serves stored objects over HTTP so presigned URLs are real URLs
// the processor fetches over the network.
type objectStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	srv     *httptest.Server
}

func newObjectStore(t *testing.T) *objectStore {
	t.Helper()
	s := &objectStore{objects: map[string][]byte{}}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/")
		s.mu.Lock()
		body, ok := s.objects[key]
		s.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		// Refuse unsigned reads, mirroring a private bucket.
		if r.URL.Query().Get("sig") == "" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_, _ = w.Write(body)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *objectStore) Put(_ context.Context, key string, body io.Reader, _ int64, _ string) (int64, error) {
	b, err := io.ReadAll(body)
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = b
	return int64(len(b)), nil
}

func (s *objectStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key)
	return nil
}

func (s *objectStore) PresignGet(_ context.Context, key string, _ time.Duration) (string, error) {
	return s.srv.URL + "/" + key + "?sig=" + url.QueryEscape("signed"), nil
}

func (s *objectStore) Ping(context.Context) error { return nil }

func (s *objectStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.objects)
}

// fakeProcessor is the external service. It can be told to fail a fixed number
// of times so the retry path can be observed end to end.
type fakeProcessor struct {
	srv        *httptest.Server
	calls      atomic.Int32
	failFirstN int32
	downloaded atomic.Int64
	sawText    atomic.Value
}

func newFakeProcessor(t *testing.T) *fakeProcessor {
	t.Helper()
	p := &fakeProcessor{}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := p.calls.Add(1)

		var req struct {
			JobID       string `json:"job_id"`
			Text        string `json:"text"`
			ContentType string `json:"content_type"`
			SizeBytes   int64  `json:"size_bytes"`
			DownloadURL string `json:"download_url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		p.sawText.Store(req.Text)

		if n <= p.failFirstN {
			http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
			return
		}

		var downloaded int64
		if req.DownloadURL != "" {
			resp, err := http.Get(req.DownloadURL)
			if err != nil {
				http.Error(w, "download failed", http.StatusBadGateway)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				http.Error(w, "download status", http.StatusBadGateway)
				return
			}
			downloaded, _ = io.Copy(io.Discard, resp.Body)
			p.downloaded.Store(downloaded)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"summary":      fmt.Sprintf("processed bytes=%d type=%s", downloaded, req.ContentType),
			"processed_at": time.Now().UTC(),
		})
	}))
	t.Cleanup(p.srv.Close)
	return p
}

// testSchema keeps this package's rows away from the storage tests, which run
// in parallel against the same TEST_DATABASE_URL and truncate as they go.
const testSchema = "test_e2e"

func ensureSchema(ctx context.Context, dsn string) error {
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer admin.Close()
	_, err = admin.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+testSchema)
	return err
}

// withSearchPath pins every connection from the pool to the test schema, so
// unqualified table names — including goose's version table — resolve there.
func withSearchPath(dsn string) string {
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "search_path=" + testSchema
}

type harness struct {
	api    http.Handler
	repo   *postgres.JobRepo
	store  *objectStore
	proc   *fakeProcessor
	worker *worker.Worker
}

func newHarness(t *testing.T, tune func(*config.Config)) *harness {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; see `make test-integration`")
	}

	ctx := context.Background()
	if err := ensureSchema(ctx, dsn); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	pool, err := postgres.NewPool(ctx, withSearchPath(dsn), 10)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	dir, _ := filepath.Abs(filepath.Join("..", "..", "migrations"))
	if err := postgres.Migrate(ctx, pool, dir); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE jobs`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	store := newObjectStore(t)
	proc := newFakeProcessor(t)

	cfg := config.Config{
		AppMode:              config.ModeAll,
		MaxTextBytes:         1 << 20,
		MaxDocBytes:          8 << 20,
		MaxImageBytes:        8 << 20,
		MaxVideoBytes:        8 << 20,
		MaxConcurrentUploads: 8,
		WorkerID:             "e2e-worker",
		WorkerConcurrency:    2,
		WorkerClaimBatch:     4,
		WorkerPollInterval:   20 * time.Millisecond,
		WorkerLease:          10 * time.Second,
		ReaperInterval:       50 * time.Millisecond,
		MaxAttempts:          5,
		RetryBackoffBase:     10 * time.Millisecond,
		RetryBackoffMax:      50 * time.Millisecond,
		S3PresignTTL:         time.Minute,
		MockTimeout:          5 * time.Second,
		MockMaxRetries:       1, // job-level retry is what we want to observe
	}
	if tune != nil {
		tune(&cfg)
	}

	repo := postgres.NewJobRepo(pool)
	svc := service.NewJobService(repo, store, slog.Default())
	api := httpapi.NewRouter(httpapi.NewHandler(cfg, svc, slog.Default(), nil), slog.Default())

	client := processor.NewHTTPClient(proc.srv.URL, cfg.MockTimeout, cfg.MockMaxRetries)
	w := worker.New(cfg, repo, store, client, slog.Default())

	workerCtx, cancel := context.WithCancel(ctx)
	w.Start(workerCtx)
	t.Cleanup(func() {
		cancel()
		w.Stop(context.Background())
	})

	return &harness{api: api, repo: repo, store: store, proc: proc, worker: w}
}

func (h *harness) submit(t *testing.T, build func(*multipart.Writer)) map[string]any {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	build(mw)
	if err := mw.Close(); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rr := httptest.NewRecorder()
	h.api.ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("submit status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// awaitStatus polls the public GET endpoint, exactly as a client would.
func (h *harness) awaitStatus(t *testing.T, id, want string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last map[string]any
	for time.Now().Before(deadline) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+id, nil)
		rr := httptest.NewRecorder()
		h.api.ServeHTTP(rr, req)
		if rr.Code == http.StatusOK {
			_ = json.Unmarshal(rr.Body.Bytes(), &last)
			if last["status"] == want {
				return last
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("job %s never reached %q; last state: %#v", id, want, last)
	return nil
}

func attachFile(t *testing.T, mw *multipart.Writer, filename, contentType string, body []byte) {
	t.Helper()
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename=%q`, filename))
	h.Set("Content-Type", contentType)
	part, err := mw.CreatePart(h)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write(body); err != nil {
		t.Fatal(err)
	}
}

func png(n int) []byte {
	sig := []byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A}
	return append(sig, bytes.Repeat([]byte{0x7}, n-len(sig))...)
}

// The headline path: upload a file, get 202 immediately, and have the worker
// pick it up, hand the processor a presigned URL, and record the result.
func TestPipelineFileUpload(t *testing.T) {
	h := newHarness(t, nil)
	data := png(64 * 1024)

	created := h.submit(t, func(mw *multipart.Writer) {
		_ = mw.WriteField("text", "holiday photo")
		attachFile(t, mw, "photo.png", "image/png", data)
	})

	if created["status"] != "pending" {
		t.Fatalf("expected an immediate 202/pending, got %#v", created)
	}
	if got := int64(created["size_bytes"].(float64)); got != int64(len(data)) {
		t.Fatalf("size_bytes=%d want %d", got, len(data))
	}

	final := h.awaitStatus(t, created["id"].(string), "completed")

	if !strings.Contains(final["result_summary"].(string), fmt.Sprintf("bytes=%d", len(data))) {
		t.Fatalf("processor did not receive the whole payload: %#v", final)
	}
	if final["text"] != "holiday photo" {
		t.Fatalf("text lost along the way: %#v", final)
	}
	if h.proc.downloaded.Load() != int64(len(data)) {
		t.Fatalf("processor downloaded %d bytes, want %d", h.proc.downloaded.Load(), len(data))
	}
	if final["completed_at"] == nil {
		t.Fatal("completed job should carry completed_at")
	}
}

func TestPipelineTextOnly(t *testing.T) {
	h := newHarness(t, nil)

	created := h.submit(t, func(mw *multipart.Writer) {
		_ = mw.WriteField("text", "just some text")
	})
	final := h.awaitStatus(t, created["id"].(string), "completed")

	if final["result_summary"] == "" {
		t.Fatalf("no summary recorded: %#v", final)
	}
	if h.store.count() != 0 {
		t.Fatal("a text-only job must not create an object")
	}
}

// A flaky processor must not lose the job: the worker retries with backoff and
// the job still completes, with the extra attempts visible to the client.
func TestPipelineRecoversFromTransientProcessorFailure(t *testing.T) {
	h := newHarness(t, nil)
	h.proc.failFirstN = 2

	created := h.submit(t, func(mw *multipart.Writer) {
		_ = mw.WriteField("text", "retry me")
	})
	final := h.awaitStatus(t, created["id"].(string), "completed")

	attempts := int(final["attempts"].(float64))
	if attempts < 3 {
		t.Fatalf("attempts=%d, expected the job to be retried", attempts)
	}
	if h.proc.calls.Load() < 3 {
		t.Fatalf("processor called %d times", h.proc.calls.Load())
	}
}

// Once the retry budget is spent the job must settle in failed, carrying the
// upstream error, rather than cycling through the queue forever.
func TestPipelineGivesUpAfterMaxAttempts(t *testing.T) {
	h := newHarness(t, func(c *config.Config) { c.MaxAttempts = 2 })
	h.proc.failFirstN = 1000 // never succeeds

	created := h.submit(t, func(mw *multipart.Writer) {
		_ = mw.WriteField("text", "doomed")
	})
	final := h.awaitStatus(t, created["id"].(string), "failed")

	if int(final["attempts"].(float64)) != 2 {
		t.Fatalf("attempts=%v want 2", final["attempts"])
	}
	if final["error_message"] == "" {
		t.Fatal("failed job should explain why")
	}

	// And it must stay failed rather than being picked up again.
	time.Sleep(200 * time.Millisecond)
	if got := h.awaitStatus(t, created["id"].(string), "failed"); got["status"] != "failed" {
		t.Fatalf("job left the terminal state: %#v", got)
	}
}

// Independent jobs submitted at once must all complete; this is the property
// that makes running several worker replicas safe.
func TestPipelineHandlesConcurrentSubmissions(t *testing.T) {
	h := newHarness(t, nil)
	const n = 12

	ids := make([]string, n)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			created := h.submit(t, func(mw *multipart.Writer) {
				_ = mw.WriteField("text", fmt.Sprintf("job %d", i))
				attachFile(t, mw, fmt.Sprintf("f%d.png", i), "image/png", png(2048))
			})
			mu.Lock()
			ids[i] = created["id"].(string)
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	for _, id := range ids {
		h.awaitStatus(t, id, "completed")
	}
	if h.store.count() != n {
		t.Fatalf("objects=%d want %d", h.store.count(), n)
	}
}

// A client that times out and retries must get its original job back. This is
// the path that depends on the real partial unique index, so it is worth
// exercising against Postgres rather than an in-memory stand-in.
func TestPipelineIdempotentRetry(t *testing.T) {
	h := newHarness(t, nil)
	data := png(8192)

	submit := func() map[string]any {
		var buf bytes.Buffer
		mw := multipart.NewWriter(&buf)
		attachFile(t, mw, "invoice.png", "image/png", data)
		if err := mw.Close(); err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", &buf)
		req.Header.Set("Content-Type", mw.FormDataContentType())
		req.Header.Set("Idempotency-Key", "invoice-2026-01")

		rr := httptest.NewRecorder()
		h.api.ServeHTTP(rr, req)
		if rr.Code != http.StatusAccepted {
			t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
		}
		var out map[string]any
		if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	first := submit()
	second := submit()

	if first["id"] != second["id"] {
		t.Fatalf("retry created a second job: %v vs %v", second["id"], first["id"])
	}
	if h.store.count() != 1 {
		t.Fatalf("objects=%d want 1: the retried upload must be cleaned up", h.store.count())
	}

	// And the single job still completes normally.
	h.awaitStatus(t, first["id"].(string), "completed")
}

// A job abandoned by a dead worker (lease left to expire) must be requeued by
// the reaper and completed by a live worker.
func TestPipelineReapsAbandonedJob(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()

	// Simulate a worker that claimed the job and then died: insert the row and
	// claim it with a lease that is already effectively spent.
	job := &domain.Job{ID: uuid.NewString(), Status: domain.StatusPending, Text: "orphaned"}
	if err := h.repo.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	if _, err := h.repo.ClaimBatch(ctx, "dead-worker", time.Millisecond, 1); err != nil {
		t.Fatal(err)
	}

	final := h.awaitStatus(t, job.ID, "completed")
	if int(final["attempts"].(float64)) < 2 {
		t.Fatalf("expected a second attempt after reaping: %#v", final)
	}
}
