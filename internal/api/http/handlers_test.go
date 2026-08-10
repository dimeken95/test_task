package httpapi_test

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
	"strings"
	"sync"
	"testing"
	"time"

	httpapi "github.com/dimeken95/test_task/internal/api/http"
	"github.com/dimeken95/test_task/internal/config"
	"github.com/dimeken95/test_task/internal/domain"
	"github.com/dimeken95/test_task/internal/service"
)

type memRepo struct {
	mu   sync.Mutex
	jobs map[string]*domain.Job
	fail error
}

func newMemRepo() *memRepo { return &memRepo{jobs: map[string]*domain.Job{}} }

func (m *memRepo) Create(_ context.Context, job *domain.Job) error {
	if m.fail != nil {
		return m.fail
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Mirrors the partial unique index on idempotency_key.
	if job.IdempotencyKey != "" {
		for _, existing := range m.jobs {
			if existing.IdempotencyKey == job.IdempotencyKey {
				return domain.ErrDuplicateKey
			}
		}
	}
	cp := *job
	m.jobs[job.ID] = &cp
	return nil
}

func (m *memRepo) GetByIdempotencyKey(_ context.Context, key string) (*domain.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if key == "" {
		return nil, domain.ErrNotFound
	}
	for _, j := range m.jobs {
		if j.IdempotencyKey == key {
			cp := *j
			return &cp, nil
		}
	}
	return nil, domain.ErrNotFound
}

func (m *memRepo) Get(_ context.Context, id string) (*domain.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	cp := *j
	return &cp, nil
}

func (m *memRepo) ClaimBatch(context.Context, string, time.Duration, int) ([]*domain.Job, error) {
	return nil, nil
}
func (m *memRepo) ExtendLease(context.Context, string, time.Duration) error { return nil }
func (m *memRepo) Complete(context.Context, string, string) error           { return nil }
func (m *memRepo) Retry(context.Context, string, string, int, time.Duration, time.Duration) (bool, error) {
	return false, nil
}
func (m *memRepo) Fail(context.Context, string, string) error { return nil }
func (m *memRepo) ReapExpired(context.Context) (int64, error) { return 0, nil }
func (m *memRepo) CountByStatus(context.Context, domain.Status) (int64, error) {
	return 0, nil
}
func (m *memRepo) Ping(context.Context) error { return nil }

type memStore struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newMemStore() *memStore { return &memStore{objects: map[string][]byte{}} }

func (s *memStore) Put(_ context.Context, key string, body io.Reader, _ int64, _ string) (int64, error) {
	b, err := io.ReadAll(body)
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = b
	return int64(len(b)), nil
}

func (s *memStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key)
	return nil
}

func (s *memStore) PresignGet(context.Context, string, time.Duration) (string, error) {
	return "http://example.test/obj", nil
}
func (s *memStore) Ping(context.Context) error { return nil }

func (s *memStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.objects)
}

func testConfig() config.Config {
	return config.Config{
		MaxTextBytes:         1 << 20,
		MaxDocBytes:          1 << 20,
		MaxImageBytes:        1 << 20,
		MaxVideoBytes:        4 << 20,
		MaxConcurrentUploads: 4,
	}
}

func newTestServer(t *testing.T, cfg config.Config) (http.Handler, *memRepo, *memStore) {
	t.Helper()
	repo, store := newMemRepo(), newMemStore()
	svc := service.NewJobService(repo, store, slog.Default())
	h := httpapi.NewHandler(cfg, svc, slog.Default(), nil)
	return httpapi.NewRouter(h, slog.Default()), repo, store
}

// pngOf builds a payload with a valid PNG signature and the requested length.
func pngOf(n int) []byte {
	sig := []byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A}
	if n <= len(sig) {
		return sig
	}
	return append(sig, bytes.Repeat([]byte{0x42}, n-len(sig))...)
}

func mp4Of(n int) []byte { return isoBoxOf("ftyp", n) }

// isoBoxOf builds an ISO base media container opening with the given top-level
// box: a 4-byte big-endian size followed by the 4-byte box type.
func isoBoxOf(box string, n int) []byte {
	head := append([]byte{0, 0, 0, 0x20}, []byte(box)...)
	head = append(head, []byte("isom")...)
	if n <= len(head) {
		return head
	}
	return append(head, bytes.Repeat([]byte{0x00}, n-len(head))...)
}

func filePart(w *multipart.Writer, field, filename, contentType string, body []byte) error {
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name=%q; filename=%q`, field, filename))
	if contentType != "" {
		h.Set("Content-Type", contentType)
	}
	part, err := w.CreatePart(h)
	if err != nil {
		return err
	}
	_, err = part.Write(body)
	return err
}

func multipartRequest(t *testing.T, build func(*multipart.Writer)) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	build(w)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req
}

func post(t *testing.T, r http.Handler, build func(*multipart.Writer)) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, multipartRequest(t, build))
	return rr
}

func decodeJob(t *testing.T, rr *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode %q: %v", rr.Body.String(), err)
	}
	return out
}

func TestCreateJobTextOnly(t *testing.T) {
	r, _, store := newTestServer(t, testConfig())

	rr := post(t, r, func(w *multipart.Writer) {
		_ = w.WriteField("text", "hello world")
	})
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	job := decodeJob(t, rr)
	if job["status"] != "pending" || job["text"] != "hello world" {
		t.Fatalf("unexpected job: %#v", job)
	}
	if rr.Header().Get("Location") == "" {
		t.Fatal("missing Location header")
	}
	if store.count() != 0 {
		t.Fatal("text-only job should not touch object storage")
	}
}

// size_bytes must reflect what was actually stored. Multipart parts carry no
// Content-Length, so this can only come from counting the streamed bytes.
func TestCreateJobReportsRealSize(t *testing.T) {
	r, _, store := newTestServer(t, testConfig())
	data := pngOf(4096)

	rr := post(t, r, func(w *multipart.Writer) {
		if err := filePart(w, "file", "a.png", "image/png", data); err != nil {
			t.Fatal(err)
		}
	})
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}

	job := decodeJob(t, rr)
	if got := int64(job["size_bytes"].(float64)); got != int64(len(data)) {
		t.Fatalf("size_bytes=%d want %d", got, len(data))
	}
	if store.count() != 1 {
		t.Fatalf("objects=%d want 1", store.count())
	}
}

// A text field that arrives after the file part must still be captured;
// multipart imposes no ordering on fields.
func TestCreateJobTextAfterFile(t *testing.T) {
	r, _, _ := newTestServer(t, testConfig())

	rr := post(t, r, func(w *multipart.Writer) {
		if err := filePart(w, "file", "a.png", "image/png", pngOf(512)); err != nil {
			t.Fatal(err)
		}
		_ = w.WriteField("text", "caption after file")
	})
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if job := decodeJob(t, rr); job["text"] != "caption after file" {
		t.Fatalf("text was dropped: %#v", job)
	}
}

func TestCreateJobNoPayload(t *testing.T) {
	r, _, _ := newTestServer(t, testConfig())
	rr := post(t, r, func(*multipart.Writer) {})
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestCreateJobRejectsNonMultipart(t *testing.T) {
	r, _, _ := newTestServer(t, testConfig())
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader(`{"text":"hi"}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", rr.Code)
	}
}

// Declaring an allowed MIME type must not be enough: the bytes have to match.
func TestCreateJobRejectsSpoofedContentType(t *testing.T) {
	elf := append([]byte{0x7f, 'E', 'L', 'F'}, bytes.Repeat([]byte{0}, 600)...)
	zip := append([]byte("PK\x03\x04"), bytes.Repeat([]byte{0}, 600)...)

	cases := []struct {
		name, filename, contentType string
		body                        []byte
	}{
		{"elf as mp4", "payload.mp4", "video/mp4", elf},
		{"elf as png", "payload.png", "image/png", elf},
		{"zip as pdf", "payload.pdf", "application/pdf", zip},
		{"elf as msword", "payload.doc", "application/msword", elf},
		// Widening the accepted MP4/MOV box list must not turn into "accept
		// anything with four bytes at offset 4".
		{"unknown box as mov", "payload.mov", "video/quicktime", isoBoxOf("junk", 600)},
		{"undersized box as mp4", "payload.mp4", "video/mp4",
			append([]byte{0, 0, 0, 0x02}, []byte("ftypisom")...)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, _, store := newTestServer(t, testConfig())
			rr := post(t, r, func(w *multipart.Writer) {
				if err := filePart(w, "file", tc.filename, tc.contentType, tc.body); err != nil {
					t.Fatal(err)
				}
			})
			if rr.Code != http.StatusUnsupportedMediaType {
				t.Fatalf("status=%d want 415, body=%s", rr.Code, rr.Body.String())
			}
			if store.count() != 0 {
				t.Fatal("rejected payload must not reach object storage")
			}
		})
	}
}

func TestCreateJobAcceptsRealMediaTypes(t *testing.T) {
	docx := append([]byte("PK\x03\x04"), bytes.Repeat([]byte{0}, 600)...)
	webp := append([]byte("RIFF\x00\x00\x00\x00WEBP"), bytes.Repeat([]byte{0}, 600)...)

	cases := []struct {
		name, filename, contentType string
		body                        []byte
	}{
		{"png", "a.png", "image/png", pngOf(600)},
		{"mp4", "a.mp4", "video/mp4", mp4Of(600)},
		// QuickTime predates the ftyp box and may lead with another top-level
		// atom; requiring ftyp alone would reject legitimate .mov files.
		{"mov leading with wide", "a.mov", "video/quicktime", isoBoxOf("wide", 600)},
		{"mov leading with moov", "a.mov", "video/quicktime", isoBoxOf("moov", 600)},
		{"webp", "a.webp", "image/webp", webp},
		{"docx", "a.docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document", docx},
		{"pdf", "a.pdf", "application/pdf", append([]byte("%PDF-1.7"), bytes.Repeat([]byte{0}, 600)...)},
		// No declared type: the extension resolves it.
		{"by extension", "a.png", "", pngOf(600)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, _, _ := newTestServer(t, testConfig())
			rr := post(t, r, func(w *multipart.Writer) {
				if err := filePart(w, "file", tc.filename, tc.contentType, tc.body); err != nil {
					t.Fatal(err)
				}
			})
			if rr.Code != http.StatusAccepted {
				t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
			}
		})
	}
}

func TestCreateJobRejectsUnknownType(t *testing.T) {
	r, _, _ := newTestServer(t, testConfig())
	rr := post(t, r, func(w *multipart.Writer) {
		if err := filePart(w, "file", "x.exe", "application/x-msdownload", []byte("MZ......")); err != nil {
			t.Fatal(err)
		}
	})
	if rr.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

// Oversized payloads are detected while streaming and must not leave an object
// behind, because the size is unknown until the body has been read.
func TestCreateJobOversizedFile(t *testing.T) {
	cfg := testConfig()
	cfg.MaxImageBytes = 1024
	r, _, store := newTestServer(t, cfg)

	rr := post(t, r, func(w *multipart.Writer) {
		if err := filePart(w, "file", "a.png", "image/png", pngOf(8192)); err != nil {
			t.Fatal(err)
		}
	})
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d want 413, body=%s", rr.Code, rr.Body.String())
	}
	if store.count() != 0 {
		t.Fatalf("orphan object left behind: %d", store.count())
	}
}

// A payload exactly at the limit is valid; one byte more is not.
func TestCreateJobExactlyAtLimit(t *testing.T) {
	cfg := testConfig()
	cfg.MaxImageBytes = 2048
	r, _, _ := newTestServer(t, cfg)

	rr := post(t, r, func(w *multipart.Writer) {
		if err := filePart(w, "file", "a.png", "image/png", pngOf(2048)); err != nil {
			t.Fatal(err)
		}
	})
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestCreateJobOversizedText(t *testing.T) {
	cfg := testConfig()
	cfg.MaxTextBytes = 16
	r, _, _ := newTestServer(t, cfg)

	rr := post(t, r, func(w *multipart.Writer) {
		_ = w.WriteField("text", strings.Repeat("x", 64))
	})
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestCreateJobRejectsSecondFilePart(t *testing.T) {
	r, _, _ := newTestServer(t, testConfig())
	rr := post(t, r, func(w *multipart.Writer) {
		if err := filePart(w, "file", "a.png", "image/png", pngOf(512)); err != nil {
			t.Fatal(err)
		}
		if err := filePart(w, "file", "b.png", "image/png", pngOf(512)); err != nil {
			t.Fatal(err)
		}
	})
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

// A DB failure after the object is written must not leave the object orphaned.
func TestCreateJobCleansUpObjectWhenPersistFails(t *testing.T) {
	repo, store := newMemRepo(), newMemStore()
	repo.fail = fmt.Errorf("database is down")
	svc := service.NewJobService(repo, store, slog.Default())
	h := httpapi.NewHandler(testConfig(), svc, slog.Default(), nil)
	r := httpapi.NewRouter(h, slog.Default())

	rr := post(t, r, func(w *multipart.Writer) {
		if err := filePart(w, "file", "a.png", "image/png", pngOf(512)); err != nil {
			t.Fatal(err)
		}
	})
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if store.count() != 0 {
		t.Fatalf("orphan object left behind: %d", store.count())
	}
}

// 5xx responses must not echo internal error chains back to the client.
func TestInternalErrorsAreOpaque(t *testing.T) {
	repo, store := newMemRepo(), newMemStore()
	repo.fail = fmt.Errorf(`pq: relation "jobs" does not exist on host db-primary-1`)
	svc := service.NewJobService(repo, store, slog.Default())
	h := httpapi.NewHandler(testConfig(), svc, slog.Default(), nil)
	r := httpapi.NewRouter(h, slog.Default())

	rr := post(t, r, func(w *multipart.Writer) {
		_ = w.WriteField("text", "hi")
	})
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d", rr.Code)
	}
	if strings.Contains(rr.Body.String(), "db-primary-1") {
		t.Fatalf("internal detail leaked: %s", rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "request_id") {
		t.Fatalf("no request_id to correlate with logs: %s", rr.Body.String())
	}
}

func TestGetJobRoundTrip(t *testing.T) {
	r, _, _ := newTestServer(t, testConfig())

	rr := post(t, r, func(w *multipart.Writer) {
		_ = w.WriteField("text", "lookup me")
	})
	id := decodeJob(t, rr)["id"].(string)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+id, nil)
	got := httptest.NewRecorder()
	r.ServeHTTP(got, req)

	if got.Code != http.StatusOK {
		t.Fatalf("status=%d", got.Code)
	}
	if decodeJob(t, got)["text"] != "lookup me" {
		t.Fatalf("unexpected body: %s", got.Body.String())
	}
}

func TestGetJobNotFound(t *testing.T) {
	r, _, _ := newTestServer(t, testConfig())
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/2a2f0f4c-0000-0000-0000-000000000000", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status=%d", rr.Code)
	}
}

// Upload slots bound resident memory; when they are exhausted the service must
// shed load with 503 rather than queue clients until it is OOM-killed.
func TestUploadConcurrencyIsShed(t *testing.T) {
	cfg := testConfig()
	cfg.MaxConcurrentUploads = 1

	gate := make(chan struct{})
	entered := make(chan struct{})
	store := &blockingStore{memStore: newMemStore(), gate: gate, entered: entered}
	svc := service.NewJobService(newMemRepo(), store, slog.Default())
	h := httpapi.NewHandler(cfg, svc, slog.Default(), nil)
	r := httpapi.NewRouter(h, slog.Default())

	// Built on this goroutine: the request runs in the background, and the
	// helper must not touch t from there.
	var held bytes.Buffer
	mw := multipart.NewWriter(&held)
	if err := filePart(mw, "file", "a.png", "image/png", pngOf(512)); err != nil {
		t.Fatal(err)
	}
	if err := mw.Close(); err != nil {
		t.Fatal(err)
	}
	holdReq := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", &held)
	holdReq.Header.Set("Content-Type", mw.FormDataContentType())

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.ServeHTTP(httptest.NewRecorder(), holdReq)
	}()
	<-entered

	rr := post(t, r, func(w *multipart.Writer) {
		_ = filePart(w, "file", "b.png", "image/png", pngOf(512))
	})
	close(gate)
	<-done

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503, body=%s", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("Retry-After") == "" {
		t.Fatal("503 should advertise Retry-After")
	}
}

type blockingStore struct {
	*memStore
	gate    chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (s *blockingStore) Put(ctx context.Context, key string, body io.Reader, size int64, ct string) (int64, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.gate
	return s.memStore.Put(ctx, key, body, size, ct)
}

func TestServiceEndpoints(t *testing.T) {
	r, _, _ := newTestServer(t, testConfig())
	for _, path := range []string{"/healthz", "/readyz", "/version", "/metrics"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("%s status=%d", path, rr.Code)
		}
	}
}

func TestReadyzReportsDependencyFailure(t *testing.T) {
	svc := service.NewJobService(newMemRepo(), newMemStore(), slog.Default())
	h := httpapi.NewHandler(testConfig(), svc, slog.Default(), func(*http.Request) error {
		return fmt.Errorf("postgres: connection refused")
	})
	r := httpapi.NewRouter(h, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", rr.Code)
	}
}
