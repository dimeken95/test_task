// Command mockprocessor stands in for the external payload-processing service.
// It is deliberately a separate process reached over HTTP so the boundary in
// the real system — network call, own failure modes, own scaling — is present
// in the assignment too.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/dimeken95/test_task/internal/observability"
)

// maxDownloadBytes bounds what the mock will pull from a presigned URL.
const maxDownloadBytes = 512 << 20

type processRequest struct {
	JobID       string `json:"job_id"`
	Text        string `json:"text"`
	ContentType string `json:"content_type"`
	SizeBytes   int64  `json:"size_bytes"`
	DownloadURL string `json:"download_url"`
}

type processResponse struct {
	Summary     string    `json:"summary"`
	ProcessedAt time.Time `json:"processed_at"`
}

type server struct {
	logger     *slog.Logger
	downloader *http.Client

	// results memoises completed jobs. The pipeline is at-least-once, so the
	// processor must tolerate seeing the same job id twice. Bounded FIFO:
	// an unbounded map would be a slow leak in a long-running demo.
	mu      sync.Mutex
	results map[string]string
	order   []string
}

// maxRememberedJobs bounds the idempotency cache. Replays arrive within a
// retry window, so forgetting the oldest entries is harmless.
const maxRememberedJobs = 10_000

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("mock processor failed", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	addr := getenv("HTTP_ADDR", ":8090")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Traces under its own service name, so a single Jaeger trace shows the
	// hop across the external-service boundary.
	shutdownTracing, err := observability.SetupTracingFor(
		ctx,
		getenv("SERVICE_NAME", "mock-processor"),
		"mock",
		os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		getenv("OTEL_EXPORTER_OTLP_INSECURE", "true") == "true",
	)
	if err != nil {
		return fmt.Errorf("tracing: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(shutdownCtx)
	}()

	s := &server{
		logger: logger,
		downloader: &http.Client{
			Timeout: 5 * time.Minute,
			// Instrumented so the presigned GET appears as a child span of the
			// processing request.
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		},
		results: make(map[string]string),
	}

	r := chi.NewRouter()
	r.Use(chimw.Recoverer)
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	r.Post("/v1/process", s.process)

	srv := &http.Server{
		Addr:              addr,
		Handler:           otelhttp.NewHandler(r, "mock-processor"),
		ReadHeaderTimeout: 10 * time.Second,
	}

	srvErr := make(chan error, 1)
	go func() {
		logger.Info("mock processor listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			srvErr <- err
		}
	}()

	select {
	case err := <-srvErr:
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func (s *server) process(w http.ResponseWriter, r *http.Request) {
	// Failure injection: lets the retry/backoff path be demonstrated on demand.
	if r.Header.Get("X-Mock-Fail") == "1" || r.URL.Query().Get("fail") == "1" {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "forced failure"})
		return
	}
	if ms, err := strconv.Atoi(r.URL.Query().Get("delay_ms")); err == nil && ms > 0 {
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
		case <-r.Context().Done():
			return
		}
	}

	var req processRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "malformed json"})
		return
	}
	if req.JobID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "job_id is required"})
		return
	}

	if summary, ok := s.cached(req.JobID); ok {
		s.logger.InfoContext(r.Context(), "idempotent replay", "job_id", req.JobID)
		writeJSON(w, http.StatusOK, processResponse{Summary: summary, ProcessedAt: time.Now().UTC()})
		return
	}

	var downloaded int64
	if req.DownloadURL != "" {
		n, err := s.download(r.Context(), req.DownloadURL)
		if err != nil {
			s.logger.ErrorContext(r.Context(), "download failed", "job_id", req.JobID, "err", err)
			// 502 is retryable for the caller: the object may still appear.
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "payload download failed"})
			return
		}
		downloaded = n
	}

	summary := buildSummary(req, downloaded)
	s.remember(req.JobID, summary)

	s.logger.InfoContext(r.Context(), "processed", "job_id", req.JobID, "summary", summary)
	writeJSON(w, http.StatusOK, processResponse{Summary: summary, ProcessedAt: time.Now().UTC()})
}

// download fetches the payload through the short-lived presigned URL. The mock
// never sees our object-store credentials.
func (s *server) download(ctx context.Context, url string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := s.downloader.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		return 0, fmt.Errorf("presigned GET returned %d", resp.StatusCode)
	}
	return io.Copy(io.Discard, io.LimitReader(resp.Body, maxDownloadBytes))
}

func buildSummary(req processRequest, downloaded int64) string {
	summary := "processed"
	if req.Text != "" {
		summary += " text_len=" + strconv.Itoa(len(req.Text))
	}
	if req.ContentType != "" {
		summary += " content_type=" + req.ContentType
	}
	if downloaded > 0 {
		summary += " downloaded_bytes=" + strconv.FormatInt(downloaded, 10)
	}
	return summary
}

func (s *server) cached(jobID string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.results[jobID]
	return v, ok
}

func (s *server) remember(jobID, summary string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, seen := s.results[jobID]; seen {
		return
	}
	if len(s.order) >= maxRememberedJobs {
		delete(s.results, s.order[0])
		s.order = s.order[1:]
	}
	s.order = append(s.order, jobID)
	s.results[jobID] = summary
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
