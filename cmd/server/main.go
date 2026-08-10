// Command server runs the payload service. One binary serves both halves:
// APP_MODE selects the HTTP API, the job worker, or both.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	httpapi "github.com/dimeken95/test_task/internal/api/http"
	"github.com/dimeken95/test_task/internal/buildinfo"
	"github.com/dimeken95/test_task/internal/config"
	"github.com/dimeken95/test_task/internal/domain"
	"github.com/dimeken95/test_task/internal/observability"
	"github.com/dimeken95/test_task/internal/processor"
	"github.com/dimeken95/test_task/internal/service"
	"github.com/dimeken95/test_task/internal/storage/postgres"
	s3store "github.com/dimeken95/test_task/internal/storage/s3"
	"github.com/dimeken95/test_task/internal/worker"
)

func main() {
	if err := run(); err != nil {
		// The logger may not exist yet if config failed.
		fmt.Fprintf(os.Stderr, `{"level":"ERROR","msg":"startup failed","err":%q}`+"\n", err.Error())
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	logger := observability.NewLogger(cfg.LogLevel)
	observability.SetBuildInfo(cfg.AppMode)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := observability.SetupTracing(ctx, cfg)
	if err != nil {
		return fmt.Errorf("tracing: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(shutdownCtx)
	}()

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return fmt.Errorf("database: %w", err)
	}
	defer pool.Close()

	if mErr := postgres.Migrate(ctx, pool, migrationsDir()); mErr != nil {
		return fmt.Errorf("migrate: %w", mErr)
	}
	if cfg.MigrateOnly {
		logger.Info("migrations applied, exiting (MIGRATE_ONLY)")
		return nil
	}

	store, err := s3store.New(cfg)
	if err != nil {
		return fmt.Errorf("object store: %w", err)
	}
	if err := store.EnsureBucket(ctx); err != nil {
		return fmt.Errorf("ensure bucket: %w", err)
	}

	repo := postgres.NewJobRepo(pool)
	jobs := service.NewJobService(repo, store, logger)

	logger.Info("starting",
		"version", buildinfo.Version,
		"commit", buildinfo.Commit,
		"mode", cfg.AppMode,
		"worker_concurrency", cfg.WorkerConcurrency,
		"max_concurrent_uploads", cfg.MaxConcurrentUploads,
		"peak_upload_memory_mib", cfg.PeakUploadBytes()>>20,
		"api_keys_configured", len(cfg.APIKeys),
	)
	if cfg.RunsAPI() && len(cfg.APIKeys) == 0 {
		logger.Warn("API authentication is disabled; set API_KEYS to require a key on /api/v1")
	}

	var w *worker.Worker
	if cfg.RunsWorker() {
		proc := processor.NewHTTPClient(cfg.MockProcessorURL, cfg.MockTimeout, cfg.MockMaxRetries)
		w = worker.New(cfg, repo, store, proc, logger)
		w.Start(ctx)

		// Queue depth is only meaningful once, so the workers publish it
		// rather than every api replica running the same COUNT.
		observability.StartPendingGauge(ctx, func(c context.Context) (int64, error) {
			return repo.CountByStatus(c, domain.StatusPending)
		}, 5*time.Second)
	}

	// draining flips before the HTTP server stops so /readyz starts failing
	// and the load balancer removes this pod while in-flight requests finish.
	var draining atomic.Bool
	h := httpapi.NewHandler(cfg, jobs, logger, func(r *http.Request) error {
		if draining.Load() {
			return errors.New("shutting down")
		}
		c, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := repo.Ping(c); err != nil {
			return fmt.Errorf("postgres: %w", err)
		}
		if err := store.Ping(c); err != nil {
			return fmt.Errorf("object store: %w", err)
		}
		return nil
	})

	srv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: httpapi.NewRouter(h, logger),
		// No WriteTimeout: uploads legitimately run for minutes. ReadHeaderTimeout
		// still closes the slow-loris hole, and MaxBytesReader bounds the body.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog:          slogErrorLog(logger),
	}

	srvErr := make(chan error, 1)
	go func() {
		logger.Info("http listening", "addr", cfg.HTTPAddr, "mode", cfg.AppMode)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			srvErr <- err
		}
	}()

	select {
	case err := <-srvErr:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}

	return shutdown(context.Background(), cfg, logger, srv, w, &draining)
}

// shutdown drains in the order that keeps requests alive: stop advertising
// readiness, let the balancer notice, close the HTTP server, then wind down
// the worker so in-flight jobs can finish and record their result.
func shutdown(
	ctx context.Context,
	cfg config.Config,
	logger *slog.Logger,
	srv *http.Server,
	w *worker.Worker,
	draining *atomic.Bool,
) error {
	logger.Info("shutting down", "drain_delay", cfg.DrainDelay, "timeout", cfg.ShutdownTimeout)
	draining.Store(true)

	shutdownCtx, cancel := context.WithTimeout(ctx, cfg.ShutdownTimeout)
	defer cancel()

	// Give kube-proxy / the LB time to observe the failing readiness probe
	// before we stop accepting connections.
	select {
	case <-time.After(cfg.DrainDelay):
	case <-shutdownCtx.Done():
	}

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("http shutdown", "err", err)
	}
	if w != nil {
		w.Stop(shutdownCtx)
	}
	logger.Info("shutdown complete")
	return nil
}

func slogErrorLog(logger *slog.Logger) *log.Logger {
	return log.New(slogWriter{logger}, "", 0)
}

type slogWriter struct{ l *slog.Logger }

func (w slogWriter) Write(p []byte) (int, error) {
	w.l.Warn("http server", "msg", strings.TrimSpace(string(p)))
	return len(p), nil
}

// migrationsDir resolves the migrations folder for both container and local runs.
func migrationsDir() string {
	if dir := os.Getenv("MIGRATIONS_DIR"); dir != "" {
		return dir
	}
	for _, c := range []string{"migrations", "/app/migrations"} {
		if st, err := os.Stat(c); err == nil && st.IsDir() {
			if abs, err := filepath.Abs(c); err == nil {
				return abs
			}
			return c
		}
	}
	return "migrations"
}
