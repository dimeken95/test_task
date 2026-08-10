package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/dimeken95/test_task/internal/config"
)

func base() config.Config {
	return config.Config{
		AppMode:              config.ModeAll,
		DatabaseURL:          "postgres://localhost/db",
		S3Bucket:             "payloads",
		MockProcessorURL:     "http://localhost:8090",
		DBMaxConns:           10,
		WorkerConcurrency:    4,
		WorkerClaimBatch:     4,
		WorkerPollInterval:   time.Second,
		WorkerLease:          2 * time.Minute,
		MaxAttempts:          5,
		MaxConcurrentUploads: 16,
		S3PartConcurrency:    4,
		S3PartSize:           8 << 20,
		S3MultipartThreshold: 16 << 20,
		RetryBackoffBase:     5 * time.Second,
		RetryBackoffMax:      10 * time.Minute,
	}
}

func TestValidateAcceptsDefaults(t *testing.T) {
	if err := base().Validate(); err != nil {
		t.Fatalf("baseline config rejected: %v", err)
	}
}

func TestValidateRejectsBadConfig(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*config.Config)
		want   string
	}{
		{"unknown mode", func(c *config.Config) { c.AppMode = "primary" }, "APP_MODE"},
		{"missing database", func(c *config.Config) { c.DatabaseURL = "" }, "DATABASE_URL"},
		{"missing bucket", func(c *config.Config) { c.S3Bucket = "" }, "S3_BUCKET"},
		{"worker without processor", func(c *config.Config) { c.MockProcessorURL = "" }, "MOCK_PROCESSOR_URL"},
		// S3 rejects non-final parts under 5 MiB, so catching it at startup
		// beats discovering it on the first large upload in production.
		{"part size below s3 minimum", func(c *config.Config) { c.S3PartSize = 1 << 20 }, "S3_PART_SIZE"},
		{"threshold below part size", func(c *config.Config) { c.S3MultipartThreshold = 1 << 20 }, "S3_MULTIPART_THRESHOLD"},
		// A lease shorter than a couple of poll cycles means the reaper steals
		// jobs from workers that are still running them.
		{"lease shorter than poll", func(c *config.Config) { c.WorkerLease = time.Second }, "WORKER_LEASE"},
		{"zero concurrency", func(c *config.Config) { c.WorkerConcurrency = 0 }, "WORKER_CONCURRENCY"},
		{"zero uploads", func(c *config.Config) { c.MaxConcurrentUploads = 0 }, "MAX_CONCURRENT_UPLOADS"},
		{"backoff max below base", func(c *config.Config) { c.RetryBackoffMax = time.Second }, "RETRY_BACKOFF_MAX"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			tc.mutate(&cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

func TestAPIModeDoesNotRequireProcessor(t *testing.T) {
	cfg := base()
	cfg.AppMode = config.ModeAPI
	cfg.MockProcessorURL = ""
	if err := cfg.Validate(); err != nil {
		t.Fatalf("api-only config rejected: %v", err)
	}
	if cfg.RunsWorker() {
		t.Fatal("api mode must not run the worker")
	}
	if !cfg.RunsAPI() {
		t.Fatal("api mode must run the api")
	}
}

func TestModePredicates(t *testing.T) {
	cases := []struct {
		mode              string
		wantAPI, wantWork bool
	}{
		{config.ModeAll, true, true},
		{config.ModeAPI, true, false},
		{config.ModeWorker, false, true},
	}
	for _, tc := range cases {
		cfg := base()
		cfg.AppMode = tc.mode
		if cfg.RunsAPI() != tc.wantAPI || cfg.RunsWorker() != tc.wantWork {
			t.Fatalf("%s: api=%v worker=%v", tc.mode, cfg.RunsAPI(), cfg.RunsWorker())
		}
	}
}

// The memory ceiling of the ingest path should be a number we can point at
// when sizing the container limit, not a guess.
func TestPeakUploadBytesIsDerivedFromKnobs(t *testing.T) {
	cfg := base()
	cfg.S3PartSize = 8 << 20
	cfg.S3PartConcurrency = 4
	cfg.MaxConcurrentUploads = 2

	// (1 producer + 2*4 queued/in-flight) parts * 8 MiB * 2 uploads
	want := int64(9) * (8 << 20) * 2
	if got := cfg.PeakUploadBytes(); got != want {
		t.Fatalf("peak=%d want %d", got, want)
	}
}

func TestLoadNormalisesMode(t *testing.T) {
	t.Setenv("APP_MODE", "  Worker  ")
	t.Setenv("DATABASE_URL", "postgres://localhost/db")
	t.Setenv("MOCK_PROCESSOR_URL", "http://localhost:8090")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.AppMode != config.ModeWorker {
		t.Fatalf("AppMode=%q; mixed-case env must not silently disable the worker", cfg.AppMode)
	}
	if !cfg.RunsWorker() {
		t.Fatal("worker did not start for APP_MODE=Worker")
	}
}

func TestLoadDerivesDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/db")
	t.Setenv("S3_ENDPOINT", "http://minio:9000")
	t.Setenv("WORKER_CONCURRENCY", "7")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.S3PresignEndpoint != "http://minio:9000" {
		t.Fatalf("presign endpoint should default to the s3 endpoint, got %q", cfg.S3PresignEndpoint)
	}
	if cfg.WorkerClaimBatch != 7 {
		t.Fatalf("claim batch should default to concurrency, got %d", cfg.WorkerClaimBatch)
	}
}
