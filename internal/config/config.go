package config

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// Mode selects which halves of the binary are started.
const (
	ModeAPI    = "api"
	ModeWorker = "worker"
	ModeAll    = "all"
)

type Config struct {
	AppMode string // api | worker | all

	HTTPAddr string

	DatabaseURL string
	DBMaxConns  int32

	S3Endpoint           string
	S3Region             string
	S3Bucket             string
	S3AccessKey          string
	S3SecretKey          string
	S3UsePathStyle       bool
	S3PresignEndpoint    string // public endpoint for presigned URLs (reachable by the processor)
	S3PresignTTL         time.Duration
	S3MultipartThreshold int64
	S3PartSize           int64
	S3PartConcurrency    int

	MockProcessorURL string
	MockTimeout      time.Duration
	MockMaxRetries   int

	WorkerID           string
	WorkerPollInterval time.Duration
	WorkerLease        time.Duration
	WorkerConcurrency  int
	WorkerClaimBatch   int
	ReaperInterval     time.Duration
	MaxAttempts        int
	RetryBackoffBase   time.Duration
	RetryBackoffMax    time.Duration

	MaxTextBytes         int64
	MaxDocBytes          int64
	MaxImageBytes        int64
	MaxVideoBytes        int64
	MaxConcurrentUploads int

	// APIKeys guards /api/v1. Empty disables authentication, which keeps the
	// demo stack usable; startup logs a warning when that is the case.
	APIKeys []string

	OTelEndpoint string
	OTelInsecure bool
	ServiceName  string
	LogLevel     string

	ShutdownTimeout time.Duration
	DrainDelay      time.Duration

	// MigrateOnly runs migrations and exits. Used by the Kubernetes migration
	// Job so schema changes land once, before any replica starts.
	MigrateOnly bool
}

func Load() (Config, error) {
	cfg := Config{
		AppMode:              strings.ToLower(strings.TrimSpace(getenv("APP_MODE", ModeAll))),
		HTTPAddr:             getenv("HTTP_ADDR", ":8080"),
		DatabaseURL:          os.Getenv("DATABASE_URL"),
		DBMaxConns:           getenvInt32("DB_MAX_CONNS", 20),
		S3Endpoint:           getenv("S3_ENDPOINT", "http://localhost:9000"),
		S3Region:             getenv("S3_REGION", "us-east-1"),
		S3Bucket:             getenv("S3_BUCKET", "payloads"),
		S3AccessKey:          getenv("S3_ACCESS_KEY", "minioadmin"),
		S3SecretKey:          getenv("S3_SECRET_KEY", "minioadmin"),
		S3UsePathStyle:       getenvBool("S3_USE_PATH_STYLE", true),
		S3PresignEndpoint:    getenv("S3_PRESIGN_ENDPOINT", ""),
		S3PresignTTL:         getenvDuration("S3_PRESIGN_TTL", 15*time.Minute),
		S3MultipartThreshold: getenvInt64("S3_MULTIPART_THRESHOLD", 16<<20), // 16 MiB
		S3PartSize:           getenvInt64("S3_PART_SIZE", 8<<20),            // 8 MiB
		S3PartConcurrency:    getenvInt("S3_PART_CONCURRENCY", 4),
		MockProcessorURL:     getenv("MOCK_PROCESSOR_URL", "http://localhost:8090"),
		MockTimeout:          getenvDuration("MOCK_TIMEOUT", 30*time.Second),
		MockMaxRetries:       getenvInt("MOCK_MAX_RETRIES", 3),
		WorkerID:             getenv("WORKER_ID", hostnameOr("worker")),
		WorkerPollInterval:   getenvDuration("WORKER_POLL_INTERVAL", 1*time.Second),
		WorkerLease:          getenvDuration("WORKER_LEASE", 2*time.Minute),
		WorkerConcurrency:    getenvInt("WORKER_CONCURRENCY", 4),
		WorkerClaimBatch:     getenvInt("WORKER_CLAIM_BATCH", 0), // 0 → derived from concurrency
		ReaperInterval:       getenvDuration("REAPER_INTERVAL", 15*time.Second),
		MaxAttempts:          getenvInt("MAX_ATTEMPTS", 5),
		RetryBackoffBase:     getenvDuration("RETRY_BACKOFF_BASE", 5*time.Second),
		RetryBackoffMax:      getenvDuration("RETRY_BACKOFF_MAX", 10*time.Minute),
		MaxTextBytes:         getenvInt64("MAX_TEXT_BYTES", 1<<20),   // 1 MiB
		MaxDocBytes:          getenvInt64("MAX_DOC_BYTES", 20<<20),   // 20 MiB
		MaxImageBytes:        getenvInt64("MAX_IMAGE_BYTES", 10<<20), // 10 MiB
		MaxVideoBytes:        getenvInt64("MAX_VIDEO_BYTES", 200<<20),
		MaxConcurrentUploads: getenvInt("MAX_CONCURRENT_UPLOADS", 16),
		APIKeys:              splitList(os.Getenv("API_KEYS")),
		OTelEndpoint:         getenv("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		OTelInsecure:         getenvBool("OTEL_EXPORTER_OTLP_INSECURE", true),
		ServiceName:          getenv("SERVICE_NAME", "payload-service"),
		LogLevel:             strings.ToLower(getenv("LOG_LEVEL", "info")),
		ShutdownTimeout:      getenvDuration("SHUTDOWN_TIMEOUT", 25*time.Second),
		DrainDelay:           getenvDuration("DRAIN_DELAY", 3*time.Second),
		MigrateOnly:          getenvBool("MIGRATE_ONLY", false),
	}

	if cfg.S3PresignEndpoint == "" {
		cfg.S3PresignEndpoint = cfg.S3Endpoint
	}
	if cfg.WorkerClaimBatch <= 0 {
		cfg.WorkerClaimBatch = cfg.WorkerConcurrency
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) RunsAPI() bool    { return c.AppMode == ModeAPI || c.AppMode == ModeAll }
func (c Config) RunsWorker() bool { return c.AppMode == ModeWorker || c.AppMode == ModeAll }

// PeakUploadBytes is the worst-case resident memory of concurrent uploads:
// each in-flight multipart upload holds the producer buffer, the queue and the
// part workers. It is reported at startup so the pod memory limit can be sized
// against a real number instead of a guess.
func (c Config) PeakUploadBytes() int64 {
	perUpload := c.S3PartSize * int64(1+2*c.S3PartConcurrency)
	return perUpload * int64(c.MaxConcurrentUploads)
}

func (c Config) Validate() error {
	switch c.AppMode {
	case ModeAPI, ModeWorker, ModeAll:
	default:
		return fmt.Errorf("APP_MODE must be api|worker|all, got %q", c.AppMode)
	}
	if c.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	if c.S3Bucket == "" {
		return fmt.Errorf("S3_BUCKET is required")
	}
	if c.RunsWorker() && c.MockProcessorURL == "" {
		return fmt.Errorf("MOCK_PROCESSOR_URL is required for worker mode")
	}
	if c.WorkerConcurrency < 1 {
		return fmt.Errorf("WORKER_CONCURRENCY must be >= 1")
	}
	if c.WorkerClaimBatch < 1 {
		return fmt.Errorf("WORKER_CLAIM_BATCH must be >= 1")
	}
	if c.MaxAttempts < 1 {
		return fmt.Errorf("MAX_ATTEMPTS must be >= 1")
	}
	if c.MaxConcurrentUploads < 1 {
		return fmt.Errorf("MAX_CONCURRENT_UPLOADS must be >= 1")
	}
	if c.S3PartConcurrency < 1 {
		return fmt.Errorf("S3_PART_CONCURRENCY must be >= 1")
	}
	if c.S3PartSize < 5<<20 {
		return fmt.Errorf("S3_PART_SIZE must be >= 5MiB (S3 requirement for non-last parts)")
	}
	if c.S3MultipartThreshold < c.S3PartSize {
		return fmt.Errorf("S3_MULTIPART_THRESHOLD must be >= S3_PART_SIZE")
	}
	if c.DBMaxConns < 1 {
		return fmt.Errorf("DB_MAX_CONNS must be >= 1")
	}
	if c.WorkerLease <= 2*c.WorkerPollInterval {
		return fmt.Errorf("WORKER_LEASE (%s) must exceed 2x WORKER_POLL_INTERVAL (%s)", c.WorkerLease, c.WorkerPollInterval)
	}
	if c.RetryBackoffMax < c.RetryBackoffBase {
		return fmt.Errorf("RETRY_BACKOFF_MAX must be >= RETRY_BACKOFF_BASE")
	}
	return nil
}

// splitList parses a comma-separated env var, dropping blanks.
func splitList(v string) []string {
	var out []string
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func getenvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func getenvInt32(key string, def int32) int32 {
	n := getenvInt64(key, int64(def))
	if n < 1 || n > math.MaxInt32 {
		return def
	}
	return int32(n)
}

func getenvInt64(key string, def int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}

func getenvDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func hostnameOr(def string) string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return def
	}
	return h
}
