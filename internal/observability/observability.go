package observability

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/dimeken95/test_task/internal/buildinfo"
	"github.com/dimeken95/test_task/internal/config"
)

// RED signals for the HTTP surface, labelled by chi route pattern so job ids
// never leak into metric cardinality.
var (
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "payload_http_requests_total",
		Help: "HTTP requests by method, route and status",
	}, []string{"method", "route", "status"})

	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "payload_http_request_duration_seconds",
		Help:    "HTTP request latency",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
	}, []string{"method", "route"})

	HTTPResponseBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "payload_http_response_bytes",
		Help:    "HTTP response size",
		Buckets: prometheus.ExponentialBuckets(128, 4, 8),
	}, []string{"method", "route"})
)

// Ingest and queue signals.
var (
	UploadsInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "payload_uploads_in_flight",
		Help: "Uploads currently holding an object-store buffer slot",
	})
	UploadsRejected = promauto.NewCounter(prometheus.CounterOpts{
		Name: "payload_uploads_rejected_total",
		Help: "Uploads shed because the concurrency limit was saturated",
	})
	JobsClaimed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "payload_jobs_claimed_total",
		Help: "Jobs claimed by workers",
	})
	JobsRetried = promauto.NewCounter(prometheus.CounterOpts{
		Name: "payload_jobs_retried_total",
		Help: "Jobs rescheduled after a transient failure",
	})
	JobsReaped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "payload_jobs_reaped_total",
		Help: "Jobs requeued after lease expiry",
	})
	ProcessorErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "payload_processor_errors_total",
		Help: "Processor call failures",
	})
	JobDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "payload_job_process_duration_seconds",
		Help:    "Job processing duration",
		Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300},
	}, []string{"status"})

	// PendingJobs is the queue depth. It is the correct autoscaling signal for
	// workers, which are I/O bound and barely move the CPU metric.
	PendingJobs = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "payload_jobs_pending",
		Help: "Jobs waiting to be claimed",
	})

	S3MultipartParts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "payload_s3_multipart_parts_total",
		Help: "S3 multipart parts uploaded",
	})
	S3UploadBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "payload_s3_upload_bytes_total",
		Help: "Bytes uploaded to object storage",
	})
	S3UploadDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "payload_s3_upload_duration_seconds",
		Help:    "Object upload duration",
		Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120},
	}, []string{"mode", "result"})

	buildInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "payload_build_info",
		Help: "Build metadata; value is always 1",
	}, []string{"version", "commit", "mode"})
)

func SetBuildInfo(mode string) {
	buildInfo.WithLabelValues(buildinfo.Version, buildinfo.Commit, mode).Set(1)
}

func ObserveJobDuration(status string, d time.Duration) {
	JobDuration.WithLabelValues(status).Observe(d.Seconds())
}

func ObserveS3Upload(mode string, d time.Duration, ok bool) {
	result := "ok"
	if !ok {
		result = "error"
	}
	S3UploadDuration.WithLabelValues(mode, result).Observe(d.Seconds())
}

// NewLogger emits JSON to stdout and stamps every record that carries a span
// with its trace id, so logs and traces join on one field.
func NewLogger(level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	base := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(&traceHandler{Handler: base})
}

type traceHandler struct{ slog.Handler }

func (h *traceHandler) Handle(ctx context.Context, rec slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		rec.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, rec)
}

func (h *traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *traceHandler) WithGroup(name string) slog.Handler {
	return &traceHandler{Handler: h.Handler.WithGroup(name)}
}

func SetupTracing(ctx context.Context, cfg config.Config) (func(context.Context) error, error) {
	return SetupTracingFor(ctx, cfg.ServiceName, cfg.AppMode, cfg.OTelEndpoint, cfg.OTelInsecure)
}

// SetupTracingFor installs the global tracer provider and propagator. It is
// exported so the mock processor traces under its own service name and the
// distributed trace continues across the external-service boundary.
func SetupTracingFor(ctx context.Context, serviceName, environment, endpoint string, insecure bool) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	res, err := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceName(serviceName),
		semconv.ServiceVersion(buildinfo.Version),
		semconv.DeploymentEnvironment(environment),
	))
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}

	// No collector configured (unit tests, local runs): keep a no-op provider
	// so instrumentation code paths still execute without exporting.
	if endpoint == "" {
		tp := sdktrace.NewTracerProvider(sdktrace.WithResource(res))
		otel.SetTracerProvider(tp)
		return tp.Shutdown, nil
	}

	opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(endpoint)}
	if insecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	exp, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// StartPendingGauge publishes queue depth for dashboards and autoscaling.
func StartPendingGauge(ctx context.Context, countFn func(context.Context) (int64, error), interval time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				n, err := countFn(ctx)
				if err != nil {
					continue
				}
				PendingJobs.Set(float64(n))
			}
		}
	}()
}
