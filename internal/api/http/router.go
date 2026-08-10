package httpapi

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"

	"github.com/dimeken95/test_task/internal/observability"
)

func NewRouter(h *Handler, logger *slog.Logger) http.Handler {
	r := chi.NewRouter()
	r.Use(chimw.RequestID)
	// Deliberately no RealIP middleware: it rewrites RemoteAddr from
	// X-Forwarded-For / X-Real-IP whether or not a trusted proxy set them, so
	// any client can forge its address (GHSA-3fxj-6jh8-hvhx). Client IPs
	// belong to the ingress, which is the only component that knows which
	// proxies are trustworthy.
	r.Use(chimw.Recoverer)
	r.Use(metrics())
	r.Use(accessLog(logger))

	r.Get("/healthz", h.Healthz)
	r.Get("/readyz", h.Readyz)
	r.Get("/version", h.Version)
	r.Handle("/metrics", promhttp.Handler())

	r.Route("/api/v1", func(r chi.Router) {
		r.Use(apiKeyAuth(h.cfg.APIKeys, logger))
		r.Post("/jobs", h.CreateJob)
		r.Get("/jobs/{id}", h.GetJob)
	})

	return otelhttp.NewHandler(r, "payload-service",
		otelhttp.WithFilter(func(req *http.Request) bool {
			switch req.URL.Path {
			case "/healthz", "/readyz", "/metrics":
				return false
			}
			return true
		}),
	)
}

// metrics records RED signals per route. The chi route pattern (not the raw
// path) is used as the label so job ids cannot explode metric cardinality.
func metrics() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/metrics" {
				next.ServeHTTP(w, r)
				return
			}
			start := time.Now()
			ww := chimw.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)

			route := routePattern(r)
			observability.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(ww.Status())).Inc()
			observability.HTTPDuration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
			observability.HTTPResponseBytes.WithLabelValues(r.Method, route).Observe(float64(ww.BytesWritten()))
		})
	}
}

func accessLog(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := chimw.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)

			attrs := []any{
				"method", r.Method,
				"path", r.URL.Path,
				"route", routePattern(r),
				"status", ww.Status(),
				"bytes", ww.BytesWritten(),
				"latency_ms", time.Since(start).Milliseconds(),
				"request_id", chimw.GetReqID(r.Context()),
			}
			// Emitting the trace id makes every log line a one-click jump into
			// the matching Jaeger trace.
			if sc := trace.SpanContextFromContext(r.Context()); sc.IsValid() {
				attrs = append(attrs, "trace_id", sc.TraceID().String(), "span_id", sc.SpanID().String())
			}
			logger.InfoContext(r.Context(), "http_request", attrs...)
		})
	}
}

func routePattern(r *http.Request) string {
	if rctx := chi.RouteContext(r.Context()); rctx != nil {
		if p := rctx.RoutePattern(); p != "" {
			return p
		}
	}
	return "unmatched"
}
