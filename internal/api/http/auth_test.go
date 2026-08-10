package httpapi_test

import (
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	httpapi "github.com/dimeken95/test_task/internal/api/http"
	"github.com/dimeken95/test_task/internal/service"
)

func guardedServer(t *testing.T, keys ...string) http.Handler {
	t.Helper()
	cfg := testConfig()
	cfg.APIKeys = keys
	svc := service.NewJobService(newMemRepo(), newMemStore(), slog.Default())
	return httpapi.NewRouter(httpapi.NewHandler(cfg, svc, slog.Default(), nil), slog.Default())
}

func submitWithHeaders(t *testing.T, r http.Handler, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	req := multipartRequest(t, func(w *multipart.Writer) {
		_ = w.WriteField("text", "guarded")
	})
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	r.ServeHTTP(rr, req)
	return rr
}

func TestAuthRejectsMissingAndWrongKeys(t *testing.T) {
	r := guardedServer(t, "s3cret-key")

	cases := []struct {
		name    string
		headers map[string]string
	}{
		{"no credentials", nil},
		{"wrong bearer", map[string]string{"Authorization": "Bearer nope"}},
		{"wrong x-api-key", map[string]string{"X-API-Key": "nope"}},
		{"empty bearer", map[string]string{"Authorization": "Bearer "}},
		{"wrong scheme", map[string]string{"Authorization": "Basic s3cret-key"}},
		// A valid key in the wrong header must not be accepted just because
		// the value matches somewhere.
		{"key as basic auth", map[string]string{"Authorization": "Basic czNjcmV0LWtleQ=="}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rr := submitWithHeaders(t, r, tc.headers)
			if rr.Code != http.StatusUnauthorized {
				t.Fatalf("status=%d want 401, body=%s", rr.Code, rr.Body.String())
			}
			if rr.Header().Get("WWW-Authenticate") == "" {
				t.Error("401 should advertise WWW-Authenticate")
			}
		})
	}
}

func TestAuthAcceptsEitherHeader(t *testing.T) {
	r := guardedServer(t, "first-key", "second-key")

	cases := []map[string]string{
		{"Authorization": "Bearer first-key"},
		{"Authorization": "Bearer second-key"},
		{"X-API-Key": "first-key"},
		{"X-API-Key": "second-key"},
	}

	for _, headers := range cases {
		rr := submitWithHeaders(t, r, headers)
		if rr.Code != http.StatusAccepted {
			t.Fatalf("%v: status=%d body=%s", headers, rr.Code, rr.Body.String())
		}
	}
}

// Probes and metrics are scraped by kubelet and Prometheus, which cannot carry
// an API key, and they expose no payload data.
func TestAuthLeavesOperationalEndpointsOpen(t *testing.T) {
	r := guardedServer(t, "s3cret-key")

	for _, path := range []string{"/healthz", "/readyz", "/version", "/metrics"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("%s status=%d, operational endpoints must stay reachable", path, rr.Code)
		}
	}
}

// No keys configured means the demo stack works out of the box; the server
// warns about it at startup.
func TestAuthDisabledWhenNoKeysConfigured(t *testing.T) {
	r := guardedServer(t)

	rr := submitWithHeaders(t, r, nil)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestAuthGuardsJobLookup(t *testing.T) {
	r := guardedServer(t, "s3cret-key")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/2a2f0f4c-0000-0000-0000-000000000000", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	// 401 before 404: an unauthenticated caller must not learn which ids exist.
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want 401", rr.Code)
	}
}
