package httpapi_test

import (
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func submitWithKey(t *testing.T, r http.Handler, key string, build func(*multipart.Writer)) *httptest.ResponseRecorder {
	t.Helper()
	req := multipartRequest(t, build)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	return rr
}

// A client that times out and retries must get its original job back, not a
// second one.
func TestIdempotentSubmitReturnsTheSameJob(t *testing.T) {
	r, _, store := newTestServer(t, testConfig())

	first := submitWithKey(t, r, "order-4711", func(w *multipart.Writer) {
		_ = w.WriteField("text", "invoice")
		_ = filePart(w, "file", "a.png", "image/png", pngOf(1024))
	})
	if first.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", first.Code, first.Body.String())
	}
	if first.Header().Get("Idempotency-Replayed") != "" {
		t.Error("the first submission is not a replay")
	}

	second := submitWithKey(t, r, "order-4711", func(w *multipart.Writer) {
		_ = w.WriteField("text", "invoice")
		_ = filePart(w, "file", "a.png", "image/png", pngOf(1024))
	})
	if second.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", second.Code, second.Body.String())
	}
	if second.Header().Get("Idempotency-Replayed") != "true" {
		t.Error("replay should be flagged so the client knows nothing was created")
	}

	firstID := decodeJob(t, first)["id"]
	if got := decodeJob(t, second)["id"]; got != firstID {
		t.Fatalf("retry created a different job: %v vs %v", got, firstID)
	}
	// The retry must not leave a second copy of the payload behind either.
	if store.count() != 1 {
		t.Fatalf("objects=%d want 1", store.count())
	}
}

func TestDifferentKeysCreateDifferentJobs(t *testing.T) {
	r, _, _ := newTestServer(t, testConfig())

	a := submitWithKey(t, r, "key-a", func(w *multipart.Writer) { _ = w.WriteField("text", "one") })
	b := submitWithKey(t, r, "key-b", func(w *multipart.Writer) { _ = w.WriteField("text", "two") })

	if decodeJob(t, a)["id"] == decodeJob(t, b)["id"] {
		t.Fatal("distinct keys must produce distinct jobs")
	}
}

// Without a key every submission is independent, which keeps the endpoint
// usable for clients that do not care.
func TestSubmissionsWithoutKeyAreIndependent(t *testing.T) {
	r, _, _ := newTestServer(t, testConfig())

	a := post(t, r, func(w *multipart.Writer) { _ = w.WriteField("text", "same") })
	b := post(t, r, func(w *multipart.Writer) { _ = w.WriteField("text", "same") })

	if decodeJob(t, a)["id"] == decodeJob(t, b)["id"] {
		t.Fatal("keyless submissions must not be deduplicated")
	}
}

// Two retries racing must still yield exactly one job; the fast-path lookup
// can miss, so the unique index has to settle it.
func TestConcurrentRetriesCreateOneJob(t *testing.T) {
	r, _, store := newTestServer(t, testConfig())

	const n = 8
	ids := make([]string, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rr := submitWithKey(t, r, "racing-key", func(w *multipart.Writer) {
				_ = filePart(w, "file", "a.png", "image/png", pngOf(512))
			})
			if rr.Code == http.StatusAccepted {
				if id, ok := decodeJob(t, rr)["id"].(string); ok {
					ids[i] = id
				}
			}
		}(i)
	}
	wg.Wait()

	distinct := map[string]bool{}
	for _, id := range ids {
		if id != "" {
			distinct[id] = true
		}
	}
	if len(distinct) != 1 {
		t.Fatalf("expected exactly one job, got %d distinct ids", len(distinct))
	}
	if store.count() != 1 {
		t.Fatalf("objects=%d want 1: losers of the race must clean up their upload", store.count())
	}
}

func TestOversizedIdempotencyKeyRejected(t *testing.T) {
	r, _, _ := newTestServer(t, testConfig())

	rr := submitWithKey(t, r, strings.Repeat("k", 300), func(w *multipart.Writer) {
		_ = w.WriteField("text", "hi")
	})
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want 400, body=%s", rr.Code, rr.Body.String())
	}
}
