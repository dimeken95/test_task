package processor_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dimeken95/test_task/internal/domain"
	"github.com/dimeken95/test_task/internal/processor"
)

func TestHTTPClientRetries5xx(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n < 3 {
			http.Error(w, "fail", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"summary":"ok","processed_at":"2024-01-01T00:00:00Z"}`))
	}))
	defer srv.Close()

	c := processor.NewHTTPClient(srv.URL, 2*time.Second, 3)
	res, err := c.Process(context.Background(), domain.ProcessInput{JobID: "j1", Text: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Summary != "ok" {
		t.Fatalf("summary=%q", res.Summary)
	}
	if calls.Load() != 3 {
		t.Fatalf("calls=%d", calls.Load())
	}
}

func TestHTTPClientNoRetry4xx(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "bad", http.StatusBadRequest)
	}))
	defer srv.Close()

	c := processor.NewHTTPClient(srv.URL, time.Second, 3)
	_, err := c.Process(context.Background(), domain.ProcessInput{JobID: "j1"})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls.Load() != 1 {
		t.Fatalf("calls=%d want 1", calls.Load())
	}
}
