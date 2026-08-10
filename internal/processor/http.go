package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/dimeken95/test_task/internal/domain"
)

var tracer = otel.Tracer("github.com/dimeken95/test_task/processor")

// maxErrorBody bounds how much of a failing response we keep for the message.
const maxErrorBody = 4 << 10

type HTTPClient struct {
	baseURL    string
	httpClient *http.Client
	maxRetries int
}

func NewHTTPClient(baseURL string, timeout time.Duration, maxRetries int) *HTTPClient {
	if maxRetries < 1 {
		maxRetries = 1
	}
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 32, // workers hammer a single upstream host
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	return &HTTPClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout:   timeout,
			Transport: otelhttp.NewTransport(transport),
		},
		maxRetries: maxRetries,
	}
}

type requestBody struct {
	JobID       string `json:"job_id"`
	Text        string `json:"text,omitempty"`
	ContentType string `json:"content_type,omitempty"`
	SizeBytes   int64  `json:"size_bytes,omitempty"`
	DownloadURL string `json:"download_url,omitempty"`
}

type responseBody struct {
	Summary     string    `json:"summary"`
	ProcessedAt time.Time `json:"processed_at"`
}

// Process calls the external processor, retrying transport faults and 5xx with
// exponential backoff and jitter. 4xx responses are wrapped in
// domain.ErrPermanent so the worker stops retrying a payload the upstream will
// never accept.
func (c *HTTPClient) Process(ctx context.Context, in domain.ProcessInput) (domain.ProcessResult, error) {
	ctx, span := tracer.Start(ctx, "Processor.Process")
	defer span.End()
	span.SetAttributes(attribute.String("job.id", in.JobID))

	payload, err := json.Marshal(requestBody{
		JobID:       in.JobID,
		Text:        in.Text,
		ContentType: in.ContentType,
		SizeBytes:   in.SizeBytes,
		DownloadURL: in.DownloadURL,
	})
	if err != nil {
		return domain.ProcessResult{}, fmt.Errorf("%w: encode request: %w", domain.ErrPermanent, err)
	}

	backoff := 200 * time.Millisecond
	for attempt := 1; ; attempt++ {
		res, retryable, err := c.doOnce(ctx, payload)
		if err == nil {
			span.SetAttributes(attribute.Int("processor.attempts", attempt))
			return res, nil
		}

		if !retryable || attempt >= c.maxRetries {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.SetAttributes(attribute.Int("processor.attempts", attempt))
			return domain.ProcessResult{}, err
		}

		// Jitter keeps a fleet of workers from retrying in lockstep after an
		// upstream blip.
		wait := backoff + time.Duration(rand.Int64N(int64(backoff/2)+1))
		select {
		case <-ctx.Done():
			return domain.ProcessResult{}, ctx.Err()
		case <-time.After(wait):
			backoff *= 2
		}
	}
}

func (c *HTTPClient) doOnce(ctx context.Context, payload []byte) (domain.ProcessResult, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/process", bytes.NewReader(payload))
	if err != nil {
		return domain.ProcessResult{}, false, fmt.Errorf("%w: build request: %w", domain.ErrPermanent, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return domain.ProcessResult{}, true, fmt.Errorf("processor request: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body) // drain so the connection is reusable
		_ = resp.Body.Close()
	}()

	switch {
	case resp.StatusCode >= 500, resp.StatusCode == http.StatusTooManyRequests:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		return domain.ProcessResult{}, true, fmt.Errorf("processor status %d: %s", resp.StatusCode, body)
	case resp.StatusCode >= 400:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		return domain.ProcessResult{}, false, fmt.Errorf("%w: processor rejected job with %d: %s",
			domain.ErrPermanent, resp.StatusCode, body)
	}

	var out responseBody
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out); err != nil {
		return domain.ProcessResult{}, true, fmt.Errorf("decode processor response: %w", err)
	}
	if out.ProcessedAt.IsZero() {
		out.ProcessedAt = time.Now().UTC()
	}
	return domain.ProcessResult{Summary: out.Summary, ProcessedAt: out.ProcessedAt}, false, nil
}
