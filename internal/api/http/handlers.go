package httpapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/propagation"

	"github.com/dimeken95/test_task/internal/buildinfo"
	"github.com/dimeken95/test_task/internal/config"
	"github.com/dimeken95/test_task/internal/domain"
	"github.com/dimeken95/test_task/internal/observability"
	"github.com/dimeken95/test_task/internal/service"
)

// framingSlackBytes is headroom added to the request ceiling for multipart
// boundaries, part headers and any fields we ignore.
const framingSlackBytes = 1 << 20

// maxIdempotencyKeyLen bounds a client-supplied key so it cannot be used to
// push arbitrary data into the index.
const maxIdempotencyKeyLen = 255

type Handler struct {
	cfg     config.Config
	jobs    *service.JobService
	logger  *slog.Logger
	readyFn func(r *http.Request) error

	// uploadSlots bounds how many requests may hold object-store part buffers
	// at once. Each in-flight multipart upload costs roughly
	// S3_PART_SIZE * (1 + 2*S3_PART_CONCURRENCY) bytes of resident memory, so
	// without this the pod memory limit is a function of client behaviour.
	uploadSlots chan struct{}
}

func NewHandler(cfg config.Config, jobs *service.JobService, logger *slog.Logger, readyFn func(*http.Request) error) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	slots := cfg.MaxConcurrentUploads
	if slots < 1 {
		slots = 1
	}
	return &Handler{
		cfg:         cfg,
		jobs:        jobs,
		logger:      logger,
		readyFn:     readyFn,
		uploadSlots: make(chan struct{}, slots),
	}
}

type jobResponse struct {
	ID            string  `json:"id"`
	Status        string  `json:"status"`
	Text          string  `json:"text,omitempty"`
	FileName      string  `json:"file_name,omitempty"`
	ContentType   string  `json:"content_type,omitempty"`
	SizeBytes     int64   `json:"size_bytes"`
	ResultSummary string  `json:"result_summary,omitempty"`
	ErrorMessage  string  `json:"error_message,omitempty"`
	Attempts      int     `json:"attempts"`
	CreatedAt     string  `json:"created_at"`
	UpdatedAt     string  `json:"updated_at"`
	CompletedAt   *string `json:"completed_at,omitempty"`
}

func toJobResponse(j *domain.Job) jobResponse {
	resp := jobResponse{
		ID:            j.ID,
		Status:        string(j.Status),
		Text:          j.Text,
		FileName:      j.FileName,
		ContentType:   j.ContentType,
		SizeBytes:     j.SizeBytes,
		ResultSummary: j.ResultSummary,
		ErrorMessage:  j.ErrorMessage,
		Attempts:      j.Attempts,
		CreatedAt:     j.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt:     j.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
	if j.CompletedAt != nil {
		s := j.CompletedAt.UTC().Format(time.RFC3339Nano)
		resp.CompletedAt = &s
	}
	return resp
}

// CreateJob streams a multipart request straight into object storage without
// buffering the form. The job id is allocated before the upload, which lets us
// keep parsing parts that follow the file — a `text` field sent after `file`
// is still picked up.
func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, h.maxRequestBytes())

	mr, err := r.MultipartReader()
	if err != nil {
		writeError(w, h.logger, r, fmt.Errorf("%w: expected multipart/form-data", domain.ErrInvalidInput))
		return
	}

	ctx := r.Context()

	// Fast path: answer a retried submission before reading the body, so a
	// client that timed out and retried does not re-upload the payload. The
	// unique index in Commit is what makes this correct under a race.
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if idempotencyKey != "" {
		if len(idempotencyKey) > maxIdempotencyKeyLen {
			writeError(w, h.logger, r, fmt.Errorf("%w: Idempotency-Key exceeds %d characters",
				domain.ErrInvalidInput, maxIdempotencyKeyLen))
			return
		}
		if existing, lookupErr := h.jobs.FindByIdempotencyKey(ctx, idempotencyKey); lookupErr == nil {
			h.writeJob(w, existing, true)
			return
		}
	}

	draft := h.jobs.NewDraft(traceContextHeader(ctx))
	draft.SetIdempotencyKey(idempotencyKey)
	defer draft.Discard(ctx)

	for {
		part, partErr := mr.NextPart()
		if errors.Is(partErr, io.EOF) {
			break
		}
		if partErr != nil {
			writeError(w, h.logger, r, mapReadError(partErr))
			return
		}

		switch part.FormName() {
		case "text":
			text, terr := readTextPart(part, h.cfg.MaxTextBytes)
			if terr != nil {
				_ = part.Close()
				writeError(w, h.logger, r, terr)
				return
			}
			draft.SetText(text)

		case "file":
			if ferr := h.attachFile(ctx, draft, part); ferr != nil {
				_ = part.Close()
				writeError(w, h.logger, r, ferr)
				return
			}

		default:
			// Unknown field: ignored. Close below drains whatever is left of
			// it, and the request as a whole is already capped by
			// MaxBytesReader, so there is nothing extra to bound here.
		}
		_ = part.Close()
	}

	job, replayed, err := draft.Commit(ctx)
	if err != nil {
		writeError(w, h.logger, r, err)
		return
	}
	h.writeJob(w, job, replayed)
}

// writeJob returns the accepted job. A replay reports the original job
// unchanged, flagged so the client can tell it did not create anything new.
func (h *Handler) writeJob(w http.ResponseWriter, job *domain.Job, replayed bool) {
	w.Header().Set("Location", "/api/v1/jobs/"+job.ID)
	if replayed {
		w.Header().Set("Idempotency-Replayed", "true")
	}
	writeJSON(w, http.StatusAccepted, toJobResponse(job))
}

// attachFile validates the declared type against the payload's magic bytes and
// streams the body to object storage while holding an upload slot.
func (h *Handler) attachFile(ctx context.Context, draft *service.Draft, part *multipart.Part) error {
	fileName := part.FileName()
	if fileName == "" {
		return fmt.Errorf("%w: file part requires a filename", domain.ErrInvalidInput)
	}

	validated, err := ValidateFile(h.cfg, fileName, part.Header.Get("Content-Type"), part)
	if err != nil {
		return err
	}

	release, err := h.acquireUploadSlot(ctx)
	if err != nil {
		return err
	}
	defer release()

	// The store cannot know the length ahead of time: multipart parts carry no
	// Content-Length, so it decides between PutObject and multipart by peeking.
	if err := draft.AttachFile(ctx, validated.FileName, validated.ContentType, 0, validated.Reader); err != nil {
		if validated.Exceeded() {
			return fmt.Errorf("%w: %s limit is %d bytes", domain.ErrPayloadTooLarge, validated.Kind, validated.MaxBytes)
		}
		return err
	}
	return nil
}

// acquireUploadSlot blocks briefly for a free slot and then sheds load, which
// is far better than queueing every client until the pod is OOM-killed.
func (h *Handler) acquireUploadSlot(ctx context.Context) (func(), error) {
	select {
	case h.uploadSlots <- struct{}{}:
		observability.UploadsInFlight.Inc()
		return func() {
			<-h.uploadSlots
			observability.UploadsInFlight.Dec()
		}, nil
	default:
	}

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case h.uploadSlots <- struct{}{}:
		observability.UploadsInFlight.Inc()
		return func() {
			<-h.uploadSlots
			observability.UploadsInFlight.Dec()
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		observability.UploadsRejected.Inc()
		return nil, domain.ErrTooManyUploads
	}
}

// maxRequestBytes is a hard ceiling on the whole request so a client cannot
// stream forever; per-kind limits are still enforced while streaming.
func (h *Handler) maxRequestBytes() int64 {
	max := h.cfg.MaxVideoBytes
	for _, n := range []int64{h.cfg.MaxDocBytes, h.cfg.MaxImageBytes} {
		if n > max {
			max = n
		}
	}
	return max + h.cfg.MaxTextBytes + framingSlackBytes
}

func readTextPart(part *multipart.Part, max int64) (string, error) {
	b, err := io.ReadAll(io.LimitReader(part, max+1))
	if err != nil {
		return "", mapReadError(err)
	}
	if int64(len(b)) > max {
		return "", fmt.Errorf("%w: text limit is %d bytes", domain.ErrPayloadTooLarge, max)
	}
	return string(b), nil
}

func mapReadError(err error) error {
	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		return fmt.Errorf("%w: request limit is %d bytes", domain.ErrPayloadTooLarge, maxErr.Limit)
	}
	if errors.Is(err, domain.ErrPayloadTooLarge) {
		return err
	}
	return fmt.Errorf("%w: %w", domain.ErrInvalidInput, err)
}

// traceContextHeader serialises the active span into W3C traceparent form so
// the worker can continue the same trace after the request is long gone.
func traceContextHeader(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)

	var b strings.Builder
	for _, k := range []string{"traceparent", "tracestate"} {
		v := carrier.Get(k)
		if v == "" {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte(';')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(v)
	}
	return b.String()
}

func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		writeError(w, h.logger, r, domain.ErrInvalidInput)
		return
	}
	job, err := h.jobs.Get(r.Context(), id)
	if err != nil {
		writeError(w, h.logger, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toJobResponse(job))
}

var (
	healthOKJSON = []byte(`{"status":"ok"}` + "\n")
	readyOKJSON  = []byte(`{"status":"ready"}` + "\n")
)

func (h *Handler) Healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(healthOKJSON)
}

func (h *Handler) Readyz(w http.ResponseWriter, r *http.Request) {
	if h.readyFn != nil {
		if err := h.readyFn(r); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, errorBody{
				Error: errorDetail{Code: "not_ready", Message: err.Error()},
			})
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(readyOKJSON)
}

func (h *Handler) Version(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"version": buildinfo.Version,
		"commit":  buildinfo.Commit,
		"mode":    h.cfg.AppMode,
	})
}
