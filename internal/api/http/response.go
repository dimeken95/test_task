package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	chimw "github.com/go-chi/chi/v5/middleware"

	"github.com/dimeken95/test_task/internal/domain"
)

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError maps domain errors to status codes. Client errors carry their
// message because it is actionable; anything unmapped is a server fault, so the
// response says nothing beyond a request id and the detail goes to the log.
func writeError(w http.ResponseWriter, logger *slog.Logger, r *http.Request, err error) {
	var (
		ctx       = r.Context()
		requestID = chimw.GetReqID(ctx)
		code      = "internal_error"
		status    = http.StatusInternalServerError
		msg       = "internal error"
	)

	switch {
	case errors.Is(err, domain.ErrNotFound):
		code, status, msg = "not_found", http.StatusNotFound, "job not found"
	case errors.Is(err, domain.ErrNoPayload), errors.Is(err, domain.ErrInvalidInput):
		code, status, msg = "bad_request", http.StatusBadRequest, err.Error()
	case errors.Is(err, domain.ErrUnsupportedMedia):
		code, status, msg = "unsupported_media_type", http.StatusUnsupportedMediaType, err.Error()
	case errors.Is(err, domain.ErrPayloadTooLarge):
		code, status, msg = "payload_too_large", http.StatusRequestEntityTooLarge, err.Error()
	case errors.Is(err, domain.ErrUnauthorized):
		code, status, msg = "unauthorized", http.StatusUnauthorized, "valid API key required"
	case errors.Is(err, domain.ErrTooManyUploads):
		code, status, msg = "too_many_uploads", http.StatusServiceUnavailable, "upload capacity exhausted, retry shortly"
		w.Header().Set("Retry-After", "5")
	case errors.Is(err, context.Canceled):
		// The client hung up; nothing useful to send.
		return
	}

	if status >= http.StatusInternalServerError && logger != nil {
		logger.ErrorContext(ctx, "request failed",
			"err", err,
			"path", r.URL.Path,
			"method", r.Method,
			"request_id", requestID,
		)
	}

	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: msg, RequestID: requestID}})
}
