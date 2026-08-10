package domain

import "errors"

var (
	ErrNotFound         = errors.New("not found")
	ErrInvalidInput     = errors.New("invalid input")
	ErrUnsupportedMedia = errors.New("unsupported media type")
	ErrPayloadTooLarge  = errors.New("payload too large")
	ErrNoPayload        = errors.New("text or file required")
	ErrTooManyUploads   = errors.New("upload capacity exhausted")
	ErrUnauthorized     = errors.New("unauthorized")

	// ErrDuplicateKey means a job with the same Idempotency-Key already
	// exists; the caller should return that job instead of creating another.
	ErrDuplicateKey = errors.New("duplicate idempotency key")

	// ErrPermanent marks a processing failure that must not be retried
	// (bad request, unsupported payload). Transport and 5xx errors are retried.
	ErrPermanent = errors.New("permanent failure")
)
