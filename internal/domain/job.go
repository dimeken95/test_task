package domain

import (
	"time"
)

type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)

type Job struct {
	ID            string
	Status        Status
	Text          string
	ObjectKey     string
	ContentType   string
	FileName      string
	SizeBytes     int64
	ResultSummary string
	ErrorMessage  string
	Attempts      int
	TraceContext  string
	// IdempotencyKey is the client-supplied Idempotency-Key, empty when none
	// was sent. A partial unique index enforces it in the database.
	IdempotencyKey string
	LockedBy       string
	LeaseUntil     *time.Time
	NextAttemptAt  *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
	CompletedAt    *time.Time
}
