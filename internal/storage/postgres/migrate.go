package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

// migrationLockKey is an arbitrary but stable key for pg_advisory_lock. Every
// replica of every deployment computes the same value, so exactly one of them
// runs goose at a time.
const migrationLockKey int64 = 8714203371

// Migrate applies pending migrations under a session-level advisory lock.
// Without it, the api and worker Deployments rolling out together would race
// inside goose's version table.
func Migrate(ctx context.Context, pool *pgxpool.Pool, migrationsDir string) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration conn: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockKey); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		// Best effort: releasing the session also drops the lock.
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, migrationLockKey)
	}()

	db := stdlib.OpenDB(*conn.Conn().Config())
	defer func() { _ = db.Close() }()

	return MigrateSQL(db, migrationsDir)
}

// MigrateSQL runs migrations against an explicit *sql.DB (used by tests).
func MigrateSQL(db *sql.DB, migrationsDir string) error {
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("goose dialect: %w", err)
	}
	goose.SetLogger(goose.NopLogger())
	if err := goose.Up(db, migrationsDir); err != nil {
		return fmt.Errorf("goose up: %w", err)
	}
	return nil
}
