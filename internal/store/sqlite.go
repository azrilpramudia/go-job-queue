package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	_ "modernc.org/sqlite"
)

type Record struct {
	ID         string
	TaskName   string
	Payload    []byte
	Timeout    time.Duration
	Priority   int
	MaxRetries int
	BaseDelay  time.Duration
	Status     string
	Error      string
	Attempts   int
	UpdatedAt  time.Time
}

type SQLite struct {
	db *sql.DB
}

func OpenSQLite(path string) (*SQLite, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create SQLite database file: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close SQLite database file: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return nil, fmt.Errorf("secure SQLite database file permissions: %w", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open SQLite database: %w", err)
	}
	db.SetMaxOpenConns(1)

	s := &SQLite{db: db}
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS jobs (
			id TEXT PRIMARY KEY,
			task_name TEXT NOT NULL,
			payload BLOB NOT NULL,
			timeout_ns INTEGER NOT NULL,
			priority INTEGER NOT NULL,
			max_retries INTEGER NOT NULL,
			base_delay_ns INTEGER NOT NULL,
			status TEXT NOT NULL,
			error TEXT NOT NULL DEFAULT '',
			attempts INTEGER NOT NULL DEFAULT 0,
			updated_at_ns INTEGER NOT NULL
		)
	`); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize SQLite schema: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS jobs_terminal_updated ON jobs(status, updated_at_ns)`); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize SQLite retention index: %w", err)
	}
	return s, nil
}

func (s *SQLite) Insert(ctx context.Context, record Record) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO jobs (
			id, task_name, payload, timeout_ns, priority, max_retries,
			base_delay_ns, status, error, attempts, updated_at_ns
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, record.ID, record.TaskName, record.Payload, int64(record.Timeout),
		record.Priority, record.MaxRetries, int64(record.BaseDelay),
		record.Status, record.Error, record.Attempts, time.Now().UnixNano())
	if err != nil {
		return fmt.Errorf("persist job %q: %w", record.ID, err)
	}
	return nil
}

func (s *SQLite) Update(ctx context.Context, record Record) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE jobs
		SET status = ?, error = ?, attempts = ?, updated_at_ns = ?,
			task_name = CASE WHEN ? IN ('done', 'failed') THEN '' ELSE task_name END,
			payload = CASE WHEN ? IN ('done', 'failed') THEN X'' ELSE payload END
		WHERE id = ?
	`, record.Status, record.Error, record.Attempts, time.Now().UnixNano(),
		record.Status, record.Status, record.ID)
	if err != nil {
		return fmt.Errorf("update persisted job %q: %w", record.ID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check persisted job update %q: %w", record.ID, err)
	}
	if rows != 1 {
		return fmt.Errorf("persisted job %q not found", record.ID)
	}
	return nil
}

func (s *SQLite) Delete(ctx context.Context, id string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM jobs WHERE id = ?`, id); err != nil {
		return fmt.Errorf("remove persisted job %q: %w", id, err)
	}
	return nil
}

func (s *SQLite) Recover(ctx context.Context) ([]Record, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, task_name, payload, timeout_ns, priority, max_retries,
			base_delay_ns, status, error, attempts, updated_at_ns
		FROM jobs
		ORDER BY updated_at_ns, id
	`)
	if err != nil {
		return nil, fmt.Errorf("load persisted jobs: %w", err)
	}
	defer rows.Close()

	var records []Record
	var interrupted []Record
	for rows.Next() {
		var record Record
		var timeout, baseDelay, updatedAt int64
		if err := rows.Scan(
			&record.ID, &record.TaskName, &record.Payload, &timeout, &record.Priority,
			&record.MaxRetries, &baseDelay, &record.Status, &record.Error,
			&record.Attempts, &updatedAt,
		); err != nil {
			return nil, fmt.Errorf("read persisted job: %w", err)
		}
		record.Timeout = time.Duration(timeout)
		record.BaseDelay = time.Duration(baseDelay)
		record.UpdatedAt = time.Unix(0, updatedAt)
		if record.Status == "running" {
			record.Status = "pending"
			record.Error = ""
			interrupted = append(interrupted, record)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate persisted jobs: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close persisted jobs query: %w", err)
	}
	for _, record := range interrupted {
		if err := s.Update(ctx, record); err != nil {
			return nil, err
		}
	}
	return records, nil
}

func (s *SQLite) Prune(ctx context.Context, cutoff time.Time, maxTerminal int) ([]string, error) {
	if maxTerminal < 0 {
		return nil, errors.New("maximum terminal jobs cannot be negative")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin job retention transaction: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT id, updated_at_ns FROM jobs
		WHERE status IN ('done', 'failed')
		ORDER BY updated_at_ns DESC, id
	`)
	if err != nil {
		return nil, fmt.Errorf("list terminal jobs for retention: %w", err)
	}

	var remove []string
	kept := 0
	for rows.Next() {
		var id string
		var updatedAt int64
		if err := rows.Scan(&id, &updatedAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("read terminal job for retention: %w", err)
		}
		if time.Unix(0, updatedAt).Before(cutoff) || kept >= maxTerminal {
			remove = append(remove, id)
			continue
		}
		kept++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate terminal jobs for retention: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close terminal jobs query: %w", err)
	}
	for _, id := range remove {
		if _, err := tx.ExecContext(ctx, `DELETE FROM jobs WHERE id = ?`, id); err != nil {
			return nil, fmt.Errorf("remove expired job %q: %w", id, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit job retention: %w", err)
	}
	return remove, nil
}

func (s *SQLite) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("close SQLite database: %w", err)
	}
	return nil
}
