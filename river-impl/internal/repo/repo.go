package repo

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/bond-kaneko/fill-job-poc/shared/task"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func InsertTask(ctx context.Context, q Querier, id string, payload task.Payload) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = q.Exec(ctx, `
		INSERT INTO tasks (id, payload, status)
		VALUES ($1, $2, 'pending')
		ON CONFLICT (id) DO NOTHING
	`, id, b)
	return err
}

func LoadPayload(ctx context.Context, q Querier, id string) (task.Payload, error) {
	var b []byte
	err := q.QueryRow(ctx, `SELECT payload FROM tasks WHERE id=$1`, id).Scan(&b)
	if err != nil {
		return task.Payload{}, err
	}
	var p task.Payload
	if err := json.Unmarshal(b, &p); err != nil {
		return task.Payload{}, err
	}
	return p, nil
}

func MarkCompleted(ctx context.Context, q Querier, id string, r task.Result) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	_, err = q.Exec(ctx, `
		UPDATE tasks SET status='completed', result=$2, filled_at=now()
		WHERE id=$1 AND status='pending'
	`, id, b)
	return err
}

func MarkFailed(ctx context.Context, q Querier, id string, reason string) error {
	_, err := q.Exec(ctx, `
		UPDATE tasks SET status='failed', fill_error=$2, filled_at=now()
		WHERE id=$1 AND status='pending'
	`, id, reason)
	return err
}

func LoadTask(ctx context.Context, q Querier, id string) (*task.Task, error) {
	var t task.Task
	var payloadJSON, resultJSON []byte
	var status string
	var fillErr *string
	var filledAt *time.Time
	err := q.QueryRow(ctx, `
		SELECT id, payload, status, result, fill_error, filled_at, created_at
		FROM tasks WHERE id=$1
	`, id).Scan(&t.ID, &payloadJSON, &status, &resultJSON, &fillErr, &filledAt, &t.CreatedAt)
	if err != nil {
		return nil, err
	}
	t.Status = task.Status(status)
	t.FillError = fillErr
	t.FilledAt = filledAt
	if err := json.Unmarshal(payloadJSON, &t.Payload); err != nil {
		return nil, err
	}
	if len(resultJSON) > 0 {
		var r task.Result
		if err := json.Unmarshal(resultJSON, &r); err != nil {
			return nil, err
		}
		t.Result = &r
	}
	return &t, nil
}

func IsNoRows(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}
