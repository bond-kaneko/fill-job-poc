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

// Querier は *pgxpool.Pool と pgx.Tx の共通サブセット。
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

func InsertJob(ctx context.Context, q Querier, taskID string, maxAttempts int) error {
	_, err := q.Exec(ctx, `
		INSERT INTO jobs (task_id, max_attempts)
		VALUES ($1, $2)
		ON CONFLICT (task_id) DO NOTHING
	`, taskID, maxAttempts)
	return err
}

type ClaimedJob struct {
	TaskID      string
	Attempt     int
	MaxAttempts int
	Payload     task.Payload
}

// ClaimNextJob は available な job を 1 件取って attempt を increment し、lease を設定する。
// 取得できなければ (nil, nil)。SKIP LOCKED で複数 worker と競合しない。
func ClaimNextJob(ctx context.Context, q Querier, leaseUntil time.Time) (*ClaimedJob, error) {
	var c ClaimedJob
	var payloadJSON []byte
	err := q.QueryRow(ctx, `
		WITH claimed AS (
			SELECT task_id FROM jobs
			WHERE available_at <= now()
			  AND (lease_until IS NULL OR lease_until < now())
			ORDER BY available_at
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs j
		SET attempt = j.attempt + 1, lease_until = $1
		FROM claimed
		WHERE j.task_id = claimed.task_id
		RETURNING j.task_id, j.attempt, j.max_attempts,
		          (SELECT payload FROM tasks WHERE id = j.task_id)
	`, leaseUntil).Scan(&c.TaskID, &c.Attempt, &c.MaxAttempts, &payloadJSON)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(payloadJSON, &c.Payload); err != nil {
		return nil, err
	}
	return &c, nil
}

// MarkCompleted は domain status を completed にし、jobs から削除する (同 tx)。
func MarkCompleted(ctx context.Context, q Querier, taskID string, result task.Result) error {
	b, err := json.Marshal(result)
	if err != nil {
		return err
	}
	if _, err := q.Exec(ctx, `
		UPDATE tasks
		SET status = 'completed', result = $2, filled_at = now()
		WHERE id = $1
	`, taskID, b); err != nil {
		return err
	}
	_, err = q.Exec(ctx, `DELETE FROM jobs WHERE task_id = $1`, taskID)
	return err
}

// MarkFailed は domain status を failed にし、jobs から削除する。
func MarkFailed(ctx context.Context, q Querier, taskID string, reason string) error {
	if _, err := q.Exec(ctx, `
		UPDATE tasks
		SET status = 'failed', fill_error = $2, filled_at = now()
		WHERE id = $1
	`, taskID, reason); err != nil {
		return err
	}
	_, err := q.Exec(ctx, `DELETE FROM jobs WHERE task_id = $1`, taskID)
	return err
}

// RequeueJob は一時失敗時に lease を解放し、available_at を遅らせる。
func RequeueJob(ctx context.Context, q Querier, taskID string, reason string, availableAt time.Time) error {
	_, err := q.Exec(ctx, `
		UPDATE jobs
		SET lease_until = NULL, last_error = $2, available_at = $3
		WHERE task_id = $1
	`, taskID, reason, availableAt)
	return err
}

// RescueStaleLeases は lease_until 切れの行を available 化する。
// 戻り値は復旧した job 数。
func RescueStaleLeases(ctx context.Context, q Querier) (int64, error) {
	tag, err := q.Exec(ctx, `
		UPDATE jobs
		SET lease_until = NULL
		WHERE lease_until IS NOT NULL AND lease_until < now()
	`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// LoadTask は test 用に task を 1 件取る。
func LoadTask(ctx context.Context, q Querier, id string) (*task.Task, error) {
	var t task.Task
	var payloadJSON, resultJSON []byte
	var status string
	var fillErr *string
	var filledAt *time.Time
	err := q.QueryRow(ctx, `
		SELECT id, payload, status, result, fill_error, filled_at, created_at
		FROM tasks WHERE id = $1
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
