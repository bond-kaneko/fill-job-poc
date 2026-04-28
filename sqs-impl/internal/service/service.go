package service

import (
	"context"
	"encoding/json"

	"github.com/bond-kaneko/fill-job-poc/shared/task"
	"github.com/bond-kaneko/fill-job-poc/sqs-impl/internal/repo"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Service struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Service {
	return &Service{pool: pool}
}

// CreateTask は domain INSERT と outbox INSERT を 1 つの tx で行う。
// SQS への enqueue は別 goroutine の relay が outbox を見て送る。
func (s *Service) CreateTask(ctx context.Context, id string, payload task.Payload) error {
	body, err := json.Marshal(struct {
		TaskID string `json:"task_id"`
	}{TaskID: id})
	if err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := repo.InsertTask(ctx, tx, id, payload); err != nil {
			return err
		}
		return repo.InsertOutbox(ctx, tx, id, body)
	})
}
