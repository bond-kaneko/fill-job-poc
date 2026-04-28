package service

import (
	"context"

	"github.com/bond-kaneko/fill-job-poc/river-impl/internal/repo"
	"github.com/bond-kaneko/fill-job-poc/river-impl/internal/worker"
	"github.com/bond-kaneko/fill-job-poc/shared/task"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

type Service struct {
	pool        *pgxpool.Pool
	client      *river.Client[pgx.Tx]
	maxAttempts int
}

func New(pool *pgxpool.Pool, client *river.Client[pgx.Tx], maxAttempts int) *Service {
	return &Service{pool: pool, client: client, maxAttempts: maxAttempts}
}

// CreateTask は domain INSERT と River InsertTx を同一 tx で行う。
// UniqueOpts.ByArgs により同一 task_id の二重 enqueue は River 側で 1 件に丸められる。
func (s *Service) CreateTask(ctx context.Context, id string, payload task.Payload) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := repo.InsertTask(ctx, tx, id, payload); err != nil {
			return err
		}
		_, err := s.client.InsertTx(ctx, tx, worker.FillTaskArgs{TaskID: id}, &river.InsertOpts{
			MaxAttempts: s.maxAttempts,
			UniqueOpts: river.UniqueOpts{
				ByArgs: true,
			},
		})
		return err
	})
}
