package worker

import (
	"context"
	"fmt"

	"github.com/bond-kaneko/fill-job-poc/river-impl/internal/repo"
	"github.com/bond-kaneko/fill-job-poc/shared/task"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

type FillTaskWorker struct {
	river.WorkerDefaults[FillTaskArgs]
	pool *pgxpool.Pool
}

func NewFillTaskWorker(pool *pgxpool.Pool) *FillTaskWorker {
	return &FillTaskWorker{pool: pool}
}

func (w *FillTaskWorker) Work(ctx context.Context, job *river.Job[FillTaskArgs]) error {
	taskID := job.Args.TaskID

	payload, err := repo.LoadPayload(ctx, w.pool, taskID)
	if err != nil {
		return fmt.Errorf("load payload: %w", err)
	}

	result, transient, perm := task.Process(ctx, payload, int(job.Attempt))

	switch {
	case perm != nil:
		// 恒久失敗: domain を failed にして JobCancel で discarded 扱い
		if err := pgx.BeginFunc(ctx, w.pool, func(tx pgx.Tx) error {
			return repo.MarkFailed(ctx, tx, taskID, "permanent: "+perm.Error())
		}); err != nil {
			return err
		}
		return river.JobCancel(perm)

	case transient != nil:
		if int(job.Attempt) >= int(job.MaxAttempts) {
			// 最終試行失敗
			if err := pgx.BeginFunc(ctx, w.pool, func(tx pgx.Tx) error {
				return repo.MarkFailed(ctx, tx, taskID, "max_attempts: "+transient.Error())
			}); err != nil {
				return err
			}
			return river.JobCancel(transient)
		}
		// transient: River に retry させる
		return transient

	default:
		// 成功: domain 更新と JobCompleteTx を同 tx で行い strong consistency
		return pgx.BeginFunc(ctx, w.pool, func(tx pgx.Tx) error {
			if err := repo.MarkCompleted(ctx, tx, taskID, *result); err != nil {
				return err
			}
			_, err := river.JobCompleteTx[*riverpgxv5.Driver](ctx, tx, job)
			return err
		})
	}
}
