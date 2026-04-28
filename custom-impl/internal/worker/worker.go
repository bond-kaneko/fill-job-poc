package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/bond-kaneko/fill-job-poc/custom-impl/internal/repo"
	"github.com/bond-kaneko/fill-job-poc/shared/task"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	LeaseDuration time.Duration             // job 保持期間
	PollInterval  time.Duration             // 空 queue 時の sleep
	RescueEvery   time.Duration             // stale lease 検出周期
	Backoff       func(attempt int) time.Duration
}

func DefaultConfig() Config {
	return Config{
		LeaseDuration: 30 * time.Second,
		PollInterval:  500 * time.Millisecond,
		RescueEvery:   15 * time.Second,
		Backoff: func(attempt int) time.Duration {
			// 1s, 4s, 9s ...
			return time.Duration(attempt*attempt) * time.Second
		},
	}
}

type Worker struct {
	pool   *pgxpool.Pool
	cfg    Config
	logger *slog.Logger
}

func New(pool *pgxpool.Pool, cfg Config, logger *slog.Logger) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{pool: pool, cfg: cfg, logger: logger}
}

// Run は ctx が cancel されるまで loop する。worker 1 並列。
func (w *Worker) Run(ctx context.Context) error {
	rescueTicker := time.NewTicker(w.cfg.RescueEvery)
	defer rescueTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-rescueTicker.C:
			n, err := repo.RescueStaleLeases(ctx, w.pool)
			if err != nil {
				w.logger.Warn("rescuer failed", "err", err)
			} else if n > 0 {
				w.logger.Info("rescued stale leases", "count", n)
			}
		default:
		}

		job, err := w.claim(ctx)
		if err != nil {
			w.logger.Warn("claim failed", "err", err)
			if !sleepCtx(ctx, w.cfg.PollInterval) {
				return ctx.Err()
			}
			continue
		}
		if job == nil {
			if !sleepCtx(ctx, w.cfg.PollInterval) {
				return ctx.Err()
			}
			continue
		}
		w.process(ctx, job)
	}
}

func (w *Worker) claim(ctx context.Context) (*repo.ClaimedJob, error) {
	leaseUntil := time.Now().Add(w.cfg.LeaseDuration)
	return repo.ClaimNextJob(ctx, w.pool, leaseUntil)
}

func (w *Worker) process(ctx context.Context, j *repo.ClaimedJob) {
	w.logger.Info("processing", "task_id", j.TaskID, "attempt", j.Attempt)

	result, transientErr, permErr := task.Process(ctx, j.Payload, j.Attempt)

	// ctx が cancel された場合 (= shutdown / crash 模擬) は何も書かずに抜ける。
	// lease が切れたら別 worker (または rescuer 後の自分) が拾う。
	if errors.Is(ctx.Err(), context.Canceled) {
		w.logger.Info("aborted mid-process", "task_id", j.TaskID)
		return
	}

	switch {
	case result != nil:
		if err := pgx.BeginFunc(ctx, w.pool, func(tx pgx.Tx) error {
			return repo.MarkCompleted(ctx, tx, j.TaskID, *result)
		}); err != nil {
			w.logger.Error("MarkCompleted failed", "task_id", j.TaskID, "err", err)
		}
	case permErr != nil:
		w.markFailed(ctx, j.TaskID, fmt.Sprintf("permanent: %s", permErr))
	case transientErr != nil:
		if j.Attempt >= j.MaxAttempts {
			w.markFailed(ctx, j.TaskID, fmt.Sprintf("max attempts exceeded: %s", transientErr))
			return
		}
		availableAt := time.Now().Add(w.cfg.Backoff(j.Attempt))
		if err := repo.RequeueJob(ctx, w.pool, j.TaskID, transientErr.Error(), availableAt); err != nil {
			w.logger.Error("RequeueJob failed", "task_id", j.TaskID, "err", err)
		}
	default:
		w.logger.Error("Process returned no value", "task_id", j.TaskID)
	}
}

func (w *Worker) markFailed(ctx context.Context, taskID, reason string) {
	if err := pgx.BeginFunc(ctx, w.pool, func(tx pgx.Tx) error {
		return repo.MarkFailed(ctx, tx, taskID, reason)
	}); err != nil {
		w.logger.Error("MarkFailed failed", "task_id", taskID, "err", err)
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
