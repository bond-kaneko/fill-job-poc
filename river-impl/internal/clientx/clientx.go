// Package clientx は River Client を作る共通処理。test と main の両方から使う。
package clientx

import (
	"github.com/bond-kaneko/fill-job-poc/river-impl/internal/worker"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

func New(pool *pgxpool.Pool, cfg *river.Config) (*river.Client[pgx.Tx], error) {
	if cfg.Workers == nil {
		workers := river.NewWorkers()
		river.AddWorker(workers, worker.NewFillTaskWorker(pool))
		cfg.Workers = workers
	}
	if cfg.Queues == nil {
		cfg.Queues = map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 1},
		}
	}
	return river.NewClient(riverpgxv5.New(pool), cfg)
}
