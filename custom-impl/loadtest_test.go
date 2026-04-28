//go:build load

package fillpoc_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/bond-kaneko/fill-job-poc/custom-impl/internal/migrate"
	"github.com/bond-kaneko/fill-job-poc/custom-impl/internal/service"
	"github.com/bond-kaneko/fill-job-poc/custom-impl/internal/worker"
	"github.com/bond-kaneko/fill-job-poc/shared/task"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const (
	loadNumTasks  = 500
	loadWorkers   = 8
	loadComputeMs = 10
	loadMaxConns  = 30
)

func TestLoadThroughput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cfg, err := pgxpool.ParseConfig(dsn())
	require.NoError(t, err)
	cfg.MaxConns = loadMaxConns
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	require.NoError(t, migrate.Reset(ctx, pool))
	require.NoError(t, migrate.Apply(ctx, pool))

	silentLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	wcfg := worker.Config{
		LeaseDuration: 10 * time.Second,
		PollInterval:  10 * time.Millisecond,
		RescueEvery:   2 * time.Second,
		Backoff:       func(attempt int) time.Duration { return 100 * time.Millisecond },
	}

	var wg sync.WaitGroup
	for range loadWorkers {
		w := worker.New(pool, wcfg, silentLogger)
		wg.Add(1)
		go func() { defer wg.Done(); _ = w.Run(ctx) }()
	}
	t.Cleanup(func() { cancel(); wg.Wait() })

	svc := service.New(pool, 3)
	enqStart := time.Now()
	for range loadNumTasks {
		_, err := svc.CreateTask(ctx, uuid.NewString(), task.Payload{Echo: "load", ComputeMs: loadComputeMs})
		require.NoError(t, err)
	}
	enqDuration := time.Since(enqStart)

	waitFor(t, 120*time.Second, func() bool {
		var n int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE status='completed'`).Scan(&n)
		return n == loadNumTasks
	})
	totalDuration := time.Since(enqStart)

	reportLoad(t, "custom-impl", pool, ctx, enqDuration, totalDuration)
}

// reportLoad は完了 task の latency 統計を出力する。
func reportLoad(t *testing.T, impl string, pool *pgxpool.Pool, ctx context.Context, enq, total time.Duration) {
	t.Helper()
	var p50, p95, p99, pmax float64
	err := pool.QueryRow(ctx, `
		SELECT
			percentile_cont(0.5)  WITHIN GROUP (ORDER BY ms) AS p50,
			percentile_cont(0.95) WITHIN GROUP (ORDER BY ms) AS p95,
			percentile_cont(0.99) WITHIN GROUP (ORDER BY ms) AS p99,
			max(ms) AS pmax
		FROM (
			SELECT EXTRACT(EPOCH FROM (filled_at - created_at)) * 1000 AS ms
			FROM tasks WHERE status='completed'
		) s
	`).Scan(&p50, &p95, &p99, &pmax)
	require.NoError(t, err)

	var failed int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE status='failed'`).Scan(&failed)

	t.Logf("=== %s load ===", impl)
	t.Logf("tasks:        %d", loadNumTasks)
	t.Logf("workers:      %d", loadWorkers)
	t.Logf("computeMs:    %d", loadComputeMs)
	t.Logf("failed:       %d", failed)
	t.Logf("enqueue:      %v (%.0f tasks/sec)", enq, float64(loadNumTasks)/enq.Seconds())
	t.Logf("end-to-end:   %v (%.0f tasks/sec)", total, float64(loadNumTasks)/total.Seconds())
	t.Logf("latency p50:  %.1f ms", p50)
	t.Logf("latency p95:  %.1f ms", p95)
	t.Logf("latency p99:  %.1f ms", p99)
	t.Logf("latency max:  %.1f ms", pmax)
	require.Equal(t, 0, failed, "no task should have failed under load")
}
