//go:build load

package fillpoc_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/bond-kaneko/fill-job-poc/shared/task"
	"github.com/bond-kaneko/fill-job-poc/sqs-impl/internal/migrate"
	"github.com/bond-kaneko/fill-job-poc/sqs-impl/internal/outbox"
	"github.com/bond-kaneko/fill-job-poc/sqs-impl/internal/service"
	"github.com/bond-kaneko/fill-job-poc/sqs-impl/internal/sqsx"
	"github.com/bond-kaneko/fill-job-poc/sqs-impl/internal/worker"
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

	sqsClient, err := sqsx.New(ctx, sqsx.DefaultOpts())
	require.NoError(t, err)
	queueURL, err := sqsx.QueueURL(ctx, sqsClient, "fill-tasks")
	require.NoError(t, err)
	drainQueue(t, ctx, sqsClient, queueURL)
	if dlqURL, err := sqsx.QueueURL(ctx, sqsClient, "fill-tasks-dlq"); err == nil {
		drainQueue(t, ctx, sqsClient, dlqURL)
	}

	silentLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	relay := outbox.New(pool, sqsClient, queueURL, 20*time.Millisecond, silentLogger)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = relay.Run(ctx) }()

	wcfg := worker.Config{
		QueueURL:          queueURL,
		VisibilityTimeout: 30 * time.Second,
		WaitTime:          1 * time.Second,
		MaxAttempts:       3,
	}
	for range loadWorkers {
		w := worker.New(pool, sqsClient, wcfg, silentLogger)
		wg.Add(1)
		go func() { defer wg.Done(); _ = w.Run(ctx) }()
	}
	t.Cleanup(func() { cancel(); wg.Wait() })

	svc := service.New(pool)
	enqStart := time.Now()
	for range loadNumTasks {
		require.NoError(t, svc.CreateTask(ctx, uuid.NewString(), task.Payload{Echo: "load", ComputeMs: loadComputeMs}))
	}
	enqDuration := time.Since(enqStart)

	waitFor(t, 180*time.Second, func() bool {
		var n int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE status='completed'`).Scan(&n)
		return n == loadNumTasks
	})
	totalDuration := time.Since(enqStart)

	reportLoad(t, "sqs-impl", pool, ctx, enqDuration, totalDuration)
}

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

	var failed, outboxLeft int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE status='failed'`).Scan(&failed)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&outboxLeft)

	t.Logf("=== %s load ===", impl)
	t.Logf("tasks:        %d", loadNumTasks)
	t.Logf("workers:      %d", loadWorkers)
	t.Logf("computeMs:    %d", loadComputeMs)
	t.Logf("failed:       %d", failed)
	t.Logf("outbox left:  %d", outboxLeft)
	t.Logf("enqueue:      %v (%.0f tasks/sec)", enq, float64(loadNumTasks)/enq.Seconds())
	t.Logf("end-to-end:   %v (%.0f tasks/sec)", total, float64(loadNumTasks)/total.Seconds())
	t.Logf("latency p50:  %.1f ms", p50)
	t.Logf("latency p95:  %.1f ms", p95)
	t.Logf("latency p99:  %.1f ms", p99)
	t.Logf("latency max:  %.1f ms", pmax)
	require.Equal(t, 0, failed, "no task should have failed under load")
}
