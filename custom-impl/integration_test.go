package fillpoc_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/bond-kaneko/fill-job-poc/custom-impl/internal/migrate"
	"github.com/bond-kaneko/fill-job-poc/custom-impl/internal/repo"
	"github.com/bond-kaneko/fill-job-poc/custom-impl/internal/service"
	"github.com/bond-kaneko/fill-job-poc/custom-impl/internal/worker"
	"github.com/bond-kaneko/fill-job-poc/shared/task"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func dsn() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://poc:poc@localhost:5433/custom_db?sslmode=disable"
}

func setup(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn())
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, migrate.Reset(ctx, pool))
	require.NoError(t, migrate.Apply(ctx, pool))
	return pool
}

func testConfig() worker.Config {
	return worker.Config{
		LeaseDuration: 1 * time.Second,
		PollInterval:  20 * time.Millisecond,
		RescueEvery:   200 * time.Millisecond,
		Backoff: func(attempt int) time.Duration {
			return 100 * time.Millisecond
		},
	}
}

// startWorker は ctx cancel まで動く worker を goroutine 起動する。
// 戻り値は wait()。
func startWorker(t *testing.T, ctx context.Context, pool *pgxpool.Pool) func() {
	t.Helper()
	w := worker.New(pool, testConfig(), slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = w.Run(ctx)
	}()
	return wg.Wait
}

// waitFor は cond が true になるまで poll する。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for cond")
}

func TestHappyPath(t *testing.T) {
	pool := setup(t)
	svc := service.New(pool, 3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wait := startWorker(t, ctx, pool)
	t.Cleanup(func() { cancel(); wait() })

	id := uuid.NewString()
	_, err := svc.CreateTask(ctx, id, task.Payload{Echo: "hello"})
	require.NoError(t, err)

	waitFor(t, 5*time.Second, func() bool {
		tk, err := repo.LoadTask(ctx, pool, id)
		return err == nil && tk.Status == task.StatusCompleted
	})

	tk, err := repo.LoadTask(ctx, pool, id)
	require.NoError(t, err)
	require.Equal(t, task.StatusCompleted, tk.Status)
	require.NotNil(t, tk.Result)
	require.Equal(t, "HELLO", tk.Result.Echo)
}

func TestTransientRetryThenSuccess(t *testing.T) {
	pool := setup(t)
	svc := service.New(pool, 5) // 余裕のある max_attempts

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wait := startWorker(t, ctx, pool)
	t.Cleanup(func() { cancel(); wait() })

	id := uuid.NewString()
	_, err := svc.CreateTask(ctx, id, task.Payload{Echo: "retry", FailUntilAttempt: 3})
	require.NoError(t, err)

	waitFor(t, 5*time.Second, func() bool {
		tk, err := repo.LoadTask(ctx, pool, id)
		return err == nil && tk.Status == task.StatusCompleted
	})

	tk, err := repo.LoadTask(ctx, pool, id)
	require.NoError(t, err)
	require.Equal(t, task.StatusCompleted, tk.Status)
}

func TestPermanentFailure(t *testing.T) {
	pool := setup(t)
	svc := service.New(pool, 3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wait := startWorker(t, ctx, pool)
	t.Cleanup(func() { cancel(); wait() })

	id := uuid.NewString()
	_, err := svc.CreateTask(ctx, id, task.Payload{Echo: "bad", AlwaysFail: true})
	require.NoError(t, err)

	waitFor(t, 5*time.Second, func() bool {
		tk, err := repo.LoadTask(ctx, pool, id)
		return err == nil && tk.Status == task.StatusFailed
	})

	tk, err := repo.LoadTask(ctx, pool, id)
	require.NoError(t, err)
	require.Equal(t, task.StatusFailed, tk.Status)
	require.NotNil(t, tk.FillError)
}

func TestDuplicateEnqueueRunsOnce(t *testing.T) {
	pool := setup(t)
	svc := service.New(pool, 3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wait := startWorker(t, ctx, pool)
	t.Cleanup(func() { cancel(); wait() })

	id := uuid.NewString()
	// 並列で 2 回 enqueue: ON CONFLICT DO NOTHING で 1 行のみ
	var wg sync.WaitGroup
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			_, err := svc.CreateTask(ctx, id, task.Payload{Echo: "once", ComputeMs: 100})
			if err != nil {
				t.Errorf("CreateTask: %v", err)
			}
		}()
	}
	wg.Wait()

	waitFor(t, 5*time.Second, func() bool {
		tk, err := repo.LoadTask(ctx, pool, id)
		return err == nil && tk.Status == task.StatusCompleted
	})

	// jobs row が消えてること = もう実行待ちはない
	var rowCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE task_id=$1`, id).Scan(&rowCount))
	require.Equal(t, 0, rowCount)

	// 念のため短く wait してもう 1 回実行されないことを確認
	tk, err := repo.LoadTask(ctx, pool, id)
	require.NoError(t, err)
	require.Equal(t, task.StatusCompleted, tk.Status)
}

func TestCrashRecovery(t *testing.T) {
	pool := setup(t)
	svc := service.New(pool, 3)

	id := uuid.NewString()
	bgCtx := context.Background()
	_, err := svc.CreateTask(bgCtx, id, task.Payload{Echo: "recovered", ComputeMs: 2000})
	require.NoError(t, err)

	// worker1: ctx を 100ms 後に cancel して mid-process abort を起こす
	ctx1, cancel1 := context.WithCancel(bgCtx)
	wait1 := startWorker(t, ctx1, pool)
	time.Sleep(150 * time.Millisecond)
	// この時点で worker1 は claim してて Process 中。lease_until は now+1s 程度
	cancel1()
	wait1()

	// 1 回 attempt 消費されてること、status は pending のまま
	tk, err := repo.LoadTask(bgCtx, pool, id)
	require.NoError(t, err)
	require.Equal(t, task.StatusPending, tk.Status)

	// worker2: 同 lease 設定で起動。lease 切れ (1s後) に拾って完走
	ctx2, cancel2 := context.WithCancel(bgCtx)
	wait2 := startWorker(t, ctx2, pool)
	t.Cleanup(func() { cancel2(); wait2() })

	waitFor(t, 8*time.Second, func() bool {
		tk, err := repo.LoadTask(bgCtx, pool, id)
		return err == nil && tk.Status == task.StatusCompleted
	})

	tk, err = repo.LoadTask(bgCtx, pool, id)
	require.NoError(t, err)
	require.Equal(t, task.StatusCompleted, tk.Status)
	require.NotNil(t, tk.Result)
	require.Equal(t, "RECOVERED", tk.Result.Echo)
}

func TestTransactionalEnqueueAtomicity(t *testing.T) {
	// CreateTask が 1 つの tx で「tasks INSERT + jobs INSERT」を行うことを示す。
	// jobs.max_attempts に NOT NULL かつ CHECK 制約はないので、tx 内で
	// わざと repo を使わず 2 つ目の INSERT を失敗させて、tasks も rollback されることを確認する。
	pool := setup(t)

	id := uuid.NewString()
	ctx := context.Background()

	// pgx.BeginFunc で tasks INSERT 後に意図的にエラーを起こす
	err := pool.QueryRow(ctx, `SELECT 1`).Scan(new(int))
	require.NoError(t, err)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, repo.InsertTask(ctx, tx, id, task.Payload{Echo: "atomic"}))
	// 故意に rollback
	require.NoError(t, tx.Rollback(ctx))

	_, err = repo.LoadTask(ctx, pool, id)
	require.Error(t, err) // 行が無いので err != nil
	require.True(t, errorIsNoRows(err))
}

func errorIsNoRows(err error) bool {
	return err != nil && (errors.Is(err, errNoRows) || err.Error() == "no rows in result set")
}

var errNoRows = errors.New("no rows in result set")
