package fillpoc_test

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/bond-kaneko/fill-job-poc/river-impl/internal/clientx"
	"github.com/bond-kaneko/fill-job-poc/river-impl/internal/migrate"
	"github.com/bond-kaneko/fill-job-poc/river-impl/internal/repo"
	"github.com/bond-kaneko/fill-job-poc/river-impl/internal/service"
	"github.com/bond-kaneko/fill-job-poc/shared/task"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"github.com/stretchr/testify/require"
)

func dsn() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://poc:poc@localhost:5433/river_db?sslmode=disable"
}

// fastRetry は test 用の即時 retry policy。
type fastRetry struct{}

func (fastRetry) NextRetry(_ *rivertype.JobRow) time.Time {
	return time.Now().Add(50 * time.Millisecond)
}

func setup(t *testing.T) (*pgxpool.Pool, *river.Client[pgx.Tx], func()) {
	t.Helper()
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn())
	require.NoError(t, err)

	require.NoError(t, migrate.Reset(ctx, pool))
	require.NoError(t, migrate.Apply(ctx, pool))

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	client, err := clientx.New(pool, &river.Config{
		Logger:               logger,
		RetryPolicy:          fastRetry{},
		JobTimeout:           500 * time.Millisecond,
		RescueStuckJobsAfter: 2 * time.Second,
	})
	require.NoError(t, err)

	require.NoError(t, client.Start(ctx))

	cleanup := func() {
		stopCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = client.Stop(stopCtx)
		pool.Close()
	}
	return pool, client, cleanup
}

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
	pool, client, cleanup := setup(t)
	defer cleanup()
	svc := service.New(pool, client, 3)

	ctx := context.Background()
	id := uuid.NewString()
	require.NoError(t, svc.CreateTask(ctx, id, task.Payload{Echo: "hello"}))

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
	pool, client, cleanup := setup(t)
	defer cleanup()
	svc := service.New(pool, client, 5)

	ctx := context.Background()
	id := uuid.NewString()
	require.NoError(t, svc.CreateTask(ctx, id, task.Payload{Echo: "retry", FailUntilAttempt: 3}))

	waitFor(t, 5*time.Second, func() bool {
		tk, err := repo.LoadTask(ctx, pool, id)
		return err == nil && tk.Status == task.StatusCompleted
	})

	tk, err := repo.LoadTask(ctx, pool, id)
	require.NoError(t, err)
	require.Equal(t, task.StatusCompleted, tk.Status)
}

func TestPermanentFailure(t *testing.T) {
	pool, client, cleanup := setup(t)
	defer cleanup()
	svc := service.New(pool, client, 3)

	ctx := context.Background()
	id := uuid.NewString()
	require.NoError(t, svc.CreateTask(ctx, id, task.Payload{Echo: "bad", AlwaysFail: true}))

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
	pool, client, cleanup := setup(t)
	defer cleanup()
	svc := service.New(pool, client, 3)

	ctx := context.Background()
	id := uuid.NewString()
	var wg sync.WaitGroup
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			if err := svc.CreateTask(ctx, id, task.Payload{Echo: "once", ComputeMs: 50}); err != nil {
				t.Errorf("CreateTask: %v", err)
			}
		}()
	}
	wg.Wait()

	waitFor(t, 5*time.Second, func() bool {
		tk, err := repo.LoadTask(ctx, pool, id)
		return err == nil && tk.Status == task.StatusCompleted
	})

	// river_job 上に同じ args の job が 1 件しか入ってないことを確認 (UniqueOpts.ByArgs)
	var jobCount int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM river_job WHERE args->>'task_id'=$1`, id,
	).Scan(&jobCount))
	require.Equal(t, 1, jobCount, "Unique opts should have deduped")
}

func TestTimeoutRecovery(t *testing.T) {
	// HangFirstAttempts=1 で attempt 1 を 60s hang させる。
	// JobTimeout=500ms なので attempt 1 は ctx.Err で transient 失敗、
	// fastRetry policy で 50ms 後に attempt 2 が走り、これは hang しないので成功する。
	// = "worker が job を完走できなかったあと、別の試行が成功する" という River の crash 復旧 semantics。
	pool, client, cleanup := setup(t)
	defer cleanup()
	svc := service.New(pool, client, 5)

	ctx := context.Background()
	id := uuid.NewString()
	require.NoError(t, svc.CreateTask(ctx, id, task.Payload{
		Echo:              "recovered",
		HangFirstAttempts: 1,
	}))

	waitFor(t, 8*time.Second, func() bool {
		tk, err := repo.LoadTask(ctx, pool, id)
		return err == nil && tk.Status == task.StatusCompleted
	})

	tk, err := repo.LoadTask(ctx, pool, id)
	require.NoError(t, err)
	require.Equal(t, task.StatusCompleted, tk.Status)
	require.Equal(t, "RECOVERED", tk.Result.Echo)
}
