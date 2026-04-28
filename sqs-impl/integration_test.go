package fillpoc_test

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/bond-kaneko/fill-job-poc/shared/task"
	"github.com/bond-kaneko/fill-job-poc/sqs-impl/internal/migrate"
	"github.com/bond-kaneko/fill-job-poc/sqs-impl/internal/outbox"
	"github.com/bond-kaneko/fill-job-poc/sqs-impl/internal/repo"
	"github.com/bond-kaneko/fill-job-poc/sqs-impl/internal/service"
	"github.com/bond-kaneko/fill-job-poc/sqs-impl/internal/sqsx"
	"github.com/bond-kaneko/fill-job-poc/sqs-impl/internal/worker"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func dsn() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://poc:poc@localhost:5433/sqs_db?sslmode=disable"
}

type harness struct {
	pool     *pgxpool.Pool
	svc      *service.Service
	queueURL string
}

func setup(t *testing.T) *harness {
	t.Helper()
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn())
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

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	runCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	relay := outbox.New(pool, sqsClient, queueURL, 50*time.Millisecond, logger)
	go func() { _ = relay.Run(runCtx) }()

	wcfg := worker.Config{
		QueueURL:          queueURL,
		VisibilityTimeout: 1 * time.Second,
		WaitTime:          1 * time.Second,
		MaxAttempts:       3,
	}
	// 2 worker 並列にして「片方が hang → もう片方が visibility timeout 切れの message を拾う」
	// crash 復旧 semantics をテストできるようにする
	for range 2 {
		w := worker.New(pool, sqsClient, wcfg, logger)
		go func() { _ = w.Run(runCtx) }()
	}

	return &harness{
		pool:     pool,
		svc:      service.New(pool),
		queueURL: queueURL,
	}
}

func drainQueue(t *testing.T, ctx context.Context, c *sqs.Client, url string) {
	t.Helper()
	for {
		resp, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(url),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     0,
		})
		if err != nil {
			return
		}
		if len(resp.Messages) == 0 {
			return
		}
		for _, m := range resp.Messages {
			_, _ = c.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(url),
				ReceiptHandle: m.ReceiptHandle,
			})
		}
	}
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
	h := setup(t)
	ctx := context.Background()
	id := uuid.NewString()
	require.NoError(t, h.svc.CreateTask(ctx, id, task.Payload{Echo: "hello"}))

	waitFor(t, 8*time.Second, func() bool {
		tk, err := repo.LoadTask(ctx, h.pool, id)
		return err == nil && tk.Status == task.StatusCompleted
	})

	tk, err := repo.LoadTask(ctx, h.pool, id)
	require.NoError(t, err)
	require.Equal(t, task.StatusCompleted, tk.Status)
	require.Equal(t, "HELLO", tk.Result.Echo)
}

func TestTransientRetryThenSuccess(t *testing.T) {
	h := setup(t)
	ctx := context.Background()
	id := uuid.NewString()
	// FailUntilAttempt=3 で attempt 1,2 が transient 失敗、3 で成功
	require.NoError(t, h.svc.CreateTask(ctx, id, task.Payload{Echo: "retry", FailUntilAttempt: 3}))

	waitFor(t, 10*time.Second, func() bool {
		tk, err := repo.LoadTask(ctx, h.pool, id)
		return err == nil && tk.Status == task.StatusCompleted
	})

	tk, err := repo.LoadTask(ctx, h.pool, id)
	require.NoError(t, err)
	require.Equal(t, task.StatusCompleted, tk.Status)
}

func TestPermanentFailure(t *testing.T) {
	h := setup(t)
	ctx := context.Background()
	id := uuid.NewString()
	require.NoError(t, h.svc.CreateTask(ctx, id, task.Payload{Echo: "bad", AlwaysFail: true}))

	waitFor(t, 8*time.Second, func() bool {
		tk, err := repo.LoadTask(ctx, h.pool, id)
		return err == nil && tk.Status == task.StatusFailed
	})

	tk, err := repo.LoadTask(ctx, h.pool, id)
	require.NoError(t, err)
	require.Equal(t, task.StatusFailed, tk.Status)
	require.NotNil(t, tk.FillError)
}

func TestDuplicateEnqueueRunsOnce(t *testing.T) {
	h := setup(t)
	ctx := context.Background()
	id := uuid.NewString()

	var wg sync.WaitGroup
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			if err := h.svc.CreateTask(ctx, id, task.Payload{Echo: "once", ComputeMs: 100}); err != nil {
				t.Errorf("CreateTask: %v", err)
			}
		}()
	}
	wg.Wait()

	waitFor(t, 8*time.Second, func() bool {
		tk, err := repo.LoadTask(ctx, h.pool, id)
		return err == nil && tk.Status == task.StatusCompleted
	})

	// outbox UNIQUE 制約により 1 行のみ INSERT される (= 1 メッセージしか送られない)
	var outboxCount int
	require.NoError(t, h.pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE task_id=$1`, id).Scan(&outboxCount))
	require.Equal(t, 0, outboxCount, "outbox should be drained")
}

func TestVisibilityRecovery(t *testing.T) {
	// HangFirstAttempts=1: attempt 1 で 60s hang して worker A は完走できない。
	// VisibilityTimeout=1s で worker B が同じメッセージを受信し、
	// attempt 2 (HangFirstAttempts 超え) なので素直に成功する。
	h := setup(t)
	ctx := context.Background()
	id := uuid.NewString()
	require.NoError(t, h.svc.CreateTask(ctx, id, task.Payload{
		Echo:              "recovered",
		HangFirstAttempts: 1,
	}))

	waitFor(t, 12*time.Second, func() bool {
		tk, err := repo.LoadTask(ctx, h.pool, id)
		return err == nil && tk.Status == task.StatusCompleted
	})

	tk, err := repo.LoadTask(ctx, h.pool, id)
	require.NoError(t, err)
	require.Equal(t, task.StatusCompleted, tk.Status)
	require.Equal(t, "RECOVERED", tk.Result.Echo)
}
