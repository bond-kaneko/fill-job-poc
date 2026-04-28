package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/bond-kaneko/fill-job-poc/shared/task"
	"github.com/bond-kaneko/fill-job-poc/sqs-impl/internal/repo"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	QueueURL          string
	VisibilityTimeout time.Duration
	WaitTime          time.Duration // SQS long-poll 時間
	MaxAttempts       int           // ApproximateReceiveCount >= これで domain failed
}

func DefaultConfig(queueURL string) Config {
	return Config{
		QueueURL:          queueURL,
		VisibilityTimeout: 30 * time.Second,
		WaitTime:          1 * time.Second,
		MaxAttempts:       3,
	}
}

type Worker struct {
	pool      *pgxpool.Pool
	sqsClient *sqs.Client
	cfg       Config
	logger    *slog.Logger
}

func New(pool *pgxpool.Pool, sqsClient *sqs.Client, cfg Config, logger *slog.Logger) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{pool: pool, sqsClient: sqsClient, cfg: cfg, logger: logger}
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := w.receiveAndProcess(ctx); err != nil && !errors.Is(err, context.Canceled) {
			w.logger.Warn("receive error", "err", err)
			if !sleepCtx(ctx, 200*time.Millisecond) {
				return ctx.Err()
			}
		}
	}
}

func (w *Worker) receiveAndProcess(ctx context.Context) error {
	resp, err := w.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:                    aws.String(w.cfg.QueueURL),
		MaxNumberOfMessages:         1,
		WaitTimeSeconds:             int32(w.cfg.WaitTime.Seconds()),
		VisibilityTimeout:           int32(w.cfg.VisibilityTimeout.Seconds()),
		MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{sqstypes.MessageSystemAttributeNameApproximateReceiveCount},
	})
	if err != nil {
		return err
	}
	for _, m := range resp.Messages {
		w.processMessage(ctx, m)
	}
	return nil
}

type messageBody struct {
	TaskID string `json:"task_id"`
}

func (w *Worker) processMessage(ctx context.Context, m sqstypes.Message) {
	var body messageBody
	if err := json.Unmarshal([]byte(*m.Body), &body); err != nil {
		w.logger.Error("invalid body", "err", err, "body", *m.Body)
		w.deleteMessage(ctx, m)
		return
	}
	taskID := body.TaskID

	receiveCount := 1
	if v, ok := m.Attributes[string(sqstypes.MessageSystemAttributeNameApproximateReceiveCount)]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			receiveCount = n
		}
	}

	payload, err := repo.LoadPayload(ctx, w.pool, taskID)
	if err != nil {
		w.logger.Error("load payload", "task_id", taskID, "err", err)
		// 削除しないで visibility timeout で redeliver させる
		return
	}

	w.logger.Info("processing", "task_id", taskID, "attempt", receiveCount)
	result, transient, perm := task.Process(ctx, payload, receiveCount)

	// ctx が cancel された場合 = shutdown / crash 模擬。何も書かずに抜ける。
	// SQS の visibility timeout 切れで別 worker が拾う。
	if errors.Is(ctx.Err(), context.Canceled) {
		w.logger.Info("aborted mid-process", "task_id", taskID)
		return
	}

	switch {
	case result != nil:
		// 順序: domain UPDATE → DeleteMessage。
		// commit 後 DeleteMessage 失敗ならまた redeliver されるが、
		// MarkCompleted は status='pending' でしか動かないので 2 回目は no-op。
		if err := pgx.BeginFunc(ctx, w.pool, func(tx pgx.Tx) error {
			return repo.MarkCompleted(ctx, tx, taskID, *result)
		}); err != nil {
			w.logger.Error("MarkCompleted failed", "task_id", taskID, "err", err)
			return
		}
		w.deleteMessage(ctx, m)

	case perm != nil:
		w.markFailed(ctx, taskID, fmt.Sprintf("permanent: %s", perm))
		w.deleteMessage(ctx, m)

	case transient != nil:
		if receiveCount >= w.cfg.MaxAttempts {
			w.markFailed(ctx, taskID, fmt.Sprintf("max_attempts(%d): %s", receiveCount, transient))
			w.deleteMessage(ctx, m)
			return
		}
		// transient: 削除しない → visibility timeout 切れで自動 redeliver
		w.logger.Info("transient, will retry via visibility timeout", "task_id", taskID, "attempt", receiveCount, "err", transient)

	default:
		w.logger.Error("Process returned no value", "task_id", taskID)
	}
}

func (w *Worker) deleteMessage(ctx context.Context, m sqstypes.Message) {
	_, err := w.sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(w.cfg.QueueURL),
		ReceiptHandle: m.ReceiptHandle,
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		w.logger.Warn("delete failed", "err", err)
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
