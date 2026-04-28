// Package outbox は tasks INSERT と同 tx で書かれた outbox 行を SQS に転送する。
// SendMessage と DELETE が別レイヤーになる以上 at-least-once になるが、
// worker 側の MarkCompleted が idempotent (status='pending' のときだけ更新)
// なので重複処理しても結果は同じ。
package outbox

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/bond-kaneko/fill-job-poc/sqs-impl/internal/repo"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Relay struct {
	pool         *pgxpool.Pool
	sqsClient    *sqs.Client
	queueURL     string
	pollInterval time.Duration
	logger       *slog.Logger
}

func New(pool *pgxpool.Pool, sqsClient *sqs.Client, queueURL string, pollInterval time.Duration, logger *slog.Logger) *Relay {
	if logger == nil {
		logger = slog.Default()
	}
	return &Relay{
		pool:         pool,
		sqsClient:    sqsClient,
		queueURL:     queueURL,
		pollInterval: pollInterval,
		logger:       logger,
	}
}

// Run は ctx が cancel されるまで loop。
func (r *Relay) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		sent, err := r.relayOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			r.logger.Warn("relay error", "err", err)
		}
		if !sent {
			if !sleepCtx(ctx, r.pollInterval) {
				return ctx.Err()
			}
		}
	}
}

// relayOnce は 1 件だけ送って DELETE する。送る対象がなければ false を返す。
//
// 構造: SELECT FOR UPDATE SKIP LOCKED → SendMessage → DELETE → COMMIT。
// SendMessage 失敗時は ROLLBACK して row は残る (次の iteration で retry)。
// SendMessage 成功 + COMMIT 失敗時は重複送信のリスクがあるが、
// worker 側 MarkCompleted の idempotency でカバーする。
func (r *Relay) relayOnce(ctx context.Context) (bool, error) {
	sent := false
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		taskID, body, err := repo.ClaimOutbox(ctx, tx)
		if err != nil {
			return err
		}
		if taskID == "" {
			return nil
		}
		_, sendErr := r.sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:    aws.String(r.queueURL),
			MessageBody: aws.String(string(body)),
		})
		if sendErr != nil {
			return sendErr
		}
		if err := repo.DeleteOutbox(ctx, tx, taskID); err != nil {
			return err
		}
		sent = true
		r.logger.Debug("relayed", "task_id", taskID)
		return nil
	})
	return sent, err
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
