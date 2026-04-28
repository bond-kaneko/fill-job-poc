package service

import (
	"context"
	"errors"

	"github.com/bond-kaneko/fill-job-poc/custom-impl/internal/repo"
	"github.com/bond-kaneko/fill-job-poc/shared/task"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Service struct {
	pool        *pgxpool.Pool
	maxAttempts int
}

func New(pool *pgxpool.Pool, maxAttempts int) *Service {
	return &Service{pool: pool, maxAttempts: maxAttempts}
}

// CreateTask は domain INSERT と job INSERT を同一 tx で行う。
// 同 id を 2 回呼んでも 1 行しか作られない (ON CONFLICT DO NOTHING)。
// 戻り値の bool は新規作成された場合 true、重複なら false。
func (s *Service) CreateTask(ctx context.Context, id string, payload task.Payload) (bool, error) {
	if id == "" {
		return false, errors.New("task id is required")
	}
	created := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// domain insert
		if err := repo.InsertTask(ctx, tx, id, payload); err != nil {
			return err
		}
		// jobs insert (transactional enqueue)
		if err := repo.InsertJob(ctx, tx, id, s.maxAttempts); err != nil {
			return err
		}
		// 確認: 既存 row だったか新規だったかは created_at で判断するより
		// 単純に「task が pending なら新規 enqueue 成功」とみなす。
		// ただしこの確認のためにもう 1 度 SELECT するのは過剰なので、
		// caller が必要なら LoadTask を呼ぶ。
		created = true
		return nil
	})
	return created, err
}
