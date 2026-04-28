package migrate

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

//go:embed 0001_tasks.sql
var tasksSQL string

// Apply は River の internal table と domain の tasks を作る。
func Apply(ctx context.Context, pool *pgxpool.Pool) error {
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), nil)
	if err != nil {
		return err
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		return err
	}
	_, err = pool.Exec(ctx, tasksSQL)
	return err
}

// Reset は test 用に schema 丸ごと再作成する。River は table 以外に enum type も
// 作るので、選択的 DROP より public schema 再作成のほうが確実。
func Reset(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public;`)
	return err
}
