package migrate

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed 0001_init.sql
var initSQL string

func Apply(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, initSQL)
	return err
}

// Reset は test 用に schema を一掃する。
func Reset(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `DROP TABLE IF EXISTS jobs, tasks CASCADE`)
	return err
}
