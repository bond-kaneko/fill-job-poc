package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/bond-kaneko/fill-job-poc/custom-impl/internal/migrate"
	"github.com/bond-kaneko/fill-job-poc/custom-impl/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	dsn := envOr("DATABASE_URL", "postgres://poc:poc@localhost:5433/custom_db?sslmode=disable")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		logger.Error("connect", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := migrate.Apply(ctx, pool); err != nil {
		logger.Error("migrate", "err", err)
		os.Exit(1)
	}

	w := worker.New(pool, worker.DefaultConfig(), logger)
	logger.Info("worker started")
	if err := w.Run(ctx); err != nil && err != context.Canceled {
		logger.Error("worker exited", "err", err)
		os.Exit(1)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
