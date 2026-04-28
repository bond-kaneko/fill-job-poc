package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/bond-kaneko/fill-job-poc/river-impl/internal/clientx"
	"github.com/bond-kaneko/fill-job-poc/river-impl/internal/migrate"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	dsn := envOr("DATABASE_URL", "postgres://poc:poc@localhost:5433/river_db?sslmode=disable")

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

	client, err := clientx.New(pool, &river.Config{
		Logger: logger,
	})
	if err != nil {
		logger.Error("river client", "err", err)
		os.Exit(1)
	}

	logger.Info("worker started")
	if err := client.Start(ctx); err != nil {
		logger.Error("start", "err", err)
		os.Exit(1)
	}
	<-ctx.Done()
	stopCtx, cancelStop := context.WithCancel(context.Background())
	defer cancelStop()
	if err := client.Stop(stopCtx); err != nil {
		logger.Warn("stop", "err", err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
