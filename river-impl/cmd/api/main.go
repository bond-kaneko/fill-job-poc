package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/bond-kaneko/fill-job-poc/river-impl/internal/api"
	"github.com/bond-kaneko/fill-job-poc/river-impl/internal/clientx"
	"github.com/bond-kaneko/fill-job-poc/river-impl/internal/migrate"
	"github.com/bond-kaneko/fill-job-poc/river-impl/internal/service"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	dsn := envOr("DATABASE_URL", "postgres://poc:poc@localhost:5433/river_db?sslmode=disable")

	ctx := context.Background()
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

	// API では client は enqueue 専用 (worker は別 cmd で動く)
	client, err := clientx.New(pool, &river.Config{
		Workers: river.NewWorkers(), // 空 = enqueue only
	})
	if err != nil {
		logger.Error("river client", "err", err)
		os.Exit(1)
	}

	svc := service.New(pool, client, 3)
	h := api.New(svc, pool)

	addr := envOr("LISTEN_ADDR", ":8081")
	logger.Info("api listening", "addr", addr)
	if err := http.ListenAndServe(addr, h.Routes()); err != nil {
		logger.Error("listen", "err", err)
		os.Exit(1)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
