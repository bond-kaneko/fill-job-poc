package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/bond-kaneko/fill-job-poc/custom-impl/internal/api"
	"github.com/bond-kaneko/fill-job-poc/custom-impl/internal/migrate"
	"github.com/bond-kaneko/fill-job-poc/custom-impl/internal/service"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	dsn := envOr("DATABASE_URL", "postgres://poc:poc@localhost:5433/custom_db?sslmode=disable")

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

	svc := service.New(pool, 3)
	h := api.New(svc, pool)

	addr := envOr("LISTEN_ADDR", ":8080")
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
