package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bond-kaneko/fill-job-poc/sqs-impl/internal/api"
	"github.com/bond-kaneko/fill-job-poc/sqs-impl/internal/migrate"
	"github.com/bond-kaneko/fill-job-poc/sqs-impl/internal/outbox"
	"github.com/bond-kaneko/fill-job-poc/sqs-impl/internal/service"
	"github.com/bond-kaneko/fill-job-poc/sqs-impl/internal/sqsx"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	dsn := envOr("DATABASE_URL", "postgres://poc:poc@localhost:5433/sqs_db?sslmode=disable")

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

	sqsClient, err := sqsx.New(ctx, sqsx.DefaultOpts())
	if err != nil {
		logger.Error("sqs client", "err", err)
		os.Exit(1)
	}
	queueURL, err := sqsx.QueueURL(ctx, sqsClient, envOr("QUEUE_NAME", "fill-tasks"))
	if err != nil {
		logger.Error("queue url", "err", err)
		os.Exit(1)
	}

	// API は relay も同居 (PoC 簡略化)。本番では別 process が普通。
	relay := outbox.New(pool, sqsClient, queueURL, 200*time.Millisecond, logger)
	go func() {
		if err := relay.Run(ctx); err != nil && err != context.Canceled {
			logger.Error("relay exited", "err", err)
		}
	}()

	svc := service.New(pool)
	h := api.New(svc, pool)

	addr := envOr("LISTEN_ADDR", ":8082")
	logger.Info("api+relay listening", "addr", addr)
	server := &http.Server{Addr: addr, Handler: h.Routes()}
	go func() {
		<-ctx.Done()
		shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = server.Shutdown(shutdownCtx)
	}()
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
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
