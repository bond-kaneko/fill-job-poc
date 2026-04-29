# fill-job-poc

A proof-of-concept that implements the same async fill-job pattern in three
ways — **River**, **SQS (with outbox)**, and **custom polling** — so the same
six scenarios can be compared side-by-side.

The domain is abstracted to a generic `Task` (id, payload, status, result) so
the comparison stays focused on the queue layer rather than business logic.

## Scenarios under test

Each implementation covers the following scenarios in `integration_test.go`.

1. **happy path**: enqueue → completed
2. **transactional enqueue / atomicity**: domain INSERT and queue insertion in the same tx
3. **transient retry → success**: `payload.fail_until_attempt=N` retries N-1 times then succeeds
4. **permanent failure → status=failed**: `payload.always_fail=true`
5. **dedup**: enqueueing the same `task_id` twice in parallel runs once
6. **crash / hang recovery**: `payload.hang_first_attempts=1` blocks the first attempt; another worker path completes it

## Layout

```
fill-job-poc/
  go.work
  docker-compose.yml            # postgres:18 + softwaremill/elasticmq
  scripts/                      # init-db.sql, elasticmq.conf

  shared/                       # generic Task domain (entity, status, Payload, Process)

  river-impl/                   # riverqueue/river + riverpgxv5
  sqs-impl/                     # aws-sdk-go-v2/service/sqs + outbox table + elasticmq
  custom-impl/                  # pgx + SKIP LOCKED + lease + rescuer
```

## Running

```sh
make up           # start postgres + elasticmq
make tidy         # go mod tidy for every module
make test         # run all integration tests
```

Per-impl:

```sh
make test-custom  # 6 tests, ~4s
make test-river   # 5 tests, ~3s
make test-sqs     # 5 tests, ~6s
```

## Lines of code (queue-specific only)

Counts include the worker loop, queue SDK setup, outbox/relay code, and any
job-specific schema or repository queries. Excluded are HTTP handlers, command
mains, the shared domain, and migration scaffolding shared with non-queue
tables.

| Component | River | SQS | Custom |
|---|---:|---:|---:|
| worker | 67 | 179 | 141 |
| relay / outbox | 0 | 103 | 0 |
| SDK / driver setup | 24 | 58 | 0 |
| args / type definitions | 9 | 0 | 0 |
| job schema + repo queries | 0 | 0 | 186 |
| **Total queue-specific** | **100** | **340** | **327** |

## Feature mapping

| Aspect | River | SQS | Custom |
|---|---|---|---|
| External infra | Postgres only | Postgres + SQS-compatible service | Postgres only |
| External library | `riverqueue/river`, `riverpgxv5` | `aws-sdk-go-v2/service/sqs` | none beyond `pgx` |
| Transactional enqueue | `client.InsertTx(ctx, tx, args, opts)` | INSERT into `outbox` in tx + relay goroutine sends via `SendMessage` and DELETEs | INSERT into `jobs` in tx |
| Success-side domain sync | `JobCompleteTx` inside the worker tx (strong consistency) | domain `UPDATE` is committed before `DeleteMessage` (at-least-once + idempotent UPDATE) | jobs `DELETE` and tasks `UPDATE` in the same tx (strong consistency) |
| Transient retry | return `error`; `RetryPolicy.NextRetry` schedules attempt | return without `DeleteMessage`; redelivered after `VisibilityTimeout` | `attempt += 1`, `available_at = now() + Backoff(attempt)` |
| Permanent failure → domain failed | `MarkFailed` then `river.JobCancel(err)` (job → `cancelled`) | `MarkFailed` then `DeleteMessage` | attempt ≥ MaxAttempts: `MarkFailed` and DELETE jobs row |
| Dedup | `UniqueOpts{ByArgs: true}` | `outbox.task_id` UNIQUE | `jobs.task_id` UNIQUE |
| Crash / hang recovery | `RescueStuckJobsAfter` rescuer (built in) | `VisibilityTimeout` redelivery | rescuer goroutine clears `lease_until < now()` |
| Per-job timeout | `Config.JobTimeout` cancels worker ctx | not enforced; `VisibilityTimeout` only governs redelivery | not enforced; only `LeaseDuration` governs reclaim |
| Built-in monitoring UI | `riverui` package available | none in elasticmq; CloudWatch on AWS | none |

## Test mapping

| Scenario | River | SQS | Custom |
|---|---|---|---|
| happy path | `TestHappyPath` | `TestHappyPath` | `TestHappyPath` |
| transactional atomicity | implicit in `InsertTx` | implicit in outbox tx | `TestTransactionalEnqueueAtomicity` |
| transient retry | `TestTransientRetryThenSuccess` | `TestTransientRetryThenSuccess` | `TestTransientRetryThenSuccess` |
| permanent failure | `TestPermanentFailure` | `TestPermanentFailure` | `TestPermanentFailure` |
| dedup | `TestDuplicateEnqueueRunsOnce` | `TestDuplicateEnqueueRunsOnce` | `TestDuplicateEnqueueRunsOnce` |
| crash / hang recovery | `TestTimeoutRecovery` | `TestVisibilityRecovery` | `TestCrashRecovery` |

## Load test

`loadtest_test.go` in each impl runs under the `load` build tag. The same
parameters are used across all three implementations so the numbers are
directly comparable.

```sh
make load           # all three
make load-custom
make load-river
make load-sqs
```

| Parameter | Value |
|---|---|
| Number of tasks | 500 |
| Worker concurrency | 8 |
| Per-task work (`payload.compute_ms`) | 10 ms |
| Postgres pool (`MaxConns`) | 30 |
| Visibility timeout (sqs) | 30 s |
| Lease duration (custom) | 10 s |
| River `FetchCooldown` / `FetchPollInterval` | 10 ms / 50 ms |

End-to-end is the wall time from enqueueing the first task to seeing all
500 in `status='completed'`. Latency is `filled_at - created_at` per task,
aggregated via `percentile_cont` in Postgres. Numbers are from a single run
on a 2024 MacBook Pro (M3 Max) against `postgres:18` and
`softwaremill/elasticmq-native:1.6.11` over Docker Desktop.

| Impl | Enqueue rate | End-to-end rate | latency p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|---:|
| custom | 768 tasks/sec | 580 tasks/sec | 120 ms | 190 ms | 201 ms | 207 ms |
| river  | 770 tasks/sec | 379 tasks/sec | 306 ms | 610 ms | 637 ms | 649 ms |
| sqs    | 508 tasks/sec | 326 tasks/sec | 271 ms | 505 ms | 531 ms | 538 ms |

All three completed 500/500 tasks with zero failures and an empty outbox
table at the end. River numbers use the tuned `FetchCooldown` /
`FetchPollInterval` shown above; with River's default values (100 ms and
1 s) end-to-end throughput drops to roughly 78 tasks/sec on the same
input.

## Local SQS endpoint

`softwaremill/elasticmq-native:1.6.11` runs in `docker-compose.yml` and
serves the SQS protocol on `:9324`. Queue config is in
`scripts/elasticmq.conf`. The Go side switches via `BaseEndpoint`:

```go
sqs.NewFromConfig(cfg, func(o *sqs.Options) {
    o.BaseEndpoint = aws.String("http://localhost:9324")
})
```

## Behavioral notes

- River and Custom achieve exactly-once domain updates by completing the queue
  state inside the same `pgx.Tx` as the domain `UPDATE`. SQS is at-least-once
  by design; `MarkCompleted` is written with `WHERE status='pending'` so a
  redelivered message after a successful commit is a no-op.
- SQS visibility timeout in tests is set to 1s for fast turnaround
  (`worker.Config.VisibilityTimeout`). The default in `cmd/worker` is 30s.
- River's `RetryPolicy` is overridden in tests to retry after 50ms. The
  default backoff is exponential.
- Custom's lease in tests is 1s (`worker.Config.LeaseDuration`); default is
  30s.
- elasticmq is configured with a `fill-tasks-dlq` DLQ at
  `maxReceiveCount=3`, but the worker's `MaxAttempts=3` handler marks the
  domain failed and deletes the message before the DLQ would activate. The
  DLQ exists but is not exercised by the tests.

## License

MIT
