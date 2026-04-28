# fill-job-poc

Async fill-job pattern を **River / SQS / 自前 polling** の 3 方式で同じシナリオに対して実装し、
「機能を満たすために何を書く必要があるか」を比較する PoC。

ドメインは汎用 `Task` (id, payload, status, result) に抽象化してある。

## 何を比較するか

下記 5〜6 シナリオを 3 方式で実装し、コード量・依存・実装パターンを比較する。

1. **happy path**: enqueue → completed
2. **transactional enqueue / atomicity**: domain INSERT と queue への登録が同 tx
3. **一時失敗 → 自動 retry**: `payload.fail_until_attempt=N` で N-1 回 retry してから成功
4. **恒久失敗 → status=failed**: `payload.always_fail=true`
5. **重複防止**: 同 task_id を並列で 2 回 enqueue → 実行は 1 回のみ
6. **クラッシュ / hang 復旧**: `payload.hang_first_attempts=1` で初回 hang、別 worker 経路で完走

## 構成

```
fill-job-poc/
  go.work                       # workspace
  docker-compose.yml            # postgres:18 + softwaremill/elasticmq
  scripts/                      # init-db.sql, elasticmq.conf

  shared/                       # 共通ドメイン (Task entity / status / Payload / Process)

  river-impl/                   # riverqueue/river + riverpgxv5
  sqs-impl/                     # aws-sdk-go-v2/service/sqs + outbox + elasticmq
  custom-impl/                  # pgx + SKIP LOCKED + 自前 lease/rescuer
```

## 動かし方

```sh
make up           # postgres + elasticmq 起動
make tidy         # 各 module の go mod tidy
make test         # 3 impl 全部の統合テストを実行
```

個別:

```sh
make test-custom  # 6 tests, ~4s
make test-river   # 5 tests, ~3s
make test-sqs     # 5 tests, ~6s
```

## 実装比較

queue 固有のコード (worker / outbox-relay / job スキーマ / SDK 設定) のみカウント。
ドメインや HTTP API・main など queue 非依存の部分は除外。

| 項目 | River | SQS | Custom |
|---|---|---|---|
| queue 固有 LOC | **100** | **340** | **327** |
| └ worker | 67 | 179 | 141 |
| └ enqueue 側 (relay 等) | 0 | 103 (outbox/relay.go) | 0 (jobs テーブル INSERT のみ) |
| └ SDK 初期化 / driver | 24 (clientx.go) | 58 (sqsx.go) | 0 |
| └ Args / 型定義 | 9 (args.go) | 0 | 0 |
| └ jobs 関連 schema/repo | 0 (River 内蔵) | 0 (outbox は relay に集約) | 186 (jobs schema + repo の job 関連 query) |
| 外部依存 (queue) | `riverqueue/river`, `riverpgxv5` | `aws-sdk-go-v2/service/sqs` + elasticmq image | なし |
| transactional enqueue | `client.InsertTx` 1 行 | outbox INSERT + relay goroutine (poll → SendMessage → DELETE) | jobs テーブルに同 tx INSERT |
| 成功時の domain 同期 | `JobCompleteTx` で同 tx (strong consistency) | DeleteMessage 前に domain tx commit (at-least-once + idempotent UPDATE) | jobs DELETE と domain UPDATE 同 tx (strong consistency) |
| 一時失敗 retry | `error` 返却で `RetryPolicy` の指数 backoff | error → ack せず visibility timeout 切れで再受信 | attempt+1 / available_at = now() + backoff |
| 恒久失敗 → failed | `river.JobCancel(err)` で discarded、その前に domain UPDATE | `ApproximateReceiveCount >= MaxAttempts` で domain UPDATE + DeleteMessage | attempt >= MaxAttempts で jobs DELETE + domain UPDATE |
| 重複防止 | `UniqueOpts{ByArgs: true}` (River dedup window) | outbox の `task_id` UNIQUE (DB 側ではじく) | `jobs.task_id` UNIQUE |
| クラッシュ復旧 | rescuer (River 内蔵、`RescueStuckJobsAfter` で発動) | visibility timeout で別 worker が再受信 | 自前 rescuer goroutine (`RescueStaleLeases`) |
| 監視 UI | River Web UI (別 package) 標準装備 | AWS Console / DLQ。LocalStack/elasticmq 単体だと無い | 自前で SELECT |

## 観察ポイント

- **River の優位**: queue 固有コードが 100 LOC で一番少ない。`InsertTx` / `JobCompleteTx` が strong consistency を 1 行で表現でき、retry / unique / 復旧をすべて library 側に寄せられる。コードを読むときに「queue 機能の実装」を気にしなくて良い分、ドメインに集中できる。
- **SQS の重さは outbox**: `relay.go` (103 LOC) + `sqsx.go` (58 LOC) = 161 LOC が「queue が DB tx に参加できない」ことの直接コスト。+ at-least-once 配信のため worker の `MarkCompleted` を idempotent (`WHERE status='pending'`) にする必要がある。
- **Custom が "ライブラリ任せの中身"**: SKIP LOCKED + lease 管理 + rescuer + backoff + dedup を全部書く。River がやってくれてる事のほぼ全てが見える。`jobs` テーブル schema と関連 query (~186 LOC) が実質ジョブキューの実装本体。
- **クラッシュ復旧の哲学差**:
  - River: heartbeat + rescuer (push 型、明示的)
  - SQS: visibility timeout (pull 型、message-driven)
  - Custom: lease + rescuer (DB-driven、間隔は自分で決める)
  - 結果としてどれも「N 秒 worker が応答しなかったら誰かが拾い直す」になるが、信頼の主体が違う。

## テスト構造

各 impl の `integration_test.go` で同等のシナリオを検証している。

| シナリオ | River | SQS | Custom |
|---|---|---|---|
| happy path | `TestHappyPath` | `TestHappyPath` | `TestHappyPath` |
| transactional atomicity | (River の InsertTx 自体が tx) | (outbox INSERT が同 tx) | `TestTransactionalEnqueueAtomicity` (rollback 時 task が残らない) |
| transient retry | `TestTransientRetryThenSuccess` | `TestTransientRetryThenSuccess` | `TestTransientRetryThenSuccess` |
| permanent failure | `TestPermanentFailure` | `TestPermanentFailure` | `TestPermanentFailure` |
| dedup | `TestDuplicateEnqueueRunsOnce` | `TestDuplicateEnqueueRunsOnce` | `TestDuplicateEnqueueRunsOnce` |
| crash / hang 復旧 | `TestTimeoutRecovery` | `TestVisibilityRecovery` | `TestCrashRecovery` |

## License

MIT
