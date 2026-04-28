CREATE TABLE IF NOT EXISTS tasks (
  id UUID PRIMARY KEY,
  payload JSONB NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending', 'completed', 'failed')),
  result JSONB,
  fill_error TEXT,
  filled_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- outbox: tx で「tasks INSERT + outbox INSERT」を一緒にコミットして
-- transactional enqueue を実現する。relay goroutine が SQS に送ってから DELETE する。
CREATE TABLE IF NOT EXISTS outbox (
  task_id UUID PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
  body JSONB NOT NULL,
  attempt INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS outbox_created_idx ON outbox (created_at);
