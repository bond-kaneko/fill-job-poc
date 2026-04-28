CREATE TABLE IF NOT EXISTS tasks (
  id UUID PRIMARY KEY,
  payload JSONB NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending', 'completed', 'failed')),
  result JSONB,
  fill_error TEXT,
  filled_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jobs (
  task_id UUID PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
  attempt INT NOT NULL DEFAULT 0,
  max_attempts INT NOT NULL DEFAULT 3,
  available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  lease_until TIMESTAMPTZ,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS jobs_available_idx ON jobs (available_at);
