CREATE TABLE IF NOT EXISTS tasks (
  id UUID PRIMARY KEY,
  payload JSONB NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending', 'completed', 'failed')),
  result JSONB,
  fill_error TEXT,
  filled_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
