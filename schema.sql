-- Run once against your Railway Postgres instance

CREATE TABLE ingest_clients (
  id               SERIAL PRIMARY KEY,
  token            TEXT NOT NULL UNIQUE,   -- URL-safe random string
  lpos_project_id  TEXT NOT NULL UNIQUE,   -- projectId from LPOS
  first_name       TEXT NOT NULL,          -- used in "Hi, [name]!" greeting
  active           BOOLEAN NOT NULL DEFAULT true,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ingest_submissions (
  id           SERIAL PRIMARY KEY,
  client_id    INTEGER NOT NULL REFERENCES ingest_clients(id),
  file_key     TEXT NOT NULL,          -- R2 object key
  file_name    TEXT NOT NULL,
  file_size    BIGINT,
  mime_type    TEXT,
  processed    BOOLEAN NOT NULL DEFAULT false,
  promoted_to  TEXT,                   -- 'assets' | 'scripts' once promoted
  promoted_at  TIMESTAMPTZ,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Migration for existing databases (run manually if table already exists)
-- ALTER TABLE ingest_submissions ADD COLUMN IF NOT EXISTS promoted_to TEXT;
-- ALTER TABLE ingest_submissions ADD COLUMN IF NOT EXISTS promoted_at TIMESTAMPTZ;

CREATE INDEX ON ingest_submissions (client_id, created_at DESC);
