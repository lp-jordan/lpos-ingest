-- Run once against your Railway Postgres instance

CREATE TABLE ingest_clients (
  id         SERIAL PRIMARY KEY,
  token      TEXT NOT NULL UNIQUE,   -- URL-safe random string, e.g. nanoid(12)
  first_name TEXT NOT NULL,
  active     BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ingest_submissions (
  id         SERIAL PRIMARY KEY,
  client_id  INTEGER NOT NULL REFERENCES ingest_clients(id),
  file_key   TEXT NOT NULL,          -- R2 object key
  file_name  TEXT NOT NULL,
  file_size  BIGINT,
  mime_type  TEXT,
  processed  BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ON ingest_submissions (client_id, created_at DESC);
