-- =============================================================================
-- deng-hydro-climate — migrate_monitoring.sql
-- Migration: API request logging
--   - create monitoring schema
--   - create monitoring.request_log table
-- =============================================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS monitoring.request_log (
    id              BIGSERIAL PRIMARY KEY,
    requested_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    method          TEXT NOT NULL,
    endpoint        TEXT NOT NULL,
    query_params    JSONB,
    status_code     INTEGER NOT NULL,
    response_ms     NUMERIC(10, 2) NOT NULL,
    client_ip       TEXT
);

CREATE INDEX IF NOT EXISTS idx_request_log_requested_at  ON monitoring.request_log (requested_at DESC);
CREATE INDEX IF NOT EXISTS idx_request_log_endpoint      ON monitoring.request_log (endpoint);
CREATE INDEX IF NOT EXISTS idx_request_log_status_code   ON monitoring.request_log (status_code);

COMMIT;
