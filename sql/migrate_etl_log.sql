-- =============================================================================
-- deng-hydro-climate — migrate_etl_log.sql
-- Migration: etl_log logging overhaul
--   - rename run_date → target_date
--   - add rows_fetched column
--   - extend status check constraint to include spy_dag statuses
-- =============================================================================

BEGIN;

-- 1. Rename run_date → target_date
ALTER TABLE bronze.etl_log
    RENAME COLUMN run_date TO target_date;

-- 2. Add rows_processed column
ALTER TABLE bronze.etl_log
    ADD COLUMN rows_processed INTEGER;

-- 3. Update status check constraint to include spy_dag statuses
ALTER TABLE bronze.etl_log
    DROP CONSTRAINT IF EXISTS etl_log_status_check;

ALTER TABLE bronze.etl_log
    ADD CONSTRAINT etl_log_status_check
    CHECK (status IN ('running', 'success', 'error', 'triggered', 'skipped'));

COMMIT;
