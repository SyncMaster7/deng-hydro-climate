-- =============================================================================
-- deng-hydro-climate — create_tables.sql
-- Runs automatically on first analytics-db startup via docker-entrypoint-initdb.d
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Schemas
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS staging;

-- ---------------------------------------------------------------------------
-- staging.hydro_raw
-- Raw hydrological observations from f_hydroseire API
-- One row per station + timestamp + measurement type (long format)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.hydro_raw (
    id                  BIGSERIAL PRIMARY KEY,
    jaam_kood           INTEGER NOT NULL,
    jaam_nimi           TEXT,
    valgala_nimi        TEXT,
    valgala_suurus_km2  NUMERIC,
    kaugus_suudmest_km  NUMERIC,
    jaam_laiuskraad     NUMERIC,
    jaam_pikkuskraad    NUMERIC,
    veekogu_nimi        TEXT,
    timeline_ts_utc     TIMESTAMP WITH TIME ZONE NOT NULL,
    timeline_ts_local   TIMESTAMP WITH TIME ZONE,
    aegrida_nimi        TEXT NOT NULL,
    vaartus             NUMERIC,
    loaded_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (jaam_kood, timeline_ts_utc, aegrida_nimi)
);

-- ---------------------------------------------------------------------------
-- staging.climate_raw
-- Raw climate observations from f_kliima_tund API
-- One row per station + timestamp + parameter (long format)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.climate_raw (
    id              BIGSERIAL PRIMARY KEY,
    jaam_kood       TEXT NOT NULL,
    jaam_nimi       TEXT,
    aasta           INTEGER NOT NULL,
    kuu             INTEGER NOT NULL,
    paev            INTEGER NOT NULL,
    tund            INTEGER NOT NULL,
    vaartus         NUMERIC,
    element_kood    TEXT NOT NULL,
    element_nimi    TEXT,
    element_yhik    TEXT,
    observation_ts  TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS (
                        make_timestamptz(aasta, kuu, paev, tund, 0, 0, 'UTC')
                    ) STORED,
    loaded_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (jaam_kood, aasta, kuu, paev, tund, element_kood)
);

-- ---------------------------------------------------------------------------
-- staging.etl_log
-- Logs every pipeline run — one row per task execution
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.etl_log (
    id              BIGSERIAL PRIMARY KEY,
    dag_id          TEXT,
    task_id         TEXT,
    run_date        DATE,
    started_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    finished_at     TIMESTAMP WITH TIME ZONE,
    rows_loaded     INTEGER,
    status          TEXT CHECK (status IN ('running', 'success', 'error')),
    error_message   TEXT
);

-- ---------------------------------------------------------------------------
-- dim_stations
-- Station reference data loaded from stations.csv (seed DAG)
-- Lives outside staging — it is a dimension, not raw data
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_stations (
    id                  BIGSERIAL PRIMARY KEY,
    station_code        TEXT NOT NULL UNIQUE,
    station_name        TEXT NOT NULL,
    station_category    TEXT NOT NULL,
    is_active           BOOLEAN NOT NULL DEFAULT true,
    loaded_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
