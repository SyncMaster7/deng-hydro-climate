# Project Progress — deng-hydro-climate

This document tracks the current state of the project, what is complete, known gaps, accepted limitations, and the next steps backlog. Updated at the end of each working session.

**Last updated:** 2026-05-29 — Session 33 complete

---

## Current Status

The core pipeline is fully operational. Data has been backfilled from 2025-01-01 and the daily pipeline runs reliably at 06:00 UTC. The public API is live. DataHub metadata cataloguing is complete with full lineage. The main open items are refinements — FastAPI fixes, dbt test coverage expansion, DataHub glossary re-entry, and metadata enrichment.

| Area | Status |
|---|---|
| Daily pipeline (fetch → ingest → dbt) | ✅ Running — 06:00 UTC daily |
| Historical backfill (2025-01-01 onwards) | ✅ Complete — ~7.8M hydro rows, ~2.9M meteo rows |
| dbt bronze → silver → gold → api | ✅ All models building |
| dbt data quality tests (bronze) | ✅ 26 tests — all passing |
| Public REST API (api.deng.ee) | ✅ Live |
| API usage monitoring | ✅ Logging to monitoring.request_log |
| Apache Superset dashboards | ✅ Running — 3 charts published |
| DataHub — PostgreSQL ingestion | ✅ All 17 tables indexed |
| DataHub — dbt lineage | ✅ Full lineage: bronze → silver → gold → api |
| DataHub — Superset ingestion | ✅ 20 charts, 3 dashboards indexed |
| DataHub — Airflow ingestion | ✅ Push-based via plugin |
| DataHub glossary | ⏳ Pending re-entry (8 items, English hierarchy) |
| FastAPI default time window fix | ⏳ Pending |
| FastAPI async monitoring refactor | ⏳ Pending |
| dbt tests — silver / gold / api layers | ⏳ Not yet done |
| DataHub metadata enrichment (DCAT-AP) | ⏳ Not yet started |
| Public catalogue frontend | ⏳ Not yet started |

---

## What Has Been Built

### Pipeline and Ingestion
- Full daily ETL pipeline: `fetch_hydro` → `fetch_meteo` → `ingest_hydro` → `ingest_meteo` → `run_dbt`
- `API_LAG_DAYS=3` offset (72h buffer) — guarantees a complete day is always available given the ~43h hydro API publish lag
- Batch UPSERT via `execute_values()` — no row-by-row inserts
- Retry config on fetch tasks: `retries=3`, exponential backoff, ~1h30m total window
- Empty response guard — raises `ValueError` and logs to `etl_log` before retrying
- All pipeline stages logged to `bronze.etl_log` with `rows_processed`, `rows_loaded`, and status
- Weekly archive DAG compresses JSON files older than 7 days into `.json.gz`

### Data Model
- Six database schemas: `ref`, `bronze`, `silver`, `gold`, `api`, `monitoring`
- `silver.hydro` — pivoted wide, EH2000 absolute elevation correction (`wl_avg_eh2000`)
- `silver.meteo` — pivoted wide, UTC timestamp constructed from local Estonian time fields
- `gold.hydro_meteo` — hydro joined to nearest meteo station (Haversine proximity, rank=1), Estonian local time
- `api` schema — fact/dim design for public API serving: 3 dimension tables + 2 incremental fact tables
- SCD2 snapshots for both station tables — point-in-time EH2000 correction for historical data
- `monitoring.request_log` — FastAPI request log with response times, status codes, client IP

### Data Quality
- 26 dbt tests on bronze layer — 16 generic (not_null, accepted_values, uniqueness) + 10 singular (physical value ranges)
- All tests use `error` severity — failure halts downstream builds
- `dbt-utils` package for composite unique key tests

### Public API
- FastAPI at `https://api.deng.ee` — 8 endpoints covering stations, elements, and observations
- Rate limited to 60 req/min per IP via `slowapi`
- asyncpg connection pool — min 2, max 10 connections
- Composite indexes on `api.observations_hydro` and `api.observations_meteo` — `(station_code, element_code, obs_ts)`
- Interactive Swagger UI at `https://api.deng.ee/docs`

### Metadata and Cataloguing
- DataHub fully integrated — PostgreSQL, dbt, Superset, and Airflow all ingested
- Full column-level lineage: `bronze.hydro` → `silver.hydro` → `gold.hydro_meteo` → Superset charts
- Tags applied to all entities: `deng`, `postgresql`, `dbt`, `superset`, schema-specific tags
- Column-level profiling on all tables
- DataHub runbook at `docs/runbook/datahub_runbook.md`

---

## Pending Items

### 1. DataHub Glossary Re-entry
Re-enter the English business glossary in **Govern → Glossary**. Was lost in Session 33 volume wipe — reference files preserved at `datahub/glossary/`.

Structure to recreate:

| Level | Type | Name | Description |
|---|---|---|---|
| Root | Node | Hydrology | Hydrological monitoring concepts. |
| Root | Node | Meteorology | |
| Root | Node | Data Engineering | Technical pipeline and ingestion metadata. Not scientific measurements. |
| Under Hydrology | Node | Measurement | Hydrological observation and measurement concepts. |
| Under Hydrology | Node | Monitoring Station | Concepts related to hydrological monitoring stations. |
| Under Hydrology | Node | Water Body | Water body concepts — rivers, lakes, reservoirs and coastal waters. |
| Under Hydrology | Node | Catchment | |
| Under Data Engineering | Term | Load Timestamp | Timestamp when the record was ingested into the data platform. Data engineering metadata only, not an observation. |

> Do NOT recreate `Hydro_meteo sõnastik` — old Estonian test hierarchy, dropped intentionally.

### 2. FastAPI Fixes

**Default time window** — change `timedelta(hours=24)` to `timedelta(days=4)` in `docker/fastapi/main.py` (line ~45), then rebuild container. The current 24h default returns empty results because freshest data is ~3 days old due to `API_LAG_DAYS=3`.

**Endpoint testing** — test all endpoints after the time window fix: `/latest`, multi-element filter, date range filter with explicit `from_ts`/`to_ts`.

**Async monitoring refactor** — middleware currently awaits the DB insert before returning the response. Refactor to `asyncio.create_task()` + extracted `_write_request_log()` helper so the client receives the response immediately and logging happens in the background.

### 3. dbt Test Coverage Expansion

Bronze layer is fully tested. Remaining layers:

| Layer | Priority | Notes |
|---|---|---|
| Source freshness (bronze) | High | Via `dbt source freshness` — separate from `dbt build`. Thresholds: hydro warn 30h/error 48h, meteo warn 26h/error 36h |
| Silver layer | Medium | Not yet defined |
| Gold layer | Medium | Not yet defined |
| API layer | Medium | Not yet defined |

### 4. DataHub Metadata Enrichment

Add DCAT-AP aligned descriptions and business context to DataHub entities. Kermo has the Estonian national portal template as a reference. This is a prerequisite for the public catalogue frontend.

### 5. Public Catalogue Frontend

A lightweight HTML/JS frontend that reads the DataHub GraphQL API at page load and auto-updates when metadata changes. Also includes research and design of the national open data portal (avaandmed.eesti.ee) submission workflow — no actual submission, school project scope only.

### 6. Superset Monitoring Dashboard

Build a Superset dashboard on top of `monitoring.request_log` covering: request counts by endpoint, error rates by status code, and response time trends over time.

---

## Accepted Limitations

These are known gaps that are intentionally not addressed within school project scope.

| Limitation | Notes |
|---|---|
| dbt tests cover bronze layer only | Silver, gold, api layers not yet tested |
| FastAPI default 24h window returns empty | Pending fix to `timedelta(days=4)` — see Pending Items |
| FastAPI monitoring middleware not fully async | Awaits DB insert before responding — async refactor pending |
| dbt singular test bounds based on partial profiling | Covers 2026 + up to May 2025 data; revisit after full historical DataHub profiling |
| DataHub glossary pending re-entry | Lost in Session 33 rebuild — reference files preserved |
| `f_kliima_minut` not ingested | 10-minute precipitation data available but out of scope |
| `wl_min_eh2000` / `wl_max_eh2000` not calculated | Only `wl_avg_eh2000` exists — min/max EH2000 not needed for project charts |
| Superset JavaScript tooltips disabled | v6 permanently disables JS tooltips regardless of config |
| MapBox API key not configured | Use deck.gl Scatterplot layer for maps instead |
| PostgreSQL audit logging not enabled | `log_connections=off`, `log_statement=none` — acceptable for training project |
| DataHub CLI version mismatch warning | Cosmetic only — both sides use `:head` rolling builds |
| Task duration not in etl_log | Available via `airflow_db.task_instance` directly |

---

## Session Log

| Session | Date | Summary |
|---|---|---|
| 1 | 2026-05-01 | Project design, data sources, architecture decisions |
| 2 | 2026-05-01 | Git setup, GitHub repo, SSH, server folders |
| 3 | 2026-05-01 | docker-compose, Dockerfile, .env, create_tables.sql, stations.csv — all services running |
| 4 | 2026-05-01 | FAB auth manager, SSH config, md5 auth fix |
| 5 | 2026-05-01 | Windows PC GitHub setup. Data flow design locked. |
| 6 | 2026-05-02 | Seed files, dimension tables, seed DAG running. Airflow JWT fix. |
| 7 | 2026-05-05 | Architecture improvements: table renaming, auto-proximity, SCD2, asset-driven DAGs |
| 8 | 2026-05-06 | Raw landing zone, bronze schema rename, archive DAG |
| 9 | 2026-05-07 | All DAGs built and verified. Bronze ingested. dbt silver+gold models built. |
| 10 | 2026-05-07 | dbt project dir flattened, profiles.yml persisted, pipeline fully automated |
| 11 | 2026-05-07 | Superset installed, connected to analytics DB, first 3 charts |
| 12 | 2026-05-09 | airflow-triggerer added. feature/superset merged to main. |
| 13 | 2026-05-09 | API publish lag investigated. Pipeline redesign planned. |
| 14 | 2026-05-09 | Schedule fixed (06:00 UTC). Empty response guard. Archive DAG bugs fixed. May 5–7 data recovered. |
| 15 | 2026-05-09 | Diagnostic session — Airflow, Superset, PostgreSQL audited. No code changes. |
| 16 | 2026-05-10 | dbt station snapshots, EH2000 correction fixed, timestamp alignment verified, fetch retry config |
| 17 | 2026-05-11 | `fetch_hydro` timeline_ts_local fix, date offset fix, URL encoding fix, etl_log overhaul |
| 18 | 2026-05-11 | (continuation of session 17) |
| 19 | 2026-05-14 | DataHub integrated into docker-compose.yml, fully running |
| 20 | 2026-05-14 | hydro_meteo_pipeline fully rewritten, API lag fixed, 2025 backfill started |
| 21 | 2026-05-20 | Backfill verified complete. October 2025 meteo anomalies documented. |
| 22 | 2026-05-20 | DataHub root causes diagnosed. Full volume wipe. Rebuild planned. |
| 23 | 2026-05-20 | DataHub production rebuild — OpenSearch + KRaft Kafka. Roles indexed. |
| 24 | 2026-05-20 | DataHub ingestion: PostgreSQL (9 tables), dbt lineage, Superset (20 charts, 3 dashboards) |
| 25 | 2026-05-22 | Planning session. Roadmap locked. Architecture decisions made. |
| 26 | 2026-05-22 | DataHub Airflow plugin installed. DAG metadata visible. All ingestion complete. |
| 27 | 2026-05-23 | `wl_avg_corrected` → `wl_avg_eh2000`, unit conversion bug fixed, dbt full-refresh, DataHub re-ingested |
| 28 | 2026-05-23 | FastAPI public API built and live. api dbt schema (5 models) deployed. api.deng.ee configured. |
| 29 | 2026-05-23 | dbt-utils added. 16 generic + 10 singular bronze tests — all passing. WT upper bound corrected to 35°C. |
| 30 | 2026-05-26 | README.md and arhitektuur.md updated with stack RAM/disk requirements and test documentation. |
| 31 | 2026-05-27 | API usage monitoring — monitoring schema, request_log table, FastAPI middleware. Swagger UI badge CSS fix. |
| 32 | 2026-05-28 | Indexes on api.observations_hydro and api.observations_meteo. Unused DataHub DAG deleted. |
| 33 | 2026-05-28 | DataHub full rebuild. Glossary exported. All three ingestion sources rebuilt with correct recipes, tags, profiling, full lineage. analytics user search_path fixed. DataHub runbook created. |

---

*deng-hydro-climate — progress.md — last updated 2026-05-29*
