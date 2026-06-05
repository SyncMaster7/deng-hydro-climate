# Project Progress — deng-hydro-climate

This document tracks the current state of the project, what is complete, known gaps, accepted limitations, and the next steps backlog. Updated at the end of each working session.

**Last updated:** 2026-06-05 — Session 40 complete

---

## Current Status

The full stack is operational. Data has been backfilled from 2025-01-01, the daily pipeline runs reliably at 06:00 UTC, the public API is live, CKAN serves as the public metadata catalogue, and the public catalogue frontend is deployed. DataHub metadata cataloguing is complete with full lineage and dbt test results visible on all bronze assets. The main open items are dbt test coverage expansion and DataHub metadata enrichment.

| Area | Status                                                |
|---|-------------------------------------------------------|
| Daily pipeline (fetch → ingest → dbt) | ✅ Running — 06:00 UTC daily                           |
| Historical backfill (2025-01-01 onwards) | ✅ Complete — ~7.8M hydro rows, ~2.9M meteo rows       |
| dbt bronze → silver → gold → api | ✅ All models building                                 |
| dbt data quality tests (bronze) | ✅ 26 tests — all passing                              |
| DataHub — dbt test results visible | ✅ PASS=36 — visible on bronze assets                  |
| Public REST API (api.deng.ee) | ✅ Live — v2.3.0                                       |
| API usage monitoring | ✅ Logging to monitoring.request_log                   |
| FastAPI default time window | ✅ Fixed — returns latest available data               |
| FastAPI async monitoring middleware | ✅ asyncio.create_task() + _write_request_log() helper |
| Apache Superset dashboards | ✅ Running — 3 charts published                        |
| Superset monitoring dashboard | ✅ Built on monitoring.request_log                     |
| DataHub — PostgreSQL ingestion | ✅ All 17 tables indexed                               |
| DataHub — dbt lineage | ✅ Full lineage: bronze → silver → gold → api          |
| DataHub — Superset ingestion | ✅ 20 charts, 3 dashboards indexed                     |
| DataHub — Airflow ingestion | ✅ Push-based via plugin                               |
| DataHub glossary | ✅ English hierarchy (8 items) — completed by Anny     |
| CKAN public catalogue (ckan.deng.ee) | ✅ Live — 2.11.4, 184 datasets, org hierarchy          |
| Public catalogue frontend (catalogue.deng.ee) | ✅ Live — static HTML, CKAN + DataHub GraphQL          |
| dbt tests — silver / gold / api layers | ⏳ Pending                                             |
| National open data portal research | ⏳ Pending                                             |

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
- dbt test results (PASS=36) visible in DataHub on all bronze assets

### Public API
- FastAPI at `https://api.deng.ee` — v2.3.0 — 8 endpoints covering stations, elements, and observations
- Rate limited to 60 req/min per IP via `slowapi`
- asyncpg connection pool — min 2, max 10 connections
- Composite indexes on `api.observations_hydro` and `api.observations_meteo` — `(station_code, element_code, obs_ts)`
- Interactive Swagger UI at `https://api.deng.ee/docs`
- Async monitoring middleware — `asyncio.create_task()` + `_write_request_log()` — non-blocking
- Default time window returns latest available data (not a fixed lookback window)

### Metadata and Cataloguing
- DataHub fully integrated — PostgreSQL, dbt, Superset, and Airflow all ingested
- Full column-level lineage: `bronze.hydro` → `silver.hydro` → `gold.hydro_meteo` → Superset charts
- Tags applied to all entities: `deng`, `postgresql`, `dbt`, `superset`, schema-specific tags
- Column-level profiling on all tables
- English business glossary (8 items) under Hydrology, Meteorology, Data Engineering nodes
- DataHub runbook at `docs/runbook/datahub_runbook.md`

### Public Catalogue
- CKAN 2.11.4 at `https://ckan.deng.ee` — public metadata discovery layer
- Org hierarchy: Keskkonnaagenuur (parent) + 8 department sub-orgs
- 184 Keskkonnaagenuur datasets imported from Jira CSV exports
- DCAT-AP 3 RDF endpoints via ckanext-dcat (euro_dcat_ap_3 profile)
- Extensions: ckanext-hierarchy, ckanext-dcat, ckanext-scheming, ckanext-spatial
- Static catalogue frontend at `https://catalogue.deng.ee` — three pages (landing, datasets, glossary)
- Frontend reads CKAN API and DataHub GraphQL live; served via Caddy file_server
- DataHub token authentication enabled; CORS configured for catalogue.deng.ee

---

## Pending Items

### 1. dbt Test Coverage Expansion

Bronze layer is fully tested. Remaining layers:

| Layer | Priority | Notes |
|---|---|---|
| Source freshness (bronze) | High | Via `dbt source freshness` — separate from `dbt build`. Thresholds: hydro warn 30h/error 48h, meteo warn 26h/error 36h |
| Silver layer | Medium | Not yet defined |
| Gold layer | Medium | Not yet defined |
| API layer | Medium | Not yet defined |

### 2. National Open Data Portal Research

Research and document the avaandmed.eesti.ee submission workflow — full harvest chain from CKAN → Estonian national portal → Drupal frontend. School project scope: research and design only, no actual submission.

---

## Accepted Limitations

These are known gaps that are intentionally not addressed within school project scope.

| Limitation | Notes |
|---|---|
| dbt tests cover bronze layer only | Silver, gold, api layers not yet tested |
| dbt singular test bounds based on partial profiling | Covers 2026 + up to May 2025 data; revisit after full historical DataHub profiling |
| `f_kliima_minut` not ingested | 10-minute precipitation data available but out of scope |
| `wl_min_eh2000` / `wl_max_eh2000` not calculated | Only `wl_avg_eh2000` exists — min/max EH2000 not needed for project charts |
| Superset JavaScript tooltips disabled | v6 permanently disables JS tooltips regardless of config |
| MapBox API key not configured | Use deck.gl Scatterplot layer for maps instead |
| PostgreSQL audit logging not enabled | `log_connections=off`, `log_statement=none` — acceptable for training project |
| DataHub CLI version mismatch warning | Cosmetic only — both sides use `:head` rolling builds |
| Task duration not in etl_log | Available via `airflow_db.task_instance` directly |
| DataHub browse tree duplicate | `deng-analytics-db` appears twice — dbt recipe emits container metadata for uncovered schemas; deferred |
| CKAN DataPusher not used | Metadata-only portal — no data upload needed |
| CKAN /catalog.json broken | rdflib 7 incompatibility — use .ttl, .xml, or .jsonld instead |
| DataHub UI-triggered ingestion version string | `:head` builds produce empty version string; deferred until images pinned to semver |
| Konuvere / Tarvastu data gaps | ~46% and ~48% coverage respectively — upstream API outages, not a pipeline error |

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
| 34 | 2026-05-29 | Full documentation restructure. README, architecture.md, progress.md, and all runbooks rewritten in English. |
| 35 | 2026-05-29 | FastAPI v2.3.0: async monitoring middleware (asyncio.create_task + _write_request_log), default time window fixed to return latest available data, Swagger UI polish. DataHub glossary confirmed done (Anny). Superset monitoring dashboard confirmed done. |
| 36 | 2026-05-30 | CKAN 2.11.4 installed at ckan.deng.ee. 4 containers. Extensions: hierarchy, dcat, scheming, spatial. RAM upgraded to 24 GB. |
| 37 | 2026-05-30 | CKAN fully populated — org hierarchy (Keskkonnaagenuur + 8 sub-orgs), 184 datasets imported. DCAT-AP 3 endpoints verified. |
| 38 | 2026-05-30 | Public catalogue frontend deployed at catalogue.deng.ee. DataHub token auth enabled. CKAN CORS configured. DataHub Caddy OPTIONS preflight handler added. |
| 39 | 2026-05-31 | (continuation of session 38 — catalogue.deng.ee refinements) |
| 40 | 2026-06-01 | DataHub dbt test results fix. Refreshed stale artifacts in datahub/artifacts/ (run_results.json was from May 22, predated Session 29 tests). Re-ran [DENG] - Lineage. Test results (PASS=36) now visible on bronze assets in DataHub. |

---

*deng-hydro-climate — progress.md — last updated 2026-06-05*
