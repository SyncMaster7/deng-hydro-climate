# Airflow Runbook — deng-hydro-climate

**Last updated:** 2026-05-29 — Session 33
**Location in repo:** `docs/runbook/airflow_runbook.md`

---

## 1. Overview

Apache Airflow 3.2.1 orchestrates all pipeline and maintenance tasks. It runs as four containers plus one one-shot init container. The Airflow metadata database is a separate PostgreSQL instance (`deng-airflow-db`) — completely isolated from the analytics database.

| Container | Purpose |
|---|---|
| `deng-airflow-apiserver` | Web UI + REST API (port 8080) |
| `deng-airflow-scheduler` | DAG evaluation and task scheduling |
| `deng-airflow-dag-processor` | DAG file parsing |
| `deng-airflow-triggerer` | Deferred tasks and sensors |
| `deng-airflow-db` | Airflow metadata database (PostgreSQL 16, port 5433) |
| `deng-airflow-init` | One-shot: DB migrate + FAB migrate + admin user creation |

**UI:** http://192.168.1.93:8080 — Login: `admin` (check `.env` for password)

**Health check:** All four indicators in the UI footer should be green — metadatabase, scheduler, triggerer, dag processor.

---

## 2. Normal Restart

```bash
cd /srv/deng-hydro-climate
docker compose stop deng-airflow-apiserver deng-airflow-scheduler deng-airflow-dag-processor deng-airflow-triggerer
docker compose up -d deng-airflow-apiserver deng-airflow-scheduler deng-airflow-dag-processor deng-airflow-triggerer
```

Wait for all four to show healthy:

```bash
watch -n 5 'docker compose ps deng-airflow-apiserver deng-airflow-scheduler deng-airflow-dag-processor deng-airflow-triggerer'
```

> Do NOT restart `deng-airflow-db` unless specifically needed — it holds all pipeline history, task state, and connections.

---

## 3. Full Teardown and Rebuild

Use this only when the Airflow metadata database is corrupt or you need a completely clean slate.

> **Warning:** This wipes all DAG run history, task logs, connections, and variables stored in the Airflow DB.

```bash
cd /srv/deng-hydro-climate

# Stop and remove all Airflow containers
docker compose stop deng-airflow-apiserver deng-airflow-scheduler deng-airflow-dag-processor deng-airflow-triggerer deng-airflow-init deng-airflow-db
docker compose rm -f deng-airflow-apiserver deng-airflow-scheduler deng-airflow-dag-processor deng-airflow-triggerer deng-airflow-init deng-airflow-db

# Wipe the metadata database volume
sudo rm -rf /data/volumes/airflow_db/*

# Rebuild and start
docker compose up -d deng-airflow-db
# Wait for deng-airflow-db to be healthy, then:
docker compose up -d deng-airflow-init
# Wait for deng-airflow-init to exit (0), then:
docker compose up -d deng-airflow-apiserver deng-airflow-scheduler deng-airflow-dag-processor deng-airflow-triggerer
```

After rebuild, re-add the DataHub connection manually (see Section 7).

---

## 4. Checking Airflow Health

```bash
# Container status
docker compose ps deng-airflow-apiserver deng-airflow-scheduler deng-airflow-dag-processor deng-airflow-triggerer

# Scheduler logs (most useful for diagnosing DAG issues)
docker compose logs --tail=50 deng-airflow-scheduler

# DAG processor logs (DAG parsing errors show here)
docker compose logs --tail=50 deng-airflow-dag-processor

# API server logs
docker compose logs --tail=50 deng-airflow-apiserver
```

---

## 5. DAG Reference

### `hydro_meteo_pipeline` — daily at 06:00 UTC

The main production pipeline. Runs every day with `catchup=True` — missed runs are automatically backfilled.

```
fetch_hydro ──► ingest_hydro ──┐
                                ├──► run_dbt
fetch_meteo ──► ingest_meteo ──┘
```

| Task | Schedule offset | Output |
|---|---|---|
| `fetch_hydro` | `target_date = data_interval_start.date() - timedelta(days=3)` | `/data/raw/hydro/hydro_{date}.json` |
| `fetch_meteo` | Same offset | `/data/raw/meteo/meteo_{date}.json` |
| `ingest_hydro` | — | Batch UPSERT → `bronze.hydro` |
| `ingest_meteo` | — | Batch UPSERT → `bronze.meteo` |
| `run_dbt` | — | `dbt build` — all layers + 26 tests |

All tasks log to `bronze.etl_log` with `rows_processed`, `rows_loaded`, and status.
Fetch tasks retry up to 3 times with exponential backoff — base 15 min, ~1h30m total window.

**Trigger manually (e.g. to re-run a specific date):**

In Airflow UI → DAGs → `hydro_meteo_pipeline` → Trigger DAG w/ config:
```json
{ "execution_date": "2025-06-15T06:00:00+00:00" }
```

Or via CLI:
```bash
docker exec -it deng-airflow-apiserver airflow dags trigger hydro_meteo_pipeline \
  --exec-date "2025-06-15T06:00:00+00:00"
```

### `seed_stations` — manual trigger only

Loads station reference data and recalculates proximity. Run this after any station CSV change.

```
load_hydrometric_stations ──► load_meteorological_stations ──► calculate_proximity ──► run_snapshot
```

Trigger in Airflow UI → DAGs → `seed_stations` → Trigger DAG.

> `calculate_proximity` is conditional — only runs if changed row count > 0 OR `ref.station_proximity` is empty.

### `archive_raw_files` — weekly, Sunday at 00:00 UTC

Compresses `/data/raw/` JSON files older than 7 days into `.json.gz`, moves them to `/data/archive/{year}/{month}/`, and deletes the originals.

---

## 6. Diagnosing Pipeline Failures

### Step 1 — Check etl_log first

```bash
docker exec -it deng-analytics-db psql -U analytics -d hydro_climate_db -c \
  "SELECT task_name, target_date, status, rows_processed, rows_loaded, error_message
   FROM bronze.etl_log
   ORDER BY started_at DESC
   LIMIT 20;"
```

`status` values: `success`, `error`, `running`
`error_message` is populated on failure — check it before looking at Airflow logs.

### Step 2 — Check Airflow task logs

In the Airflow UI: DAGs → `hydro_meteo_pipeline` → click the failed run → click the failed task → Logs tab.

Or via CLI:
```bash
# List recent task instances
docker exec -it deng-airflow-apiserver airflow tasks states-for-dag-run \
  hydro_meteo_pipeline <dag_run_id>
```

### Step 3 — Check raw files exist

If `ingest_hydro` or `ingest_meteo` failed, verify the raw file was created by `fetch_`:

```bash
ls -lh /data/raw/hydro/
ls -lh /data/raw/meteo/
```

If the file is missing, the fetch task failed — check the API response in the task log for the full URL that was called.

### Step 4 — Check dbt failure

If `run_dbt` failed, a data quality test likely failed. Check:

```bash
docker exec -it deng-analytics-db psql -U analytics -d hydro_climate_db -c \
  "SELECT task_name, target_date, error_message
   FROM bronze.etl_log
   WHERE task_name = 'run_dbt'
   ORDER BY started_at DESC
   LIMIT 5;"
```

Then run dbt manually to see the full test output (see [`docs/runbook/dbt_runbook.md`](dbt_runbook.md)).

### Common Issues

| Symptom | Likely cause | Fix |
|---|---|---|
| `fetch_hydro` fails with ValueError | API returned empty response | Check API availability; task will retry automatically (3×, ~1h30m) |
| `fetch_hydro` fails with 404 | URL encoding issue | Verify query string is built manually, not via `params=` |
| `ingest_hydro` fails with unique constraint | Duplicate rows in API response | Deduplication should handle this — check error_message in etl_log |
| `run_dbt` fails | Data quality test failure | Run dbt manually to see which test failed |
| All tasks stuck in queued | Scheduler not running | Restart scheduler container |
| DAGs not appearing in UI | Parse error in DAG file | Check dag-processor logs |

---

## 7. Connections and Variables

### DataHub Connection

Required for the DataHub Airflow plugin (push-based lineage). Must exist in the Airflow metadata DB.

```bash
docker exec -it deng-airflow-apiserver airflow connections add datahub_rest_default \
  --conn-type HTTP \
  --conn-host http://deng-datahub-gms \
  --conn-port 8090
```

Verify it exists:
```bash
docker exec -it deng-airflow-apiserver airflow connections get datahub_rest_default
```

### Checking All Connections

```bash
docker exec -it deng-airflow-apiserver airflow connections list
```

---

## 8. Accessing the Airflow Metadata Database

For direct DB queries (task duration analysis, run history):

```bash
docker exec -it deng-airflow-db psql -U airflow -d airflow_db
```

Useful queries:

```sql
-- Recent task instance durations
SELECT dag_id, task_id, run_id,
       start_date, end_date,
       extract(epoch FROM (end_date - start_date)) AS duration_seconds,
       state
FROM task_instance
ORDER BY start_date DESC
LIMIT 20;

-- Failed runs in last 7 days
SELECT dag_id, run_id, state, start_date, end_date
FROM dag_run
WHERE state = 'failed'
  AND start_date > now() - interval '7 days'
ORDER BY start_date DESC;

-- Average task duration by task
SELECT dag_id, task_id,
       round(avg(extract(epoch FROM (end_date - start_date)))::numeric, 1) AS avg_seconds,
       count(*) AS run_count
FROM task_instance
WHERE state = 'success'
GROUP BY dag_id, task_id
ORDER BY dag_id, avg_seconds DESC;
```

---

## 9. Key Configuration Notes

- **Airflow version:** 3.2.1 — imports from `airflow.sdk`, NOT `airflow.decorators`
- **TaskFlow API** used throughout — `@task` decorator pattern
- **`catchup=True`** on `hydro_meteo_pipeline` — missed runs are backfilled automatically
- **`start_date=2025-01-01`** — backfill complete as of Session 21
- **`API_LAG_DAYS=3`** — all tasks fetch `data_interval_start.date() - timedelta(days=3)`
- **`fetch_hydro`** builds the query string manually (not via `params=`) to prevent the `requests` library from URL-encoding colons as `%3A`, which caused 404 errors from the PostgREST API
- **`fetch_hydro`** filters by `timeline_ts_local`, not `timeline_ts_utc` — timezone-safe for EET/EEST
- **`ingestion/`** folder is mounted at `/opt/airflow/ingestion` in all Airflow containers — `haversine.py` imported from there
- **DataHub plugin:** `acryl-datahub-airflow-plugin[airflow3]` v1.6.0 installed in the Airflow image — push-based, fires on every DAG run

---

*deng-hydro-climate — Airflow Runbook — v1.0 — 2026-05-29*
