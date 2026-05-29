# dbt Runbook — deng-hydro-climate

**Last updated:** 2026-05-29 — Session 33
**Location in repo:** `docs/runbook/dbt_runbook.md`

---

## 1. Overview

dbt Core 1.9.x runs in the `deng-dbt` container. It handles all data transformations from bronze through to the api serving layer, and runs 26 automated data quality tests on every build. The daily `run_dbt` Airflow task calls `dbt build` inside this container.

| Container | Purpose |
|---|---|
| `deng-dbt` | Interactive dbt development and manual builds |

The dbt project is mounted at `/dbt` inside the container (flattened — no subdirectory). All build output and logs must be written to `/tmp` — the mounted project directory is read-only from the Airflow container.

---

## 2. Key Commands

All commands are run from the server. The `-it` flag is safe to use for interactive sessions; omit it for scripted use.

### Incremental build (normal daily run)

```bash
docker exec -it deng-dbt dbt build \
  --project-dir /dbt \
  --profiles-dir /dbt \
  --log-path /tmp/dbt_logs \
  --target-path /tmp/dbt_target
```

Runs all models incrementally and executes all 26 tests. This is what Airflow calls daily.

### Full rebuild (use after schema changes)

```bash
docker exec -it deng-dbt dbt build \
  --full-refresh \
  --project-dir /dbt \
  --profiles-dir /dbt \
  --log-path /tmp/dbt_logs \
  --target-path /tmp/dbt_target
```

Drops and recreates all incremental models from scratch. Required after column renames, new columns, or structural changes to any model.

### Single schema rebuild

```bash
# api schema only
docker exec -it deng-dbt dbt build \
  --select api \
  --full-refresh \
  --project-dir /dbt \
  --profiles-dir /dbt \
  --log-path /tmp/dbt_logs \
  --target-path /tmp/dbt_target

# silver schema only
docker exec -it deng-dbt dbt build \
  --select silver \
  --full-refresh \
  --project-dir /dbt \
  --profiles-dir /dbt \
  --log-path /tmp/dbt_logs \
  --target-path /tmp/dbt_target
```

### Snapshots only

```bash
docker exec -it deng-dbt dbt snapshot \
  --project-dir /dbt \
  --profiles-dir /dbt \
  --log-path /tmp \
  --target-path /tmp/dbt-target
```

Run this manually when station reference data changes (after `seed_stations` DAG completes).

### Tests only

```bash
# All bronze tests
docker exec -it deng-dbt dbt test \
  --select source:bronze \
  --project-dir /dbt \
  --profiles-dir /dbt \
  --log-path /tmp/dbt_logs \
  --target-path /tmp/dbt_target

# All tests across all layers
docker exec -it deng-dbt dbt test \
  --project-dir /dbt \
  --profiles-dir /dbt \
  --log-path /tmp/dbt_logs \
  --target-path /tmp/dbt_target
```

### Source freshness check

```bash
docker exec -it deng-dbt dbt source freshness \
  --project-dir /dbt \
  --profiles-dir /dbt \
  --log-path /tmp/dbt_logs \
  --target-path /tmp/dbt_target
```

Runs separately from `dbt build` — not included in the daily Airflow run. Freshness thresholds (pending implementation): hydro warn 30h/error 48h, meteo warn 26h/error 36h.

### Install packages (after fresh clone)

```bash
docker exec -it deng-dbt dbt deps \
  --project-dir /dbt \
  --profiles-dir /dbt
```

Required after cloning the repo — installs `dbt-utils` from `packages.yml`.

### Generate docs (for DataHub artifacts)

```bash
docker exec -it deng-dbt dbt docs generate \
  --project-dir /dbt \
  --profiles-dir /dbt \
  --log-path /tmp/dbt_logs \
  --target-path /tmp/dbt_target
```

Then copy artifacts to the DataHub artifacts folder:

```bash
docker cp deng-dbt:/tmp/dbt_target/manifest.json /srv/deng-hydro-climate/datahub/artifacts/manifest.json
docker cp deng-dbt:/tmp/dbt_target/catalog.json /srv/deng-hydro-climate/datahub/artifacts/catalog.json
docker cp deng-dbt:/tmp/dbt_target/run_results.json /srv/deng-hydro-climate/datahub/artifacts/run_results.json
```

Only needed when dbt structure changes (new models, renamed columns, updated descriptions, new tests) — not for routine daily pipeline runs.

---

## 3. Project Structure

```
dbt_project/
├── dbt_project.yml
├── packages.yml                   ← dbt-utils dependency
├── profiles.yml                   ← connection to analytics-db
├── macros/
│   └── generate_schema_name.sql   ← returns custom_schema_name as-is (no prefix)
├── models/
│   ├── sources/
│   │   └── sources.yml            ← source definitions + 16 generic tests
│   ├── silver/
│   │   ├── hydro.sql              ← pivot wide + EH2000 correction
│   │   └── meteo.sql              ← pivot wide + UTC timestamp construction
│   ├── gold/
│   │   └── hydro_meteo.sql        ← join silver.hydro + silver.meteo on proximity_rank=1
│   └── api/                       ← 5 models for FastAPI serving layer
│       ├── stations_hydro.sql
│       ├── stations_meteo.sql
│       ├── measurement_types.sql
│       ├── observations_hydro.sql
│       └── observations_meteo.sql
├── snapshots/
│   ├── snap_hydro_stations.sql    ← SCD2 snapshot of ref.hydrometric_stations
│   └── snap_meteo_stations.sql    ← SCD2 snapshot of ref.meteorological_stations
└── tests/                         ← 10 singular SQL test files
    ├── bronze_hydro_wl_range.sql
    ├── bronze_hydro_wt_range.sql
    ├── bronze_hydro_discharge_range.sql
    ├── bronze_hydro_no_future_timestamps.sql
    ├── bronze_meteo_temperature_range.sql
    ├── bronze_meteo_precipitation_non_negative.sql
    ├── bronze_meteo_humidity_range.sql
    ├── bronze_meteo_pressure_range.sql
    ├── bronze_meteo_wind_speed_non_negative.sql
    └── bronze_meteo_tund_range.sql
```

---

## 4. Data Quality Tests

26 tests run automatically on every `dbt build`. All target the bronze layer. Severity is `error` — a failing test halts all downstream model builds (silver, gold, api).

### Generic tests (16) — `models/sources/sources.yml`

| Table | Test | Detail |
|---|---|---|
| `bronze.hydro` | `not_null` | `jaam_kood`, `timeline_ts_utc`, `timeline_ts_local`, `aegrida_nimi`, `loaded_at` |
| `bronze.hydro` | `accepted_values` | `aegrida_nimi` — 9 known measurement types |
| `bronze.hydro` | `unique_combination_of_columns` | `(jaam_kood, timeline_ts_utc, aegrida_nimi)` |
| `bronze.meteo` | `not_null` | `jaam_kood`, `aasta`, `kuu`, `paev`, `tund`, `element_kood`, `loaded_at` |
| `bronze.meteo` | `accepted_values` | `element_kood` — 10 known element codes |
| `bronze.meteo` | `unique_combination_of_columns` | `(jaam_kood, aasta, kuu, paev, tund, element_kood)` |

### Singular tests (10) — `tests/`

| Test file | Check |
|---|---|
| `bronze_hydro_wl_range.sql` | Water level: −100 to 1500 cm |
| `bronze_hydro_wt_range.sql` | Water temperature: −5 to 35°C |
| `bronze_hydro_discharge_range.sql` | Discharge: −300 to 15,000 m³/s |
| `bronze_hydro_no_future_timestamps.sql` | `timeline_ts_utc` not in the future |
| `bronze_meteo_temperature_range.sql` | Air temperature (TA, TAN1H, TAX1H): −40 to 35°C |
| `bronze_meteo_precipitation_non_negative.sql` | Precipitation (PR1H) ≥ 0 mm |
| `bronze_meteo_humidity_range.sql` | Relative humidity (RH): 0–100% |
| `bronze_meteo_pressure_range.sql` | Air pressure (PA0): 950–1060 hPa |
| `bronze_meteo_wind_speed_non_negative.sql` | Wind speed (WS10M, WSX1H) ≥ 0 m/s |
| `bronze_meteo_tund_range.sql` | Hour of day: 0–23 |

> `SDUR1H` (sunshine duration) is deliberately untested — negative values are a known sensor calibration artefact in the source data, preserved in bronze by design.

---

## 5. Diagnosing Failures

### Step 1 — Check etl_log

```bash
docker exec -it deng-analytics-db psql -U analytics -d hydro_climate_db -c \
  "SELECT task_name, target_date, status, error_message
   FROM bronze.etl_log
   WHERE task_name = 'run_dbt'
   ORDER BY started_at DESC
   LIMIT 5;"
```

### Step 2 — Run dbt manually to see full output

```bash
docker exec -it deng-dbt dbt build \
  --project-dir /dbt \
  --profiles-dir /dbt \
  --log-path /tmp/dbt_logs \
  --target-path /tmp/dbt_target
```

Test failures print clearly in the output — the failing test name and the offending rows.

### Step 3 — Run only tests to isolate quickly

```bash
docker exec -it deng-dbt dbt test \
  --select source:bronze \
  --project-dir /dbt \
  --profiles-dir /dbt \
  --log-path /tmp/dbt_logs \
  --target-path /tmp/dbt_target
```

### Step 4 — Check dbt logs

```bash
docker exec -it deng-dbt cat /tmp/dbt_logs/dbt.log | tail -100
```

### Common Issues

| Symptom | Likely cause | Fix |
|---|---|---|
| Test failure — `accepted_values` on `aegrida_nimi` | New measurement type appeared in API | Add the new value to `accepted_values` list in `sources.yml` |
| Test failure — value out of range | Anomalous reading in source data | Investigate the value; if legitimate (e.g. extreme weather), widen the bound in the singular test |
| Model compilation error | Column reference wrong after rename | Check model SQL — use `aegrida_nimi` not `aegrida_kood`; `tund` not `kellaaeg` |
| Incremental model not picking up new rows | Missing rows in filter clause | Run with `--full-refresh` to rebuild from scratch |
| `dbt deps` error | Package registry unreachable | Check network; retry |
| `UndefinedTable` on source | `ref()` used instead of `source()` | In api models, use `source('ref', 'hydrometric_stations')` not `ref('hydrometric_stations')` |

---

## 6. Key Design Notes

- **`generate_schema_name.sql` macro** — returns `custom_schema_name` as-is. Models land directly in their own schemas (`bronze`, `silver`, `gold`, `api`) without the dbt project name prefix. Do not remove this macro.
- **`silver/hydro.sql`** — joins `snap_hydro_stations` (SCD2 snapshot) for point-in-time EH2000 correction. Uses COALESCE fallback to `ref.hydrometric_stations` for data predating 2026-05-09 (before snapshots existed).
- **api schema sources** — `ref.hydrometric_stations` and `ref.meteorological_stations` are dbt **sources** loaded by the Airflow seed DAG, not dbt models. Always reference them with `source('ref', 'hydrometric_stations')`, never `ref('hydrometric_stations')`.
- **Incremental models** (`api.observations_hydro`, `api.observations_meteo`) — unique key is `(station_code, obs_ts, element_code)`. `published_at` is `now()` at dbt run time — represents when data became available on the API.
- **`--log-path /tmp` and `--target-path /tmp/dbt_target`** are required on every command — the mounted `dbt_project/` directory is read-only from the Airflow container and cannot be written to.
- **`dbt-utils`** package provides `unique_combination_of_columns` test for composite unique key validation. Run `dbt deps` after a fresh clone before any build.

---

## 7. When to Run a Full Rebuild

| Situation | Command |
|---|---|
| Column renamed in any model | `dbt build --full-refresh` |
| New column added to an incremental model | `dbt build --select <model> --full-refresh` |
| EH2000 formula changed in `silver/hydro.sql` | `dbt build --full-refresh` |
| api schema structure changed | `dbt build --select api --full-refresh` |
| Snapshot logic changed | `dbt snapshot` (snapshots are always full — no `--full-refresh` needed) |
| After DataHub volume wipe | Regenerate artifacts with `dbt docs generate`, then copy to `datahub/artifacts/` |

---

*deng-hydro-climate — dbt Runbook — v1.0 — 2026-05-29*
