# DataHub Runbook — deng-hydro-climate

**Last updated:** 2026-05-28 — Session 33  
**Author:** kermo  
**Location in repo:** `docs/runbook/datahub_runbook.md`

---

## 1. Overview

DataHub is the metadata catalogue and lineage platform for the deng-hydro-climate stack. It indexes all PostgreSQL tables, dbt models, and Superset dashboards — providing data discovery, column-level lineage, and profiling statistics.

### Architecture

```
datahub-mysql      ──┐
datahub-opensearch ──┤──► datahub-system-update ──► datahub-gms ──► datahub-actions
datahub-broker     ──┘                                               └──► datahub-frontend
```

| Container | Purpose |
|---|---|
| deng-datahub-mysql | Internal metadata store (MySQL 8.2) |
| deng-datahub-opensearch | Search and graph indices (OpenSearch 2.19.3) |
| deng-datahub-broker | Kafka KRaft broker — no ZooKeeper (cp-kafka 8.0.0) |
| deng-datahub-system-update | One-shot bootstrap (runs once, restart: "no") |
| deng-datahub-gms | DataHub backend metadata API |
| deng-datahub-actions | Event automation + ingestion executor |
| deng-datahub-frontend | DataHub web UI |

### Access

| Interface | URL |
|---|---|
| DataHub UI (internal) | http://192.168.1.93:9002 |
| DataHub UI (public) | https://datahub.deng.ee |
| GMS API | http://192.168.1.93:8090/api/graphql |

### Credentials

Admin credentials are set via `user.props` at bootstrap — no manual password change needed after first start. Check password manager for the current admin password.

---

## 2. Normal Restart

Use this after a server reboot or when containers need to be restarted. Do NOT re-run `datahub-system-update` — it is `restart: "no"` and does not need to re-run.

```bash
cd /srv/deng-hydro-climate
docker compose up -d datahub-mysql datahub-opensearch datahub-broker datahub-gms datahub-actions datahub-frontend
```

Wait for GMS and frontend to show `healthy`:

```bash
watch -n 5 'docker compose ps datahub-gms datahub-actions datahub-frontend'
```

---

## 3. Full Rebuild (Volume Wipe)

Use this when DataHub state is corrupt, has stale/duplicate entities, or after a major version upgrade.

### Step 1 — Export glossary first

Before wiping, export the current glossary to a reference file:

```bash
# From Mac
curl -s -X POST http://192.168.1.93:8090/api/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "{ getRootGlossaryNodes(input: { start: 0, count: 200 }) { nodes { urn properties { name description } glossaryChildrenSearch(input: { query: \"\", count: 200 }) { searchResults { entity { ... on GlossaryTerm { urn properties { name description } } ... on GlossaryNode { urn properties { name description } } } } } } } }"
  }' | tee ~/datahub_glossary_export.json | python3 -m json.tool
```

Reference files are also kept in the project: `datahub/glossary/datahub_glossary_reference.md` and `.csv`.

### Step 2 — Wipe containers and volumes

```bash
cd /srv/deng-hydro-climate

docker compose stop datahub-gms datahub-actions datahub-frontend datahub-system-update datahub-broker datahub-opensearch datahub-mysql

docker compose rm -f datahub-gms datahub-actions datahub-frontend datahub-system-update datahub-broker datahub-opensearch datahub-mysql

docker volume rm deng-hydro-climate_datahub-mysql deng-hydro-climate_datahub-opensearch deng-hydro-climate_datahub-broker
```

Verify volumes are gone:

```bash
docker volume ls | grep datahub
```

### Step 3 — Bootstrap Stage 1: infrastructure

```bash
docker compose up -d datahub-mysql datahub-opensearch datahub-broker
```

Watch until all three show `healthy`:

```bash
watch -n 5 'docker compose ps datahub-mysql datahub-opensearch datahub-broker'
```

### Step 4 — Bootstrap Stage 2: system update

```bash
docker compose up -d datahub-system-update
```

Wait for `Exited (0)`:

```bash
watch -n 5 'docker compose ps -a datahub-system-update'
```

### Step 5 — Bootstrap Stage 3: application layer

```bash
docker compose up -d datahub-gms datahub-actions datahub-frontend
```

Watch until GMS and frontend are healthy:

```bash
watch -n 5 'docker compose ps datahub-gms datahub-actions datahub-frontend'
```

### Step 6 — Verify roles indexed (CRITICAL)

Do not proceed until this returns `"value": 3`:

```bash
docker exec deng-datahub-opensearch curl -s http://localhost:9200/datahubroleindex_v2/_search | python3 -m json.tool | grep '"value"'
```

### Step 7 — Regenerate dbt artifacts

```bash
docker exec -it deng-dbt dbt docs generate \
  --project-dir /dbt \
  --profiles-dir /dbt \
  --log-path /tmp/dbt_logs \
  --target-path /tmp/dbt_target

docker cp deng-dbt:/tmp/dbt_target/manifest.json /srv/deng-hydro-climate/datahub/artifacts/manifest.json
docker cp deng-dbt:/tmp/dbt_target/catalog.json /srv/deng-hydro-climate/datahub/artifacts/catalog.json
docker cp deng-dbt:/tmp/dbt_target/run_results.json /srv/deng-hydro-climate/datahub/artifacts/run_results.json
```

### Step 8 — Run ingestion sources in order

In DataHub UI go to **Ingestion → Create new source** and create the three sources below in order. Recipes are in `datahub/recipes/`. Run each one manually after creating it.

| Order | Name | Type | Recipe file | Schedule |
|---|---|---|---|---|
| 1 | `[DENG] - Database` | postgres | `postgres_recipe.yml` | `0 0 1 * *` |
| 2 | `[DENG] - Lineage` | dbt | `dbt_recipe.yml` | `0 1 1 * *` |
| 3 | `[DENG] - Dashboards` | superset | `superset_recipe.yml` | `0 2 1 * *` |

**Run order is mandatory** — PostgreSQL must complete before dbt, dbt before Superset.

---

## 4. Ingestion Sources

### Overview

All ingestion is managed via the DataHub UI (**Ingestion** tab). Recipes are stored in `datahub/recipes/` as reference — they are not run via CLI.

### [DENG] - Database (PostgreSQL)

- **What it ingests:** All 17 tables across bronze, silver, gold, ref, api, monitoring, snapshots schemas
- **Profiling:** Full column-level (null counts, min, max, mean, sample values)
- **Tags applied:** `deng`, `postgresql` on all tables + schema-specific tags (`bronze`, `silver`, etc.)
- **Schedule:** 1st of month at 00:00 UTC
- **Runtime:** ~10 minutes (profiling bronze.hydro and bronze.meteo is the bottleneck)

### [DENG] - Lineage (dbt)

- **What it ingests:** Model lineage, column descriptions, test results
- **Schedule:** 1st of month at 01:00 UTC (after Database)
- **Tags applied:** `deng`, `dbt`
- **Runtime:** ~30 seconds

#### When to re-run dbt ingestion outside the schedule

Re-run manually when the dbt project structure changes:
- New model added
- Column renamed
- Column descriptions added or updated in `schema.yml`
- New tests added

Do NOT re-run for routine daily Airflow pipeline runs — those don't change the structure.

#### Regenerating dbt artifacts before re-running

Always regenerate artifacts when re-running after a structure change:

```bash
docker exec -it deng-dbt dbt docs generate \
  --project-dir /dbt \
  --profiles-dir /dbt \
  --log-path /tmp/dbt_logs \
  --target-path /tmp/dbt_target

docker cp deng-dbt:/tmp/dbt_target/manifest.json /srv/deng-hydro-climate/datahub/artifacts/manifest.json
docker cp deng-dbt:/tmp/dbt_target/catalog.json /srv/deng-hydro-climate/datahub/artifacts/catalog.json
docker cp deng-dbt:/tmp/dbt_target/run_results.json /srv/deng-hydro-climate/datahub/artifacts/run_results.json
```

### [DENG] - Dashboards (Superset)

- **What it ingests:** Charts, dashboards, datasets, lineage back to PostgreSQL tables
- **Schedule:** 1st of month at 02:00 UTC (after Lineage)
- **Tags applied:** `deng`, `superset`
- **Runtime:** ~15 seconds
- **No schedule needed** — run manually when Superset dashboards change

---

## 5. Known Configuration Notes

### analytics user search_path

The `analytics` PostgreSQL user has a custom `search_path` set permanently to allow DataHub profiling queries to find tables without schema prefix:

```sql
ALTER ROLE analytics SET search_path TO bronze, silver, gold, ref, api, monitoring, snapshots, public;
```

This was applied in Session 33. If the analytics-db container is recreated from scratch, re-apply this.

### platform_instance

`platform_instance` is intentionally NOT set in the PostgreSQL recipe. This keeps URNs in the format `hydro_climate_db.bronze.hydro` which matches what dbt produces, enabling correct lineage stitching. Do not add `platform_instance` to the PostgreSQL recipe.

### fail_safe_threshold

All three recipes have `fail_safe_threshold: 100` set. This is required because after a volume wipe, stateful ingestion detects 100% entity replacement and would otherwise block the run as a safety measure. This is expected behaviour after a wipe.

### Airflow ingestion

Airflow DAG metadata is ingested via the push-based `acryl-datahub-airflow-plugin[airflow3]` plugin installed directly in the Airflow container. No UI ingestion source needed for Airflow — it pushes automatically on each DAG run.

---

## 6. Tags

The following tags are created in DataHub and applied automatically via ingestion transformers:

| Tag | Applied to |
|---|---|
| `deng` | All datasets, charts, dashboards |
| `postgresql` | All PostgreSQL tables |
| `dbt` | All dbt models |
| `superset` | All Superset charts and dashboards |
| `bronze` | Tables in bronze schema |
| `silver` | Tables in silver schema |
| `gold` | Tables in gold schema |
| `ref` | Tables in ref schema |
| `api` | Tables in api schema |
| `monitoring` | Tables in monitoring schema |
| `snapshots` | Tables in snapshots schema |

If tags are missing after a rebuild, re-run the PostgreSQL ingestion source — tags are applied by transformers during ingestion.

---

## 7. Troubleshooting

### Roles not indexed after bootstrap

If `datahubroleindex_v2` returns `"value": 0` instead of 3, GMS did not fully initialise. Check:

```bash
docker compose logs datahub-gms | tail -50
```

Usually caused by GMS starting before OpenSearch is fully ready. Wait a few minutes and check again, or restart GMS:

```bash
docker compose restart datahub-gms
```

### Duplicate folders / stale entities

Caused by multiple ingestion runs with different `platform_instance` values, or failed stateful ingestion cleanup. Solution: full volume wipe and rebuild.

### Ingestion login failure (Superset)

Superset login must be tested from inside the Docker network. The `admin` user is used for ingestion (not `kermok`). Test login:

```bash
docker exec deng-datahub-actions curl -s -X POST http://superset:8088/api/v1/security/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "<password>", "provider": "db", "refresh": true}'
```

### profiling stats missing (UndefinedTable errors)

Caused by missing `search_path` on the analytics user. Re-apply:

```bash
docker exec -it deng-analytics-db psql -U analytics -d hydro_climate_db -c \
  "ALTER ROLE analytics SET search_path TO bronze, silver, gold, ref, api, monitoring, snapshots, public;"
```

### dbt lineage not stitching to PostgreSQL tables

Check that `target_platform_instance` is NOT set in the dbt recipe, and that `platform_instance` is NOT set in the PostgreSQL recipe. Both sides must produce URNs in the same format: `hydro_climate_db.<schema>.<table>`.

Verify URN format by checking the dataset URL in DataHub UI — it should be:
`urn:li:dataset:(urn:li:dataPlatform:postgres,hydro_climate_db.bronze.hydro,PROD)`

---

## 8. Named Volumes

| Volume | Purpose |
|---|---|
| `deng-hydro-climate_datahub-mysql` | DataHub internal metadata (ingestion sources, users, policies) |
| `deng-hydro-climate_datahub-opensearch` | Search and graph indices |
| `deng-hydro-climate_datahub-broker` | Kafka KRaft data |

All three must be wiped together for a clean rebuild.

---

*deng-hydro-climate — DataHub Runbook — v1.0 — 2026-05-28*
