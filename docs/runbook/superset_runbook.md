# Superset Runbook — deng-hydro-climate

**Last updated:** 2026-05-29 — Session 33
**Location in repo:** `docs/runbook/superset_runbook.md`

---

## 1. Overview

Apache Superset 6.0.1 provides internal dashboards and exploratory visualisation on top of the analytics database. It runs as two containers plus one one-shot init container, with its own PostgreSQL metadata database.

| Item | Detail |
|---|---|
| Container | `deng-superset` |
| Internal port | 8088 |
| Public URL | https://superset.deng.ee |
| Local URL | http://192.168.1.93:8088 |
| Metadata DB container | `deng-superset-db` (PostgreSQL 16, port 5434) |

**Current dashboards:** 3 published, covering pipeline monitoring and hydro/meteo observations.

> Dashboard development is split between Superset (Kermo — internal/operational) and Tableau (Thea — business-facing, connects directly to `gold.hydro_meteo`).

---

## 2. Normal Restart

```bash
cd /srv/deng-hydro-climate
docker compose stop deng-superset
docker compose up -d deng-superset
```

> **Never use `docker compose restart`** — it does not re-read configuration changes reliably. Always use `stop` followed by `up`.

Check it came back healthy:

```bash
watch -n 5 'docker compose ps deng-superset'
```

---

## 3. Full Teardown and Rebuild

Use this when the Superset metadata database is corrupt, dashboards need to be wiped clean, or after a major version upgrade.

> **Warning:** This wipes all dashboards, charts, datasets, and user accounts stored in the Superset metadata DB. Rebuild from scratch.

```bash
cd /srv/deng-hydro-climate

# Stop and remove Superset containers
docker compose down deng-superset deng-superset-init deng-superset-db

# Wipe the metadata database volume
sudo rm -rf /data/volumes/superset_db/*

# Rebuild images
docker compose build deng-superset-init deng-superset

# Start DB first, then init, then app
docker compose up -d deng-superset-db
# Wait for deng-superset-db to be healthy, then:
docker compose up -d deng-superset-init
# Follow init logs — wait for Exited (0):
docker compose logs -f deng-superset-init
# Then start Superset:
docker compose up -d deng-superset
```

After rebuild, re-add the database connection and recreate dashboards — see Sections 4 and 5.

---

## 4. Database Connection

Superset connects to `hydro_climate_db` using the `analytics` user. This connection must exist before any datasets or charts can be created.

**Connection settings:**

| Field | Value |
|---|---|
| Display name | `deng-hydro-meteo` |
| Host | `analytics-db` (internal Docker hostname — **not** 192.168.1.93) |
| Port | `5432` |
| Database | `hydro_climate_db` |
| User | `analytics` |
| Password | See `.env` → `ANALYTICS_DB_PASSWORD` |

To add in the Superset UI: **Settings → Database Connections → + Database → PostgreSQL** → fill in the fields above.

---

## 5. Users

| User | Role | Access | Notes |
|---|---|---|---|
| `kermok` | Admin | Full admin | Primary admin account |
| `admin` | Admin | Full admin | Break-glass only |
| `kairik` | Viewer | Read-only via superset.deng.ee | Active |
| `aivol` | — | — | Never logged in |

To manage users: **Settings → List Users**

To add a new user: **Settings → List Users → + User**

---

## 6. Known Limitations

These are permanent constraints of the current stack — not bugs to fix.

| Limitation | Detail |
|---|---|
| JavaScript tooltips disabled | Superset v6 permanently disables JS tooltips regardless of config settings |
| MapBox API key not configured | Use deck.gl Scatterplot layer for map-based charts instead of MapBox |

---

## 7. Diagnosing Issues

### Superset not loading

```bash
# Check container status
docker compose ps deng-superset deng-superset-db

# Check logs
docker compose logs --tail=50 deng-superset
docker compose logs --tail=50 deng-superset-db
```

### Charts showing no data

1. Verify the database connection is healthy: **Settings → Database Connections → `deng-hydro-meteo` → Test Connection**
2. Check the dataset's table/schema still exists in `hydro_climate_db`
3. If using a virtual dataset (custom SQL), run the SQL directly against the analytics DB to verify it returns data

### DataHub not showing Superset charts after ingestion

The Superset ingestion recipe requires `ingest_datasets: true` — without it, upstream lineage to PostgreSQL tables is not captured. Check the recipe in `datahub/recipes/superset_recipe.yml` and re-run the `[DENG] - Dashboards` ingestion source in DataHub UI.

### Superset metadata DB not starting

```bash
docker compose logs --tail=50 deng-superset-db
```

If the data volume is corrupt, wipe it and rebuild (Section 3).

---

## 8. Connecting Tableau

Tableau (managed by Thea) connects directly to the analytics database — not through Superset.

**Tableau connection settings:**

| Field | Value |
|---|---|
| Server | 192.168.1.93 |
| Port | 5432 |
| Database | `hydro_climate_db` |
| Schema | `gold` |
| Table | `hydro_meteo` |
| User | `analytics` |
| Password | See `.env` → `ANALYTICS_DB_PASSWORD` |

> Tableau always reads from `gold.hydro_meteo`. Use `observation_ts_local` for all time-based axes — not `timeline_ts_utc`.

---

*deng-hydro-climate — Superset Runbook — v1.0 — 2026-05-29*
