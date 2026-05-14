# deng-hydro-climate

> **How do weather conditions drive water levels and runoff across Estonian river catchments — and which rivers respond most strongly to rainfall events?**

A production-style data engineering pipeline monitoring Estonian hydrological and meteorological conditions, built on a self-hosted server at Keskkonnaagenuur (Estonian Environment Agency).

---

## Stack

| Layer | Technology | Version |
|---|---|---|
| Orchestration | Apache Airflow (TaskFlow API) | 3.2.1 |
| Transformation | dbt Core | 1.9.x |
| Analytics DB | PostgreSQL + pgduckdb extension | 16 |
| Visualisation | Apache Superset | 6.0.1 |
| Metadata | DataHub | head (latest) |
| Containerisation | Docker Compose | — |
| Language | Python 3 | 3.x |

---

## Architecture

Data flows through a medallion architecture with automated dbt transformation and a full metadata layer:

```
Estonian Environment Agency APIs
        │
        ├── f_hydroseire  (76 stations, hourly)
        └── f_kliima_tund (25 stations, hourly)
                │
                ▼
        /data/raw/{source}/          ← Raw JSON landing zone (max 7 days)
                │
                ▼
        bronze.hydro / bronze.meteo  ← Exact API response, no transformation
                │
                ▼ dbt
        silver.hydro / silver.meteo  ← Cleaned, typed, pivoted wide
                │
                ▼ dbt
        gold.hydro_meteo             ← Joined, EH2000-corrected, local timestamps
                │
                ├──► Apache Superset  (dashboards)
                └──► DataHub          (metadata catalogue + lineage)
```

### DAGs

| DAG | Schedule | Purpose |
|---|---|---|
| `seed_stations` | Manual | Load station CSVs, auto-calculate haversine proximity, run dbt snapshots |
| `hydro_meteo_pipeline` | `0 6 * * *` (UTC) | Fetch → ingest → dbt build (previous day's data) |
| `archive_raw_files` | `0 0 * * 0` (Sunday) | Compress raw JSON files older than 7 days to `.json.gz` |

### Database Schemas

| Schema | Contents |
|---|---|
| `bronze` | Raw ingested data — exact API response, UPSERT on unique key |
| `ref` | Station reference tables seeded from CSV + SCD2 snapshots |
| `silver` | dbt models — cleaned, pivoted from long to wide |
| `gold` | dbt models — hydro joined to nearest meteo station, local timestamps |

---

## Services

| Service | Container | Port | Purpose |
|---|---|---|---|
| analytics-db | deng-analytics-db | 5432 | Application data (hydro + climate) |
| airflow-apiserver | deng-airflow-apiserver | 8080 | Airflow UI + REST API |
| airflow-scheduler | deng-airflow-scheduler | — | DAG evaluation |
| airflow-dag-processor | deng-airflow-dag-processor | — | DAG file parsing |
| airflow-triggerer | deng-airflow-triggerer | — | Deferred tasks and sensors |
| airflow-db | deng-airflow-db | 5433 | Airflow metadata DB |
| dbt | deng-dbt | — | Interactive dbt development |
| superset | deng-superset | 8088 | Dashboards and visualisation |
| superset-db | deng-superset-db | 5434 | Superset metadata DB |
| datahub-gms | deng-datahub-gms | 8090 | DataHub metadata REST API |
| datahub-frontend | deng-datahub-frontend | 9002 | DataHub web UI |
| datahub-mysql | deng-datahub-mysql | — | DataHub internal store |
| datahub-elasticsearch | deng-datahub-elasticsearch | — | DataHub search + graph index |
| datahub-broker | deng-datahub-broker | 9092 | Kafka event streaming |
| datahub-schema-registry | deng-datahub-schema-registry | — | Kafka schema management |
| datahub-zookeeper | deng-datahub-zookeeper | — | Kafka coordination |

**One-shot containers** (run once on first start, exit 0): `airflow-init`, `superset-init`, `datahub-mysql-setup`, `datahub-elasticsearch-setup`, `datahub-kafka-setup`, `datahub-upgrade`

---

## Data Sources

| Source | Endpoint | Stations | Granularity |
|---|---|---|---|
| Hydrological API | `keskkonnaandmed.envir.ee/f_hydroseire` | 76 | Hourly |
| Meteorological API | `keskkonnaandmed.envir.ee/f_kliima_tund` | 25 | Hourly |
| Station metadata | `seeds/*.csv` | 76 + 25 | Static |
| Station proximity | Auto-generated (haversine) | 228 pairs | On station change |

---

## Infrastructure

- **Server:** Dell PowerEdge T640 — VMware ESXi 7 — `deng-vm` (Ubuntu 24.04.4 LTS)
- **Project root:** `/srv/deng-hydro-climate/` (in Git)
- **Raw landing zone:** `/data/raw/{hydro,meteo}/` — max 7 days, then archived
- **Archive:** `/data/archive/{source}/{year}/{month}/` — gzip compressed
- **DB volumes:** `/data/volumes/{postgres_data,airflow_db,superset_db}/`

---

## Project Structure

```
/srv/deng-hydro-climate/
├── docker-compose.yml          # All services — Airflow, dbt, Superset, DataHub
├── .env                        # Never in Git
├── .env.example
├── docker/
│   ├── airflow/Dockerfile      # Airflow 3.2.1 + dbt-postgres
│   └── superset/               # Custom Superset build + config
├── dags/
│   ├── seed_stations.py        # Station seeding + proximity calculation
│   ├── hydro_meteo_pipeline.py # Main daily ingestion pipeline
│   └── archive_raw_files.py    # Weekly archiving DAG
├── dbt_project/                # dbt models, snapshots, profiles.yml
├── datahub/
│   └── recipes/                # DataHub ingestion recipe YAMLs
├── ingestion/                  # Python fetch + ingest scripts
├── seeds/                      # Station CSV reference data
└── sql/
    └── create_tables.sql       # Bronze schema DDL
```

---

## Git Workflow

GitHub Flow — feature branches merged to `main` via Pull Request.

| Machine | Role |
|---|---|
| MacBook / Windows PC | Write code, commit, push feature branch, open PR |
| GitHub | Review, merge to `main` |
| Server (`deng-vm`) | `git pull origin main` only — never pushes |

```bash
git checkout -b feature/your-branch-name
# make changes
git add .
git commit -m "describe what changed"
git push origin feature/your-branch-name
# Open GitHub → create and merge PR
git checkout main && git pull origin main
```

---

## Setup

### Prerequisites
- Docker and Docker Compose on `deng-vm`
- SSH access configured (`~/.ssh/config` with `deng-vm` shortcut)

### First-time install

```bash
# 1. Clone
git clone git@github.com:SyncMaster7/deng-hydro-climate.git
cd deng-hydro-climate

# 2. Configure environment
cp .env.example .env
# Fill in all values — never commit .env

# 3. Create host data directories
sudo mkdir -p /data/raw/hydro /data/raw/meteo
sudo mkdir -p /data/archive/hydro /data/archive/meteo
sudo mkdir -p /data/volumes/postgres_data /data/volumes/airflow_db /data/volumes/superset_db
sudo chown -R 999:999 /data/volumes/

# 4. Start the pipeline stack
docker compose up -d analytics-db airflow-db
# Wait for healthy, then:
docker compose up -d

# 5. Trigger seed_stations DAG from Airflow UI (http://server:8080)

# 6. DataHub first-start requires a staged bootstrap — see datahub-integration-guide.html
```

### DataHub bootstrap
DataHub requires a specific startup order on first install. See `datahub-integration-guide.html` for the full sequence with commands.

---

## Known Behaviours

| Behaviour | Detail |
|---|---|
| Hydro API publish lag | ~10+ hours behind real time. At 06:00 UTC, previous day is reliably available. |
| Meteo publish window | Daily batch at ~05:01 EET. All meteo endpoints publish in the same 2-minute window. |
| EH2000 correction | 15 coastal stations have gauge zero at −500 cm EH2000. Offset applied in dbt using `station_altitude_msl_m IS NOT NULL`. |
| Timezone handling | Hydro filtered by `timeline_ts_local` (not UTC) — Estonia is UTC+3 in summer. Meteo observation_ts constructed using `Europe/Tallinn`. |
| URL encoding | `fetch_hydro` builds query string manually (not via `params=`) to prevent requests library encoding colons as `%3A`. |
| DataHub signing key | Required since DataHub CLI 1.5. Set `DATAHUB_TOKEN_SERVICE_SIGNING_KEY` and `DATAHUB_TOKEN_SERVICE_SALT` in `.env`. |

---

## License

Internal training project — Keskkonnaagenuur (Estonian Environment Agency).
