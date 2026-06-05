# Estonian Hydrological Monitoring Pipeline

A production-like data engineering pipeline for analyzing how environmental factors like air temperature, precipitation etc affect water level fluctuations at monitoring stations, and which factors have the strongest impact on water level changes. The system ingests hourly hydrological and meteorological observations from the Estonian Environment Agency, transforms them through a medallion architecture, and exposes the data via a public Tableau dashboard and public REST API.

Built on a self-hosted Dell PowerEdge T640 server running Docker on Ubuntu 24.04. Data is sourced from the [Estonian Environment Agency](https://keskkonnaagentuur.ee/en) via the open environmental data API at [keskkonnaandmed.envir.ee](https://keskkonnaandmed.envir.ee).

### Synopsis

deng-hydro-climate is a full-stack data engineering project that automates the daily collection, transformation, and serving of environmental monitoring data. It covers 76 hydrometric stations and 25 meteorological stations across Estonia, with data going back to 2025-01-01.

### Motivation

This project was built as a hands-on training exercise in data engineering, with the goal of applying production patterns — orchestration, data quality testing, metadata cataloguing, and public API delivery — to a real-world environmental dataset. The data source is the Estonian Environment Agency's open API. The business questions driving the analysis are about the relationship between precipitation, temperature, and river water levels across Estonian catchments.

---

## Business Questions

- How do environmental factors affect water level fluctuations at monitoring stations?
- Which environmental factors have the strongest impact on water level changes?

---

## Getting Started

### Prerequisites

- Docker and Docker Compose installed
- Git
- A `.env` file configured from `.env.example` (passwords, secrets, DB credentials)
- ~16 GB RAM and ~60 GB disk available on the host

### Installing

```bash
# 1. Clone the repository
git clone https://github.com/SyncMaster7/deng-hydro-climate.git
cd deng-hydro-climate

# 2. Configure environment variables
cp .env.example .env
# Edit .env — set all passwords, secrets, and connection strings

# 3. Start all services
docker compose up -d --build

# 4. Install dbt packages
docker exec -it deng-dbt dbt deps \
  --project-dir /dbt \
  --profiles-dir /dbt

# 5. Load station reference data
# In Airflow UI — trigger seed_stations DAG manually
# This loads hydrometric and meteorological station CSVs and calculates station proximity pairs
```

After the seed DAG completes, the daily pipeline (`hydro_meteo_pipeline`) will run automatically at 06:00 UTC and begin populating data. For historical backfill, Airflow's `catchup=True` handles this automatically from `start_date`.

---

## Service URLs

| Service | Public URL | Local |
|---|---|---|
| Airflow UI | https://airflow.deng.ee | http://localhost:8080 |
| Superset | https://superset.deng.ee | http://localhost:8088 |
| DataHub | https://datahub.deng.ee | http://localhost:9002 |
| FastAPI / Swagger | https://api.deng.ee/docs | http://localhost:8000/docs |
| CKAN catalogue | https://ckan.deng.ee | http://localhost:5000 |
| Public catalogue frontend | https://catalogue.deng.ee | — |

---

## API Reference

The public REST API is available at `https://api.deng.ee`. Full interactive documentation is at `https://api.deng.ee/docs`.

Key endpoints:

| Endpoint | Description |
|---|---|
| `GET /v1/stations/hydro` | All 76 hydrometric stations |
| `GET /v1/stations/meteo` | All 25 meteorological stations |
| `GET /v1/elements` | All measurement element codes |
| `GET /v1/observations/hydro` | Hydro observations (filter by station, element, date range) |
| `GET /v1/observations/hydro/latest` | Latest observation per station per element |
| `GET /v1/observations/meteo` | Meteo observations |
| `GET /health` | Health check |

>Global rate limited to 60 requests/minute. No authentication required.

For full parameter documentation see [`docs/runbook/fastapi_runbook.md`](docs/runbook/fastapi_runbook.md).

---

## Data Quality Tests

dbt runs 26 automated tests on every `dbt build` — all targeting the bronze layer. A test failure (severity: `error`) halts all downstream model builds, preventing bad data from reaching silver, gold, and api layers.

Tests are split into 16 generic tests (defined in `models/sources/sources.yml`) covering nullability, accepted values, and uniqueness, and 10 singular tests (defined in `tests/`) covering physical value ranges for water level, water temperature, discharge, air temperature, precipitation, humidity, pressure, wind speed, and hour-of-day format.

Run tests manually:

```bash
docker exec -it deng-dbt dbt test \
  --select source:bronze \
  --project-dir /dbt \
  --profiles-dir /dbt \
  --log-path /tmp/dbt_logs \
  --target-path /tmp/dbt_target
```

Full test documentation is in [`docs/architecture.md`](docs/architecture.md).

---

## Built With

| Component | Tool | Version | RAM        | Disk |
|---|---|---|------------|---|
| Orchestration | Apache Airflow (TaskFlow API) | 3.2.1 | ~3 GB      | ~2 GB |
| Transformation | dbt Core + dbt-utils | 1.9.x | ~512 MB    | ~512 MB |
| Database | PostgreSQL + pgduckdb (analytics-db) | 16 | ~3 GB      | ~5 GB |
| Database | PostgreSQL (airflow-db + superset-db) | 16 | ~1 GB      | ~200 MB |
| Dashboards | Apache Superset | 6.0.1 | ~1.5 GB    | ~1 GB |
| Dashboards | Tableau | — | —          | — |
| Data Catalogue | DataHub | latest (head) | ~8 GB      | ~5 GB |
| Public Catalogue | CKAN | 2.11.4 | ~1 GB      | ~1 GB |
| Public API | FastAPI + asyncpg + slowapi | 0.115.6 | ~512 MB    | ~512 MB |
| Raw data + archive | /data/raw + /data/archive | — | —          | ~200 MB |
| Docker images | /var/lib/docker (active images) | — | —          | ~28 GB |
| Docker build cache | /var/lib/docker (reclaimable) | — | —          | ~8 GB |
| Language | Python 3 | 3.12 | —          | — |
| Version control | Git / GitHub | — | —          | — |
| **Total (recommended minimum)** | | | **~24 GB** | **~60 GB** |

> Recommended minimums include headroom for stable operation. Measured on Dell PowerEdge T640, Ubuntu 24.04, with full backfill from 2025-01-01: total RAM in use is ~6.8 GB active + ~3.3 GB swap on an 11 GB VM — the system runs but is memory-constrained, making 16 GB a genuine minimum. Disk: 54 GB used on a 98 GB volume, with Docker images (~28 GB active) as the largest single consumer. Analytics-db data volume is currently ~4.6 GB and will grow over time. Build cache (~8 GB) is fully reclaimable via `docker builder prune`.

---

## Documentation

| Document | Description |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Full architecture deep-dive — data model, pipeline design, EH2000 correction, data quality tests |
| [`docs/progress.md`](docs/progress.md) | Current project status, known gaps, and next steps |
| [`docs/runbook/datahub_runbook.md`](docs/runbook/datahub_runbook.md) | DataHub — normal restart, full rebuild, ingestion sources, troubleshooting |
| [`docs/runbook/airflow_runbook.md`](docs/runbook/airflow_runbook.md) | Airflow — DAG reference, restart, DB access, debugging |
| [`docs/runbook/dbt_runbook.md`](docs/runbook/dbt_runbook.md) | dbt — build commands, full-refresh, snapshots, tests |
| [`docs/runbook/fastapi_runbook.md`](docs/runbook/fastapi_runbook.md) | FastAPI — endpoints, query parameters, restart, monitoring |
| [`docs/runbook/superset_runbook.md`](docs/runbook/superset_runbook.md) | Superset — restart, rebuild, users, DB connection |

Academic documentation (Estonian): [`docs/school/`](docs/school/)

---

## Repository Structure

```
.
├── README.md
├── docker-compose.yml
├── .env.example
├── .gitignore
├── dags/
│   ├── hydro_meteo_pipeline.py       ← daily pipeline (fetch → ingest → dbt)
│   ├── seed_stations.py              ← station reference data + proximity calculation
│   └── archive_raw_files.py          ← weekly raw file compression and archival
├── dbt_project/
│   ├── dbt_project.yml
│   ├── packages.yml                  ← dbt-utils dependency
│   ├── profiles.yml
│   ├── macros/
│   │   └── generate_schema_name.sql
│   ├── models/
│   │   ├── api/                      ← FastAPI serving layer (5 models)
│   │   ├── gold/
│   │   │   └── hydro_meteo.sql
│   │   ├── silver/
│   │   │   ├── hydro.sql
│   │   │   └── meteo.sql
│   │   └── sources/
│   │       └── sources.yml           ← source definitions + generic tests
│   ├── snapshots/
│   │   ├── snap_hydro_stations.sql
│   │   └── snap_meteo_stations.sql
│   └── tests/                        ← singular tests (10 SQL files)
├── docker/
│   ├── airflow/
│   ├── ckan/                         ← custom CKAN Dockerfile (pip extensions)
│   ├── datahub-actions/
│   ├── fastapi/
│   └── superset/
├── datahub/
│   ├── artifacts/                    ← dbt artifacts for DataHub lineage
│   ├── glossary/                     ← glossary reference files
│   └── recipes/                      ← ingestion recipe references
├── docs/
│   ├── architecture.md
│   ├── progress.md
│   ├── runbook/
│   │   ├── datahub_runbook.md
│   │   ├── airflow_runbook.md
│   │   ├── dbt_runbook.md
│   │   ├── fastapi_runbook.md
│   │   └── superset_runbook.md
│   └── school/                       ← Estonian academic documentation
│       ├── arhitektuur.md
│       └── progress.md
├── ingestion/
│   └── haversine.py
├── seeds/
│   ├── hydrometric_stations.csv
│   ├── meteorological_stations.csv
│   └── station_proximity.csv
└── sql/
    ├── create_tables.sql
    └── migrate_monitoring.sql
```

---

## Security and Configuration

All secrets (passwords, API keys, DB credentials) are stored in `.env`, which is listed in `.gitignore` and never committed to the repository. Only `.env.example` is tracked — it shows the required variable structure without real values.

Database authentication uses `POSTGRES_HOST_AUTH_METHOD=md5` for both PostgreSQL services. This project contains no personal data — all data is publicly available environmental monitoring measurements from the Estonian Environment Agency.

---

## Licence

The source code in this repository is available under the [MIT License](LICENSE).

Data used in this project is sourced from the Estonian Environment Agency and is published under the [Creative Commons Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/) licence. Attribution: Keskkonnaagentuur, [keskkonnaportaal.ee](https://www.keskkonnaportaal.ee).

---

## Project Team

| Name | Role | Contribution |
|---|---|---|
| Thea | Project coordination, dashboard development | Project management and scheduling; Tableau dashboards for business users |
| Kairi | Research, methodology, documentation | Project structure, documentation, requirements analysis |
| Anny | Business analyst, application lead | DataHub administration; business glossary and data descriptions |
| Aivo | Data governance, metadata management | Data governance processes, metadata standards, DataHub setup |
| Kermo | Technical infrastructure, backend, Python | Server, Docker, Airflow orchestration, Python automation, dbt, DataHub integrations |
