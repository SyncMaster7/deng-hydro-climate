# deng-hydro-climate

A production-grade data engineering pipeline monitoring Estonian hydrological and climate conditions.

## Business Question

> How do weather conditions — precipitation, temperature, and snow depth — drive water levels and runoff across Estonian river catchments, and which rivers respond most strongly and most quickly to rainfall events?

## Overview

This pipeline ingests daily hydrological and climate measurements from the Estonian Environment Agency APIs, transforms them through a medallion architecture (staging → intermediate → marts), and produces a fact table that links weather conditions to river behaviour across 79 monitoring stations.

## Data Sources

| Source | Endpoint | Description |
|---|---|---|
| Hydrological monitoring | `keskkonnaandmed.envir.ee/f_hydroseire` | Water level, temperature, runoff — daily, from 2012 |
| Climate measurements | `keskkonnaandmed.envir.ee/f_kliima_paev` | Precipitation, temperature, snow depth — daily, from 1991 |
| Station list | `seeds/stations.csv` | 79 monitoring stations (river, lake, coastal) |

## Technical Stack

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow (TaskFlow API) |
| Transformation | dbt Core (medallion architecture) |
| Database | PostgreSQL with pgduckdb extension |
| Containerisation | Docker Compose |
| Language | Python 3 |
| Version control | Git / GitHub |

## Infrastructure

- **Server:** Dell PowerEdge T640, VMware ESXi 7
- **VM:** `deng-vm`, Ubuntu Server 24.04
- **Project root:** `/srv/deng-hydro-climate/`
- **Data volumes:** `/data/volumes/`

## Project Structure

```
deng-hydro-climate/
├── docker-compose.yml
├── .env.example
├── seeds/
│   └── stations.csv          # 79 monitoring stations
├── sql/
│   └── create_tables.sql     # Database initialisation
├── ingestion/
│   ├── fetch_hydro.py
│   ├── fetch_climate.py
│   └── load_staging.py
├── dags/
│   ├── hydro_climate_pipeline.py   # Main daily DAG
│   └── seed_stations.py            # One-time station seed DAG
└── dbt_project/
    ├── models/
    │   ├── staging/
    │   ├── intermediate/
    │   └── marts/
    └── tests/
```

## Pipeline Architecture

```
stations.csv ──────────────────────► seed_stations DAG ──► dim_stations
                                                                │
f_kliima_paev (climate API) ────────┐                          │
                                     ├──► Airflow daily DAG    │
f_hydroseire (hydro API) ───────────┘         │                │
                                              ▼                │
                                    staging.climate_raw         │
                                    staging.hydro_raw           │
                                              │                 │
                                              ▼                 │
                                    intermediate layer ◄────────┘
                                    (clean, typed, spatial join)
                                              │
                                              ▼
                                    marts layer
                                    fct_daily_conditions
                                    obt_river_health
```

## Getting Started

### Prerequisites
- Docker and Docker Compose installed
- Access to the `deng-vm` server

### Setup

1. Clone the repository:
   ```bash
   git clone git@github.com:SyncMaster7/deng-hydro-climate.git
   ```

2. Copy the environment file and fill in credentials:
   ```bash
   cp .env.example .env
   ```

3. Start services:
   ```bash
   docker compose up -d
   ```

4. Run the station seed DAG once to populate `dim_stations`.

5. Trigger the main pipeline or let it run on its daily schedule.

## Branching Strategy

| Branch | Purpose |
|---|---|
| `main` | Production — protected, never push directly |
| `feature/*` | New features and models |
| `fix/*` | Bug fixes |

All changes to `main` go through a Pull Request with at least one review.

## Environment Variables

Copy `.env.example` to `.env` and fill in the values. Never commit `.env`.

## License

Internal project — Estonian Environment Agency (Keskkonnaagenuur).
