# =============================================================================
# deng-hydro-climate — seed_stations.py
# Loads station reference data from CSV seed files into dimension tables
# Trigger: manual only (schedule=None)
# Tasks:
#   seed_hydrometric_stations  ─┐
#                                ├─► seed_station_proximity
#   seed_meteorological_stations─┘
# =============================================================================

import csv
import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

CONN_ID = "analytics_db"
SEEDS_PATH = "/opt/airflow/seeds"


@dag(
    dag_id="seed_stations",
    description="Load hydrometric, meteorological, and proximity seed data into dimension tables",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["seed", "reference"],
)
def seed_stations():

    # -------------------------------------------------------------------------
    # Task 1a — Hydrometric stations
    # -------------------------------------------------------------------------
    @task()
    def seed_hydrometric_stations():
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        sql = """
            INSERT INTO ref.hydrometric_stations (
                station_code,
                station_category,
                station_name,
                station_fullname,
                water_body,
                catchment_name,
                catchment_size_km2,
                distance_from_mouth_km,
                station_altitude_msl_m,
                latitude,
                longitude,
                is_active
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (station_code) DO UPDATE SET
                station_category       = EXCLUDED.station_category,
                station_name           = EXCLUDED.station_name,
                station_fullname       = EXCLUDED.station_fullname,
                water_body             = EXCLUDED.water_body,
                catchment_name         = EXCLUDED.catchment_name,
                catchment_size_km2     = EXCLUDED.catchment_size_km2,
                distance_from_mouth_km = EXCLUDED.distance_from_mouth_km,
                station_altitude_msl_m = EXCLUDED.station_altitude_msl_m,
                latitude               = EXCLUDED.latitude,
                longitude              = EXCLUDED.longitude,
                is_active              = EXCLUDED.is_active,
                loaded_at              = NOW();
        """

        rows = 0
        with open(f"{SEEDS_PATH}/hydrometric_stations.csv", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cursor.execute(sql, (
                    int(row["station_code"]),
                    row["station_category"],
                    row["station_name"],
                    row["station_fullname"] or None,
                    row["water_body"] or None,
                    row["catchment_name"] or None,
                    float(row["catchment_size_km2"]) if row["catchment_size_km2"] else None,
                    float(row["distance_from_mouth_km"]) if row["distance_from_mouth_km"] else None,
                    float(row["station_altitude_msl_m"]) if row["station_altitude_msl_m"] else None,
                    float(row["latitude"]),
                    float(row["longitude"]),
                    row["is_active"].strip().lower() == "true",
                ))
                rows += 1

        conn.commit()
        cursor.close()
        conn.close()
        log.info("Upserted %d rows into hydrometric_stations", rows)
        return rows

    # -------------------------------------------------------------------------
    # Task 1b — Meteorological stations
    # -------------------------------------------------------------------------
    @task()
    def seed_meteorological_stations():
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        sql = """
            INSERT INTO ref.meteorological_stations (
                station_code,
                station_category,
                station_name,
                latitude,
                longitude,
                altitude_m,
                is_active
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (station_code) DO UPDATE SET
                station_category = EXCLUDED.station_category,
                station_name     = EXCLUDED.station_name,
                latitude         = EXCLUDED.latitude,
                longitude        = EXCLUDED.longitude,
                altitude_m       = EXCLUDED.altitude_m,
                is_active        = EXCLUDED.is_active,
                loaded_at        = NOW();
        """

        rows = 0
        with open(f"{SEEDS_PATH}/meteorological_stations.csv", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cursor.execute(sql, (
                    row["station_code"],
                    row["station_category"],
                    row["station_name"],
                    float(row["latitude"]),
                    float(row["longitude"]),
                    float(row["altitude_m"]) if row["altitude_m"] else None,
                    row["is_active"].strip().lower() == "true",
                ))
                rows += 1

        conn.commit()
        cursor.close()
        conn.close()
        log.info("Upserted %d rows into meteorological_stations", rows)
        return rows

    # -------------------------------------------------------------------------
    # Task 2 — Station proximity (depends on both station tasks)
    # -------------------------------------------------------------------------
    @task()
    def seed_station_proximity():
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        sql = """
            INSERT INTO ref.station_proximity (
                hydro_station_code,
                meteo_station_code,
                distance_km,
                proximity_rank
            ) VALUES (
                %s, %s, %s, %s
            )
            ON CONFLICT (hydro_station_code, proximity_rank) DO UPDATE SET
                meteo_station_code = EXCLUDED.meteo_station_code,
                distance_km        = EXCLUDED.distance_km,
                loaded_at          = NOW();
        """

        rows = 0
        with open(f"{SEEDS_PATH}/station_proximity.csv", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cursor.execute(sql, (
                    int(row["hydro_station_code"]),
                    row["meteo_station_code"],
                    float(row["distance_km"]),
                    int(row["proximity_rank"]),
                ))
                rows += 1

        conn.commit()
        cursor.close()
        conn.close()
        log.info("Upserted %d rows into station_proximity", rows)
        return rows

    # -------------------------------------------------------------------------
    # Task dependencies
    # -------------------------------------------------------------------------
    hydro = seed_hydrometric_stations()
    meteo = seed_meteorological_stations()
    proximity = seed_station_proximity()

    [hydro, meteo] >> proximity


seed_stations()
