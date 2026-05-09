# =============================================================================
# deng-hydro-climate — dags/seed_stations.py
# Loads station reference data from CSV seed files into ref tables
# Calculates station proximity matrix using haversine formula
# Trigger: manual only (schedule=None)
#
# Tasks:
#   load_hydrometric_stations  ─┐
#                               ├─► calculate_proximity (conditional)
#   load_meteorological_stations┘
# =============================================================================

import csv
import logging
import sys

sys.path.insert(0, "/opt/airflow")

from pendulum import datetime

from airflow.sdk import dag, task, Asset
from airflow.providers.postgres.hooks.postgres import PostgresHook

from ingestion.haversine import haversine_km

log = logging.getLogger(__name__)

CONN_ID = "analytics_db"
SEEDS_PATH = "/opt/airflow/seeds"

# Airflow Assets — emitted by this DAG, consumed by downstream DAGs
asset_hydrometric_stations    = Asset("ref/hydrometric_stations")
asset_meteorological_stations = Asset("ref/meteorological_stations")
asset_station_proximity       = Asset("ref/station_proximity")


@dag(
    dag_id="seed_stations",
    description="Load station reference data and auto-generate proximity matrix",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["seed", "reference"],
)
def seed_stations():

    # -------------------------------------------------------------------------
    # Task 1a — Hydrometric stations (parallel)
    # Returns count of rows actually inserted or updated (0 if nothing changed)
    # -------------------------------------------------------------------------
    @task(outlets=[asset_hydrometric_stations])
    def load_hydrometric_stations() -> int:
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
                loaded_at              = NOW()
            WHERE (
                EXCLUDED.station_category       IS DISTINCT FROM ref.hydrometric_stations.station_category OR
                EXCLUDED.station_name           IS DISTINCT FROM ref.hydrometric_stations.station_name OR
                EXCLUDED.station_fullname       IS DISTINCT FROM ref.hydrometric_stations.station_fullname OR
                EXCLUDED.water_body             IS DISTINCT FROM ref.hydrometric_stations.water_body OR
                EXCLUDED.catchment_name         IS DISTINCT FROM ref.hydrometric_stations.catchment_name OR
                EXCLUDED.catchment_size_km2     IS DISTINCT FROM ref.hydrometric_stations.catchment_size_km2 OR
                EXCLUDED.distance_from_mouth_km IS DISTINCT FROM ref.hydrometric_stations.distance_from_mouth_km OR
                EXCLUDED.station_altitude_msl_m IS DISTINCT FROM ref.hydrometric_stations.station_altitude_msl_m OR
                EXCLUDED.latitude               IS DISTINCT FROM ref.hydrometric_stations.latitude OR
                EXCLUDED.longitude              IS DISTINCT FROM ref.hydrometric_stations.longitude OR
                EXCLUDED.is_active              IS DISTINCT FROM ref.hydrometric_stations.is_active
            );
        """

        changed = 0
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
                changed += cursor.rowcount  # 1 if inserted or updated, 0 if unchanged

        conn.commit()
        cursor.close()
        conn.close()
        log.info("ref.hydrometric_stations: %d rows changed", changed)
        return changed

    # -------------------------------------------------------------------------
    # Task 1b — Meteorological stations (parallel)
    # Returns count of rows actually inserted or updated (0 if nothing changed)
    # -------------------------------------------------------------------------
    @task(outlets=[asset_meteorological_stations])
    def load_meteorological_stations() -> int:
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
                loaded_at        = NOW()
            WHERE (
                EXCLUDED.station_category IS DISTINCT FROM ref.meteorological_stations.station_category OR
                EXCLUDED.station_name     IS DISTINCT FROM ref.meteorological_stations.station_name OR
                EXCLUDED.latitude         IS DISTINCT FROM ref.meteorological_stations.latitude OR
                EXCLUDED.longitude        IS DISTINCT FROM ref.meteorological_stations.longitude OR
                EXCLUDED.altitude_m       IS DISTINCT FROM ref.meteorological_stations.altitude_m OR
                EXCLUDED.is_active        IS DISTINCT FROM ref.meteorological_stations.is_active
            );
        """

        changed = 0
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
                changed += cursor.rowcount  # 1 if inserted or updated, 0 if unchanged

        conn.commit()
        cursor.close()
        conn.close()
        log.info("ref.meteorological_stations: %d rows changed", changed)
        return changed

    # -------------------------------------------------------------------------
    # Task 2 — Calculate proximity matrix (haversine, conditional)
    # Runs if either load task changed rows OR proximity table is empty
    # -------------------------------------------------------------------------
    @task(outlets=[asset_station_proximity])
    def calculate_proximity(hydro_count: int, meteo_count: int) -> int:
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Check if proximity table is empty
        cursor.execute("SELECT COUNT(*) FROM ref.station_proximity;")
        proximity_empty = cursor.fetchone()[0] == 0

        if hydro_count == 0 and meteo_count == 0 and not proximity_empty:
            log.info(
                "No station changes and proximity table is populated — skipping recalculation"
            )
            cursor.close()
            conn.close()
            return 0

        log.info(
            "Recalculating proximity matrix (hydro_count=%d, meteo_count=%d, proximity_empty=%s)",
            hydro_count, meteo_count, proximity_empty,
        )

        # Load all active stations from DB
        cursor.execute(
            "SELECT station_code, latitude, longitude FROM ref.hydrometric_stations WHERE is_active = true;"
        )
        hydro_stations = cursor.fetchall()

        cursor.execute(
            "SELECT station_code, latitude, longitude FROM ref.meteorological_stations WHERE is_active = true;"
        )
        meteo_stations = cursor.fetchall()

        upsert_sql = """
            INSERT INTO ref.station_proximity (
                hydro_station_code,
                meteo_station_code,
                distance_km,
                proximity_rank
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT (hydro_station_code, proximity_rank) DO UPDATE SET
                meteo_station_code = EXCLUDED.meteo_station_code,
                distance_km        = EXCLUDED.distance_km,
                loaded_at          = NOW();
        """

        rows = 0
        for h_code, h_lat, h_lon in hydro_stations:
            distances = [
                (haversine_km(float(h_lat), float(h_lon), float(m_lat), float(m_lon)), m_code)
                for m_code, m_lat, m_lon in meteo_stations
            ]
            distances.sort(key=lambda x: x[0])

            for rank, (dist_km, m_code) in enumerate(distances[:3], start=1):
                cursor.execute(upsert_sql, (h_code, m_code, round(dist_km, 3), rank))
                rows += 1

        conn.commit()
        cursor.close()
        conn.close()
        log.info("Upserted %d rows into ref.station_proximity", rows)
        return rows

    # -------------------------------------------------------------------------
    # Task 3 — Run dbt snapshots (always, after proximity is up to date)
    # -------------------------------------------------------------------------
    @task
    def run_snapshot() -> None:
        import subprocess
        result = subprocess.run(
            ["dbt", "snapshot", "--project-dir", "/opt/airflow/dbt_project", "--log-path", "/tmp"],
            capture_output=True,
            text=True,
        )
        log.info(result.stdout)
        if result.returncode != 0:
            log.error(result.stderr)
            raise RuntimeError(f"dbt snapshot failed:\n{result.stderr}")
        log.info("dbt snapshot completed successfully")

    # -------------------------------------------------------------------------
    # Task dependencies — loads run in parallel, proximity waits for both,
    # snapshots run last (always, unconditionally)
    # -------------------------------------------------------------------------
    hydro = load_hydrometric_stations()
    meteo = load_meteorological_stations()
    proximity = calculate_proximity(hydro_count=hydro, meteo_count=meteo)
    proximity >> run_snapshot()


seed_stations()
