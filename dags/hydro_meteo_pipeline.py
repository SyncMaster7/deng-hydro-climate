# =============================================================================
# deng-hydro-climate — dags/hydro_meteo_pipeline.py
# Daily ingestion pipeline — fetches hydro and meteo data for one day,
# saves raw JSON to /data/raw/, ingests into bronze layer, triggers dbt
#
# Tasks:
#   fetch_hydro  ──► ingest_hydro ──┐
#                                   ├──► run_dbt
#   fetch_meteo  ──► ingest_meteo ──┘
#
# Schedule: 0 6 * * * — after meteo daily batch publishes (~02:01 UTC)
# =============================================================================

import json
import logging
import os
from datetime import timedelta

import requests
from pendulum import datetime

from airflow.sdk import dag, task, Asset
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

CONN_ID       = "analytics_db"
RAW_HYDRO_DIR = "/data/raw/hydro"
RAW_METEO_DIR = "/data/raw/meteo"

HYDRO_API_URL = "https://keskkonnaandmed.envir.ee/f_hydroseire"
METEO_API_URL = "https://keskkonnaandmed.envir.ee/f_kliima_tund"

# Airflow Assets
asset_hydro_raw_file = Asset("raw/hydro_file")
asset_meteo_raw_file = Asset("raw/meteo_file")
asset_hydro_bronze   = Asset("bronze/hydro")
asset_meteo_bronze   = Asset("bronze/meteo")


@dag(
    dag_id="hydro_meteo_pipeline",
    description="Daily fetch → raw file → bronze ingestion → dbt for hydro and meteo data",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    tags=["ingestion", "bronze"],
)
def hydro_meteo_pipeline():

    # -------------------------------------------------------------------------
    # Task 1a — Fetch hydro data for target date, save to raw landing zone
    # -------------------------------------------------------------------------
    @task(outlets=[asset_hydro_raw_file])
    def fetch_hydro(data_interval_start=None) -> str:
        date = data_interval_start.date()
        date_next = date + timedelta(days=1)

        params = [
            ("timeline_ts_utc", f"gte.{date}T00:00:00"),
            ("timeline_ts_utc", f"lt.{date_next}T00:00:00"),
        ]

        log.info("Fetching hydro data for %s", date)
        response = requests.get(HYDRO_API_URL, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        if not data:
            raise ValueError(f"API returned empty response for hydro on {date} — aborting to avoid writing empty file")

        log.info("Received %d hydro records for %s", len(data), date)

        os.makedirs(RAW_HYDRO_DIR, exist_ok=True)
        file_path = f"{RAW_HYDRO_DIR}/hydro_{date}.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

        log.info("Saved hydro raw file: %s", file_path)
        return file_path

    # -------------------------------------------------------------------------
    # Task 1b — Fetch meteo data for target date, save to raw landing zone
    # -------------------------------------------------------------------------
    @task(outlets=[asset_meteo_raw_file])
    def fetch_meteo(data_interval_start=None) -> str:
        date = data_interval_start.date()

        params = {
            "aasta": f"eq.{date.year}",
            "kuu":   f"eq.{date.month}",
            "paev":  f"eq.{date.day}",
        }

        log.info("Fetching meteo data for %s", date)
        response = requests.get(METEO_API_URL, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        if not data:
            raise ValueError(f"API returned empty response for meteo on {date} — aborting to avoid writing empty file")

        log.info("Received %d meteo records for %s", len(data), date)

        os.makedirs(RAW_METEO_DIR, exist_ok=True)
        file_path = f"{RAW_METEO_DIR}/meteo_{date}.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

        log.info("Saved meteo raw file: %s", file_path)
        return file_path

    # -------------------------------------------------------------------------
    # Task 2a — Ingest hydro raw file into bronze.hydro
    # -------------------------------------------------------------------------
    @task(outlets=[asset_hydro_bronze])
    def ingest_hydro(file_path: str, data_interval_start=None) -> int:
        date = data_interval_start.date()
        source_file = os.path.basename(file_path)

        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Log start
        cursor.execute("""
            INSERT INTO bronze.etl_log (dag_id, task_id, run_date, source_file, status)
            VALUES (%s, %s, %s, %s, 'running') RETURNING id;
        """, ("hydro_meteo_pipeline", "ingest_hydro", date, source_file))
        log_id = cursor.fetchone()[0]
        conn.commit()

        try:
            with open(file_path, encoding="utf-8") as f:
                records = json.load(f)

            sql = """
                INSERT INTO bronze.hydro (
                    jaam_kood,
                    jaam_nimi,
                    jaam_taisnimi,
                    valgala_nimi,
                    valgala_suurus_km2,
                    kaugus_suudmest_km,
                    jaam_laiuskraad,
                    jaam_pikkuskraad,
                    veekogu_nimi,
                    timeline_ts_utc,
                    timeline_ts_local,
                    aegrida_kood,
                    aegrida_nimi,
                    vaartus,
                    source_file
                ) VALUES (
                    %(jaam_kood)s,
                    %(jaam_nimi)s,
                    %(jaam_taisnimi)s,
                    %(valgala_nimi)s,
                    %(valgala_suurus_km2)s,
                    %(kaugus_suudmest_km)s,
                    %(jaam_laiuskraad)s,
                    %(jaam_pikkuskraad)s,
                    %(veekogu_nimi)s,
                    %(timeline_ts_utc)s,
                    %(timeline_ts_local)s,
                    %(aegrida_kood)s,
                    %(aegrida_nimi)s,
                    %(vaartus)s,
                    %(source_file)s
                )
                ON CONFLICT (jaam_kood, timeline_ts_utc, aegrida_kood)
                DO UPDATE SET
                    vaartus     = EXCLUDED.vaartus,
                    loaded_at   = NOW();
            """

            rows = [{**r, "source_file": source_file} for r in records]
            cursor.executemany(sql, rows)
            rows_loaded = len(rows)
            conn.commit()

            cursor.execute("""
                UPDATE bronze.etl_log
                SET finished_at = NOW(), status = 'success', rows_loaded = %s
                WHERE id = %s;
            """, (rows_loaded, log_id))
            conn.commit()

            log.info("Ingested %d hydro rows for %s", rows_loaded, date)
            return rows_loaded

        except Exception as e:
            cursor.execute("""
                UPDATE bronze.etl_log
                SET finished_at = NOW(), status = 'error', error_message = %s
                WHERE id = %s;
            """, (str(e), log_id))
            conn.commit()
            raise
        finally:
            cursor.close()
            conn.close()

    # -------------------------------------------------------------------------
    # Task 2b — Ingest meteo raw file into bronze.meteo
    # -------------------------------------------------------------------------
    @task(outlets=[asset_meteo_bronze])
    def ingest_meteo(file_path: str, data_interval_start=None) -> int:
        date = data_interval_start.date()
        source_file = os.path.basename(file_path)

        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Log start
        cursor.execute("""
            INSERT INTO bronze.etl_log (dag_id, task_id, run_date, source_file, status)
            VALUES (%s, %s, %s, %s, 'running') RETURNING id;
        """, ("hydro_meteo_pipeline", "ingest_meteo", date, source_file))
        log_id = cursor.fetchone()[0]
        conn.commit()

        try:
            with open(file_path, encoding="utf-8") as f:
                records = json.load(f)

            sql = """
                INSERT INTO bronze.meteo (
                    jaam_kood,
                    jaam_nimi,
                    aasta,
                    kuu,
                    paev,
                    kellaaeg,
                    element_kood,
                    vaartus,
                    source_file
                ) VALUES (
                    %(jaam_kood)s,
                    %(jaam_nimi)s,
                    %(aasta)s,
                    %(kuu)s,
                    %(paev)s,
                    %(kellaaeg)s,
                    %(element_kood)s,
                    %(vaartus)s,
                    %(source_file)s
                )
                ON CONFLICT (jaam_kood, aasta, kuu, paev, kellaaeg, element_kood)
                DO UPDATE SET
                    vaartus     = EXCLUDED.vaartus,
                    loaded_at   = NOW();
            """

            rows = [{**r, "source_file": source_file} for r in records]
            cursor.executemany(sql, rows)
            rows_loaded = len(rows)
            conn.commit()

            cursor.execute("""
                UPDATE bronze.etl_log
                SET finished_at = NOW(), status = 'success', rows_loaded = %s
                WHERE id = %s;
            """, (rows_loaded, log_id))
            conn.commit()

            log.info("Ingested %d meteo rows for %s", rows_loaded, date)
            return rows_loaded

        except Exception as e:
            cursor.execute("""
                UPDATE bronze.etl_log
                SET finished_at = NOW(), status = 'error', error_message = %s
                WHERE id = %s;
            """, (str(e), log_id))
            conn.commit()
            raise
        finally:
            cursor.close()
            conn.close()

    # -------------------------------------------------------------------------
    # Task 3 — Run dbt (asset-driven, triggers when both bronze assets ready)
    # -------------------------------------------------------------------------
    @task()
    def run_dbt(hydro_rows: int, meteo_rows: int) -> None:
        import subprocess
        log.info("Running dbt build (hydro_rows=%d, meteo_rows=%d)", hydro_rows, meteo_rows)
        result = subprocess.run(
            [
                "dbt", "build",
                "--project-dir", "/opt/airflow/dbt_project",
                "--profiles-dir", "/home/airflow/.dbt",
                "--log-path", "/tmp/dbt_logs",
                "--target-path", "/tmp/dbt_target"
            ],
            capture_output=True,
            text=True,
        )
        log.info(result.stdout)
        if result.returncode != 0:
            log.error(result.stderr)
            raise RuntimeError(f"dbt build failed:\n{result.stderr}")
        log.info("dbt build completed successfully")

    # -------------------------------------------------------------------------
    # Task dependencies
    # -------------------------------------------------------------------------
    hydro_file = fetch_hydro()
    meteo_file = fetch_meteo()

    hydro_rows = ingest_hydro(file_path=hydro_file)
    meteo_rows = ingest_meteo(file_path=meteo_file)

    run_dbt(hydro_rows=hydro_rows, meteo_rows=meteo_rows)


hydro_meteo_pipeline()
