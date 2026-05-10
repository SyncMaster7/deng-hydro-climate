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
# Schedule: daily — each run covers exactly one day via data_interval_start
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
    @task(
        outlets=[asset_hydro_raw_file],
        retries=3,
        retry_delay=timedelta(minutes=15),
        retry_exponential_backoff=True,
    )
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
    @task(
        outlets=[asset_meteo_raw_file],
        retries=3,
        retry_delay=timedelta(minutes=15),
        retry_exponential_backoff=True,
    )
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
                    aegrida_nimi,
                    vaartus
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (jaam_kood, timeline_ts_utc, aegrida_nimi) DO UPDATE SET
                    jaam_nimi          = EXCLUDED.jaam_nimi,
                    jaam_taisnimi      = EXCLUDED.jaam_taisnimi,
                    valgala_nimi       = EXCLUDED.valgala_nimi,
                    valgala_suurus_km2 = EXCLUDED.valgala_suurus_km2,
                    kaugus_suudmest_km = EXCLUDED.kaugus_suudmest_km,
                    jaam_laiuskraad    = EXCLUDED.jaam_laiuskraad,
                    jaam_pikkuskraad   = EXCLUDED.jaam_pikkuskraad,
                    veekogu_nimi       = EXCLUDED.veekogu_nimi,
                    timeline_ts_local  = EXCLUDED.timeline_ts_local,
                    vaartus            = EXCLUDED.vaartus,
                    loaded_at          = NOW();
            """

            rows = 0
            for r in records:
                cursor.execute(sql, (
                    r["jaam_kood"],
                    r.get("jaam_nimi"),
                    r.get("jaam_taisnimi"),
                    r.get("valgala_nimi"),
                    r.get("valgala_suurus_km2"),
                    r.get("kaugus_suudmest_km"),
                    r.get("jaam_laiuskraad"),
                    r.get("jaam_pikkuskraad"),
                    r.get("veekogu_nimi"),
                    r["timeline_ts_utc"],
                    r.get("timeline_ts_local"),
                    r["aegrida_nimi"],
                    r.get("vaartus"),
                ))
                rows += 1

            cursor.execute("""
                UPDATE bronze.etl_log
                SET finished_at = NOW(), rows_loaded = %s, status = 'success'
                WHERE id = %s;
            """, (rows, log_id))
            conn.commit()
            log.info("Ingested %d rows into bronze.hydro from %s", rows, source_file)
            return rows

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
                    tund,
                    vaartus,
                    element_kood,
                    element_nimi_eng,
                    element_yhik_eng,
                    avaandmed_ts
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (jaam_kood, aasta, kuu, paev, tund, element_kood) DO UPDATE SET
                    jaam_nimi        = EXCLUDED.jaam_nimi,
                    vaartus          = EXCLUDED.vaartus,
                    element_nimi_eng = EXCLUDED.element_nimi_eng,
                    element_yhik_eng = EXCLUDED.element_yhik_eng,
                    avaandmed_ts     = EXCLUDED.avaandmed_ts,
                    loaded_at        = NOW();
            """

            rows = 0
            for r in records:
                cursor.execute(sql, (
                    r["jaam_kood"],
                    r.get("jaam_nimi"),
                    r["aasta"],
                    r["kuu"],
                    r["paev"],
                    r["tund"],
                    r.get("vaartus"),
                    r["element_kood"],
                    r.get("element_nimi_eng"),
                    r.get("element_yhik_eng"),
                    r.get("avaandmed_ts"),
                ))
                rows += 1

            cursor.execute("""
                UPDATE bronze.etl_log
                SET finished_at = NOW(), rows_loaded = %s, status = 'success'
                WHERE id = %s;
            """, (rows, log_id))
            conn.commit()
            log.info("Ingested %d rows into bronze.meteo from %s", rows, source_file)
            return rows

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
