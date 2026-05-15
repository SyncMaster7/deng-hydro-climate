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
# Schedule: daily at 06:00 UTC — each run fetches data_interval_start - 3 days
# NOTE: API publishes data with ~43h lag, so days=3 (72h buffer) ensures a
#       complete day is always available before ingestion.
# Backfill start: 2025-01-01
# =============================================================================

import json
import logging
import os
import subprocess
from contextlib import contextmanager
from datetime import date, timedelta
from typing import Iterator

import pendulum
import requests
from psycopg2.extras import execute_values

from airflow.sdk import dag, task, Asset
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

CONN_ID       = "analytics_db"
RAW_HYDRO_DIR = "/data/raw/hydro"
RAW_METEO_DIR = "/data/raw/meteo"
API_LAG_DAYS  = 3  # API publishes with ~43h lag; 3 days (72h) ensures completeness

HYDRO_API_URL = "https://keskkonnaandmed.envir.ee/f_hydroseire"
METEO_API_URL = "https://keskkonnaandmed.envir.ee/f_kliima_tund"

# Airflow Assets
asset_hydro_raw_file = Asset("raw/hydro_file")
asset_meteo_raw_file = Asset("raw/meteo_file")
asset_hydro_bronze   = Asset("bronze/hydro")
asset_meteo_bronze   = Asset("bronze/meteo")


# -----------------------------------------------------------------------------
# Helpers — ETL log management
#
# Two patterns are needed:
#
# 1. etl_log_context — context manager for ingest and dbt tasks where the
#    DB connection stays open for the entire task duration. Handles
#    insert-running / update-success / update-error / close automatically.
#
# 2. etl_log_start / etl_log_finish / etl_log_error — plain functions for
#    fetch tasks, where the connection must be closed before the HTTP call
#    (to avoid holding an idle connection during a potentially slow API
#    request) and re-opened afterwards to write the result.
# -----------------------------------------------------------------------------

def etl_log_start(
    dag_id: str,
    task_id: str,
    target_date: date | None = None,
    source_file: str | None = None,
) -> int:
    """Insert a 'running' log entry and return its id."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO bronze.etl_log (dag_id, task_id, target_date, source_file, status)
            VALUES (%s, %s, %s, %s, 'running') RETURNING id;
            """,
            (dag_id, task_id, target_date, source_file),
        )
        log_id = cursor.fetchone()[0]
        conn.commit()
        return log_id
    finally:
        cursor.close()
        conn.close()


def etl_log_finish(log_id: int, rows_processed: int | None = None, source_file: str | None = None) -> None:
    """Mark a log entry as success."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE bronze.etl_log
            SET finished_at = NOW(), status = 'success', rows_processed = %s, source_file = %s
            WHERE id = %s;
            """,
            (rows_processed, source_file, log_id),
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def etl_log_error(log_id: int, error: Exception) -> None:
    """Mark a log entry as error."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE bronze.etl_log
            SET finished_at = NOW(), status = 'error', error_message = %s
            WHERE id = %s;
            """,
            (str(error), log_id),
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()


@contextmanager
def etl_log_context(
    dag_id: str,
    task_id: str,
    target_date: date | None = None,
    source_file: str | None = None,
) -> Iterator[tuple]:
    """Context manager for tasks that keep the DB connection open throughout."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO bronze.etl_log (dag_id, task_id, target_date, source_file, status)
            VALUES (%s, %s, %s, %s, 'running') RETURNING id;
            """,
            (dag_id, task_id, target_date, source_file),
        )
        log_id = cursor.fetchone()[0]
        conn.commit()

        yield cursor, conn, log_id

        cursor.execute(
            "UPDATE bronze.etl_log SET finished_at = NOW(), status = 'success' WHERE id = %s;",
            (log_id,),
        )
        conn.commit()

    except Exception as e:
        cursor.execute(
            """
            UPDATE bronze.etl_log
            SET finished_at = NOW(), status = 'error', error_message = %s
            WHERE id = %s;
            """,
            (str(e), log_id),
        )
        conn.commit()
        raise
    finally:
        cursor.close()
        conn.close()


@dag(
    dag_id="hydro_meteo_pipeline",
    description="Daily fetch → raw file → bronze ingestion → dbt for hydro and meteo data",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
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
    def fetch_hydro(data_interval_start: pendulum.DateTime | None = None) -> str:
        target_date = data_interval_start.date() - timedelta(days=API_LAG_DAYS)
        date_next   = target_date + timedelta(days=1)

        # Build query string manually to avoid requests encoding colons in timestamps
        query = (
            f"timeline_ts_local=gte.{target_date}T00:00:00"
            f"&timeline_ts_local=lt.{date_next}T00:00:00"
        )

        # Write 'running' entry then immediately release the connection
        # so it is not held idle during the HTTP request (up to 60s timeout)
        log_id = etl_log_start("hydro_meteo_pipeline", "fetch_hydro", target_date)

        try:
            full_url = f"{HYDRO_API_URL}?{query}"
            log.info("Fetching hydro data for local date %s — URL: %s", target_date, full_url)
            response = requests.get(full_url, timeout=60)
            response.raise_for_status()
            data = response.json()

            if not data:
                raise ValueError(
                    f"API returned empty response for hydro on {target_date} "
                    "— aborting to avoid writing empty file"
                )

            rows_processed = len(data)
            log.info("Received %d hydro records for %s", rows_processed, target_date)

            os.makedirs(RAW_HYDRO_DIR, exist_ok=True)
            file_path = f"{RAW_HYDRO_DIR}/hydro_{target_date}.json"
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)

            etl_log_finish(log_id, rows_processed=rows_processed, source_file=os.path.basename(file_path))
            log.info("Saved hydro raw file: %s", file_path)
            return file_path

        except Exception as e:
            etl_log_error(log_id, e)
            raise

    # -------------------------------------------------------------------------
    # Task 1b — Fetch meteo data for target date, save to raw landing zone
    # -------------------------------------------------------------------------
    @task(
        outlets=[asset_meteo_raw_file],
        retries=3,
        retry_delay=timedelta(minutes=15),
        retry_exponential_backoff=True,
    )
    def fetch_meteo(data_interval_start: pendulum.DateTime | None = None) -> str:
        target_date = data_interval_start.date() - timedelta(days=API_LAG_DAYS)

        params = {
            "aasta": f"eq.{target_date.year}",
            "kuu":   f"eq.{target_date.month}",
            "paev":  f"eq.{target_date.day}",
        }

        # Write 'running' entry then immediately release the connection
        # so it is not held idle during the HTTP request (up to 60s timeout)
        log_id = etl_log_start("hydro_meteo_pipeline", "fetch_meteo", target_date)

        try:
            prepared = requests.Request("GET", METEO_API_URL, params=params).prepare()
            log.info("Fetching meteo data for %s — URL: %s", target_date, prepared.url)
            response = requests.get(METEO_API_URL, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

            if not data:
                raise ValueError(
                    f"API returned empty response for meteo on {target_date} "
                    "— aborting to avoid writing empty file"
                )

            rows_processed = len(data)
            log.info("Received %d meteo records for %s", rows_processed, target_date)

            os.makedirs(RAW_METEO_DIR, exist_ok=True)
            file_path = f"{RAW_METEO_DIR}/meteo_{target_date}.json"
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)

            etl_log_finish(log_id, rows_processed=rows_processed, source_file=os.path.basename(file_path))
            log.info("Saved meteo raw file: %s", file_path)
            return file_path

        except Exception as e:
            etl_log_error(log_id, e)
            raise

    # -------------------------------------------------------------------------
    # Task 2a — Ingest hydro raw file into bronze.hydro
    # -------------------------------------------------------------------------
    @task(outlets=[asset_hydro_bronze])
    def ingest_hydro(
        file_path: str,
        data_interval_start: pendulum.DateTime | None = None,
    ) -> int:
        target_date = data_interval_start.date() - timedelta(days=API_LAG_DAYS)
        source_file = os.path.basename(file_path)

        with open(file_path, encoding="utf-8") as f:
            records = json.load(f)

        rows_processed = len(records)

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
            ) VALUES %s
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

        values = [
            (
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
            )
            for r in records
        ]

        with etl_log_context(
            "hydro_meteo_pipeline", "ingest_hydro", target_date, source_file
        ) as (cursor, conn, log_id):
            execute_values(cursor, sql, values)
            rows_loaded = len(values)
            cursor.execute(
                """
                UPDATE bronze.etl_log
                SET rows_processed = %s, rows_loaded = %s
                WHERE id = %s;
                """,
                (rows_processed, rows_loaded, log_id),
            )
            conn.commit()

        log.info("Ingested %d rows into bronze.hydro from %s", rows_loaded, source_file)
        return rows_loaded

    # -------------------------------------------------------------------------
    # Task 2b — Ingest meteo raw file into bronze.meteo
    # -------------------------------------------------------------------------
    @task(outlets=[asset_meteo_bronze])
    def ingest_meteo(
        file_path: str,
        data_interval_start: pendulum.DateTime | None = None,
    ) -> int:
        target_date = data_interval_start.date() - timedelta(days=API_LAG_DAYS)
        source_file = os.path.basename(file_path)

        with open(file_path, encoding="utf-8") as f:
            records = json.load(f)

        rows_processed = len(records)

        # Deduplicate by unique key before UPSERT.
        # The source API (f_kliima_tund) occasionally publishes corrections:
        # the same (jaam_kood, aasta, kuu, paev, tund, element_kood) appears
        # twice in a single day's response with a later avaandmed_ts and a
        # revised vaartus (confirmed for AJROOM01 / SDUR1H / 2026-04-29).
        # execute_values + ON CONFLICT DO UPDATE raises CardinalityViolation
        # when two rows in the same batch target the same unique key.
        # Fix: keep the row with the latest avaandmed_ts (the correction wins).
        seen: dict[tuple, dict] = {}
        for r in records:
            key = (r["jaam_kood"], r["aasta"], r["kuu"], r["paev"], r["tund"], r["element_kood"])
            if key not in seen or r["avaandmed_ts"] > seen[key]["avaandmed_ts"]:
                seen[key] = r
        dedup_count = rows_processed - len(seen)
        if dedup_count:
            log.warning(
                "Meteo dedup: removed %d duplicate row(s) for %s — keeping latest avaandmed_ts",
                dedup_count,
                target_date,
            )
        records = list(seen.values())

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
            ) VALUES %s
            ON CONFLICT (jaam_kood, aasta, kuu, paev, tund, element_kood) DO UPDATE SET
                jaam_nimi        = EXCLUDED.jaam_nimi,
                vaartus          = EXCLUDED.vaartus,
                element_nimi_eng = EXCLUDED.element_nimi_eng,
                element_yhik_eng = EXCLUDED.element_yhik_eng,
                avaandmed_ts     = EXCLUDED.avaandmed_ts,
                loaded_at        = NOW();
        """

        values = [
            (
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
            )
            for r in records
        ]

        with etl_log_context(
            "hydro_meteo_pipeline", "ingest_meteo", target_date, source_file
        ) as (cursor, conn, log_id):
            execute_values(cursor, sql, values)
            rows_loaded = len(values)
            cursor.execute(
                """
                UPDATE bronze.etl_log
                SET rows_processed = %s, rows_loaded = %s
                WHERE id = %s;
                """,
                (rows_processed, rows_loaded, log_id),
            )
            conn.commit()

        log.info("Ingested %d rows into bronze.meteo from %s", rows_loaded, source_file)
        return rows_loaded

    # -------------------------------------------------------------------------
    # Task 3 — Run dbt (triggers when both ingest tasks complete)
    # hydro_rows and meteo_rows are accepted solely to establish the task
    # dependency — Airflow requires both ingest tasks to succeed before this
    # runs. They are logged for visibility but do not influence dbt execution.
    # -------------------------------------------------------------------------
    @task()
    def run_dbt(hydro_rows: int, meteo_rows: int) -> None:
        with etl_log_context("hydro_meteo_pipeline", "run_dbt") as (cursor, conn, log_id):
            log.info("Running dbt build (hydro_rows=%d, meteo_rows=%d)", hydro_rows, meteo_rows)

            result = subprocess.run(
                [
                    "dbt", "build",
                    "--project-dir", "/opt/airflow/dbt_project",
                    "--profiles-dir", "/home/airflow/.dbt",
                    "--log-path",    "/tmp/dbt_logs",
                    "--target-path", "/tmp/dbt_target",
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

