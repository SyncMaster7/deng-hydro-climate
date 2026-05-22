# =============================================================================
# deng-hydro-climate — dags/datahub_refresh_dbt_metadata.py
# Manual trigger only — refreshes DataHub with latest dbt metadata.
#
# Run this DAG after any changes to dbt model/source descriptions in
# schema.yml or sources.yml to propagate updates to DataHub.
#
# Tasks:
#   dbt_docs_generate ──► copy_artifacts ──► datahub_ingest_dbt
#
# Schedule: manual trigger only (None)
# =============================================================================

import logging
import shutil
import subprocess

import pendulum

from airflow.sdk import dag, task

log = logging.getLogger(__name__)

DBT_PROJECT_DIR  = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/home/airflow/.dbt"
DBT_TARGET_PATH  = "/tmp/dbt_target"
DBT_LOG_PATH     = "/tmp/dbt_logs"

ARTIFACTS_SRC_DIR = "/tmp/dbt_target"
ARTIFACTS_DST_DIR = "/opt/airflow/datahub/artifacts"

DATAHUB_RECIPE   = "/datahub/recipes/dbt_recipe.yml"
DATAHUB_GMS_URL  = "http://datahub-gms:8080"


@dag(
    dag_id="datahub_refresh_dbt_metadata",
    description="Manual trigger — generates dbt docs, copies artifacts, re-ingests into DataHub",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["datahub", "metadata"],
)
def datahub_refresh_dbt_metadata():

    # -------------------------------------------------------------------------
    # Task 1 — Generate dbt docs (writes manifest.json + catalog.json)
    # -------------------------------------------------------------------------
    @task()
    def dbt_docs_generate() -> None:
        log.info("Running dbt docs generate...")

        result = subprocess.run(
            [
                "dbt", "docs", "generate",
                "--project-dir", DBT_PROJECT_DIR,
                "--profiles-dir", DBT_PROFILES_DIR,
                "--log-path",    DBT_LOG_PATH,
                "--target-path", DBT_TARGET_PATH,
            ],
            capture_output=True,
            text=True,
        )
        log.info(result.stdout)

        if result.returncode != 0:
            log.error(result.stderr)
            raise RuntimeError(f"dbt docs generate failed:\n{result.stderr}")

        log.info("dbt docs generate completed successfully")

    # -------------------------------------------------------------------------
    # Task 2 — Copy artifacts to datahub/artifacts/ (shared with datahub-actions)
    # -------------------------------------------------------------------------
    @task()
    def copy_artifacts() -> None:
        artifacts = ["manifest.json", "catalog.json", "run_results.json"]

        for filename in artifacts:
            src = f"{ARTIFACTS_SRC_DIR}/{filename}"
            dst = f"{ARTIFACTS_DST_DIR}/{filename}"
            log.info("Copying %s → %s", src, dst)
            shutil.copy2(src, dst)

        log.info("All artifacts copied to %s", ARTIFACTS_DST_DIR)

    # -------------------------------------------------------------------------
    # Task 3 — Re-ingest dbt metadata into DataHub
    # -------------------------------------------------------------------------
    @task()
    def datahub_ingest_dbt() -> None:
        log.info("Running DataHub dbt ingestion recipe...")

        result = subprocess.run(
            [
                "datahub", "ingest",
                "-c", DATAHUB_RECIPE,
            ],
            capture_output=True,
            text=True,
            env={
                "PATH": "/usr/local/bin:/usr/bin:/bin",
                "HOME": "/home/airflow",
            },
        )
        log.info(result.stdout)

        if result.returncode != 0:
            log.error(result.stderr)
            raise RuntimeError(f"DataHub dbt ingestion failed:\n{result.stderr}")

        log.info("DataHub dbt ingestion completed successfully")

    # -------------------------------------------------------------------------
    # Task dependencies
    # -------------------------------------------------------------------------
    dbt_docs_generate() >> copy_artifacts() >> datahub_ingest_dbt()


datahub_refresh_dbt_metadata()
