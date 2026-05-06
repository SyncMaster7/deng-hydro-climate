# =============================================================================
# deng-hydro-climate — dags/archive_raw_files.py
# Compresses raw JSON files older than 7 days into dated archive folders
# Fully independent — no asset dependencies on hydro_meteo_pipeline
#
# Schedule: daily (easily changed — main reason for a separate DAG)
# =============================================================================

import gzip
import logging
import os
import shutil
from datetime import datetime, timezone, timedelta

from pendulum import datetime as pendulum_datetime

from airflow.sdk import dag, task

log = logging.getLogger(__name__)

RAW_DIRS = {
    "hydro": "/data/raw/hydro",
    "meteo": "/data/raw/meteo",
}
ARCHIVE_BASE = "/data/archive"
MAX_AGE_DAYS = 7


@dag(
    dag_id="archive_raw_files",
    description="Compress raw JSON files older than 7 days into partitioned archive",
    schedule="@daily",
    start_date=pendulum_datetime(2026, 5, 1, tz="UTC"),
    catchup=False,
    tags=["archive", "maintenance"],
)
def archive_raw_files():

    @task()
    def archive_old_files() -> dict:
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=MAX_AGE_DAYS)
        results = {"archived": 0, "skipped": 0}

        for source, raw_dir in RAW_DIRS.items():
            if not os.path.exists(raw_dir):
                log.info("Raw dir does not exist, skipping: %s", raw_dir)
                continue

            for filename in os.listdir(raw_dir):
                if not filename.endswith(".json"):
                    continue

                file_path = os.path.join(raw_dir, filename)
                file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path), tz=timezone.utc)

                if file_mtime >= cutoff:
                    log.info("Skipping (too recent): %s", filename)
                    results["skipped"] += 1
                    continue

                # Parse year/month from file modification time for archive path
                year  = file_mtime.strftime("%Y")
                month = file_mtime.strftime("%m")

                archive_dir = os.path.join(ARCHIVE_BASE, source, year, month)
                os.makedirs(archive_dir, exist_ok=True)

                archive_path = os.path.join(archive_dir, filename + ".gz")

                # Compress and archive
                with open(file_path, "rb") as f_in:
                    with gzip.open(archive_path, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)

                os.remove(file_path)
                log.info("Archived: %s → %s", file_path, archive_path)
                results["archived"] += 1

        log.info("Archive complete — archived: %d, skipped: %d", results["archived"], results["skipped"])
        return results


    archive_old_files()


archive_raw_files()
