# =============================================================================
# deng-hydro-climate — dags/archive_raw_files.py
# Compresses raw JSON files older than 7 days into dated archive folders
# Fully independent — no asset dependencies on hydro_meteo_pipeline
#
# Schedule: 0 0 * * 0 — weekly, Sunday midnight UTC
# =============================================================================

import gzip
import logging
import os
import shutil
from datetime import date, datetime, timezone, timedelta

from pendulum import datetime as pendulum_datetime

from airflow.sdk import dag, task

log = logging.getLogger(__name__)

RAW_DIRS = {
    "hydro": "/data/raw/hydro",
    "meteo": "/data/raw/meteo",
}
ARCHIVE_BASE = "/data/archive"
MAX_AGE_DAYS = 7


def _parse_date_from_filename(filename: str) -> date | None:
    """
    Extract the date from a raw file name.

    Expected patterns:
      hydro_YYYY-MM-DD.json
      meteo_YYYY-MM-DD.json

    Returns a date object on success, None if the filename does not match.
    """
    # Strip .json suffix, then take the last token after the first underscore
    # e.g. "hydro_2026-05-01.json" → "2026-05-01"
    stem = filename.removesuffix(".json")          # "hydro_2026-05-01"
    parts = stem.split("_", maxsplit=1)            # ["hydro", "2026-05-01"]
    if len(parts) != 2:
        return None
    try:
        return date.fromisoformat(parts[1])        # date(2026, 5, 1)
    except ValueError:
        return None


@dag(
    dag_id="archive_raw_files",
    description="Compress raw JSON files older than 7 days into partitioned archive",
    schedule="0 0 * * 0",
    start_date=pendulum_datetime(2026, 5, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["archive", "maintenance"],
)
def archive_raw_files():

    @task()
    def archive_old_files() -> dict:
        cutoff = datetime.now(tz=timezone.utc).date() - timedelta(days=MAX_AGE_DAYS)
        results = {"archived": 0, "skipped": 0, "unrecognised": 0}

        for source, raw_dir in RAW_DIRS.items():
            if not os.path.exists(raw_dir):
                log.info("Raw dir does not exist, skipping: %s", raw_dir)
                continue

            for filename in os.listdir(raw_dir):
                if not filename.endswith(".json"):
                    continue

                file_date = _parse_date_from_filename(filename)

                if file_date is None:
                    log.warning("Cannot parse date from filename, skipping: %s", filename)
                    results["unrecognised"] += 1
                    continue

                if file_date >= cutoff:
                    log.info("Skipping (too recent): %s (file date %s, cutoff %s)", filename, file_date, cutoff)
                    results["skipped"] += 1
                    continue

                # Derive archive path from the file's own date, not today's date
                year  = file_date.strftime("%Y")
                month = file_date.strftime("%m")

                archive_dir = os.path.join(ARCHIVE_BASE, source, year, month)
                os.makedirs(archive_dir, exist_ok=True)

                file_path    = os.path.join(raw_dir, filename)
                archive_path = os.path.join(archive_dir, filename + ".gz")

                # Compress and archive
                with open(file_path, "rb") as f_in:
                    with gzip.open(archive_path, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)

                os.remove(file_path)
                log.info("Archived: %s → %s", file_path, archive_path)
                results["archived"] += 1

        log.info(
            "Archive complete — archived: %d, skipped: %d, unrecognised: %d",
            results["archived"], results["skipped"], results["unrecognised"],
        )
        return results


    archive_old_files()


archive_raw_files()
