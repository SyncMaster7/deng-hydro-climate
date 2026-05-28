# DataHub Ingestion Recipes

Recipes for ingesting metadata from the deng-hydro-climate stack into DataHub.

## Run order

Always run in this order — each recipe builds on the previous:

| # | Recipe | What it ingests |
|---|---|---|
| 1 | `postgres_recipe.yml` | Tables, columns, schemas (bronze, silver, gold, ref) |
| 2 | `dbt_recipe.yml` | Model lineage, column descriptions, test results |
| 4 | `superset_recipe.yml` | Dashboard and chart lineage back to gold tables |

## How to run

Recipes are executed from inside the `datahub-actions` container, which has the CLI
installed and can reach GMS at `http://datahub-gms:8080`.

The `datahub/` directory is mounted into the container at `/datahub/`.

```bash
# PostgreSQL (run first — no dependencies)
docker compose exec datahub-actions datahub ingest -c /datahub/recipes/postgres_recipe.yml

# dbt (requires dbt artifacts — see note in recipe file)
docker compose exec datahub-actions datahub ingest -c /datahub/recipes/dbt_recipe.yml

# Airflow
docker compose exec datahub-actions datahub ingest -c /datahub/recipes/airflow_recipe.yml

# Superset (run last)
docker compose exec datahub-actions datahub ingest -c /datahub/recipes/superset_recipe.yml
```

## dbt artifacts

The dbt recipe requires pre-generated artifact files in `datahub/artifacts/`:
- `manifest.json`
- `catalog.json`
- `run_results.json`

Generate them on the server:
```bash
docker compose exec dbt dbt docs generate \
  --project-dir /dbt \
  --profiles-dir /root/.dbt \
  --target-path /tmp/dbt-target

# Then copy out of the container
docker cp deng-dbt:/tmp/dbt-target/manifest.json ./datahub/artifacts/
docker cp deng-dbt:/tmp/dbt-target/catalog.json ./datahub/artifacts/
docker cp deng-dbt:/tmp/dbt-target/run_results.json ./datahub/artifacts/
```

## Credentials

Recipes use environment variables from `.env`. The actions container inherits them
automatically via `docker compose exec`.
