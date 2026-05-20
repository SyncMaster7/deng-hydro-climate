FROM acryldata/datahub-actions:head

USER root
RUN pip install --no-cache-dir \
    "acryl-datahub[postgres]" \
    "acryl-datahub[dbt-core]" \
    "acryl-datahub[superset]" \
    "acryl-datahub-airflow-plugin"
USER datahub
