FROM apache/airflow:3.2.1

# Install dbt-postgres, Postgres provider, and DataHub Airflow plugin
RUN pip install --no-cache-dir \
    dbt-postgres==1.9.* \
    apache-airflow-providers-postgres \
    acryl-datahub-airflow-plugin[airflow3]