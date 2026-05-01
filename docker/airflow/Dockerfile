FROM apache/airflow:3.2.1

# Install dbt-postgres and the Postgres provider for Airflow
RUN pip install --no-cache-dir \
    dbt-postgres==1.9.* \
    apache-airflow-providers-postgres
