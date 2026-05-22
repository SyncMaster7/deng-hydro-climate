FROM apache/airflow:3.2.1

# Install dbt-postgres, Postgres provider, and DataHub Airflow plugin
# click-spinner pre-installed to avoid broken source build on Python 3.13
RUN pip install --no-cache-dir \
    click-spinner==0.1.10 \
    dbt-postgres==1.9.* \
    apache-airflow-providers-postgres \
    acryl-datahub-airflow-plugin[airflow3]