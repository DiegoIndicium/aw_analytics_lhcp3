# Astro Runtime Dockerfile
FROM quay.io/astronomer/astro-runtime:9.1.0

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy packages and install system dependencies
COPY packages.txt .
RUN apt-get update && cat packages.txt | xargs apt-get install -y

# Copy dbt project
COPY include/dbt_project /usr/local/airflow/include/dbt_project

# Set environment variables
ENV DBT_PROFILES_DIR=/usr/local/airflow/include/dbt_project/profiles
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True

# Copy DAGs and plugins
COPY dags/ ${AIRFLOW_HOME}/dags/
COPY plugins/ ${AIRFLOW_HOME}/plugins/

# Create necessary directories
RUN mkdir -p /usr/local/airflow/include/dbt_project/profiles && \
    mkdir -p /usr/local/airflow/include/dbt_project/logs && \
    mkdir -p /usr/local/airflow/include/dbt_project/target

# Set proper permissions
RUN chown -R astro:astro /usr/local/airflow/include/dbt_project