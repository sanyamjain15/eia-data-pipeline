# Dockerfile for dbt inside Airflow container
FROM apache/airflow:2.8.1

USER root

# Install system-level dependencies for dbt-postgres
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    git \
    && apt-get clean

# Switch back to airflow user
USER airflow

# Install dbt-postgres (or dbt-core + dbt-postgres)
RUN pip install --no-cache-dir dbt-postgres==1.7.9  # or latest compatible with Python 3.9 if needed
