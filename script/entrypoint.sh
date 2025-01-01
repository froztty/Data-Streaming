#!/bin/bash
set -e

# Install requirements if the file exists
if [ -e "/opt/airflow/requirements.txt" ]; then
  $(command -v pip) install --user -r requirements.txt
fi

# Initialize Airflow DB and create user if necessary
if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

# Upgrade the Airflow DB
$(command -v airflow) db upgrade

# Start Airflow DB
exec airflow webserver