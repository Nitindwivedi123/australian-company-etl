#!/bin/bash
set -e

echo "[INFO] Waiting for Postgres to be ready..."
sleep 5

airflow db upgrade

echo "[INFO] Ensuring Airflow admin user exists..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password airflow || true

echo "[INFO] Starting scheduler..."
airflow scheduler &

echo "[INFO] Starting webserver..."
exec airflow webserver
