FROM apache/airflow:2.8.0-python3.10

COPY --chmod=755 airflow/entrypoint.sh /opt/airflow/entrypoint.sh

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt


