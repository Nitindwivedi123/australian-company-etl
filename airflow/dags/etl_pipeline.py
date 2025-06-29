# airflow/dags/australian_company_etl.py

import sys
from datetime import datetime

# make sure your project root (with scripts/) is on the Python path
sys.path.append("/opt/project")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# These imports will only run when the tasks actually execute,
# not at DAG-parsing time.
from scripts.extract_commoncrawl import run as extract_cc
from scripts.extract_abr        import run as extract_abr
from scripts.entity_matching    import run as match_entities
from scripts.run_quality_checks import run as quality_checks

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 28),
    "retries": 1,
}

with DAG(
    dag_id="australian_company_etl",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # 1️ Extract Common Crawl
    t1 = PythonOperator(
        task_id="extract_commoncrawl",
        python_callable=extract_cc,
    )

    # 2️ Extract ABR
    t2 = PythonOperator(
        task_id="extract_abr",
        python_callable=extract_abr,
    )

    # 3 Entity matching (RapidFuzz + LLM fallback)
    t3 = PythonOperator(
        task_id="entity_matching",
        python_callable=match_entities,
    )

    # 4️ Run quality checks
    t4 = PythonOperator(
    task_id="run_quality_checks",
    python_callable=quality_checks,
)

    # define the pipeline order
    [t1, t2] >> t3 >> t4
