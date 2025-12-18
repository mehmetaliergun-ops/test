from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

with DAG(
    dag_id="minio_to_postgres_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dataops", "spark", "ssh"]
) as dag:

    run_spark_job = SSHOperator(
        task_id="run_cleaning_job",
        ssh_conn_id="spark_ssh",
        command="""
        cd /dataops &&
        git pull origin main &&
        python spark_jobs/clean_transactions.py
        """
    )

    run_spark_job
