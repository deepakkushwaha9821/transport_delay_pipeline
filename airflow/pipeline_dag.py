from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="transport_delay_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["transport"]
):

    upload = BashOperator(
        task_id="upload_raw_s3",
        bash_command="""
        source ~/transport_delay_pipeline/venv/bin/activate
        python ~/transport_delay_pipeline/ingestion/upload_to_s3.py
        """
    )

    spark_etl = BashOperator(
        task_id="spark_etl",
        bash_command="""
        source ~/transport_delay_pipeline/venv/bin/activate
        spark-submit --master local[*] ~/transport_delay_pipeline/etl_spark/spark_transform.py
        """
    )

    load_db = BashOperator(
        task_id="load_postgres",
        bash_command="""
        source ~/transport_delay_pipeline/venv/bin/activate
        python ~/transport_delay_pipeline/load/load_to_postgres.py
        """
    )

    upload >> spark_etl >> load_db
