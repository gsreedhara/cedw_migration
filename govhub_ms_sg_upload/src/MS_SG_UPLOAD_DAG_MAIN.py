# import libraries
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from pathlib import Path

# Default parameters for the workflow
default_args = {
    'depends_on_past': False,
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 20),
    'retries': 0
}

with DAG(
        'GOVHUB_MS_SG_UPLOAD', # Name of the DAG / workflow
        default_args=default_args,
        catchup=False,
        schedule='@once' # Every minute (You will need to change this!)
) as dag:
    # This is an initialization operator that does nothing. 
    start_task = EmptyOperator(
        task_id='start_task', # The name of the sub-task in the workflow.
    )

    # Calls THANX_POST_SLACK.py
    process_upload = BashOperator(
        task_id='UPLOAD_PROCESS',
        bash_command= 'sh /home/ubuntu/airflow/dags/govhub_ms_sg_upload/runscript_1st.sh'
    )

    start_task >> process_upload

