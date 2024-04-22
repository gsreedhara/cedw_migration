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
        'THANX_APP_REFRESH', # Name of the DAG / workflow
        default_args=default_args,
        catchup=False,
        schedule='@once' # Every minute (You will need to change this!)
) as dag:
    # This is an initialization operator that does nothing. 
    start_task = EmptyOperator(
        task_id='start_task', # The name of the sub-task in the workflow.
    )

    # Calls THANX_POST_SLACK.py
    slack_post = BashOperator(
        task_id='POST_SLACK',
        bash_command= 'python3 /usr/local/airflow/dags/THANX_SCRIPTS/THANX_POST_SLACK.py' # add command line parameters (omitted for security)
    )

    # Calls THANX_STG_SLACK_MSGS.py
    slack_stg = BashOperator(
        task_id='STG_SLACK',
        bash_command='python3 /usr/local/airflow/dags/THANX_SCRIPTS/THANX_STG_SLACK_MSGS.py' # add command line parameters (omitted for security)
    )



