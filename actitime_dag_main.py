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
    'start_date': datetime(2022, 11, 1),
    'retries': 0
}

with DAG(
        'ACTITIME_FULL_REFRESH', # Name of the DAG / workflow
        default_args=default_args,
        catchup=False,
        schedule='@once' # Every minute (You will need to change this!)
) as dag:
    # This operator does nothing. 
    start_task = EmptyOperator(
        task_id='start_task', # The name of the sub-task in the workflow.
        dag=dag # When using the "with Dag(...)" syntax you could leave this out
    )

    # With the PythonOperator you can run a python function.
    actitime_task_main = BashOperator(
        task_id='actitime_main_task',
        bash_command='python3 /usr/local/airflow/dags/actitime_etl_main.py'
    )

    # Define the order in which the tasks are supposed to run
    # You can also define paralell tasks by using an array 
    # I.e. task1 >> [task2a, task2b] >> task3
    start_task >> actitime_task_main 