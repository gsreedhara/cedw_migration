from airflow import DAG
from datetime import datetime
import io
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator

import io
import snowflake.connector as sc
import os
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.pandas_tools import pd_writer
import yaml as yaml
import pandas as pd

#loads the configuration and connection variables
with open('/home/ubuntu/airflow/dags/ACTITIME/creds.yml', 'r') as f:
#with open('creds.yml', 'r') as f:
    data = yaml.safe_load(f)

def establish_sflake_cedw_con():
    """This will be modularized later"""
    sf_conn = sc.connect(
    user = data.get('SF_USER'),
    password = data.get('SF_password'),
    account = data.get('SF_account'),
    database = data.get('SF_CE_database'),
    schema = data.get('SF_schema'),
    warehouse = data.get('SF_warehouse'),
    role = data.get('SF_role')
    )
    print("Returning Conn for SFlake")
    return sf_conn



#def test_func_something():
#   print("All good")
#    return 0

#default_args={"owner":"airflow","email_on_failure":True,"email":["gsreedhara@pxltd.ca"]}
#with DAG(dag_id="test_email", start_date=datetime(2024,8,7),schedule_interval="@once",default_args=default_args) as dag:
#    test_func=PythonOperator(task_id="test_func",python_callable=test_func_something)
#    test_email=EmailOperator(task_id="test_email", to="gsreedhara@pxltd.ca",subject="SomethingHappened",html_content="""<h1>Test Email from Meself </h1>""")#
#
#    test_func >> test_email
default_args={"owner":"airflow","email_on_failure":True,"email":["gsreedhara@pxltd.ca"]}
with DAG(dag_id='absence_trigger', start_date=datetime(2024, 2, 16),schedule_interval="@once",default_args=default_args ) as dag:
    sf_conn = establish_sflake_cedw_con()
    cur = sf_conn.cursor()
    T0 = None
    try:
        cur.execute("select RECIPIENTS, BODY, SUBJECT FROM EMPL_LEAVE_SEND_EMAIL WHERE processed_ind='N'")
    #for (RECIPIENTS, BODY, SUBJECT) in cur:
    #    print(f"Starts here REC: {RECIPIENTS}\n,BODY: {BODY}\n, SUBJ: {SUBJECT} \n\n\n")
        df = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
        if df.empty:
            df = pd.DataFrame({'RECIPIENTS':['gsreedhara@pxltd.ca'],
                          'BODY':['Nothing to process for Absence'],
                          'SUBJECT':['NOTHIGN TO PROCESS IN ABSENCE']})
        for index, row in df.iterrows():
            #print(row['RECIPIENTS'], row['BODY'], row['SUBJECT'])
            T1 = EmailOperator(
                task_id=f"email_task_{index}", to=f"{row['RECIPIENTS']}", subject=f"{row['SUBJECT']}", html_content=f"{row['BODY']}")
            if T0 is not None:
                T0 >> T1
            T0 = T1
        T2 = EmptyOperator(
                task_id='end_task'
            )

    finally:
        cur.execute("Update EMPL_LEAVE_SEND_EMAIL set Processed_ind='Y' where processed_ind='N'")
        cur.close()
    T1 >> T2
