# -- * **************************************************************************
# -- * File Name        : actitime_etl_main.py
# -- *
# -- * Description      : Contains the main Actitime ETL logic
# -- *                    Connects to MYSQL (actitime) and Snowflake (CEDW)
# -- *                    Truncates and loads data from Actitime to Snowflake
# -- *
# -- * Purpose          : Used to run the ETL logic for actitime
# -- * Date Written     : Apr 2024
# -- * Input Parameters : N/A
# -- * Input Files      : N/A
# -- * Output Parameters: N/A
# -- * Tables Read      : N/A - The functions support access to all tables
# -- * Tables Updated/Inserted   : N/A
# -- * Run Frequency    : Every 15-30 mins
# -- * Developed By     : Giridhar Sreedhara - ProjectX
# -- * Code Location    : https://github.com/TBD
# -- *
# -- * **************************************************************************
# -- * **************************************************************************
#packages to be imported
#pip install mysql-connector-python
#pip install snowflake-connector-python


#import above packages
import snowflake.connector as sc
import mysql.connector as mc
import csv
import yaml

with open('creds.yml', 'r') as f:
    data = yaml.safe_load(f)

def establish_mysql_actitime_conn():
    """This will be later modularized. Once we know its working"""
    mq_conn = mc.MySQLConnection (
    host = data.get('AT_HOST'), 
    user = data.get('AT_USER'),
    password = data.get('AT_PWD') ,
    database = data.get('AT_DB')
    )
    print("Returning Conn for MySQL")
    return mq_conn
    

def establish_sflake_cedw_con():
    """This will be modularized later"""
    sf_conn = sc.connect(
    user = data.get('SF_USER'),
    password = data.get('SF_password'),
    account = data.get('SF_account'),
    role = data.get('SF_role'),
    session_parameters={
        'QUERY_TAG': 'Actitime_ETL',
        }
    )


def main():
    """This will be modularized later"""
    mysql_conn = establish_mysql_actitime_conn()
    print("Established mysql conn")
    mysql_crsr = mysql_conn.cursor()
    print("Established mysql cursor")
    sf_conn = establish_sflake_cedw_con()
    print("Established sflake conn")
    sf_crsr = sf_conn.cursor()
    print("Established mysql crsr")

    #Step 1 - Truncate Snowflake target table
 
    ret_1 = sf_conn.cursor().execute("Truncate table PXLTD_CEDW_DEV.MAIN.Actitime_Customer")
    ret_2 = sf_conn.cursor().execute("Truncate table PXLTD_CEDW_DEV.MAIN.Actitime_Project")
    ret_3 = sf_conn.cursor().execute("Truncate table PXLTD_CEDW_DEV.MAIN.Actitime_task")
    ret_4 = sf_conn.cursor().execute("Truncate table PXLTD_CEDW_DEV.MAIN.Actitime_AT_USER")
    ret_5 = sf_conn.cursor().execute("Truncate table PXLTD_CEDW_DEV.MAIN.Actitime_TT_RECORD")
    ret_6 = sf_conn.cursor().execute("Truncate table PXLTD_CEDW_DEV.MAIN.Actitime_TT_REVISION")
    ret_7 = sf_conn.cursor().execute("Truncate table PXLTD_CEDW_DEV.MAIN.Actitime_USER_PROJECT")
    ret_8 = sf_conn.cursor().execute("Truncate table PXLTD_CEDW_DEV.MAIN.Actitime_task_Land")
    ret_9 = sf_conn.cursor().execute("Truncate table PXLTD_CEDW_DEV.MAIN.Actitime_AT_USER_land")
    print(f"the return of the truncate is : Actitime_task :{ret_1}, Actitime_Customer: {ret_2},Actitime_Project: {ret_3},  Actitime_Task: {ret_4}, Actitime_AT_USER: {ret_5},  ACTITIME_TT_RECORD: {ret_6}, ACTITIME_TT_REVISION: {ret_7} ")
    # write table data from MYSQL to SNOWFLAKE
    # Cycle through the tables list and load data
    table_list=['customer','project','task','at_user','tt_record','tt_revision','user_project']
    for tablename in table_list:
        mysql_crsr.execute(f"SELECT * FROM {tablename}")
        print(f"Got data from {tablename} table")
        with open(f"{tablename}.csv", "w", newline='') as csv_file: 
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow([i[0] for i in mysql_crsr.description]) # write headers
                csv_writer.writerows(mysql_crsr)
        #sf_crsr.execute("create or replace stage my_stage file_format = 'my_csv_format'")
        sf_crsr.execute(f"PUT file://{tablename}.csv @~/staged AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
        #sfq.execute("COPY INTO abc_table")
        sf_crsr.execute("use database PXLTD_CEDW_DEV")
        sf_crsr.execute("use SCHEMA MAIN")
        sf_crsr.execute("use WAREHOUSE COMPUTE_WH")
        if 'task' in tablename or 'at_user' in tablename:
             sf_crsr.execute(f"copy into PXLTD_CEDW_DEV.MAIN.ACTITIME_{tablename}_LAND from @~/staged/{tablename}.csv.gz file_format='my_csv_format'")
        else:
             sf_crsr.execute(f"copy into PXLTD_CEDW_DEV.MAIN.ACTITIME_{tablename} from @~/staged/{tablename}.csv.gz file_format='my_csv_format'")
    print("Inserting from LAND task table to main table")
    insString="INSERT INTO PXLTD_CEDW_DEV.MAIN.ACTITIME_TASK (ID,CUSTOMER_ID,PROJECT_ID,CREATE_TIMESTAMP,COMPLETION_DATE,NAME,NAME_LOWER,DESCRIPTION,DEADLINE_DATE,BILLING_TYPE_ID,BUDGET) SELECT ID,CUSTOMER_ID,PROJECT_ID,CREATE_TIMESTAMP,COMPLETION_DATE,NAME,NAME_LOWER,DESCRIPTION,DEADLINE_DATE,BILLING_TYPE_ID,BUDGET FROM PXLTD_CEDW_DEV.MAIN.ACTITIME_TASK_LAND"
    sf_conn.cursor().execute(insString)
    print("Inserting from LAND AT_USER table to main table")
    insString="INSERT INTO PXLTD_CEDW_DEV.MAIN.ACTITIME_AT_USER \
    (id,username,username_lower,md5_password,first_name,middle_name,last_name,email,is_enabled,overtime_tracking,overtime_level, \
    is_locked,hire_date,release_date,all_projects_assigned,user_group_id,is_enabled_ap,first_name_lower,last_name_lower,middle_name_lower,initial_schedule,initial_schedule_total,\
    is_default_initial_schedule,requires_ltr_approval,\
    is_pto,def_pto_rules,keep_all_tasks_from_previous_week,requires_tt_approval,manage_all_users_tt,\
    manage_all_users_ltr,is_sick,def_sick_rules,is_activated,time_zone_group_id,receive_user_changes_notification\
    )\
    SELECT \
    id,username,username_lower,md5_password,first_name,middle_name,last_name,email,is_enabled,overtime_tracking,overtime_level,\
    is_locked,hire_date,release_date,all_projects_assigned,user_group_id,is_enabled_ap,first_name_lower,last_name_lower,middle_name_lower,initial_schedule,initial_schedule_total,\
    is_default_initial_schedule,requires_ltr_approval,\
    is_pto,def_pto_rules,keep_all_tasks_from_previous_week,requires_tt_approval,manage_all_users_tt,\
    manage_all_users_ltr,is_sick,def_sick_rules,is_activated,time_zone_group_id,receive_user_changes_notification\
    FROM PXLTD_CEDW_DEV.MAIN.ACTITIME_AT_USER_LAND"
    sf_conn.cursor().execute(insString)
    print('Steps complete')
    sf_crsr.close()
    sf_conn.close()
    mysql_crsr.close()
    mysql_conn.close()

if __name__ == '__main__':
    main()
