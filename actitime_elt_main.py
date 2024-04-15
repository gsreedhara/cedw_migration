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

def establish_mysql_actitime_conn():
    """This will be later modularized. Once we know its working"""
    mq_conn = mc.MySQLConnection (
    host="34.193.31.187", #"34.193.31.187",
    user="testuser",
    password="mytesting123" ,
    database="actitime"
    )
    print("Returning Conn for MySQL")
    return mq_conn
    

def establish_sflake_cedw_con():
    """This will be modularized later"""
    sf_conn = sc.connect(
    user='gsreedhara',
    password='!Sb04Me0!0',
    account='nlxoxef-vp32965',
    role='DBADMIN',
    session_parameters={
        'QUERY_TAG': 'Actitime_ETL',
        }
    )
    print("Returning Conn for SFlake")
    return sf_conn

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
 
    ret_1 = sf_conn.cursor().execute("Truncate table PXLTD_CEDW.MAIN.Actitime_Customer")
    ret_2 = sf_conn.cursor().execute("Truncate table PXLTD_CEDW.MAIN.Actitime_Project")
    ret_3 = sf_conn.cursor().execute("Truncate table PXLTD_CEDW.MAIN.Actitime_task")
    ret_4 = sf_conn.cursor().execute("Truncate table PXLTD_CEDW.MAIN.Actitime_AT_USER")
    ret_5 = sf_conn.cursor().execute("Truncate table PXLTD_CEDW.MAIN.Actitime_TT_RECORD")
    ret_6 = sf_conn.cursor().execute("Truncate table PXLTD_CEDW.MAIN.Actitime_TT_REVISION")
    ret_7 = sf_conn.cursor().execute("Truncate table PXLTD_CEDW.MAIN.Actitime_USER_PROJECT")
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
        sf_crsr.execute("create or replace stage my_stage file_format = my_csv_format;")
        sf_crsr.execute(f"PUT file://~/Library/CloudStorage/OneDrive-ProjectXLtd/My Documents/pythonprojects/git repos/cedw_migration/{tablename}.csv @~/staged AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
        #sfq.execute("COPY INTO abc_table")
        sf_crsr.execute(f"copy into PXLTD_CEDW.MAIN.ACTITIME_{tablename} from @~/staged file_format = (format_name = 'my_csv_format') on_error = continue")
        
    print('Steps complete')
    sf_crsr.close()
    sf_conn.close()
    mysql_crsr.close()
    mysql_conn.close()

if __name__ == '__main__':
    main()