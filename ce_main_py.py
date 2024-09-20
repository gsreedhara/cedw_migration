# -- * **************************************************************************
# -- * File Name        : ce_etl_main.py
# -- *
# -- * Description      : Contains the main CE ETL logic
# -- *                    Fetches data from the XLS file that has proj estimates
# -- *                    Truncates and loads data from Excel file to CEDW - snowflake
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

import pandas as pd
import openpyxl
import boto3
import snowflake.connector as sc
import os
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.pandas_tools import pd_writer
import yaml as yaml

#loads the configuration and connection variables
with open('creds.yml', 'r') as f:
    data = yaml.safe_load(f)



def readExcelHiddenSheet(filename, sheetname):
    xls = pd.ExcelFile(filename)
    df = pd.read_excel(filename,sheetname)
    return df


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



def main():
    sf_conn = establish_sflake_cedw_con()
    sf_conn.cursor().execute("Truncate table PXLTD_CEDW_DEV.MAIN.estmt_transf")
    sf_conn.cursor().execute("Truncate table PXLTD_CEDW_DEV.MAIN.ext_woms")
    sf_conn.cursor().execute("Truncate table PXLTD_CEDW_DEV.MAIN.LD_ESTMT_LNDNG")


    __location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
    fullFilename=[]
    for file in os.listdir(__location__):
        if file.endswith(".xlsx"):
            
            fullFilename.append((os.path.join("__location__", file)).split('/')[-1])
            #print(fullFilename)
            if len(fullFilename)>0:
                for tablename in fullFilename:
                    df = readExcelHiddenSheet(tablename, 'Upload-ReadOnly')
                    df.rename(columns={"Employee ID": "EMPL_ID"}, inplace=True)
                    df.rename(columns={"Project ID": "PROJ_ID"}, inplace=True)
                    df.rename(columns={"Project Start Date": "PROJ_STRT_DT"}, inplace=True)
                    df.rename(columns={"Resource Name": "RESRC_NM"}, inplace=True)
                    df.rename(columns={"Role": "RL"}, inplace=True)
                    df.rename(columns={"Type": "TYP"}, inplace=True)
                    df.rename(columns={"Client": "CLNT_NM"}, inplace=True)
                    df.rename(columns={"Client ID": "CLNT_ID"}, inplace=True)
                    df.rename(columns={"Project Name": "PROJ_NM"}, inplace=True)
                    df.rename(columns={"TPS_Type": "TPS_TYP"}, inplace=True)
                    df.rename(columns={"Cost": "CST"}, inplace=True)
                    df.rename(columns={"Rate": "RT"}, inplace=True)
                    df.rename(columns={"Hours": "HRS"}, inplace=True)
                    df.rename(columns={"Resource Cost": "RESRC_CST"}, inplace=True)
                    df.rename(columns={"Resource Revenue": "RESRC_REV"}, inplace=True)
                    df["PROJ_STRT_DT"] = pd.to_datetime(df["PROJ_STRT_DT"])

                    sf_conn.cursor().execute("TRUNCATE TABLE LD_ESTMT_LNDNG")
                    sf_conn.cursor().execute("TRUNCATE TABLE LD_ESTMT_LNDNG_TEMP")
                    sf_conn.cursor().execute("TRUNCATE TABLE estmt_transf")
                    sf_conn.cursor().execute("TRUNCATE TABLE ext_woms")
                    write_pandas(conn=sf_conn,df=df,table_name='LD_ESTMT_LNDNG_TEMP',use_logical_type=True)
                    sf_conn.cursor().execute("insert into PXLTD_CEDW_DEV.main.LD_ESTMT_LNDNG select * from PXLTD_CEDW_DEV.main.LD_ESTMT_LNDNG_TEMP where EMPL_ID is not null;")
          
                    sf_conn.cursor().execute("call PXLTD_CEDW_DEV.MAIN.SP_SSIS_CE_UPLOAD()")
                    sf_conn.cursor().execute("call PXLTD_CEDW_DEV.MAIN.SP_UPDATE_POP_FROM_EST_LNDNG()")
                    os.rename(f'{__location__}/{tablename}', f'{__location__}/archive_CE/processed_{tablename}')



    # we will park this for later. 
    # after the file is read, it will be pushed to S3 archive. and removed in the current folder
    #os.remove()


if __name__ == '__main__':
    main()








