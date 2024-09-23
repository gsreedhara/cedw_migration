#-- CSV2Snow : Run on Macbook: read CSVs data into Snowflake Tables 
# Note: Snowflake tables will be overwriten (Truncated before loading data into it)

# Run Command Line:
# python CSV2Snow.py > CSV2Snow.log

#!pip install pandas
#!pip install snowflake-connector-python
#!pip install "snowflake-connector-python[pandas]"

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import numpy as np
import warnings
import sys
import traceback
import os
import numpy as np

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.setFormatter(formatter)

file_handler = logging.FileHandler('SNOW2CSV_20240922.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)


logger.addHandler(file_handler)
logger.addHandler(stdout_handler)

# This function Converts String into "Binary" (Used for converting Encrypted String "Byte" SQL Server data type to Snowflake "Binary" data)
def String2Binary(string_value):
    result = ''.join(format(ord(i), '08b') for i in string_value)
    return(result)

def ReadNullable_list(cur, tableName, db_d):
    cur.execute(f"SELECT COLUMN_NAME from {db_d}.TEST.NULLABLE_AND_IDENTITY  where table_name='{tableName}' and NULLABLE=0 and IDENTITY_COL=0")
    df = cur.fetch_pandas_all()
    return df

def ReadIdentity_list(cur, tableName, db_d):
    cur.execute(f"SELECT COLUMN_NAME from {db_d}.TEST.NULLABLE_AND_IDENTITY  where table_name='{tableName}' and IDENTITY_COL=1")
    df = cur.fetch_pandas_all()
    return df


# This procedure Refresh data from CSV file to Snowflake Table
def CSV_2_Snowflake_Table(csv_s, folder_s, tbl_d, db_d):
    nrows_d = 0
    nrows_s = 0
    output  = ''
    ex_message = ''
    
    con_destination = snowflake.connector.connect(user='srvc_usr_etl_batch_load', password='ProjectX123@', account='al57695.ca-central-1.aws',
        warehouse='COMPUTE_WH', database= db_d, schema='TEST', role='DBADMIN')
    cur= con_destination.cursor()
    logger.info("----------------------------------------------------------------------------------")
    logger.info("  Source Folder: ", folder_s, " - CSV File: ", csv_s, '\n')
    cursor = con_destination.cursor()
    #get identity and nullable columns
    #create an empty_list
    nullable_df = ReadNullable_list(cur, tbl_d, db_d)
    nullable_list = nullable_df['COLUMN_NAME'].tolist()
    if not nullable_list:
        logger.info("List is empty. Creating a dummy list")
        nullable_list=['empty']
    
    #empty_list
    identity_df = ReadIdentity_list(cur,tbl_d,db_d)
    identity_list = identity_df['COLUMN_NAME'].tolist()
    if not identity_list:
        logger.info("List is empty. Creating a dummy list")
        identity_df=['empty']
    

    try:
        # Fetching source CSV data into Dataframe
        warnings.filterwarnings('ignore')
        #SourceData= pd.read_csv('/Users/giridharsreedhara/Desktop/'+ folder_s + '/'+ csv_s)
        SourceData= pd.read_csv('/Users/giridharsreedhara/Desktop/'+ folder_s + '/'+ csv_s, na_filter=False)
        # Get number of records in Source CSV file
        nrows_s =len(SourceData)
        logger.info("  - CSV File Name     : ", csv_s, " - Records: ", nrows_s)
        
        # Change columns name in dataframe to uppercase - Fixing snowflake connector issue "invalid identifier from pandas dataframe"
        SourceData.columns = map(lambda x: str(x).upper(), SourceData.columns)

       # Drop columns that have all null values from the dataframev - Fixing snowflake load issue "Expression type does not match column data type"
        SourceData.drop(SourceData.columns[SourceData.isnull().all()], inplace=True, axis=1)

        # Replace NaN values with '' for object data types columns that has names without the sufix "DT", "_TS" or "_DATE" (ie. is not a DATE or TIMESTAMP) 
        # This means replace NaN values with '' for String columns - Fixing snow flake load issue "NULL result in a non-nullable column"
        str_columns = [col for col in SourceData.columns if col.endswith(('_DT', '_TS', '_DATE')) and SourceData[col].dtype == 'object']
        SourceData[str_columns] = SourceData[str_columns].fillna('2999-12-31')

        SourceData = SourceData.replace(r'^\s*$', np.nan, regex=True)
        #print(SourceData.head(10))
        #print(SourceData.columns.tolist())

        #get all column names
        col_list= list(SourceData.columns.values.tolist())
        last_col = col_list[-1]

        #fixing the issue where last column is NaN and of timestamp format. 
        if '_TS' in last_col:
                SourceData[last_col].replace(np.nan, r'2999-12-31', regex=True, inplace=True)
        
        for col in str_columns:
                SourceData[col].replace(np.nan, r'2999-12-31', regex=True, inplace=True)
                #SourceData[SourceData.iloc[:,-1:]] =  SourceData[SourceData.iloc[:,-1:]].fillna('')
        
        # Replace NaN values with '' for object data types columns that has names without the sufix "DT", "_TS" or "_DATE" (ie. is not a DATE or TIMESTAMP) 
        # This means replace NaN values with '' for String columns - Fixing snow flake load issue "NULL result in a non-nullable column"
        str_columns = [col for col in SourceData.columns if not col.endswith(('_DT', '_TS', '_DATE')) and SourceData[col].dtype == 'object']     
        #SourceData[str_columns] = SourceData[str_columns].fillna('')        
        #check if the columns fall under identity columns and drop column in dataframe
        for col in str_columns:
            if col in identity_list:
                SourceData = SourceData.drop(col, axis=1)
        #check if columns are in the non-nullable column list
        for col in str_columns:
            if col in nullable_list:
                SourceData
                SourceData.col = SourceData.col.fillna('')
        
        logger.info(SourceData.head(10))
        
        #print("Source data after replacing Blank with NaN")
        #print(SourceData)
        # Convert Binary Encrypted data types - Apply String2Binary function to convert each Encrypted String "Byte" column to Snowflake "Binary" data type
        # Example for this issue exit at table: EMPL_SCR_DTL in dataabse PXLTD_CEDW
        encrypted_columns = [col for col in SourceData.columns if col.endswith(('_ENCRPTD')) and SourceData[col].dtype == 'object']
        for column in encrypted_columns:
            SourceData[column] = SourceData[column].apply(String2Binary)
        logger.info(SourceData.head(20))
        # Write the table data from the DataFrame to Snowflake table (with overwrite or append mode)
        success, nchunks, nrows, output= write_pandas(con_destination, SourceData, tbl_d, database=db_d, schema='TEST', auto_create_table=False, overwrite=True)
        for col in str_columns:
            cur.execute(F"UPDATE {db_d}.TEST.{tbl_d} SET  {col}=NULL where {col}='2999-12-31'")
        # Get number of records in Snowflake table after refreshing it
        nrows_d = pd.read_sql_query(f'SELECT count(*) FROM {db_d}.TEST.'+tbl_d, con_destination).iloc[0][0]
        warnings.filterwarnings('default')
    except BaseException as ex:
        ex_type, ex_value, ex_traceback = sys.exc_info() 
        ex_message = str(ex_value).replace('002023 (22000): SQL compilation error:\n', '').replace('100072 (22000):', '')
    finally:
        logger.info("  - Staging output    : ", output)
        logger.info("  - Destination Table : ", tbl_d, " - Records: ", nrows_d)
        if nrows_s == nrows_d:
            msg_record_not_loaded = ""
        else :    
            logger.error("\n  - Data Load Error   : %s" %ex_message)
            msg_record_not_loaded = "( " + str(nrows_s - nrows_d) + " records not loaded )"
            logger.error(msg_record_not_loaded)
        if nrows_d <1 or nrows_s <0:
            logger.error("Nothing to Load")
        else:
            logger.error("\n  -** % Records Loaded: ",  round(nrows_d/nrows_s*100, 2), "%", msg_record_not_loaded)
            

        con_destination.close()
#-----------------------------------------
path = "/Users/giridharsreedhara/Desktop/PXLTD_CEDW"
files = os.listdir(path)
for file in files:
    #print(file)
    tablenm = file.split('.')[0]
    CSV_2_Snowflake_Table(f'{file}', 'PXLTD_CEDW', f'{tablenm}', 'PXLTD_CEDW_DEV')
