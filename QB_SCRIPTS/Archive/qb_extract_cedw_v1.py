import db_functions as db
import time
import logging
from datetime import datetime

''' The overall program will retrieve all the table and column data that
    was created and formated by the process "populate_selection_table" and
    stored in the table "Third_Party_Source_Etl_Execution".
    The process will extract data from the QuickBooks source and insert
    it into a SQL Server reporting database '''


def prep_QB_columns(src_column_names):
    ''' For use in the data selection from QB the column statements must
        have a specific format. All doulble quotes are removed and then
        will be added back for the column name Desc
    '''
    fmt_column_names = src_column_names.replace('"','')
    fmt_column_names = fmt_column_names.replace(',Desc,',',"Desc",')
    if fmt_column_names.count(',Desc') != 0 and \
            fmt_column_names.count(',Description') == 0:
        fmt_column_names = fmt_column_names.replace(',Desc',',"Desc"')
        
    return(fmt_column_names)


def replace_dt_columns(column_data):
    ''' This function will format all datetime data types which are returned
        from the select statement. It modifies the python datetime type to
        a string format accepted by SQL Server
    '''
    new_list = []
    for column in column_data:
        if isinstance(column, datetime):      
            date_str_fmt = '{:%Y-%m-%d %H:%M:%S}'.format(column)
            new_list.append(date_str_fmt)
        elif isinstance(column, bool):
            new_list.append(column)
        else:
            new_list.append(column)
            
    return new_list

process_start_time = time.time()
fetch_row_size = 3000   # Data will be processed in batches to manage memory

''' Define the Automation and Control Database'''
ctl_database_nm = 'QB_STAGING'
ctl_database_owner = 'dbo'
ctl_server_nm = 'localhost'  
ctl_db_user = 'ETL_User'
ctl_db_pwd = 'ETL_User@pxltd'
ctl_db_driver = 'SQL Server Native Client 11.0'

''' Define the QB Source Database '''
QB_src_database_nm = ''
QB_src_database_owner = ''
QB_src_server_nm = '' 
QB_src_db_user = ''
QB_src_db_pwd = ''
QB_src_db_driver = ''
QB_DSN = 'DSN=QuickBooks Data'
QB_src_db_type = 'QB'

''' Define the Target Database '''
tgt_database_nm = 'QB_STAGING'
tgt_database_owner = 'dbo'
tgt_server_nm = 'localhost'  
tgt_db_user = 'F_ACME_ETL'  #   'ETL_User'
tgt_db_pwd = 'Hv@2RmbrSmHw' #   'ETL_User@pxltd'
tgt_db_driver = 'SQL Server Native Client 11.0'

#  Setup and configure the Log file
log_file = 'C:\QB_SCRIPTS\ETL_Logs\ETL_Log_' + datetime.today().strftime('%Y-%m-%d') + '.log'
logging.basicConfig(filename=log_file, level=logging.INFO, filemode='w', format='%(asctime)s %(message)s')
logging.info('Start of process')

''' Create a connection to the Control database'''
ctl_dbConn = db.DBConnection(ctl_server_nm, ctl_database_nm, ctl_db_user, ctl_db_pwd, ctl_db_driver)
logging.info('Opened Control DB connection')

''' Create a connection to the Source database '''
src_dbConn = db.DBConnection_QB(QB_DSN)
logging.info('Opened Source DB connection')

''' Create a connection to the Target database'''
tgt_dbConn = db.DBConnection(tgt_server_nm, tgt_database_nm, tgt_db_user, tgt_db_pwd, tgt_db_driver)
logging.info('Opened Target DB connection')

''' Retrieve all the valid tables from the QB ETL execution table '''
selectStmt = "SELECT Source_Table_Nm, Source_Column_Names FROM dbo." \
             "[Third_Party_Source_Etl_Execution]" \
             " WHERE process_ind = 'Y' AND Source_Db_Nm = 'QB_CEDW';"
valid_table_rows = db.selectRows(ctl_dbConn, selectStmt)
num_of_table_rows = len(valid_table_rows)

logging.info('Number of Tables to Process -> %d', num_of_table_rows)
#print('Number of Rows-> ', num_of_table_rows)

tables_processed = 0

''' Read through the tables to be extracted from '''
for source_table in valid_table_rows:
    extract_start_time = time.time()
    tables_processed += 1
    table_name = source_table[0]    
    column_names = source_table[1]
    sq_column_names = prep_QB_columns(source_table[1]) # format the columns

#    print('Table name -> ', table_name)
    logging.info('Processing Table -> %s', table_name)

    ''' This table is a special case, it contains a salary column which is
        required to be encrypted. The column 'xx' is added to the target table
        and will contain the raw (unencrypted value). It will be set to NULL
        just after the data is loaded. The existing list of columns is modified
        by adding xx to the insert and rate to the Select
    '''
    if table_name == 'EmployeeEarning':
        column_names += ',temp_rate'   # PayrollInfoEarningsRate_ENCRPTD
        sq_column_names += ',PayrollInfoEarningsRate'        
         
    ''' Select and process the table. Create the Select Statement from the
        extract table '''
    selectStmt = "SELECT " + sq_column_names + " FROM " + table_name
#    print('Select -> ', selectStmt)
    cur = src_dbConn.cursor()
    cur.execute(selectStmt)

    ''' Before inserting rows remove all the previous rows, truncate table'''
    full_table_nm = tgt_database_nm + "." + tgt_database_owner + ".[" + \
                     table_name + ']'
    db.truncateTable(tgt_dbConn, full_table_nm)
    logging.info('Table Truncated -> %s', full_table_nm)

    ''' Build the Insert statement, use the columns in the table, determine
        the number of columns by counting the number of commas in the
        column_names column and adding one more. Use these as place holders
        in the form a "?", then use the execute many statement to insert
        all the rows for all values from the list '''
    insertStmt = "INSERT INTO " + tgt_database_nm + "." + tgt_database_owner + ".[" + table_name + \
                 "] (" + column_names + ") VALUES ("

    ''' find the number of columns in the names There will be one less comma
        so add another at the end'''
    for i in range(column_names.count(',')):
        insertStmt += '?,'

    insertStmt += '?)'  # Add the final placeholder

    # Set the indicator to control a batch fetch for large tables        
    data_available = True
    
    ''' Set up a while loop to manage large source tables. This will set up a
        cursor to fetch a predetermined number of rows.'''
    while data_available == True:
        # This select returns partial query results to manage large tables
        batch_values = cur.fetchmany(fetch_row_size)
        num_of_src_rows = len(batch_values)

        ''' Check for the number of rows returned, if 0 or less than the fetch
            size set the data available indicator to exit the while loop '''
        if num_of_src_rows < fetch_row_size or num_of_src_rows == 0:
            data_available = False

        ''' If there are 0 rows then don't process or try to insert '''
        if num_of_src_rows > 0:
            fmt_rows = []
            for batch_row in batch_values:
                fmt_row = replace_dt_columns(batch_row) # Format the datetimes

                ''' Create a formatted row list for all row inserts for bulk
                    loading except the EmployeeEarnings table, this needs to
                    be managed with single inserts to avoid i=an internal
                    pyodbc binding error
                '''
                fmt_rows.append(fmt_row)
                ''' The following statement will insert the row one at a time '''
                if table_name == 'EmployeeEarning':
                    db.insertRowWithValues(tgt_dbConn, insertStmt, fmt_row)

            logging.info('Number of rows Selected -> %d', num_of_src_rows)
            # This manages the bulk load
            if table_name != 'EmployeeEarning':
                db.insertMultipleRows(tgt_dbConn, insertStmt, fmt_rows)
                logging.info('Multiple Insert Executed for -> %s', table_name)

    ''' The following statement executes a SP which updates the encrypted
        column with the content of a raw data column 'xx' and then sets the
        raw column to NULL. This is for only the EmployeeEarning table
    '''
    if table_name == 'EmployeeEarning':
        db.executeSqlStmt(tgt_dbConn,
                          "exec [PXLTD_CEDW].[dbo].SP_ENCRYPT_QB_SAL")
        
    extract_end_time = time.time()
#    print('ETL Execution elapsed time -> ', extract_end_time - extract_start_time)
    logging.info('ETL Execution elapsed time -> %0.4f', extract_end_time - extract_start_time)
    cur.close()

ctl_dbConn.close()  # Close the Control database connection
logging.info('Closed Control DB connection')
src_dbConn.close()  # Close the Source database connection
logging.info('Closed Source DB connection')
tgt_dbConn.close()  # Close the Target database connection
logging.info('Closed Target DB connection')

process_end_time = time.time()
print('Total Elapsed Seconds -> ', process_end_time - process_start_time)
logging.info('Total Execution elapsed time -> %0.4f', process_end_time - process_start_time)

