# -- * **************************************************************************
# -- * File Name        : populate_selection_table_cedw.py
# -- *
# -- * Description      :    
# -- *                   Step 1: Read the Json files to get the database
# -- *                      connection paramaters 
# -- *
# -- *                   Step 2: Establish conections to the 
# -- *                      QuickBooks source and Control DB
# -- *
# -- *                   Step 3: Retrieve a list of the valid tables and their
# -- *                      respctive column names 
# -- *
# -- *			 Step 4: Insert all the column names as a string into
# -- *                      the control table Third_Party_Source_Etl_Execution
# -- *
# -- *			 Step 5: Close all the DB Conections		
# -- *
# -- * Purpose          : Extract all the Table names and columns name from the
# -- *                      QuickBooks source to be used as a select string
# -- *                      for both the source and target DB's. This will
# -- *                      remove decrepency issues when QB is updated.
# -- *
# -- * Date Written     : Dec 2019
# -- * Input Parameters : N/A
# -- * Input Files      : ConnectionData.json
# -- * Output Parameters: N/A
# -- * Tables Read      : ALL QB tables
# -- * Tables Updated/Inserted   : Third_Party_Source_Etl_Execution
# -- * Run Frequency    : As needed for QB upgrades
# -- * Developed By     : Steve Wilson - ProjectX
# -- * Code Location    : https://github.com/PXLabs/QuickBooks_ETL
# -- *   
# -- * Version   	Date   		Modified by     Description
# -- * v1.0	   	Dec 12, 2019	Steve Wilson	Initial Code created
# -- * v2.0	   	Dec 19, 2019	Steve Wilson	Added connection security
# -- * **************************************************************************
# -- * **************************************************************************
# Packages Required
#
# -- * **************************************************************************
# Libraries Required
#
import time
import logging
import json
from datetime import datetime
import db_functions as db

''' The overall program is to retrieve all the table and column names from
    the Quickbook source, format to be used in SQL statements and store the
    results in the Control database (table Third_Party_Source_Etl_Execution).
    The results of this process will be used by the qb_extract program to
    extract from QB and load into a SQL server DB for reporting purposes. '''

def getTableNames(dbConn, owner, dbType):
    """
        This function is used to return a list of tables for a specifc database owner. 
        It is passed a DB connection and the owner.

        :param dbConn: The databbase connection to use
        :param owner: The owner of the Database to retrieve the tables from
        :param dbType: The type od database to extract from, (differnect syntax
                is required depending on the database.
        :return : Returns a list of tables
    """
    table_list = []
    if dbType == 'QB':  # QuickBooks
        selectStmt = "sp_tables"  # @table_owner = '" + owner + "'"
    elif dbType == 'MYSQL':
        selectStmt = "show tables"
    else:   # This will cover SQL Server
        selectStmt = "sp_tables @table_owner = '" + owner + "'"
    table_rows = db.getAllValues(dbConn, selectStmt)

    ''' Iterate through the list of table definition and extract only the
        table names '''
    for row in table_rows:
        if dbType == 'MYSQL':
            table_list.append(row[0])   # First column
        else:
            table_list.append(row[2])   # Third column

    return table_list


def getColumnNames(dbConn, owner, table, dbType):
    """
        This function is used to return a list of Columns for a specifc table.
        The function also conforms the datatypes across different databases.

        :param dbConn: The databbase connection to use
        :param owner: The owner of the Database to retrieve the tables from
        :param table: The table name to select columns for.
        :param dbType: The type od database to extract from, (differnect syntax
                    is required depending on the database.
        :return: A list of columns
    """
    column_list = []

    if dbType == 'QB':      # QuickBooks
        selectStmt = "sp_columns " + table
    elif dbType == 'MYSQL':
        selectStmt = "SHOW columns FROM " + table
    else:                   # SQL Server
        selectStmt = "sp_columns @table_name = '" + table + "'"

    if len(owner) > 0 and dbType != 'QB':
        selectStmt += ", @table_owner= '" + owner + "'"

    column_rows = db.getAllValues(dbConn, selectStmt)

    ''' Iterate through the list of column descriptions and extract only
        the Column names '''
    for row in column_rows:
        if dbType == 'MYSQL':
            column_name = row[0]
        else:
            column_name = row[3]

        column_list.append(column_name)
    return column_list 


def main():
    try:
        process_start_time = time.time()
        
        # Setup and configure the Log file
        log_file = 'C:\QB_SCRIPTS\ETL_Logs\DB_Table_Selection_Log_' + \
                   datetime.today().strftime('%Y-%m-%d') + '.log'
        logging.basicConfig(filename=log_file, level=logging.INFO,
                            filemode='w', format='%(asctime)s %(message)s')
        logging.info('*****************************************************' \
                        '***************************')
        logging.info('Start of process')

        '''Obtain the db login parameters. The file contents will be restricted
                    as it contains passwords
        '''
        try:
            with open("C:\QB_SCRIPTS\ConnectionData.json") as f:
                DbConnectionData = json.load(f)
            logging.info('Retrieved connection data')
        except Exception as err:
            logging.info('Error reading connection data error - %s', str(err))
            raise()

        # Define Control and QB Source Database
        ctl_con_data = db.DbConData(DbConnectionData, "ControlConnection")
        QB_con_data = db.DbConData(DbConnectionData, "QbSourceConnection")      

        ''' Create a connection to the Control database'''
        ctl_dbConn = db.DBConnection(ctl_con_data.db_server_name,
                                     ctl_con_data.db_name,
                                     ctl_con_data.db_user,
                                     ctl_con_data.db_pwd,
                                     ctl_con_data.db_driver,
                                     ctl_con_data.db_port)                

        ''' Create a connection to the QB Source database '''
        QB_src_dbConn = db.DBConnection_QB(QB_con_data.DSN)
        logging.info('Opened QB Source DB connection')


        ''' Retrieve all the tables from the QuickBooks Source '''
        src_QB_tables = getTableNames(QB_src_dbConn,
                                      QB_con_data.db_owner,
                                      QB_con_data.db_type)
        logging.info('Number of QB Source Tables Retrieved -> %d',
                     len(src_QB_tables))
        print('Number of QB Source Rows-> ', len(src_QB_tables))


        ''' Before inserting rows remove all the previous rows '''
        deleteStmt = "DELETE FROM " + ctl_con_data.db_name + "." + \
                     ctl_con_data.db_owner + ".[Third_Party_Source_Etl_Execution]" \
                     "WHERE Source_Db_Nm = 'QB_CEDW'"
        delete_cnt = db.executeSqlStmt(ctl_dbConn, deleteStmt)
        logging.info('Number of rows Deleted for QB_CEDW-> %d', delete_cnt)

        ''' Obtain all the column names from the QB Source tables foreach table '''
        for table_nm in src_QB_tables:
            column_txt = ''   # This will contain the formatted list of columns
            QB_src_column_names = getColumnNames(QB_src_dbConn,
                                                 QB_con_data.db_owner,
                                                 table_nm,
                                                 QB_con_data.db_type)

            for column_name in QB_src_column_names:
                ''' Ignore private data that should not be extraxcted '''
                if column_name in ('PayrollInfoEarningsRate','SSN','SIN',
                                   'NiNumber','BirthDate',
                                   'AdditionalNotesRetNote'):
                    logging.info('Found column <%s> in table <%s>', column_name,
                                 table_nm)
                # There are a number of tables with the note columns, only the
                #    employee related notes are considered personel information '''
                elif table_nm in ('Employee', 'EmployeeAddtionalNote', \
                                  'EmployeeEarning') and column_name in \
                                  ('Notes'):
                    logging.info('Found column <%s> in table <%s>',
                                 column_name, table_nm)
                else:       # prefix every column with a comma except the 1st
                    if len(column_txt) > 0:
                        column_txt += ','
                    column_txt += '"' + column_name + '"'
                    
        #    print(table_nm)
            active_ind = 'Y'    # Default all rows to active in the DB
            ''' The table "ItemAssembliesCanBuild" is not used and causes an
                Internal QB error when selecting from the table, rather than
                removing it just make it inactive '''
            if table_nm == 'ItemAssembliesCanBuild':    
                active_ind = 'N'

            # Format the insert statement
            insertStmt = "INSERT INTO [dbo].[Third_Party_Source_Etl_Execution]" \
                         "([Source_Db_Nm],[Source_Table_Nm]," \
                         "[Source_Column_Names],[Target_Table_Nm]," \
                         "[Process_Ind]) VALUES ('QB_CEDW','" + table_nm \
                         + "','" + column_txt + "','" \
                         + table_nm + "','" + active_ind + "');"

            db.insertRow(ctl_dbConn, insertStmt)


        ctl_dbConn.close()  # Close the Control database connection
        logging.info('Closed Control DB connection')
        QB_src_dbConn.close()  # Close the QB Source database connection
        logging.info('Closed QB Source DB connection')

        process_end_time = time.time()
        #print('Total Elapsed Seconds -> ', process_end_time - process_start_time)
        logging.info('Total Execution elapsed time -> %0.4f',
                     process_end_time - process_start_time)

    except Exception as err:
        logging.info('Error in QB Compare process - %s', str(err))
            
        print('*** Process Failed ***')
        logging.info('Execution ended. Error code is %d',1)

if __name__ == '__main__':
    main()
