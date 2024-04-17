# -- * **************************************************************************
# -- * File Name        : populate_selection_table_cedw.py
# -- *
# -- * Description      :
# -- *                   Step 1: Read the Json files to get the database
# -- *                      connection parameters
# -- *
# -- *                   Step 2: Establish connections to the
# -- *                      QuickBooks source and Control DB
# -- *
# -- *                   Step 3: Retrieve a list of the valid tables and their
# -- *                      respective column names
# -- *
# -- *			 Step 4: Insert all the column names as a string into
# -- *                      the control table Third_Party_Source_Etl_Execution
# -- *
# -- *			 Step 5: Close all the DB Connections
# -- *
# -- * Purpose          : Extract all the Table names and columns name from the
# -- *                      QuickBooks source to be used as a select string
# -- *                      for both the source and target DB's. This will
# -- *                      remove discrepancy issues when QB is updated.
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
import dbUtil as db

''' The overall program is to retrieve all the table and column names from
    the Quickbook source, format to be used in SQL statements and store the
    results in the Control database (table Third_Party_Source_Etl_Execution).
    The results of this process will be used by the qb_extract program to
    extract from QB and load into a SQL server DB for reporting purposes. '''


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
            with open("C:\QB_SCRIPTS\ConnectionDataTest.json") as f:
                DbConnectionData = json.load(f)
            logging.info('Retrieved connection data')
        except Exception as err:
            logging.info(('Error reading connection data error - {}'.format(
                str(err))))
            raise ()

        ''' Create a connection to the QB Target database'''
        ctl_dbConn = db.dbConn(DbConnectionData, 'ControlConnection')
        logging.info('Opened QB Target DB connection')

        ''' Create a connection to the QB Source database '''
        QB_src_dbConn = db.dbConn(DbConnectionData, 'QbSourceConnection')
        logging.info('Opened QB Source DB connection')

        ''' Retrieve all the tables from the Base (Source) Database for the
                    QuickBooks Source '''
        src_QB_tables = QB_src_dbConn.getTableNames()
        logging.info('Number of QB Source Tables Retrieved -> {}'.format(
            len(src_QB_tables)))
        print('Number of QB Source Rows-> {}'.format(len(src_QB_tables)))

        ''' Before inserting rows remove all the previous rows '''
        deleteStmt = (("DELETE FROM {}.{}.[Third_Party_Source_Etl_Execution]"
                        "WHERE Source_Db_Nm = '{}'".format(
                            ctl_dbConn.getDbName(),
                            ctl_dbConn.getDbOwner(),
                            'QB_CEDW')))

        delete_cnt = ctl_dbConn.executeSqlStmt(deleteStmt)
        logging.info('Number of rows Deleted for QB_CEDW-> %d', delete_cnt)

        ''' Obtain all the column names from the QB Source tables foreach table '''
        for table_nm in src_QB_tables:
            column_txt = ''  # This will contain the formatted list of columns
            QB_src_column_names = QB_src_dbConn.getColumnNames(table_nm)

            for column_name_and_type in QB_src_column_names:
                column_name = column_name_and_type[0]
                ''' Ignore private data that should not be extraxcted '''
                if column_name in ('PayrollInfoEarningsRate', 'SSN', 'SIN',
                                   'NiNumber', 'BirthDate',
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
                else:  # prefix every column with a comma except the 1st
                    if len(column_txt) > 0:
                        column_txt += ','
                    column_txt += '"' + column_name + '"'

            #    print(table_nm)
            active_ind = 'Y'  # Default all rows to active in the DB
            ''' The table "ItemAssembliesCanBuild" is not used and causes an
                Internal QB error when selecting from the table, rather than
                removing it just make it inactive '''
            if table_nm == 'ItemAssembliesCanBuild':
                active_ind = 'N'

            # Format the insert statement
            insertStmt = ("INSERT INTO [dbo].[Third_Party_Source_Etl_Execution]"
                         "([Source_Db_Nm],[Source_Table_Nm],"
                         "[Source_Column_Names],[Target_Table_Nm],"
                          "[Extract_Process_Active_Ind],[Extract_Processed_Ind]) "
                          "VALUES ('QB_CEDW','{}','{}','{}','{}','N');"
                          .format(table_nm,
                                  column_txt,
                                  table_nm,
                                  active_ind))

            ctl_dbConn.insertRow(insertStmt)


        process_end_time = time.time()
        # print('Total Elapsed Seconds -> ', process_end_time - process_start_time)
        logging.info('Total Execution elapsed time -> %0.4f',
                     process_end_time - process_start_time)

    except Exception as err:
        logging.info('Error in QB Compare process - %s', str(err))

        print('*** Process Failed ***')
        logging.info('Execution ended. Error code is %d', 1)


if __name__ == '__main__':
    main()
