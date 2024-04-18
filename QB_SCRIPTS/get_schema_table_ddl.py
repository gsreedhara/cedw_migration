# -- * **************************************************************************
# -- * File Name        : get_schema_table_ddl.py
# -- *
# -- * Description      :
# -- *                   Step 1: Read the Json files to get the database
# -- *                      connection parameters
# -- *
# -- *                   Step 2: Establish connections to the QuickBooks DB
# -- *
# -- *                   Step 3: Retrieve the ddl of a specific table
# -- *
# -- *			 Step 4: Log all differences
# -- *
# -- *			 Step 5: Close the QB DB Connections
# -- *
# -- * Purpose          : Extract column names and data types from the
# -- *                      QuickBooks source from a list of tables. I use this to
# -- *                      know what db changes to make to the target in conjunction
# -- *                      with the new tables as output of the compare program.
# -- *
# -- * Date Written     : Nov 2021
# -- * Input Parameters : N/A
# -- * Input Files      : ConnectionData.json
# -- * Output Parameters: N/A
# -- * Tables Read      : ALL QB tables
# -- * Tables Updated/Inserted   : N/A
# -- * Run Frequency    : As needed for QB upgrades
# -- * Developed By     : Steve Wilson - ProjectX
# -- *
# -- * Version   	Date   		Modified by     Description
# -- * v1.0	   	Nov 08, 2021	Steve Wilson	Initial Code created
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


def getColumnDDL(dbConn, table_name):
    """ 
    """
    column_rows = dbConn.getColumnNames(table_name)
    column_list = ''

    for row in column_rows:
        #print(row)
        
        column_type = row[1].upper()
        column_name = row[0]
        
        if column_name == 'PayrollInfoEarningsRate':
            continue

        ''' The same datatypes are represented by different naming conventions
            for example in QB the TIMESTAMP is equivalent to DATETIME 2
            in SQL Server '''
        
        if column_type == 'DATETIME2' or column_type == 'TIMESTAMP':
            column_type = 'DATETIME'
        elif column_type == 'DECIMAL':
            column_type = 'NUMERIC'
        elif column_type == 'INT':
            column_type = 'INTEGER'
        elif column_type == 'TINYINT':
            column_type = 'BIT'
        elif column_type == 'SQL_VARIANT':
            column_type = 'BLOB'
        elif column_type in ('CHAR', 'NCHAR', 'NVARCHAR'):
            column_type = 'VARCHAR'
        elif column_type == 'DOUBLE':
            column_type = 'FLOAT'

        if column_type in ('DATETIME', 'INTEGER', 'BIT', 'FLOAT', 'DATE'):
            column_list += (f',[{column_name}] {column_type}\n')
        elif column_type in ('NUMERIC'):
            column_list += (f',[{column_name}] {column_type}({row[3]},{row[4]})\n')
        elif column_type in ('VARCHAR'):
            column_list += (f',[{column_name}] {column_type}({row[2]})\n')
        else:
            column_list += (f'*** Unknown data type **** ,[{column_name}] {column_type}\n')

    return column_list  # Return the List





def main():
    try:
        process_start_time = time.time()

        log_file = 'DB_DDL_Log_' + \
                   datetime.today().strftime('%Y-%m-%d') + '.log'
        logging.basicConfig(filename=log_file, level=logging.INFO,
                            filemode='w', format='%(asctime)s %(message)s')
        logging.info(('*****************************************************' 
                     '***************************'))
        logging.info('Start of process')

        ''' Obtain the db login parameters. The file contents will be restricted
            as it contains passwords'''
        try:
            with open("C:\QB_SCRIPTS\ConnectionData.json") as f:
                DbConnectionData = json.load(f)
            logging.info('Retrieved connection data')
        except Exception as err:
            logging.info('Error reading connection data error - %s', str(err))
            raise ()
            
            
        ''' Obtain the file with the list of tables to get ddl for '''
        try:
            with open("C:\QB_SCRIPTS\ddl_table_names.txt") as ddl_f:
                all_tables = ddl_f.read().splitlines()
            logging.info('Retrieved connection data')
        except Exception as err:
            logging.info('Error reading connection data error - %s', str(err))
            raise ()


        ''' Create a connection to the QB Source database '''
        
        QB_src_dbConn = db.dbConn(DbConnectionData, 'QbSourceConnection')
        logging.info('Opened QB Source DB connection')
        
            
        ''' Retrieve all the tables from the Base (Source) Database for the
            QuickBooks Source '''
        '''
        src_QB_tables = QB_src_dbConn.getTableNames()
        logging.info('Number of QB Source Tables Retrieved -> {}'.format(
            len(src_QB_tables)))
        print('Number of QB Source Rows-> {}'.format(len(src_QB_tables)))
        '''

        for table_name in all_tables:
            print('<',table_name,'>')
            logging.info(f'*************** DDL for -> {table_name} *********')
            column_def = getColumnDDL(QB_src_dbConn, table_name)
            logging.info(f'{column_def}')              
            
            # Check if table name in list of src_tables
            '''if table_name in src_QB_tables:
                print('Table found')
                QB_src_column_names = standardiseColumnType(QB_src_dbConn,
                                                            table)
            else:
                print(f'Table NOT found in QB -> {table_name}')
                ##### example logging.info(f'Rows to insert -> {fmt_rows}')
            '''


        
        process_end_time = time.time()
        print('Total Elapsed Seconds -> ', process_end_time - process_start_time)
        logging.info('Total Execution elapsed time -> %0.4f',
                     process_end_time - process_start_time)

    except Exception as err:
        logging.info('Error in QB Compare process - %s', str(err))

        print('*** Process Failed ***')
        logging.info('Execution ended. Error code is %d', 1)


if __name__ == '__main__':
    main()
