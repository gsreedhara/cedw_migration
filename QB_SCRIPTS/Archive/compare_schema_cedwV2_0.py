# -- * **************************************************************************
# -- * File Name        : compare_schema_cedw.py
# -- *
# -- * Description      :    
# -- *                   Step 1: Read the Json files to get the database
# -- *                      connection paramaters 
# -- *
# -- *                   Step 2: Establish conections to the 
# -- *                      QuickBooks source and CEDW QuickBooks target DB
# -- *
# -- *                   Step 3: Retrieve a list of the valid tables and their
# -- *                      respctive column names and datatypes for comparrison
# -- *
# -- *			 Step 4: Log all differences
# -- *
# -- *			 Step 5: Close all the DB Conections		
# -- *
# -- * Purpose          : Extract all the Table names and columns from the
# -- *                      QuickBooks source compare its content the CEDW
# -- *                      target database for descrepencies
# -- *
# -- * Date Written     : Dec 2019
# -- * Input Parameters : N/A
# -- * Input Files      : ConnectionData.json
# -- * Output Parameters: N/A
# -- * Tables Read      : ALL QB tables
# -- * Tables Updated/Inserted   : N/A
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
    if dbType == 'QB':              # Specifc to QuickBooks 
        selectStmt = "sp_tables"  
    elif dbType == 'MYSQL':         # Specific to MySql
        selectStmt = "show tables"
    else:                           # All others, but only tested for SQL Server
        selectStmt = "sp_tables @table_owner = '" + owner + "'"
        
    table_rows = db.getAllValues(dbConn, selectStmt)

    ''' Iterate through the row list and extract only the table names '''
    for row in table_rows:
        if dbType == 'MYSQL':       # MySQL only returns the name of the table
            table_list.append(row[0])
        else:                       # QB returns the table and other attributes
            table_list.append(row[2])   # table name is in the 3rd position

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
    column_datatype_list = []
    
    if dbType == 'QB':
        selectStmt = "sp_columns " + table      # QB selection syntax
    elif dbType == 'MYSQL':
        selectStmt = "SHOW columns FROM " + table   # MySql syntax
    else:
        selectStmt = "sp_columns @table_name = '" + table + "'" # SQl Server

    ''' For the QB execution the owner may not exist, depends on the version'''
    if len(owner) > 0 and dbType != 'QB':
        selectStmt += ", @table_owner= '" + owner + "'"

    column_rows = db.getAllValues(dbConn, selectStmt)
    #    print(column_rows[0])

    ''' Iterate through the row list and extract only the Column names.
        For comparison set all values to uppercase '''
    for row in column_rows:
        if dbType == 'MYSQL':
            parts = row[1].split('(')
            column_type = parts[0].upper()
            column_name = row[0]
        else:
            column_type = row[5].upper()
            column_name = row[3]

        ''' The same datatypes are represented by different naming conventions
            for example in QB the TIMESTAMP is equivelant to DATETIME 2
            in SQL Server '''
        if column_type == 'DATETIME' or column_type == 'TIMESTAMP':
            column_type = 'DATETIME2'
        elif column_type == 'DECIMAL':
            column_type = 'NUMERIC'
        elif column_type == 'INT':
            column_type = 'INTEGER'
        elif column_type == 'TINYINT':
            column_type = 'BIT'
        elif column_type == 'SQL_VARIANT':
            column_type = 'BLOB'
        elif column_type == 'CHAR' or column_type == 'NCHAR' or column_type == 'NVARCHAR':
            column_type = 'VARCHAR'
        elif column_type == 'DOUBLE':
            column_type = 'FLOAT'

        column_list.append(column_name)
        column_datatype_list.append(column_name + '-' + column_type)

    return column_list, column_datatype_list  # Return the List


def List_Diff(li1, li2):
    """ Python code to get difference of two lists. Using set() """
    return (list(set(li1) - set(li2)))


def main():
    try:
        process_start_time = time.time()

        log_file = 'C:\QB_SCRIPTS\ETL_Logs\DB_Compare_Log_' + \
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

        # Define QB Source and QB Target Database  
        QB_con_data = db.DbConData(DbConnectionData, "QbSourceConnection")    
        tgt_con_data = db.DbConData(DbConnectionData, "QbTargetConnection")
                

        ''' Create a connection to the QB Source database '''
        QB_src_dbConn = db.DBConnection_QB(QB_con_data.DSN)
        logging.info('Opened QB Source DB connection')

        ''' Create a connection to the Target database'''
        tgt_dbConn = db.DBConnection(tgt_con_data.db_server_name,
                                     tgt_con_data.db_name,
                                     tgt_con_data.db_user,
                                     tgt_con_data.db_pwd,
                                     tgt_con_data.db_driver,
                                     tgt_con_data.db_port)
        logging.info('Opened Target DB connection')


        ''' Retrieve all the tables from the Base (Source) Database for the
            QuickBooks Source '''
        src_QB_tables = getTableNames(QB_src_dbConn,
                                      QB_con_data.db_owner,
                                      QB_con_data.db_type)
        logging.info('Number of QB Source Tables Retrieved -> %d',
                     len(src_QB_tables))
        print('Number of QB Source Rows-> ', len(src_QB_tables))


        ''' Retrieve all the tables from the Compare (Target) Database for the QB table Owner '''
        tgt_QB_tables = getTableNames(tgt_dbConn,
                                      tgt_con_data.db_owner,
                                      tgt_con_data.db_type)
        logging.info('Number of QB Target Tables Retrieved -> %d',
                     len(tgt_QB_tables))
        print('Number of QB Target Rows-> ', len(tgt_QB_tables))


        ''' Compare the Source list to the Target List for QB'''
        diff_QB_list = List_Diff(src_QB_tables, tgt_QB_tables)
        logging.warning('**********\nTables found in the QuickBook Source ' \
                        'and not in the Target -> %s', diff_QB_list)


        ''' Compare The columns in the QB Source tables to the QB Target Tables '''
        for table in src_QB_tables:
            #    print(table)
            if diff_QB_list.count(table) == 0:
                QB_src_column_names, QB_src_column_desc = getColumnNames(
                                                QB_src_dbConn,
                                                QB_con_data.db_owner,
                                                table,
                                                QB_con_data.db_type)
                QB_tgt_column_names, QB_tgt_column_desc = getColumnNames(
                                                tgt_dbConn,
                                                tgt_con_data.db_owner,
                                                table,
                                                tgt_con_data.db_type)

                ''' First lets compare the columns and determine which columns
                    are in the source but not in the target '''
                diff_QB_Column_list = List_Diff(QB_src_column_names,
                                                QB_tgt_column_names)
                if len(diff_QB_Column_list) > 0:
                    logging.warning('Columns found in the table <%s> and not ' \
                            'in the target -> %s',table, diff_QB_Column_list)

                ''' Now compare the columns and datatypes to determine which columns and data
                    types are in the source but not in the target '''
                diff_QB_Column_desc = List_Diff(QB_src_column_desc,
                                                QB_tgt_column_desc)
                if len(diff_QB_Column_desc) > 0:
                    logging.warning('Columns data type in table <%s> different' \
                            'in the target -> %s',table, diff_QB_Column_desc)
                    print(table, diff_QB_Column_desc)


        QB_src_dbConn.close()  # Close the QB Source database connection
        logging.info('Closed QB Source DB connection')
        tgt_dbConn.close()  # Close the Target database connection
        logging.info('Closed Target DB connection')

        process_end_time = time.time()
        print('Total Elapsed Seconds -> ',
              process_end_time - process_start_time)
        logging.info('Total Execution elapsed time -> %0.4f',
                     process_end_time - process_start_time)
        logging.info('Execution ended. Error code is %d',0)

    except Exception as err:
        logging.info('Error in QB Compare process - %s', str(err))
            
        print('*** Process Failed ***')
        logging.info('Execution ended. Error code is %d',1)

if __name__ == '__main__':
    main()
