# -- * **************************************************************************
# -- * File Name        : schema_compare_cedw.py
# -- *
# -- * Description      :
# -- *                   Step 1: Read the Json files to get the database
# -- *                      connection parameters
# -- *
# -- *                   Step 2: Establish connections to the
# -- *                      QuickBooks source and CEDW QuickBooks target DB
# -- *
# -- *                   Step 3: Retrieve a list of the valid tables and their
# -- *                      respective column names and data types for comparison
# -- *
# -- *			 Step 4: Log all differences
# -- *
# -- *			 Step 5: Close all the DB Connections
# -- *
# -- * Purpose          : Extract all the Table names and columns from the
# -- *                      QuickBooks source compare its content the CEDW
# -- *                      target database for discrepancies
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
# -- * v2.1             Jan 6, 2020     Steve Wilson    Added new DB Class functionality
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


def standardiseColumnType(dbConn, table_name):
    """ The call to get Column Names returns a list which includes the
        column name, datatype and precision of the datatype. This function
        standardizes on the different datatype definitions from different
        databases
    """
    column_rows = dbConn.getColumnNames(table_name)
    column_list = []

    for row in column_rows:
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

        column_list.append([column_name, column_type, row[2]])

    return column_list  # Return the List


def compare_column_names(table_name, src_column_names, tgt_column_names):
    """ Compares 2 lists of columns for the specified table. Each list
            contains column names, datatypes and the size of the datatype

        Return: A formatted string of missing columns, columns with mismatched
            datatypes and if the datatype size is smaller in the target table
            then the source column
    """
    diff_column_str = ''

    ''' Foreach entry in the source columns lookup the value in the target
        table '''
    for src_name, src_type, src_size in src_column_names:
        column_found_ind = False

        for tgt_name, tgt_type, tgt_size in tgt_column_names:
            if src_name == tgt_name:
                column_found_ind = True

                # ignore data type differences if the target is a larger type
                if src_type != tgt_type:
                    if src_type == 'BIT' and tgt_type in ('SMALLINT',
                                                          'INTEGER'):
                        pass
                    elif src_type in ('DATETIME', 'DATE') and tgt_type == 'VARCHAR':
                        pass
                    elif src_type == 'INTEGER' and tgt_type == 'BIGINT':
                        pass
                    else:
                        print('Data Type Mismatch ', src_name, src_type, tgt_type)
                        diff_column_str += ("\nTable '{}' src column '{}' {} - {} "
                                            "- DATA TYPE MISMATCH".format(table_name,
                                                                          src_name, src_type, tgt_type))
                elif int(src_size) > int(tgt_size) and src_type not in (
                        'BIGINT', 'FLOAT', 'DATE', 'INTEGER', 'SMALLINT'):
                    print('Data Size Mismatch ', src_name, src_type, src_size, tgt_size)
                    diff_column_str += ("\nTable '{}' column '{}' "
                                        "datatype {} {}-{}"
                                        " - DATA SIZE MISMATCH".format(table_name,
                                                                       src_name, src_type, src_size, tgt_size))
                else:
                    continue

        if column_found_ind == False:
            print('Column Not Found ', src_name)
            if src_type not in (
                        'BIGINT', 'FLOAT', 'DATE', 'INTEGER', 'SMALLINT'):
                diff_column_str += ("\nTable '{}' column '{}' datatype {} {} -"
                                " NOT FOUND".format(table_name, src_name,
                                                    src_type, src_size))
            else:
                diff_column_str += ("\nTable '{}' column '{}' datatype {} -"
                                " NOT FOUND".format(table_name, src_name,
                                                    src_type))

    return diff_column_str


def List_Diff(li1, li2):
    """ Python code to get difference of two lists. Using set() """
    return (list(set(li1) - set(li2)))


def main():
    try:
        process_start_time = time.time()

        log_file = 'DB_Compare_Log_' + \
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


        ''' Create a connection to the QB Source database '''
        QB_src_dbConn = db.dbConn(DbConnectionData, 'QbSourceConnection')
        logging.info('Opened QB Source DB connection')

        ''' Create a connection to the QB Target database'''
        QB_tgt_dbConn = db.dbConn(DbConnectionData, 'QbTargetConnection')
        logging.info('Opened QB Target DB connection')

        ''' Retrieve all the tables from the Base (Source) Database for the
            QuickBooks Source '''
        src_QB_tables = QB_src_dbConn.getTableNames()
        logging.info('Number of QB Source Tables Retrieved -> {}'.format(
            len(src_QB_tables)))
        print('Number of QB Source Rows-> {}'.format(len(src_QB_tables)))

        ''' Retrieve all the tables from the Compare (Target) Database for
            the QB table Owner '''
        tgt_QB_tables = QB_tgt_dbConn.getTableNames()
        logging.info('Number of QB Target Tables Retrieved -> {}'.format(
            len(tgt_QB_tables)))
        print('Number of QB Target Rows-> {}'.format(len(tgt_QB_tables)))


        ''' Compare the Source list to the Target List for QB'''
        diff_QB_list = List_Diff(src_QB_tables, tgt_QB_tables)
        logging.warning(('\n**************************************************'
                         '**********\nTables found in the QuickBook Source '
                         'and not in the Target -> {}'.format(diff_QB_list)))


        logging.info(('***QB COLUMNS **************************************'
                      '***************************'))

        ''' Compare The columns in the QB Source tables to the QB Target Tables '''
        QB_columns_found = False
        for table in src_QB_tables:
            #    print(table)
            if diff_QB_list.count(table) == 0:
                # get the source column details
                QB_src_column_names = standardiseColumnType(QB_src_dbConn,
                                                            table)
                # get the target column details
                QB_tgt_column_names = standardiseColumnType(QB_tgt_dbConn,
                                                            table)

                diff_column_str = compare_column_names(table,
                                                       QB_src_column_names,
                                                       QB_tgt_column_names)
                if len(diff_column_str) > 0:
                    logging.warning(diff_column_str)
                    QB_columns_found = True

        if QB_columns_found == False:
            logging.warning('%%%% No Column Issues Found %%%%')

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
