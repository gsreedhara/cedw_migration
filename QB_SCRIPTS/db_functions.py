# -- * **************************************************************************
# -- * File Name        : db_functions.py
# -- *
# -- * Description      : Contains database specific functions which open
# -- *                    connections and manange datbbase functionality.		
# -- *                    There is no main programs, this only contains functions
# -- *
# -- * Purpose          : Used to allow reuse of database functionality
# -- * Date Written     : Dec 2019
# -- * Input Parameters : N/A
# -- * Input Files      : N/A
# -- * Output Parameters: N/A
# -- * Tables Read      : N/A - The functions support access to all tables   
# -- * Tables Updated/Inserted   : N/A
# -- * Run Frequency    : N/A  - These functions are called from other programs
# -- * Developed By     : Steve Wilson - ProjectX
# -- * Code Location    : https://github.com/PXLabs/QuickBooks_ETL
# -- *   
# -- * Version   	Date   		Modified by     Description
# -- * v1.0	   	Dec 12, 2019	Steve Wilson	Initial Code created
# -- * v2.0	   	Dec 12, 2019	Steve Wilson	Added Connection class
# -- *
# -- * **************************************************************************
# -- * **************************************************************************
# Packages Required
#
# -- * **************************************************************************
# Libraries Required
#
import pyodbc
import logging

class DbConData:
    """ Database Connection Class definition, retrieved from a Json file. """
    def __init__(self, jsonData, connection_nm):
        self.db_name = jsonData[connection_nm]["db_name"]
        self.db_owner = jsonData[connection_nm]["db_owner"]
        self.db_server_name = jsonData[connection_nm]["db_server_nm"]
        self.db_user = jsonData[connection_nm]["db_user"]
        self.db_pwd = jsonData[connection_nm]["db_pwd"]
        self.db_driver = jsonData[connection_nm]["db_driver"]
        self.db_type = jsonData[connection_nm]["db_type"]
        self.db_port = jsonData[connection_nm]["db_port"]
        self.DSN = jsonData[connection_nm]["DSN"]

    def __str__(self):
        display_str = 'Server Name -> ' + self.db_server_name + '    DB Name -> ' + self.db_name \
                      + '   Owner -> ' + self.db_owner + '\n' + 'User -> ' + self.db_user \
                      + '    Pwd -> ' + self.db_pwd + '    Port -> ' + self.db_port +  '\n' \
                      + 'DB Type -> ' + self.db_type + '    Driver -> ' + self.db_driver + '\n' \
                      + 'DSN -> ' + self.DSN
        return(display_str)


def DBConnection(serverName, DB, userName, pwd, dbDriver, port):
    """ Creates a Database Connection

    keyword Arguments:
    :param serverName: IP address of the Database Server
    :param DB: The default Database Name
    :param userName: The user name for the database access
    :param pwd: The password for the the associated database user
    :param dbDriver: The Database driver to be used by the connection
    :param port: The port used to access the database server

    :return: A Database connection object
    """
    try:
        db = pyodbc.connect(f"server={serverName};"
                            f"database={DB};"
                            f"uid={userName};"
                            f"pwd={pwd};"
                            f"port={port};"
                            f"driver={dbDriver}")
        return db
    except Exception as err:
        logging.error("DB Connection Failure %s", str(err))
        raise()

def DBConnection_QB(dsn):
    try:
        db = pyodbc.connect(dsn)
        return db
    except Exception as err:
        logging.error("QuickBooks DB Connection Failure %s", str(err))
        raise()

'''
def DBConnection_MySql(serverName, DB, userName, pwd):
    try:
        #        db = pymysql.connect(f"host='{serverName}',user='{userName}',passwd='{pwd}',db='{DB}'")

        db = pymysql.connect(host='localhost', user='mysqlread', passwd='read2018', db='teeonpos')

        return db
    except Exception as err:
        print("Unable to Establish DB Connection -> ", str(err))
        exit()
'''

def getCount(dbConn, sql_stmt):
    """
    Use the SQL execute statement to perform a Count

    :param dbConn: The DB connection established with the DbConnection method
    :param sql_stmt: The SQL statement to be executed, must be a single result
    :return: The value of the count SQL.
    """

    try:
        cur = dbConn.cursor()
        cur.execute(sql_stmt)
        count_value = cur.fetchone()[0]  # This select only returns a count value
        return count_value
    except Exception as err:
        logging.error("Unable to Execute count -> for %s \n  ERROR %s",
                      sql_stmt, str(err))
        raise()


def executeSqlStmt(dbConn, sql_stmt):
    """
    Execute a SQL Statement where no result is required. This could be a Delete, Update, Insert,
    truncate or execution of a stored procedure

    :param dbConn: The DB connection established with the DbConnection method
    :param sql_stmt: The SQL statement to be executed, must be a single result
    :return: The rowcount value, this may only be valid for delete, update or insert
    """
    try:
        cur = dbConn.cursor()
        cur.execute(sql_stmt)
        cur.commit()
        return cur.rowcount
    except pyodbc.Error as err:
        logging.error("Unable to Execute SQL -> for %s   \nERROR %s",
                      sql_stmt, str(err))
        raise()

def truncateTable(dbConn, table_nm):
    """
    Execute a SQL Truncate Statement.

    :param dbConn: The DB connection established with the DbConnection method
    :param sql_stmt: The SQL Truncate statement to be executed
    :return: The rowcount value, this may only be valid for delete, update or insert
    """
    sql_stmt = 'TRUNCATE TABLE ' + table_nm
    return executeSqlStmt(dbConn, sql_stmt)

def insertRow(dbConn, sql_stmt):
    """
    Execute an Insert statement

    :param dbConn: The DB connection established with the DbConnection method
    :param sql_stmt: The SQL statement to be executed, must be a single result
    :return: The rowcount value.
    """
    return executeSqlStmt(dbConn, sql_stmt)


def getAllValues(dbConn, sql_stmt):
    """
    Execute a Select SQL Statement and fetch all rows. This would be used where the expected row
    count would NOT be large (less than 25,000 rows) otherwise there will be memory issues

    :param dbConn: The DB connection established with the DbConnection method
    :param sql_stmt: The SQL select statement to be executed
    :return: A list of rows
    """
    try:
        cur = dbConn.cursor()
        cur.execute(sql_stmt)
        all_values = cur.fetchall()  # This select returns all the query results
        return all_values
    except pyodbc.Error as err:
        logging.error("Unable to Execute Multiple Value SQL -> for %s   " \
                      "\nERROR %s", sql_stmt, str(err))
        raise()

def insertMultipleRows(dbConn, sql_stmt, row_list):
    """
    Execute a SQL Bulk Insert Statement for a list of rows. This will be used for very quick inserts
    for multiple rows. It will take advantage of placeholders in the insert statement which will match
    the passed list

    :param dbConn: The DB connection established with the DbConnection method
    :param sql_stmt: The SQL statement to be executed, must be a single result
    :param row_list: The list of rows to be inserted.
    :return: The rowcount value
    """
    try:
        cur = dbConn.cursor()
        cur.fast_executemany = True     # This is the process for SQL Server
        cur.executemany(sql_stmt, row_list)
        cur.commit()
        return
    except pyodbc.Error as err:
        logging.error("Unable to Insert Multiple Rows SQL -> for %s   " \
                      "\nERROR %s", sql_stmt, str(err))
        raise()

def insertRowWithValues(dbConn, sql_stmt, row_values):
    """
    Execute a single row Insert Statement for a list of values.
    
    :param dbConn: The DB connection established with the DbConnection method
    :param sql_stmt: The SQL statement to be executed, must be a single result
    :param row_list: The list of rows to be inserted.
    :return: The rowcount value
    """
    try:
        cur = dbConn.cursor()
        cur.execute(sql_stmt, row_values)
        cur.commit()
        cur.close()
        return
    except pyodbc.Error as err:
        logging.error("Unable to Execute Single row SQL -> for %s   " \
                      "\nERROR %s", sql_stmt, str(err))
        raise()
