# -- * **************************************************************************
# -- * File Name        : dbUtil.py
# -- *
# -- * Description      : Contains database specific Classes and functions which open
# -- *                    connections and manage database functionality.
# -- *                    There is no main programs, this only contains functionality
# -- *
# -- * Purpose          : Used to manage database connections and functionality
# -- * Date Written     : Dec 2019
# -- * Input Parameters : N/A
# -- * Input Files      : N/A
# -- * Output Parameters: N/A
# -- * Tables Read      : N/A - The functions support access to all tables
# -- * Tables Updated/Inserted   : N/A
# -- * Run Frequency    : N/A  - These Classes/functions are used by other programs
# -- * Developed By     : Steve Wilson - ProjectX
# -- * Code Location    : https://github.com/PXLabs/QuickBooks_ETL
# -- *
# -- * Version   	Date   		Modified by     Description
# -- * v1.0	   	Dec 28, 2019	Steve Wilson	Initial Code created
# -- *
# -- * **************************************************************************
# -- * **************************************************************************
# Packages Required
#
# -- * **************************************************************************
# Libraries Required
#
import pyodbc
#import pymysql
import logging
import snowflake.connector
import pandas as pd


class DbConData:
    """ Database Connection Class definition, assign the values retrieved from a
        Json file. """

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
                      + '    Pwd -> ' + self.db_pwd + '    Port -> ' + self.db_port + '\n' \
                      + 'DB Type -> ' + self.db_type + '    Driver -> ' + self.db_driver + '\n' \
                      + 'DSN -> ' + self.DSN
        return (display_str)


class dbConn:
    """
        This class is used to create a Database connection using the contents of a JSON file
        and the name of the connection required. It will also open and provide all database
        functionality for the requested connection
    """

    def __init__(self, jsonData, connection_nm):
        """
            The initialization method reads the conents of the passed JSON file and the
            requested connection name, retrieves the data and establishes a database
            connection depending on the database type

        :param jsonData: Connection data from a JSON file
        :param connection_nm: The connection name to use from the JSON file
        """
        self.connection_nm = connection_nm
        self.db_connection_data = DbConData(jsonData, connection_nm)
        self.fetch_cur = None
        self._open_connection = False

        if self.getDbType() == 'MS_SQL':
            ''' Create a MS SQL connection to the database'''
            self.dbConn = self.DBConnection()
            self._open_connection = True
        elif self.getDbType() == 'QB':
            ''' Create a QuickBooks connection to the database'''
            self.dbConn = self.DBConnection_QB()
            self._open_connection = True
        elif self.getDbType() == 'MYSQL':
            ''' Create a MySQl connection to the database'''
            self.dbConn = self.DBConnection_MySql()
            self._open_connection = True
        elif self.getDbType() == 'SNOW':
            ''' Create a Snowflake connection to the database'''
            self.dbConn = self.DBConnection_Snowflake()
        else:
            logging.error("DB Connection Value is invalid {}".format(self.getDbType()))
            raise ()

        logging.info('Opened DB Connection "{}"'.format(self.connection_nm))

    def __del__(self):
        self.closeConnection()

    def closeConnection(self):
        """ Close the database connection ** cleanup ** """
        logging.info('Closing DB Connection "{}"'.format(self.connection_nm))
        if self._open_connection == True:
            self._open_connection = False
            self.dbConn.close()

    def getDbServerName(self):
        return self.db_connection_data.db_server_name

    def getDbName(self):
        return self.db_connection_data.db_name

    def getDbUserName(self):
        return self.db_connection_data.db_user

    def getDbUserPwd(self):
        return self.db_connection_data.db_pwd

    def getDbDriver(self):
        return self.db_connection_data.db_driver

    def getDbPort(self):
        return self.db_connection_data.db_port

    def getDbType(self):
        return self.db_connection_data.db_type

    def getDbOwner(self):
        return self.db_connection_data.db_owner

    def getDbDSN(self):
        return self.db_connection_data.DSN

    def DBConnection(self):
        """ Creates a MS SQL Database Connection object based on the contents of the JSON file

        :return: A Database connection object
        """
        try:
            db = pyodbc.connect(f"server={self.getDbServerName()};"
                                f"database={self.getDbName()};"
                                f"uid={self.getDbUserName()};"
                                f"pwd={self.getDbUserPwd()};"
                                f"port={self.getDbPort()};"
                                f"driver={self.getDbDriver()}")
            return db
        except Exception as err:
            logging.error("DB Connection Failure for {} - {}".format(
                self.connection_nm, str(err)))
            raise ()

    def DBConnection_QB(self):
        """
        Creates a QuickBooks connection object to the underlying database using pyodbc

        :return: A Database connection object
        """
        try:  # dsn: Data source name to connect to QuickBooks
            db = pyodbc.connect(self.getDbDSN())
            return db
        except Exception as err:
            logging.error("DB Connection Failure for {} - {}".format(
                self.connection_nm, str(err)))
            raise ()

    def DBConnection_MySql(self):
        """
        Creates a MySQL connection object to the underlying database using pymyodbc

        :return: A Database connection object
        """
        try:
            #db = pymysql.connect(host='localhost', user='mysqlread', passwd='read2018', db='teeonpos')

            return 0
        except Exception as err:
            logging.error("DB Connection Failure for {} - {}".format(
                self.connection_nm, str(err)))
            raise ()

    def DBConnection_Snowflake(self):
        """
        Creates a Snowflake connection

        :return: A Database connection object
        """
        try: # dsn: account number to connect to Snowflake
            db = snowflake.connector.connect(
                    user=self.getDbUserName(),
                    password=self.getDbUserPwd(),
                    account=self.getDbDSN(),
                    warehouse=self.getDbServerName(),
                    role='dbdev')
            return db
        except Exception as err:
            logging.error("Snowflake DB Connection Failure {}".format(str(err)))
        raise ()

    def getCount(self, sql_stmt):
        """
        Use the SQL execute statement to perform a Count

        :param sql_stmt: The SQL statement to be executed, must be a single result
        :return: The value of the count SQL.
        """
        try:
            cur = self.dbConn.cursor()
            cur.execute(sql_stmt)
            count_value = cur.fetchone()[0]  # This select only returns a count value
            return count_value
        except Exception as err:
            logging.error("Unable to Execute count -> for {}\n  ERROR {}".format(
                sql_stmt, str(err)))
            raise ()

    def executeSqlStmt(self, sql_stmt):
        """
        Execute a SQL Statement where no result is required. This could be a Delete, Update, Insert,
        truncate or execution of a stored procedure

        :param sql_stmt: The SQL statement to be executed, must be a single result
        :return: The rowcount value, this may only be valid for delete, update or insert
        """
        try:
            cur = self.dbConn.cursor()
            cur.execute(sql_stmt)
            cur.execute("commit")
            return cur.rowcount
        except pyodbc.Error as err:
            logging.error("Unable to Execute SQL -> for {}\nERROR {}".format(
                sql_stmt, str(err)))
            raise ()

    def truncateTable(self, table_nm):
        """
        Execute a SQL Truncate Statement.

        :param sql_stmt: The SQL Truncate statement to be executed
        :return: The rowcount value, this may only be valid for delete, update or insert
        """
        sql_stmt = 'TRUNCATE TABLE ' + table_nm
        return self.executeSqlStmt(sql_stmt)

    def insertRow(self, sql_stmt):
        """
        Execute an Insert statement

        :param sql_stmt: The SQL statement to be executed, must be a single result
        :return: The Sql execute value.
        """
        return self.executeSqlStmt(sql_stmt)

    def updateRow(self, sql_stmt):
        """
        Execute an Update statement

        :param sql_stmt: The SQL statement to be executed, must be a single result
        :return: The Sql execute value.
        """
        return self.executeSqlStmt(sql_stmt)

    def selectRows(self, sql_stmt):
        """
        Execute a Select SQL Statement and fetch all rows. This would be used where the expected row
        count would NOT be large (less than 25,000 rows) otherwise there will be memory issues

        :param sql_stmt: The SQL select statement to be executed
        :return: A list of rows
        """
        try:
            cur = self.dbConn.cursor()
            cur.execute(sql_stmt)
            all_values = cur.fetchall()  # This select returns all the query results
            return all_values
        except pyodbc.Error as err:
            logging.error(("Unable to Execute Multiple Value SQL -> for {}"
                           "\nERROR {}".format(sql_stmt, str(err))))
            raise ()

    def selectRowsWithCursor(self, sql_stmt, fetch_size):
        """
        Execute a Select SQL Statement and fetch the passed number of rows. This would be used where
        the expected row count would be large (greater than 25,000 rows) to avoid memory issues

        :param sql_stmt: The SQL select statement to be executed
        :fetch_size: The number of rows to fetch
        :return: A list of rows
        """
        try:
            self.fetch_cur = self.dbConn.cursor()
            self.fetch_cur.execute(sql_stmt)
            fetch_values = self.fetch_cur.fetchmany(fetch_size)  # This select returns limited query results
            return fetch_values
        except pyodbc.Error as err:
            logging.error(("Unable to Execute Multiple Value SQL -> for {}"
                           "\nERROR {}".format(sql_stmt, str(err))))
            raise ()

    def selectRowsWithCursorNextFetch(self, sql_stmt, fetch_size):
        """
        Execute a Select SQL Statement and fetch the passed number of rows. This would be used where
        the expected row count would be large (greater than 25,000 rows) to avoid memory issues

        :param sql_stmt: The SQL select statement to be executed
        :fetch_size: The number of rows to fetch
        :return: A list of rows
        """
        try:
            fetch_values = self.fetch_cur.fetchmany(fetch_size)  # This select returns limited query results
            return fetch_values
        except pyodbc.Error as err:
            logging.error(("Unable to Execute Multiple Value SQL -> for {}"
                           "\nERROR {}".format(sql_stmt, str(err))))
            raise ()

    def insertMultipleRows(self, sql_stmt, row_list):
        """
        Execute a SQL Bulk Insert Statement for a list of rows. This will be used for very quick inserts
        for multiple rows. It will take advantage of placeholders in the insert statement which will match
        the passed list

        :param sql_stmt: The SQL statement to be executed, must be a single result
        :param row_list: The list of rows to be inserted.
        :return: The rowcount value
        """
        try:
            cur = self.dbConn.cursor()
            cur.fast_executemany = True  # This is the process for SQL Server
            print(sql_stmt) 
            print(row_list)
            
            cur.executemany(sql_stmt, row_list)
            cur.execute("commit")
            return
        except pyodbc.Error as err:
            logging.error(("Unable to Insert Multiple Rows SQL -> for {}"
                           "\nERROR {}".format(sql_stmt, str(err))))
            raise ()

    def insertRowWithValues(self, sql_stmt, row_values):
        """
        Execute a single row Insert Statement for a list of values.

        :param sql_stmt: The SQL statement to be executed, must be a single result
        :param row_list: The list of rows to be inserted.
        :return: The rowcount value
        """
        try:
            cur = self.dbConn.cursor()
            cur.execute(sql_stmt, row_values)
            cur.execute("commit")
            cur.close()
            return
        except pyodbc.Error as err:
            logging.error("Unable to Execute Single row SQL -> for %s   " \
                          "\nERROR %s", sql_stmt, str(err))
            raise ()

    def getTableNames(self):
        """
            This function is used to return a list of tables for a specifc database owner.
            It is passed a DB connection and the owner.

            :return : Returns a list of tables
        """
        table_list = []
        if self.getDbType() == 'MS_SQL':
            selectStmt = ("sp_tables @table_owner = '{}'".format(self.getDbOwner()))
        elif self.getDbType() == 'QB':
            selectStmt = "sp_tables"
        elif self.getDbType() == 'MYSQL':
            selectStmt = "show tables"
        else:
            raise ()

        table_rows = self.selectRows(selectStmt)

        ''' Iterate through the row list and extract only the table names '''
        for row in table_rows:
            if self.getDbType() == 'MYSQL':
                table_list.append(row[0])
            elif self.getDbType() in ('QB', 'MS_SQL'):
                table_list.append(row[2])

        return table_list  # Return the List

    def getColumnNames(self, table_nm):
        """
            This function is used to return a list of Columns for a specific table.
            The function also conforms the data types across different databases.

            :param table_nm: The table name to select columns for.
            :return: A list of columns, datatype and size of data type(0 if N/A)
        """
        column_list = []

        if self.getDbType() == 'MS_SQL':
            if len(self.getDbOwner()) > 0:
                selectStmt = ("sp_columns @table_name = '{}', @table_owner= '{}'".format(
                    table_nm, self.getDbOwner()))
            else:
                selectStmt = ("sp_columns @table_name = '{}'".format(table_nm))
        elif self.getDbType() == 'QB':
            selectStmt = "sp_columns {}".format(table_nm)
        elif self.getDbType() == 'MYSQL':
            selectStmt = "SHOW columns FROM {}".format(table_nm)
        else:
            raise ()

        column_rows = self.selectRows(selectStmt)

        # print(column_rows)
        ''' Iterate through the row list and extract the Column names
            and the data type and size '''
        for row in column_rows:
            if self.getDbType() == 'MYSQL':
                parts = row[1].split('(')
                column_type = parts[0].upper()

                if len(parts) > 1:
                    size_parts = parts[1].split(')')
                    if len(size_parts[0]) > 0:
                        column_size = size_parts[0]
                else:
                    column_size = '0'
                column_list.append([row[0], column_type, column_size])
            elif self.getDbType() in ('QB', 'MS_SQL'):
                column_list.append([row[3], row[5], row[7]])

        return column_list  # Return the List of column names
