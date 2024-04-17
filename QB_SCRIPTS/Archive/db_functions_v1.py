import pyodbc
#import pymysql


def DBConnection(serverName, DB, userName, pwd, dbDriver):
    try:
        db = pyodbc.connect(f"server={serverName};"
                            f"database={DB};"
                            f"uid={userName};"
                            f"pwd={pwd};"
                            "port=1433;"
                            f"driver={dbDriver}")
        return db
    except Exception as err:
        print("Unable to Establish DB Connection -> ", str(err))
        exit()


def DBConnection_QB(dsn):
    try:
        db = pyodbc.connect(dsn)
        return db
    except Exception as err:
        print("Unable to Establish DB Connection -> ", str(err))
        qblog = open("C:\QB_SCRIPTS\qblog.txt","w")
        qblog.write(str(err))
        exit()

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
    count_value = -1
    try:
        cur = dbConn.cursor()
        cur.execute(sql_stmt)
        count_value = cur.fetchone()[0]  # This select only returns a count value
        cur.close()
        return count_value
    except Exception as err:
        print("Unable to Execute count -> ", str(err), '\n', sql_stmt)
        exit()


def executeSqlStmt(dbConn, sql_stmt):
    try:
        cur = dbConn.cursor()
        cur.execute(sql_stmt)
        cur.commit()
        cur.close()
        return cur.rowcount
    except pyodbc.Error as err:
        print("Unable to Execute SQl Statement -> ", str(err), '\n', sql_stmt)
        exit()

def truncateTable(dbConn, table_nm):
    sql_stmt = 'TRUNCATE TABLE ' + table_nm
    executeSqlStmt(dbConn, sql_stmt)

def insertRow(dbConn, sql_stmt):
    executeSqlStmt(dbConn, sql_stmt)


def getSingleValue(dbConn, sql_stmt):
    try:
        cur = dbConn.cursor()
        cur.execute(sql_stmt)
        single_value = cur.fetchone()[0]  # This select only returns a count value
        cur.close()
        return single_value
    except pyodbc.Error as err:
        print("Unable to Execute Single Value statement -> ", str(err), '\n', sql_stmt)
        exit()


def getAllValues(dbConn, sql_stmt):
    try:
        cur = dbConn.cursor()
        cur.execute(sql_stmt)
        all_values = cur.fetchall()  # This select returns all the query results
        cur.close()
        return all_values
    except pyodbc.Error as err:
        print("Unable to Execute Multiple Value statement -> ", str(err), '\n', sql_stmt)
        exit()


def insertMultipleRows(dbConn, sql_stmt, row_list):
    try:
        cur = dbConn.cursor()
        cur.fast_executemany = True
        cur.executemany(sql_stmt, row_list)
        cur.commit()
        cur.close()
        return
    except pyodbc.Error as err:
        print("Unable to Execute Multiple row SQL -> ", str(err), '\n', sql_stmt)
        exit()

def insertRowWithValues(dbConn, sql_stmt, row_list):
    try:
        cur = dbConn.cursor()
        cur.execute(sql_stmt, row_list)
        cur.commit()
        cur.close()
        return
    except pyodbc.Error as err:
        print("Unable to Execute Single row SQL -> ", str(err), '\n', sql_stmt)
        exit()


def selectRows(dbConn, sql_stmt):
    try:
        cur = dbConn.cursor()
        cur.execute(sql_stmt)
        rows = cur.fetchall()
        cur.close()
        return rows
    except pyodbc.Error as err:
        print("Unable to Execute Select SQL -> ", str(err), '\n', sql_stmt)
        exit()
