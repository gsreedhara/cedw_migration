import pymysql.cursors

# Connect to the database
connection = pymysql.connect(host='172.30.3.165',
                             user='devuser',
                             password='pxduser@720',
                             database='actitime',
                             #charset='utf8mb4',
                             #port=3306,
                             cursorclass=pymysql.cursors.DictCursor)

with connection:
    with connection.cursor() as cursor:
        # Create a new record
        sql = "select count(1) from task"
        cursor.execute(sql)

    # connection is not autocommit by default. So you must commit to save
    # your changes.
    #connection.commit()

    with connection.cursor() as cursor:
        # Read a single record
        sql = "select count(*) from task"
        cursor.execute(sql)
        result = cursor.fetchone()
        print(result)