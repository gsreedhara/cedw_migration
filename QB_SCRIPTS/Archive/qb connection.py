import pyodbc 

cn = pyodbc.connect('DSN=QuickBooks Data')
cursor = cn.cursor()

cursor.execute("SELECT Top 10 Name FROM Customer")

for row in cursor.fetchall():
	print (row)

cursor.close()

cn.close() 