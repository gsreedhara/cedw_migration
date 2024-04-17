# -- * **************************************************************************
# -- * File Name        : qb_extract_cedw.py
# -- *
# -- * Description      :    
# -- *                   Step 1: Read the Json files to get the database
# -- *                      connection paramaters and the email notification
# -- *                      addresses
# -- *
# -- *                   Step 2: Establish conections to the Automation/Control,
# -- *                      QuickBooks source and CEDW QuickBooks target DB's
# -- *
# -- *                   Step 3: Retrieve a list of the valid tables and their
# -- *                      respctive column names for extract
# -- *
# -- *			 Step 4: For each table prepare and execute a select 
# -- *                      from the source and prepare and execute an insert
# -- *                      into the target table
# -- *
# -- *			 Step 5: Close all the DB Conections		
# -- *
# -- * Purpose          : Extract all the data from the QuickBooks source and
# -- *                      load it into the CEDW target for reporting purposes
# -- * Date Written     : Dec 2019
# -- * Input Parameters : N/A
# -- * Input Files      : EmailNotification.json, ConnectionData.json
# -- * Output Parameters: N/A
# -- * Tables Read      : Third_Party_Source_Etl_Execution and ALL tables contained
# -- *                      in this table (includes the source and target tables)   
# -- * Tables Updated/Inserted   : ALL tables in the
# -- *                              Third_Party_Source_Etl_Execution table
# -- * Run Frequency    : Every 4 hours
# -- * Developed By     : Steve Wilson - ProjectX
# -- * Code Location    : https://github.com/PXLabs/QuickBooksExtract
# -- *   
# -- * Version   	Date   		Modified by     Description
# -- * v1.0	   	Dec 12, 2019	Steve Wilson	Initial Code created
# -- * v2.0	   	Dec 18, 2019	Steve Wilson	Added notification &
# -- *                                                  connection security
# -- * v2.1	   	Dec 19, 2019	Steve Wilson	Cleaned up notification
# -- * **************************************************************************
# -- * **************************************************************************
# Packages Required
#
# -- * **************************************************************************
# Libraries Required
#
import time
import logging
from datetime import datetime
import json
import smtplib
import db_functions as db
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

""" The overall program will retrieve all the table and column data that
    was created and formated by the process "populate_selection_table" and
    stored in the table "Third_Party_Source_Etl_Execution".
    The process will extract data from the QuickBooks source and insert
    it into a SQL Server reporting database
"""

def getEmailUsers(emailData, notificationType):
    """ Retrieve the email addresses to notify for failure of success """
    email_list = emailData[notificationType]['email_address']
    return email_list #'SuccessEmail'


def notifyEmailUsers(process_execution_ind, email_users, msg_text):
    """
        Opens an email server and sends aeither a success or failure message
        to the passed recipients
        
        :param process_execution_ind: Success 'S' or Failure 'F' indicator
        :param email_users: List of emal user to send a message to
        :param msg_text: Message to send to the email list
        :return: None 
    """
    mail_server = smtplib.SMTP(host='pxltd-ca.mail.protection.outlook.com',
                                   port='25')
    logging.info('Opened Mail Server')
    msg = MIMEMultipart()       # create a message
    msg['From'] = 'sqlalerts@pxltd.ca'
    msg['To'] = ', '.join(email_users)

    if process_execution_ind == 'S':
        msg['Subject'] = 'QuickBooks extract Successful'
    else:
        msg['Subject'] = 'QuickBooks extract Failed'
        
    message_body = MIMEText(msg_text, "plain")
    msg.attach(message_body)
    mail_server.send_message(msg)
    
    if process_execution_ind == 'S':
        logging.info('Sent success email to %s', msg['To'])
    else:
        logging.info('Sent Failure email to %s', msg['To'])

    mail_server.quit()
    

def prep_QB_columns(src_column_names):
    """
        For use in the data selection from QB the column statements must
        have a specific format. All doulble quotes are removed and then
        will be added back for the column name Desc
        
        :param src_column_names: The string of column names to be formatted
        :return: A formatted string 
    """
    
    fmt_column_names = src_column_names.replace('"','')
    fmt_column_names = fmt_column_names.replace(',Desc,',',"Desc",')
    if fmt_column_names.count(',Desc') != 0 and \
            fmt_column_names.count(',Description') == 0:
        fmt_column_names = fmt_column_names.replace(',Desc',',"Desc"')
        
    return(fmt_column_names)


def replace_dt_columns(column_data):
    """
        This function will format all datetime data types which are returned
        from the select statement. It modifies the python datetime type to
        a string format accepted by SQL Server

        :param column_data: The string of date time data type to be formatted
        :return: A formatted date string
    """
    new_list = []
    for column in column_data:
        if isinstance(column, datetime):      
            date_str_fmt = '{:%Y-%m-%d %H:%M:%S}'.format(column)
            new_list.append(date_str_fmt)
        elif isinstance(column, bool):
            new_list.append(column)
        else:
            new_list.append(column)
            
    return new_list

def main():
    try:
        process_start_time = time.time()
        fetch_row_size = 3000   # process in batches to manage memory

        #  Setup and configure the Log file
        log_file = 'C:\QB_SCRIPTS\ETL_Logs\ETL_Log_' + \
                   datetime.today().strftime('%Y-%m-%d') + '.log'
        logging.basicConfig(filename=log_file, level=logging.INFO, \
                            filemode='a', format='%(asctime)s %(message)s')
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

        # Obtain the notification recipients from a file.
        try:
            with open("C:\QB_SCRIPTS\EmailNotification.json") as f:
                emailNotification = json.load(f)

            successEmailUsers = getEmailUsers(emailNotification, 'SuccessEmail')
            failureEmailUsers = getEmailUsers(emailNotification, 'FailureEmail')
            logging.info('Retrieved email notification data')
        except:
            logging.info('Error reading Email Notification data %s', str(err))
            raise()

        # Define Control, QB Source and QB Target Database
        ctl_con_data = db.DbConData(DbConnectionData, "ControlConnection")  
        QB_con_data = db.DbConData(DbConnectionData, "QbSourceConnection")    
        tgt_con_data = db.DbConData(DbConnectionData, "QbTargetConnection")   

        ''' Create a connection to the Control database'''
        ctl_dbConn = db.DBConnection(ctl_con_data.db_server_name,
                                     ctl_con_data.db_name,
                                     ctl_con_data.db_user,
                                     ctl_con_data.db_pwd,
                                     ctl_con_data.db_driver,
                                     ctl_con_data.db_port)
        logging.info('Opened Control DB connection')

        ''' Create a connection to the Source database '''
        src_dbConn = db.DBConnection_QB(QB_con_data.DSN)
        logging.info('Opened Source DB connection')

        ''' Create a connection to the Target database'''
        tgt_dbConn = db.DBConnection(tgt_con_data.db_server_name,
                                     tgt_con_data.db_name,
                                     tgt_con_data.db_user,
                                     tgt_con_data.db_pwd,
                                     tgt_con_data.db_driver,
                                     tgt_con_data.db_port)
        logging.info('Opened Target DB connection')

        ''' Retrieve all the valid tables from the QB ETL execution table '''
        selectStmt = "SELECT Source_Table_Nm, Source_Column_Names FROM dbo." \
                     "[Third_Party_Source_Etl_Execution]" \
                     " WHERE process_ind = 'Y' AND Source_Db_Nm = 'QB_CEDW';"
        valid_table_rows = db.getAllValues(ctl_dbConn, selectStmt)
        num_of_table_rows = len(valid_table_rows)

        logging.info('Number of Tables to Process -> %d', num_of_table_rows)
        print('Number of Rows-> ', num_of_table_rows)

        tables_processed = 0

        ''' Read through the tables to be extracted from '''
        for source_table in valid_table_rows:
            extract_start_time = time.time()
            tables_processed += 1
            table_name = source_table[0]    
            column_names = source_table[1]
            # format the columns
            sq_column_names = prep_QB_columns(source_table[1]) 

            print('Table name -> ', table_name)
            logging.info('Processing Table -> %s', table_name)

            ''' This table is a special case, it contains a salary column
                which is required to be encrypted. The column 'xx' is added
                to the target table and will contain the raw (unencrypted
                value). It will be set to NULL just after the data is loaded.
                The existing list of columns is modified by adding xx to
                the insert and rate to the Select
            '''
            if table_name == 'EmployeeEarning':
                column_names += ',temp_rate'   # PayrollInfoEarningsRate_ENCRPTD
                sq_column_names += ',PayrollInfoEarningsRate'        
                 
            ''' Select and process the table. Create the Select Statement
                from the extract table '''
            selectStmt = "SELECT " + sq_column_names + " FROM " + table_name

            cur = src_dbConn.cursor()
            cur.execute(selectStmt)

            ''' Before inserting rows remove all the previous rows, truncate
                table'''
            full_table_nm = tgt_con_data.db_name + "." + tgt_con_data.db_owner \
                + ".[" + table_name + ']'
            db.truncateTable(tgt_dbConn, full_table_nm)
            logging.info('Table Truncated -> %s', full_table_nm)

            ''' Build the Insert statement, use the columns in the table,
                determine the number of columns by counting the number of
                commas in the column_names column and adding one more.
                Use these as place holders in the form a "?", then use the
                execute many statement to insert all the rows for all values
                from the list '''
            insertStmt = "INSERT INTO " + tgt_con_data.db_name + "." + \
                         tgt_con_data.db_owner + ".[" + table_name + \
                         "] (" + column_names + ") VALUES ("

            ''' find the number of columns in the names There will be one
                less comma so add another at the end'''
            for i in range(column_names.count(',')):
                insertStmt += '?,'

            insertStmt += '?)'  # Add the final placeholder

            # Set the indicator to control a batch fetch for large tables        
            data_available = True
            
            ''' Set up a while loop to manage large source tables. This will
                set up a cursor to fetch a predetermined number of rows.'''
            while data_available == True:
                # select returns partial query results to manage large tables
                batch_values = cur.fetchmany(fetch_row_size)
                num_of_src_rows = len(batch_values)

                ''' Check for the number of rows returned, if 0 or less than
                    the fetch size set the data available indicator to exit
                    the while loop '''
                if num_of_src_rows < fetch_row_size or num_of_src_rows == 0:
                    data_available = False

                ''' If there are 0 rows then don't process or try to insert '''
                if num_of_src_rows > 0:
                    fmt_rows = []
                    for batch_row in batch_values:
                        fmt_row = replace_dt_columns(batch_row) #Format the datetimes

                        ''' Create a formatted row list for all row inserts
                            for bulk loading except the EmployeeEarnings
                            table, this needs to be managed with single inserts
                            to avoid an internal pyodbc binding error
                        '''
                        fmt_rows.append(fmt_row)
                        ''' The following statement will insert the row one
                            at a time '''
                        if table_name == 'EmployeeEarning':
                            db.insertRowWithValues(tgt_dbConn,
                                                   insertStmt,
                                                   fmt_row)

                    logging.info('Number of rows Selected -> %d',
                                 num_of_src_rows)
                    # This manages the bulk load
                    if table_name != 'EmployeeEarning':
                        db.insertMultipleRows(tgt_dbConn, insertStmt, fmt_rows)
                        logging.info('Multiple Insert Executed for -> %s',
                                     table_name)

            ''' The following statement executes a SP which updates the
                encrypted column with the content of a raw data column 'xx'
                and then sets the raw column to NULL. This is for only the
                EmployeeEarning table
            '''
            if table_name == 'EmployeeEarning':
                db.executeSqlStmt(tgt_dbConn,
                                  "exec [PXLTD_CEDW].[dbo].SP_ENCRYPT_QB_SAL")
                
            extract_end_time = time.time()
            logging.info('ETL Execution elapsed time -> %0.4f',
                         extract_end_time - extract_start_time)
            cur.close()

        db.executeSqlStmt(tgt_dbConn, "exec [PXLTD_CEDW].[dbo].SP_QBAppend")
        logging.info('Executed the Stored Procedure SP_QBAppend')

        ctl_dbConn.close()  # Close the Control database connection
        logging.info('Closed Control DB connection')
        src_dbConn.close()  # Close the Source database connection
        logging.info('Closed Source DB connection')
        tgt_dbConn.close()  # Close the Target database connection
        logging.info('Closed Target DB connection')

        process_end_time = time.time()
        print('Total Elapsed Seconds -> ', process_end_time - process_start_time)

        logging.info('Number of Tables processed -> %s', tables_processed)
        logging.info('Total Execution elapsed time -> %0.4f',
                     process_end_time - process_start_time)

        ''' Create and send the Success email '''
        msg_text = 'QuickBBooks extract successful\n' \
           'Extracted %d tables\n' \
           'Execution time %0.4f minutes' % (tables_processed,
                                (process_end_time - process_start_time)/60)
        notifyEmailUsers('S', successEmailUsers, msg_text)
            
        logging.info('Execution ended. Error code is %d',0)
        
 
    except Exception as err:
        logging.info('Error in QB Extract process - %s', str(err))
        
        ''' Create and send the Failure email '''
        msg_text = 'QuickBBooks extract FAILED\nError %s' \
                   '\nPlease Review log file %s' % (str(err), log_file)
        notifyEmailUsers('F', successEmailUsers, msg_text)
            
        print('*** Process Failed ***')
        logging.info('Execution ended. Error code is %d',1)

if __name__ == '__main__':
    main()
