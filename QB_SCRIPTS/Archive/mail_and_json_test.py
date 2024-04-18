import logging
import time
import json
from datetime import datetime
import smtplib
#import db_functions_v2 as db
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

process_start_time = time.time()

#log_file = 'ETL_Logs\ETL_Log_' + datetime.today().strftime('%Y-%m-%d') + '.log'
log_file = 'ETL_Log_' + datetime.today().strftime('%Y-%m-%d') + '.log'

logging.basicConfig(filename=log_file, level=logging.INFO, filemode='a', format='%(asctime)s %(message)s')
logging.info('********************************************************************************')
logging.info('Start of process')



s = smtplib.SMTP(host='pxltd-ca.mail.protection.outlook.com',port='25')
logging.info('Opened Maol Server')

msg = MIMEMultipart()       # create a message
msg['From'] = 'sqlalerts@pxltd.ca'
msg['To'] = 'swilson@pxltd.ca'
msg['Subject'] = 'QuickBooks extract completed'

process_end_time = time.time()

msg_text = 'QuickBBooks extract successful\n' \
           'Extracted %d tables\n' \
           'Execution time %0.4f minutes'% (150, (process_end_time - process_start_time)/60)
print(msg_text)
message_body = MIMEText(msg_text, "plain")
msg.attach(message_body)
s.send_message(msg)
print(s, msg)



s.quit()




print('Total Elapsed Seconds -> ', process_end_time - process_start_time)
logging.info('Total Execution elapsed time -> %0.4f', process_end_time - process_start_time)

