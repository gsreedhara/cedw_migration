# -- * *********************************************************************************
# -- * File Name        : PY_THANX_STG_SLACK_MESSAGES.py
# -- *
# -- * Description      :    
# -- *                   Step1: Retrieve channel messages from slack
# -- *                   Step2: Go over all of them and check the new ones
# -- *				     Step3: Save it in the Stage Table (RCGNTN_MSG_SLACK - PXLTD_THANX)
# -- *				     Step4: Check modified messages from slack
# -- *				     Step5: Save the modifications in the Stage Table
# -- *
# -- * Purpose          : Insert new and updated messages from Slack on Stage table
# -- *                    
# -- * Date Written     : Jan 2019
# -- * Input Parameters : server_name, db_name, table_name, SLACK_TOKEN, SLACK_CHANNEL_ID, days
# -- * Output Parameters: none
# -- * Tables Read      : Slack Channel, RCGNTN_MSG_SLACK
# -- *                    
# -- * Tables Updated   : RCGNTN_MSG_SLACK
# -- * Run Frequency    : Every 3 minutes
# -- * Developed By     : Tatiana Tagliari - ProjectX
# -- * Changeman Member :
# -- *   
# -- * Version   	Date   		Modified by     Description
# -- * v1.0	   		15Jan2019					Code created
# -- * *********************************************************************************

# -- * *********************************************************************************
# PACKAGES NEEDED
#pip install slackclient
#pip install pprint
#pip install deepdiff

# -- * *********************************************************************************
# LIBRARIES NEEDED
import sys
import pyodbc
import json
import pprint
from slack_sdk import WebClient
from datetime import datetime, timedelta
import snowflake.connector
import ssl
import os

# -- * *********************************************************************************
#PARAMETERS
#server_name = sys.argv[1] 
#db_name = sys.argv[2]
#table_name = sys.argv[3]
#SLACK_TOKEN = sys.argv[4]
#SLACK_CHANNEL_ID = sys.argv[5]

server_name = os.environ['SERVER_NAME']
db_name = os.environ['DATABASE']
table_name = os.environ['TABLE_NAME']
SLACK_TOKEN = os.environ['SLACK_TOKEN']
SLACK_CHANNEL_ID = os.environ['CHANNEL_ID']
days = int(30)

#days to consider when retrieving modified messages from Slack:
#days = int(sys.argv[6])


# -- * *********************************************************************************
#CONNECTION WITH SQL SERVER DATABASE AND SLACK CHANNEL
connection = snowflake.connector.connect(
                    user=os.environ['USER'],
                    password=os.environ['PASSWORD'],
                    account=os.environ['ACCOUNT'],
                    warehouse=os.environ['WAREHOUSE'],
                    role=os.environ['ROLE'],
                    database=os.environ['DATABASE'])

cursor = connection.cursor()

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE
    
#Create a dictionary called users_information with email as keys and usernames as values
global sc
sc = WebClient(token=SLACK_TOKEN, ssl=ssl_context)

# -- * *********************************************************************************
#RETRIEVING USERS' INFORMATION FROM SLACK

#All users and their emails, ids, and usernames
#DO NOT PUT THIS STEP INSIDE THE FUNCTION! ERROR: KeyError "members"

user_list = sc.api_call(api_method="users.list", params={'channel': SLACK_CHANNEL_ID})

users_nameEmail = {}
users_idEmail = {}
users_idName = {}
users_nameID = {}

if "members" in user_list:
    for each_user in user_list["members"]:
        if "email" in list(each_user["profile"].keys()):
            users_nameEmail[each_user["name"]] = each_user["profile"]["email"]
            users_idEmail[each_user["id"]] = each_user["profile"]["email"]
            users_idName[each_user["id"]] = each_user["name"]
            users_nameID[each_user["name"]] = each_user["id"]


# -- * *********************************************************************************
#Find the email corresponding to name or id
def returnUserEmail(name = None, id = None):
	#pprint.pprint(users_information.get("ttagliari"))
	if name != None:
		return users_nameEmail.get(name)
	elif id != None:
		return users_idEmail.get(id)

#Find the name corresponding to id
def returnUserName(id = None):
	if id != None:
		return users_idName.get(id)

#Find the id corresponding to name
def returnUserid(name = None):
	if name != None:
		return users_nameID.get(name)

# -- * *********************************************************************************
#Check valid Slack ID
def validateSlackID(str = None):
	result = False
	if (str.find("U") > 0 or str.find("W") > 0) and len(str) == 9:
		result = True
	return result


#Extracting the TO users from the message text and returning it as a dictionary
def returnToUsers(text = None):
	textClean = text
	values = {}
	to_user_ids_aux = ""
	to_user_ids = ""
	to_user_names = ""
	to_user_emails = ""
	count = 0
	count = text.count("<")

	while count > 0:
		to_user_init_index = textClean.find("<")
		to_user_fina_index = textClean.find(">")
		#pprint.pprint(textClean[to_user_init_index+2 :to_user_fina_index])
		
		to_user_ids_aux = str(textClean[to_user_init_index+2 :to_user_fina_index] or "")
		#if validateSlackID(to_user_ids_aux):
		to_user_ids = to_user_ids +"|"+ to_user_ids_aux

		to_user_names = to_user_names +"|"+ str(returnUserName(textClean[to_user_init_index+2 :to_user_fina_index]) or "")
		to_user_emails = to_user_emails +"|"+ str(returnUserEmail(name = None, id = textClean[to_user_init_index+2 :to_user_fina_index]) or "")
		textClean = textClean[to_user_fina_index+1 : None]
		count -= 1

		values = {"ids":to_user_ids[1:None],"names":to_user_names[1:None],"emails":to_user_emails[1:None],"textClean":textClean.strip()}
	return values
	#pprint.pprint(to_user_ids[1:None])
	#pprint.pprint(to_user_names[1:None])
	#pprint.pprint(to_user_emails[1:None])
	#pprint.pprint(textClean)

# -- * *********************************************************************************
#Returning the last Slack timestamp imported
def returnLastTS():
	lastTS = 0
	cursor.execute("SELECT MAX(SRC_TS) AS SRC_TS FROM MAIN."+table_name) 
	result_set = cursor.fetchone()
	for row in result_set:
		if row != None:
			lastTS = eval(row)
	return lastTS

# -- * *********************************************************************************
#Return date as timestamp
def toTimestamp(dt, epoch=datetime(1970,1,1)):
    td = dt - epoch
    return (td.microseconds + (td.seconds + td.days * 86400) * 10**6) / 10**6 

#timestamp30 = datetime.now() + timedelta(-30)
#pprint.pprint(timestamp30) 
#pprint.pprint(toTimestamp(timestamp30))

# -- * *********************************************************************************
#Getting all messages from database from the last (parameter days) days into a dictionary for update comparision
def importPreviousMessages(days):
	prevMessages = {}
	key = 0

	dateToCheck = datetime.now() + timedelta(-days)
	timestampPrev = toTimestamp(dateToCheck)

	cursor.execute("SELECT SRC_TS, SRC_THRD_TS, MSG_TXT, RPLY_CNT, RPLY_USRS_CNT, LTST_RPLY_TS, RPLY_USRS_SCTN, RPLIES_SCTN, REACTN_SCTN\
	,TO_USR_EMAIL, TO_USRNM, TO_USR_SRC_ID, ICNS, EDITED\
		 FROM "+table_name+" WHERE SRC_TS > "+str(timestampPrev)+" \
		 AND INTRNL_TS IN (SELECT MAX(INTRNL_TS) AS INTRNL_TS FROM "+table_name+" GROUP BY SRC_TS)")

	result_set = cursor.fetchall()
	for row in result_set:
		if row != None:
			prevMessages[row[0]] = {"SRC_THRD_TS":row[1], "MSG_TXT":row[2], "RPLY_CNT":row[3], "RPLY_USRS_CNT":row[4], "LTST_RPLY_TS":row[5]\
			, "RPLY_USRS_SCTN":row[6], "RPLIES_SCTN":row[7], "REACTN_SCTN":row[8], "TO_USR_EMAIL":row[9], "TO_USRNM":row[10], "TO_USR_SRC_ID":row[11]\
			, "ICNS":row[12], "EDITED":row[13]}
			#prevMessages = {"SRC_TS":row[0],"REACTN_SCTN":row[1],"EDITED":row[2],"MSG_TXT":row[3]}
	
	return prevMessages


# -- * *********************************************************************************
#Staging NEW messages and threads from Slack
lastTimestamp = returnLastTS()

channel_history = sc.conversations_history(channel=SLACK_CHANNEL_ID)

#channel_history = sc.api_call(api_method="conversations.history", channel_id=SLACK_CHANNEL_ID, count = 1000, inclusive = True, oldest = lastTimestamp)
if "messages" in channel_history:
	for each_msg in channel_history["messages"]:

		# -- * ****************************************************************************
		#Auxiliar variables
		text_to_aux = returnToUsers(each_msg["text"])

		if "username" in each_msg:
			from_user_name = each_msg["username"]
			from_user_id = returnUserid(each_msg["username"])
		elif "user" in each_msg:
			from_user_id = each_msg["user"]
			from_user_name = returnUserName(each_msg["user"])

		from_user_email = returnUserEmail(from_user_name,from_user_id)

		# -- * ****************************************************************************
		#Database Parameters
		SRC_TS = None
		SRC_THRD_TS = None
		MSG_TYP = None
		MSG_SBTYP = None
		MSG_ID = None
		CLNT_MSG_ID = None
		MSG_TXT = None
		FRM_USR_SRC_ID = None
		PRNT_USR_SRC_ID = None
		ATTCHMNTS_SCTN = None
		FLS_SCTN = None
		UPLD = None
		DSPL_AS_BOT = None
		RPLY_CNT = None
		RPLY_USRS_CNT = None
		LTST_RPLY_TS = None
		RPLY_USRS_SCTN = None
		RPLIES_SCTN = None
		SBSCRBD = None
		REACTN_SCTN = None
		ROOT_SCTN = None
		FRM_USR_EMAIL = None
		TO_USR_EMAIL = None
		FRM_USRNM = None
		TO_USRNM = None
		TO_USR_SRC_ID = None
		ICNS = None
		BOT_ID = None
		EDITED = None

		if "ts" in each_msg:
			SRC_TS = each_msg["ts"]
			#pprint.pprint(each_msg["ts"])

		if "thread_ts" in each_msg:	
			SRC_THRD_TS = each_msg["thread_ts"]
		#  	#pprint.pprint(each_msg["thread_ts"])

		if "type" in each_msg:
			MSG_TYP = each_msg["type"]
		# 	#pprint.pprint(each_msg["type"])

		if "subtype" in each_msg:
			MSG_SBTYP = each_msg["subtype"]
		#  	#pprint.pprint(each_msg["subtype"])

		if "message_id" in each_msg:
			MSG_ID = each_msg["message_id"]
		#  	#pprint.pprint(each_msg["message_id"])

		if "client_message_id" in each_msg:
			CLNT_MSG_ID = each_msg["client_message_id"]
		#  	#pprint.pprint(each_msg["client_message_id"])
		
		MSG_TXT = text_to_aux.get("textClean")
		# #pprint.pprint(text_to_aux.get("textClean"))
		
		FRM_USR_SRC_ID = from_user_id
		# #pprint.pprint(from_user_id)

		if "parent_user_id" in each_msg:
			PRNT_USR_SRC_ID = each_msg["parent_user_id"]
		#  	#pprint.pprint(each_msg["parent_user_id"])
		
		if "attachments" in each_msg:
			ATTCHMNTS_SCTN = json.dumps(each_msg["attachments"])
		#  	#pprint.pprint(each_msg["attachments"])
		
		if "FLS_SCTN" in each_msg:
			FLS_SCTN = each_msg["FLS_SCTN"]
		# 	#pprint.pprint(each_msg["FLS_SCTN"])

		if "upload" in each_msg:
			UPLD = each_msg["upload"]
		#  	#pprint.pprint(each_msg["upload"])

		if "display_as_bot" in each_msg:
			DSPL_AS_BOT = each_msg["display_as_bot"]
		#  	#pprint.pprint(each_msg["display_as_bot"])
		
		if "reply_count" in each_msg:	
			RPLY_CNT = each_msg["reply_count"]
		#  	#pprint.pprint(each_msg["reply_count"])

		if "reply_users_count" in each_msg:
			RPLY_USRS_CNT = each_msg["reply_users_count"]
		#  	#pprint.pprint(each_msg["reply_users_count"])

		if "latest_reply" in each_msg:
			LTST_RPLY_TS = each_msg["latest_reply"]
		#  	#pprint.pprint(each_msg["latest_reply"])

		if "reply_users" in each_msg:
			RPLY_USRS_SCTN = json.dumps(each_msg["reply_users"])
		#  	#pprint.pprint(each_msg["reply_users"])

		if "replies" in each_msg:	
			RPLY_USRS_SCTN = json.dumps(each_msg["replies"])
		#  	#pprint.pprint(each_msg["replies"])

		if "subscribed" in each_msg:
			SBSCRBD = each_msg["subscribed"]
		#  	#pprint.pprint(each_msg["subscribed"])
		
		if "reactions" in each_msg:
			REACTN_SCTN = json.dumps(each_msg["reactions"])
		#  	#pprint.pprint(each_msg["reactions"])

		if "root" in each_msg:
			ROOT_SCTN = each_msg["root"]
		#  	#pprint.pprint(each_msg["root"])
		
		FRM_USR_EMAIL = from_user_email
		# #pprint.pprint(from_user_email)

		TO_USR_EMAIL = text_to_aux.get("emails")
		# #pprint.pprint(text_to_aux.get("emails"))

		FRM_USRNM = from_user_name
		# #pprint.pprint(from_user_name)

		TO_USRNM = text_to_aux.get("names")
		# #pprint.pprint(text_to_aux.get("names"))
		
		TO_USR_SRC_ID = text_to_aux.get("ids")
		# #pprint.pprint(text_to_aux.get("ids"))
	 
		if "icons" in each_msg:
			ICNS = json.dumps(each_msg["icons"])
		#  	#pprint.pprint(each_msg["icons"])

		if "bot_id" in each_msg:
			BOT_ID = each_msg["bot_id"]
		#  	#pprint.pprint(each_msg["bot_id"])

		if "edited" in each_msg:
			EDITED = json.dumps(each_msg["edited"])
		#  	#pprint.pprint(each_msg["bot_id"])

		#pprint.pprint(each_msg)

		SQLCommand = ("INSERT INTO MAIN."+table_name+" (SRC_TS, SRC_THRD_TS, MSG_TYP, MSG_SBTYP, MSG_ID, CLNT_MSG_ID, MSG_TXT, FRM_USR_SRC_ID, PRNT_USR_SRC_ID, ATTCHMNTS_SCTN\
		,FLS_SCTN, UPLD, DSPL_AS_BOT, RPLY_CNT, RPLY_USRS_CNT, LTST_RPLY_TS, RPLY_USRS_SCTN, RPLIES_SCTN, SBSCRBD, REACTN_SCTN\
		,ROOT_SCTN, FRM_USR_EMAIL, TO_USR_EMAIL, FRM_USRNM, TO_USRNM, TO_USR_SRC_ID, ICNS, BOT_ID,EDITED) \
		VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")    

		Values = [SRC_TS, SRC_THRD_TS, MSG_TYP, MSG_SBTYP, MSG_ID, CLNT_MSG_ID, MSG_TXT, FRM_USR_SRC_ID, PRNT_USR_SRC_ID, ATTCHMNTS_SCTN\
		,FLS_SCTN, UPLD, DSPL_AS_BOT, RPLY_CNT, RPLY_USRS_CNT, LTST_RPLY_TS, RPLY_USRS_SCTN, RPLIES_SCTN, SBSCRBD, REACTN_SCTN\
		,ROOT_SCTN, FRM_USR_EMAIL, TO_USR_EMAIL, FRM_USRNM, TO_USRNM, TO_USR_SRC_ID, ICNS, BOT_ID, EDITED] 

		cursor.execute(SQLCommand,Values)        
		connection.commit()

		#pprint.pprint(each_msg)
		#pprint.pprint("-------------------Message staged successfully--------------------")



# # -- * *********************************************************************************
#Comparing messages from Slack and Stage table, and insert the edited ones
dateToCheck = datetime.now() + timedelta(-days)
timestampPrev = toTimestamp(dateToCheck)

prevMessagesSlack = {}
prevMessages = {}
tsUpdated = {}

#Retrieving messages from database
prevMessages = importPreviousMessages(days)
#pprint.pprint(previousMessages)

channel_history = sc.api_call("conversations.history", channel = SLACK_CHANNEL_ID, count = 1000, inclusive = True, oldest = timestampPrev)
if "messages" in channel_history:
	for each_msg in channel_history["messages"]:

		# -- * ****************************************************************************
		#Auxiliar variables
		text_to_aux = returnToUsers(each_msg["text"])

		if "username" in each_msg:
			from_user_name = each_msg["username"]
			from_user_id = returnUserid(each_msg["username"])
		elif "user" in each_msg:
			from_user_id = each_msg["user"]
			from_user_name = returnUserName(each_msg["user"])

		from_user_email = returnUserEmail(from_user_name,from_user_id)

		# -- * ****************************************************************************
		#Database Parameters
		SRC_TS = None
		SRC_THRD_TS = None
		MSG_TYP = None
		MSG_SBTYP = None
		MSG_ID = None
		CLNT_MSG_ID = None
		MSG_TXT = None
		FRM_USR_SRC_ID = None
		PRNT_USR_SRC_ID = None
		ATTCHMNTS_SCTN = None
		FLS_SCTN = None
		UPLD = None
		DSPL_AS_BOT = None
		RPLY_CNT = None
		RPLY_USRS_CNT = None
		LTST_RPLY_TS = None
		RPLY_USRS_SCTN = None
		RPLIES_SCTN = None
		SBSCRBD = None
		REACTN_SCTN = None
		ROOT_SCTN = None
		FRM_USR_EMAIL = None
		TO_USR_EMAIL = None
		FRM_USRNM = None
		TO_USRNM = None
		TO_USR_SRC_ID = None
		ICNS = None
		BOT_ID = None
		EDITED = None

		if "ts" in each_msg:
			SRC_TS = each_msg["ts"]
			#pprint.pprint(each_msg["ts"])

		if "thread_ts" in each_msg:	
			SRC_THRD_TS = each_msg["thread_ts"]
		#  	#pprint.pprint(each_msg["thread_ts"])

		if "type" in each_msg:
			MSG_TYP = each_msg["type"]
		# 	#pprint.pprint(each_msg["type"])

		if "subtype" in each_msg:
			MSG_SBTYP = each_msg["subtype"]
		#  	#pprint.pprint(each_msg["subtype"])

		if "message_id" in each_msg:
			MSG_ID = each_msg["message_id"]
		#  	#pprint.pprint(each_msg["message_id"])

		if "client_message_id" in each_msg:
			CLNT_MSG_ID = each_msg["client_message_id"]
		#  	#pprint.pprint(each_msg["client_message_id"])
		
		MSG_TXT = text_to_aux.get("textClean")
		# #pprint.pprint(text_to_aux.get("textClean"))
		
		FRM_USR_SRC_ID = from_user_id
		# #pprint.pprint(from_user_id)

		if "parent_user_id" in each_msg:
			PRNT_USR_SRC_ID = each_msg["parent_user_id"]
		#  	#pprint.pprint(each_msg["parent_user_id"])
		
		if "attachments" in each_msg:
			ATTCHMNTS_SCTN = json.dumps(each_msg["attachments"])
		#  	#pprint.pprint(each_msg["attachments"])
		
		if "FLS_SCTN" in each_msg:
			FLS_SCTN = each_msg["FLS_SCTN"]
		# 	#pprint.pprint(each_msg["FLS_SCTN"])

		if "upload" in each_msg:
			UPLD = each_msg["upload"]
		#  	#pprint.pprint(each_msg["upload"])

		if "display_as_bot" in each_msg:
			DSPL_AS_BOT = each_msg["display_as_bot"]
		#  	#pprint.pprint(each_msg["display_as_bot"])
		
		if "reply_count" in each_msg:	
			RPLY_CNT = str(each_msg["reply_count"])
		#  	#pprint.pprint(each_msg["reply_count"])

		if "reply_users_count" in each_msg:
			RPLY_USRS_CNT = str(each_msg["reply_users_count"])
		#  	#pprint.pprint(each_msg["reply_users_count"])

		if "latest_reply" in each_msg:
			LTST_RPLY_TS = each_msg["latest_reply"]
		#  	#pprint.pprint(each_msg["latest_reply"])

		if "reply_users" in each_msg:
			RPLY_USRS_SCTN = json.dumps(each_msg["reply_users"])
		#  	#pprint.pprint(each_msg["reply_users"])

		if "replies" in each_msg:	
			RPLY_USRS_SCTN = json.dumps(each_msg["replies"])
		#  	#pprint.pprint(each_msg["replies"])

		if "subscribed" in each_msg:
			SBSCRBD = each_msg["subscribed"]
		#  	#pprint.pprint(each_msg["subscribed"])
		
		if "reactions" in each_msg:
			REACTN_SCTN = json.dumps(each_msg["reactions"])
		#  	#pprint.pprint(each_msg["reactions"])

		if "root" in each_msg:
			ROOT_SCTN = each_msg["root"]
		#  	#pprint.pprint(each_msg["root"])
		
		FRM_USR_EMAIL = from_user_email
		# #pprint.pprint(from_user_email)

		TO_USR_EMAIL = text_to_aux.get("emails")
		# #pprint.pprint(text_to_aux.get("emails"))

		FRM_USRNM = from_user_name
		# #pprint.pprint(from_user_name)

		TO_USRNM = text_to_aux.get("names")
		# #pprint.pprint(text_to_aux.get("names"))
		
		TO_USR_SRC_ID = text_to_aux.get("ids")
		# #pprint.pprint(text_to_aux.get("ids"))
	 
		if "icons" in each_msg:
			ICNS = json.dumps(each_msg["icons"])
		#  	#pprint.pprint(each_msg["icons"])

		if "bot_id" in each_msg:
			BOT_ID = each_msg["bot_id"]
		#  	#pprint.pprint(each_msg["bot_id"])

		if "edited" in each_msg:
			EDITED = json.dumps(each_msg["edited"])
		#  	#pprint.pprint(each_msg["bot_id"])


		prevMessagesSlack[SRC_TS] = {"SRC_THRD_TS":SRC_THRD_TS, "MSG_TXT":MSG_TXT, "RPLY_CNT":RPLY_CNT, "RPLY_USRS_CNT":RPLY_USRS_CNT, \
		"LTST_RPLY_TS":LTST_RPLY_TS, "RPLY_USRS_SCTN":RPLY_USRS_SCTN, "RPLIES_SCTN":RPLIES_SCTN, "REACTN_SCTN":REACTN_SCTN\
		,"TO_USR_EMAIL":TO_USR_EMAIL, "TO_USRNM":TO_USRNM, "TO_USR_SRC_ID":TO_USR_SRC_ID, "ICNS":ICNS, "EDITED":EDITED}
		

		#pprint.pprint("---------------------------------------")
		#pprint.pprint(each_msg)

		#Insert into database the updated ones"
		if prevMessagesSlack.get(SRC_TS) != prevMessages.get(SRC_TS):
			
			SQLCommand = ("INSERT INTO "+table_name+" (SRC_TS, SRC_THRD_TS, MSG_TYP, MSG_SBTYP, MSG_ID, CLNT_MSG_ID, MSG_TXT, FRM_USR_SRC_ID, PRNT_USR_SRC_ID, ATTCHMNTS_SCTN\
			,FLS_SCTN, UPLD, DSPL_AS_BOT, RPLY_CNT, RPLY_USRS_CNT, LTST_RPLY_TS, RPLY_USRS_SCTN, RPLIES_SCTN, SBSCRBD, REACTN_SCTN\
			,ROOT_SCTN, FRM_USR_EMAIL, TO_USR_EMAIL, FRM_USRNM, TO_USRNM, TO_USR_SRC_ID, ICNS, BOT_ID,EDITED) \
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")    

			Values = [SRC_TS, SRC_THRD_TS, MSG_TYP, MSG_SBTYP, MSG_ID, CLNT_MSG_ID, MSG_TXT, FRM_USR_SRC_ID, PRNT_USR_SRC_ID, ATTCHMNTS_SCTN\
			,FLS_SCTN, UPLD, DSPL_AS_BOT, RPLY_CNT, RPLY_USRS_CNT, LTST_RPLY_TS, RPLY_USRS_SCTN, RPLIES_SCTN, SBSCRBD, REACTN_SCTN\
			,ROOT_SCTN, FRM_USR_EMAIL, TO_USR_EMAIL, FRM_USRNM, TO_USRNM, TO_USR_SRC_ID, ICNS, BOT_ID, EDITED] 

			cursor.execute(SQLCommand,Values)        
			connection.commit()

			tsUpdated[SRC_TS] = {"SRC_THRD_TS":SRC_THRD_TS, "MSG_TXT":MSG_TXT, "RPLY_CNT":RPLY_CNT, "RPLY_USRS_CNT":RPLY_USRS_CNT, \
		"LTST_RPLY_TS":LTST_RPLY_TS, "RPLY_USRS_SCTN":RPLY_USRS_SCTN, "RPLIES_SCTN":RPLIES_SCTN, "REACTN_SCTN":REACTN_SCTN\
		,"TO_USR_EMAIL":TO_USR_EMAIL, "TO_USRNM":TO_USRNM, "TO_USR_SRC_ID":TO_USR_SRC_ID, "ICNS":ICNS, "EDITED":EDITED}
			
			#pprint.pprint("---------------------------------------")
			#pprint.pprint(prevMessagesSlack.get(SRC_TS))
			#pprint.pprint(prevMessages.get(SRC_TS))

sys.exit()
