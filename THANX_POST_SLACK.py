# *********************************************************************************
# 
# * Description 
# *              Step 1: The 1st step involves connecting and pulling Data from CEDW 
# *                         
# *                     
# *              Step 2: The 2nd step of this script is to pull user_information from Slack to map 
# *                      employee emails to slack usernames
# *
# *              Step 3: The 3rd step of this script is to post messages and reactions to the appropriate users
# *                     in the ThanX channel (updates processed columns with a Y Flag)
# *
# *              Step 4: The 4th step is to delete all of the rows that are processed and more than 1 day old
# * 
# * Purpose          : This procedure posts messages from Project X's MSTR Recognition App to the Slack ThanX Channel
# *                    
# * Date Written     : 
# * Input Parameters : Driver, Server_Name, Database_Name, Slack Token, Channel_Name
# * Output Parameters: Recognition Message
# * Tables Read      : RCGNTN_MSG_WRK,RCGNTN_MSG_TO_WRK,RCGNTN_MSG_VALUE_WRK
# * Flag tables      : PRCSSD_IND (Y = Processed, N = Not Processed)
# *                    
# * Tables Updated   : 
# * Run Frequency    : 1 minute
# * Developed By     : Nadim Younes 
# * https://github.com/PXLabs/RecognitionApp
# *   
# * Version     Date         Modified by      Description
# * v1.0     2019/08/01     Nadim Younes      First Sprint (Foundational Code)
# * v2.0     2019/11/01     Nadim Younes      Second Iteration 
# * v3.0     2019/16/01     Nadim Younes      Third Iteration
# * v4.0     2019/17/01     Nadim Younes      Final Version (Test Cases Validated)
# **********************************************************************************


from slack_sdk import WebClient
import pandas as pd
import numpy as np
import time
import sys
import snowflake.connector
import ssl

#driver = sys.argv[1]
#server_name = sys.argv[2]
#db_name = sys.argv[3]
# slack_token = sys.argv[4]
slack_token = "xoxp-152818971696-530149612055-531694369057-7f8b0256251cb890b1ef5b46eadb2923"
#channel_name = sys.argv[5]
channel_name = "thanx-test"

# ********************************************************************
# Section 1 # Pulling Data From SQL Server
# ********************************************************************

#Create a Connection with Microsot SQL Management Studio using Pyodbc's connect function 
def create_connection():
    
    db = snowflake.connector.connect(
                    user="aturnbull",
                    password="projectX2024",
                    account="nlxoxef-vp32965",
                    warehouse="COMPUTE_WH",
                    role="dbdev",
                    database="PXLTD_THANX")
    return db
    
def run_queries(connection):
    #Run SQL query to pull information from the messaging table 
    query_from = "SELECT RCGNTN_MSG_ID,RCGNTN_FRM_USR_EMAIL,RCGNTN_MSG_TXT,RCGNTN_TAG_DESC,PRCSSD_IND FROM \
    MAIN.RCGNTN_MSG_WRK WHERE PRCSSD_IND ='N'"
    #Use pd.read_sql to create a pandas Dataframe from this data
    from_df = pd.read_sql(query_from,connection)

    if len(from_df) > 0:
        pass
    else:
        sys.exit()

    #Run SQL query to pull dataframe with Receiver Email(s)
    query_to = "SELECT RCGNTN_MSG_ID,RCGNTN_TO_USR_EMAIL FROM MAIN.RCGNTN_MSG_TO_WRK"
    to_df = pd.read_sql(query_to,connection)
    #Run SQL query to pull dataframe with Company Values included in Message
    query_values = "SELECT RCGNTN_MSG_ID,RCGNTN_VALUES_DESC FROM MAIN.RCGNTN_MSG_VALUE_WRK WHERE \
    RCGNTN_MSG_ID IN (SELECT RCGNTN_MSG_ID FROM MAIN.RCGNTN_MSG_WRK)"
    value_df = pd.read_sql(query_values,connection)

    if len(from_df["RCGNTN_MSG_ID"].unique()) > len(to_df["RCGNTN_MSG_ID"].unique()):
        no_to_ids = np.setdiff1d(from_df["RCGNTN_MSG_ID"].values,to_df["RCGNTN_MSG_ID"].values)
        for diff in no_to_ids:
            from_df.drop(from_df[from_df['RCGNTN_MSG_ID'] == diff].index,axis=0)
    else:
        pass
        
    #Perform a join operation on the receiver and sender tables where the key is the RCGNTN_MSG_ID Column 
    
    final_message_df_id = from_df.merge(to_df,on = "RCGNTN_MSG_ID").merge(value_df, how = "left",on = "RCGNTN_MSG_ID")
    final_message_df_id.fillna(" ",axis=0,inplace=True)


    return final_message_df_id

# ********************************************************************
# Section 2 # Mapping Emails to Usernames
# ********************************************************************

def create_merged_dataframe(slack_token,df_without_usernames):
    print(df_without_usernames.columns)
    #List Necessary Token Variable to Pull Data from Slack 
    SLACK_TOKEN = slack_token
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    #Create a dictionary called users_information with email as keys and usernames as values
    global sc
    sc = WebClient(token=SLACK_TOKEN, ssl=ssl_context)
    
    # Re-trying the api call to address unexpected failures
    rc=3
    user_list={}
    while rc<=3:
        try:
            user_list = sc.api_call("users.list")
            rc=4
        except:
            print("There was an exception and the API call to get users list didnt work")
            rc = rc + 1
    if user_list is None:
        sys.exit()
    users_information = {}
    for i in user_list["members"]:
        if "email" in list(i["profile"].keys()):
            users_information[i["profile"]["email"]] = i["name"]
    #Convert this dictionary to a Pandas Dataframe to allow it to be joined to other DFs
    username_email_df = pd.DataFrame.from_dict(users_information,orient="index").reset_index().rename(columns={"index":"email",0:"slack_username"})
    #Join 1 (Receiver_Usernames)
    df_without_usernames = pd.merge(df_without_usernames, username_email_df, how ='right',
        left_on = "RCGNTN_TO_USR_EMAIL",right_on = "email")
    #Join 2 (Sender_Usernames)
    df_without_usernames = pd.merge(df_without_usernames, username_email_df, how ='right',
        left_on = "RCGNTN_FRM_USR_EMAIL",right_on = "email")
    #Rename Recently Added Columns to more descriptive Names
    df_without_usernames.rename(columns = {"slack_username_y":"slack_frm_username",
        "slack_username_x":"slack_to_username"},inplace = True)
    #Drop Unecessary email columns
    df_without_usernames.drop(columns = ["email_x","email_y"],axis = 1,inplace = True)
    #Drop Nulls
    df_without_usernames.dropna(axis = 0,inplace = True)
    #Cast the ID column to integer (was a float)
    df_without_usernames["RCGNTN_MSG_ID"] = df_without_usernames.RCGNTN_MSG_ID.astype(int)
    
    return df_without_usernames

# *************************************************************************
# Section 3 # Posting Messages,Adding Reactions, Updating Indicator Columns
# *************************************************************************

def post_message_and_add_reactions(connection,df_with_usernames,slack_token,channel_name):
    #Create the pyodbc cursor that permits the executing of SQL queries
    global cursor   
    cursor = connection.cursor()
    print(df_with_usernames.columns)
    #This loop posts a message (1), reacts to the message (2)
    for index in df_with_usernames["RCGNTN_MSG_ID"].sort_values(ascending=True).unique():

        #Define Variables that will be posted
        timestamps=[]
        
        #In the case the message_text field is blank
        message_text = list(set(df_with_usernames.loc[df_with_usernames ["RCGNTN_MSG_ID"] == index]["RCGNTN_MSG_TXT"]))[0]
        tag = list(set(df_with_usernames.loc[df_with_usernames ["RCGNTN_MSG_ID"] == index]["RCGNTN_TAG_DESC"]))[0]
        from_username = list(set(df_with_usernames.loc[df_with_usernames ["RCGNTN_MSG_ID"] == index]["slack_frm_username"]))[0]
        to_usernames = list(set(df_with_usernames.loc[df_with_usernames ["RCGNTN_MSG_ID"] == index]["slack_to_username"]))
        if len(df_with_usernames.loc[df_with_usernames ["RCGNTN_MSG_ID"] == index]) == 0:
            values = [" "]
        else:
            values = list(set(df_with_usernames.loc[df_with_usernames ["RCGNTN_MSG_ID"] == index]["RCGNTN_VALUES_DESC"]))
        
        #These are conditional statements that post the message based on the corresponding combination of receivers and values 
        #In the case there are multiple receivers/values, the messages will be parsed differently 
        #One receiver and One Value
        if len(to_usernames) == 1  and len(values) == 1:
            sc.api_call("chat.postMessage", text = "@" + to_usernames[0] + " " + tag + " " + 
                message_text + " " + values[0], channel = str(channel_name) , 
                username = from_username, parse = "full", icon_emoji = ":thanx:")
    
        #Multiple Receivers and One Value
        elif len(to_usernames) > 1 and len(values) == 1:
            sc.api_call("chat.postMessage",text = "@" + " @".join(to_usernames) + " " + 
                tag + " " + message_text + " " + values[0],
                channel = str(channel_name) , username = from_username, parse = "full", icon_emoji = ":thanx:")

        #One Receiver and Multiple Values
        elif len(to_usernames) == 1 and len(values) > 1:
            sc.api_call("chat.postMessage",text = "@" + to_usernames[0] + " " + 
                tag + " " + message_text + " " + " ".join(values),
                channel = str(channel_name) , username = from_username, parse = "full", icon_emoji = ":thanx:")
        
        #Multiple Receivers and Multiple Values 
        elif len(to_usernames) > 1 and len(values) > 1:
            sc.api_call("chat.postMessage",text = "@" + " @".join(to_usernames) + " " + 
                tag + " " + message_text + " " + " ".join(values),
                channel = str(channel_name) , username = from_username, parse = "full", icon_emoji = ":thanx:")


        # Part 2 - Adding Reactions
        #The Reactions method requires channel_id hence a quick pull and map is done to get the appropriate channel_id given the input channel_name 
        channels_list = sc.api_call(method="conversations.list")
        channels_information = {}
        for channel in channels_list["channels"]: 
            channels_information[channel["name"]] = channel["id"]
        #gets the channel_id based on channel_name 
        channel_id = channels_information.get(str(channel_name))

        #The Reaction Method also requires the message timestamp
        #Hence the message data is pulled, and the message with the highest Unix timestamp (being the most recent) will have the appropriate reaction added to it
        channel_history = sc.api_call(method="conversations.history",channel=str(channel_id))
        for i,j in enumerate(channel_history["messages"]):
            if "bot_message" in list((channel_history["messages"][i].values())):
                timestamps.append(channel_history["messages"][i]["ts"])
            
        ts = max(timestamps)
        
        #Loop Through the Values list and create conditional statements based on whether there are multiple values or one value
        #The replace method is used to properly process the emojis for posting   
        for emoji in values:
            if len(values) > 1:
                sc.api_call(method = "reactions.add", name = tag.replace(":",""),
                    channel = str(channel_id),timestamp = str(ts))
            #2- Multiple Values
                sc.api_call(method = "reactions.add", name = emoji.replace(":","") ,
                    channel = str(channel_id),timestamp = str(ts))
            elif len(values) == 1 and " " not in values:
                sc.api_call(method = "reactions.add", name = tag.replace(":",""),
                    channel = str(channel_id),timestamp = str(ts))
            #2- One Value
                sc.api_call(method = "reactions.add", name = values[0].replace(":",""),
                    channel = str(channel_id),timestamp = str(ts))
            elif len(values) == 1 and " " in values:
                 sc.api_call(method = "reactions.add", name = tag.replace(":",""),
                    channel = str(channel_id),timestamp = str(ts))
            
            # Execute the update statement based on the MSG /MSG_ID that was just posted 
            cursor.execute("UPDATE RCGNTN_MSG_WRK SET PRCSSD_IND ='Y' WHERE RCGNTN_MSG_ID = ?",int(index))
            #Required to complete a update transaction 
            connection.commit()
           
def delete_day_old_rows(connection):

    cursor.execute("DELETE FROM MAIN.RCGNTN_MSG_WRK WHERE RCGNTN_TS < dateadd(day, -1, GETDATE()) AND PRCSSD_IND = 'Y'")
    cursor.execute("DELETE FROM MAIN.RCGNTN_MSG_TO_WRK WHERE RCGNTN_TS < dateadd(day, -1, GETDATE()) AND RCGNTN_MSG_ID NOT IN (SELECT RCGNTN_MSG_ID \
        FROM MAIN.RCGNTN_MSG_WRK)")
    cursor.execute("DELETE FROM MAIN.RCGNTN_MSG_VALUE_WRK WHERE RCGNTN_TS < dateadd(day, -1, GETDATE()) AND RCGNTN_MSG_ID NOT IN (SELECT RCGNTN_MSG_ID \
        FROM MAIN.RCGNTN_MSG_WRK)")
    connection.commit()
    cursor.close()
    #close connection

def main():
    create_connection()
    connection = create_connection()
    run_queries(connection)
    df_without_usernames = run_queries(connection)
    create_merged_dataframe(slack_token,df_without_usernames)
    df_with_usernames = create_merged_dataframe(slack_token,df_without_usernames)
    post_message_and_add_reactions(connection,df_with_usernames ,slack_token,channel_name)
    delete_day_old_rows(connection)
    sys.exit()

if __name__ == '__main__':
    main()
