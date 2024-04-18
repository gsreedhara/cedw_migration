# -- * **************************************************************************
# -- * File Name        : etlControl.py
# -- *
# -- * Description      : This set of python methods are designed as a     
# -- *                    wrapper around the API calls to the ETL Control
# -- *                    API. It removes the complexity and execution rules
# -- *                    by providing 3 seperate wrapper methods. These methods
# -- *                    are to be executed by the calling ETL program. (see
# -- *                    "Project X Architecture - Automation Control" doc)
# -- *
# -- *                   etlStartProcess Method: is called before the execution 
# -- *                      of the ETL code. 
# -- *
# -- *                   getProcessStepData Method: is called before the 
# -- *                      execution of the ETL code and after the
# -- *                      initialization method.
# -- *
# -- *			 etlEndProcess Method: is called after the execution of 
# -- *                      the ETL code
# -- *		
# -- *
# -- * Purpose          : Interface with the ETL Control API
# -- * Date Written     : Feb 2020
# -- * Input Parameters : see individual Methods
# -- * Input Files      : N/A
# -- * Output Parameters: N/A
# -- * Tables Read      : N/A (the API will access the database)
# -- * Tables Updated/Inserted   :N/A (the API will access the database)
# -- *                              
# -- * Run Frequency    : On demand, these are only wrapper methods which will
# -- *                    be called from an ETL program
# -- * Developed By     : Steve Wilson - ProjectX
# -- * Code Location    : https://github.com/PXLabs/ETL_Control_API
# -- *   
# -- * Version   	Date   		Modified by     Description
# -- * v1.0	   	Feb 28, 2020	Steve Wilson	Initial Code created
# -- * **************************************************************************
# -- * **************************************************************************
#
# Libraries Required
#
import requests
import logging
from datetime import timedelta
from datetime import datetime

# IP_Address = 'http://54.82.70.133:5000'   # Dev API
IP_Address = 'http://apps.pxltd.ca:5000/'    # Prod API

def getEtlStatus(ETL_step_nm):
    ''' This calls the ETL API with a Process Step Name in order to retrieve the recent
    execution status code

    :param ETL_step_nm: The name of the ETL Step entry to retrieve the status value
    :return: The status description for the passed ETL Step (process) name
    '''
    try:
        resp = requests.get(f'{IP_Address}/controlAPI/getProcessStepStatus/{ETL_step_nm}')

        if resp.status_code != 200:
            # This means something went wrong.
            logging.error(f'Error Retrieving Status for "{ETL_step_nm}" {resp.status_code}')
            raise Exception(f'GET /controlAPI/getProcessStepStatus/ {resp.status_code}')

        resp_data = resp.json()  # convert the response from a JSON to Dict data type
        # logging.info(f'Retrieved Status for "{ETL_step_nm}" - {resp_data["Response"]}')

        return (resp_data['Response'])

    except Exception as e:
        print(e)


def getEtlIncrementType(ETL_step_nm):
    ''' This calls the ETL API with a Process Step Name in order to retrieve the Increment Type

    :param ETL_step_nm: The name of the ETL Step entry to retrieve the status value
    :return: The code value of the increment type for the passed ETL Step (process) name
    '''
    try:
        resp = requests.get(f'{IP_Address}/controlAPI/getProcessStepIncrementType/{ETL_step_nm}')
        if resp.status_code != 200:
            # This means something went wrong.
            logging.error(f'Error Retrieving Increment Type for "{ETL_step_nm}" {resp.status_code}')
            raise Exception(f'GET /controlAPI/getProcessStepIncrementType/ {resp.status_code}')

        resp_data = resp.json()  # convert the response from a JSON to Dict data type
        # logging.info(f'Retrieved Increment Type for "{ETL_step_nm}" - {resp_data["Response"]}')

        return (resp_data['Response'])
    except Exception as e:
        print(e)


def setEtlMinMaxDates(ETL_step_nm, start_date_str_value, end_date_str_value):
    ''' This calls the ETL API with a Process Step Name and updates the Start and End Datetimes for
    the Process Step

    :param ETL_step_nm: ETL_step_nm: The name of the ETL Step entry to retrieve the status value
    :param start_date_str_value: The string format of the start date to be updated
    :param end_date_str_value: The string format of the end date to be updated
    :return: The request status code from the put command
    '''

    try:
        # Prepare the JSON values to be passed
        body = {"Execution_Start_Ts": start_date_str_value, "Execution_End_Ts": end_date_str_value}
        # print('Body ', body)

        resp = requests.put(f'{IP_Address}/controlAPI/updateExecutionStartAndEndTime/{ETL_step_nm}',
                            json=body)
        if resp.status_code != 200:
            # This means something went wrong.
            logging.error(f'Error updating Start and End Dates for "{ETL_step_nm}" - {resp.status_code}')
            raise Exception(f'PUT /updateExecutionStartAndEndTime/ {resp.status_code}')

        return resp

    except Exception as e:
        print('Exception ', e)


def setEtlMinMaxValues(ETL_step_nm, start_str_value, end_str_value):
    ''' This calls the ETL API with a Process Step Name and updates the Start and End String
     Values for the Process Step

    :param ETL_step_nm: ETL_step_nm: The name of the ETL Step entry to retrieve the status value
    :param start_str_value: The string format of the start Value to be updated
    :param end_str_value: The string format of the end Value to be updated
    :return: The request status code from the put command
    '''

    try:
        # Prepare the JSON values to be passed
        body = {"Execution_Start_Value": start_str_value, "Execution_End_Value": end_str_value}
        # print('Body ', body)

        resp = requests.put(f'{IP_Address}/controlAPI/updateExecutionStartAndEndValue/{ETL_step_nm}',
                            json=body)
        if resp.status_code != 200:
            # This means something went wrong.
            logging.error(f'Error updating Start and End Values for "{ETL_step_nm}" - {resp.status_code}')
            raise Exception(f'PUT /updateExecutionStartAndEndValue/ {resp.status_code}')

        return resp

    except Exception as e:
        print('Exception ', e)


def updateProcessStepStatus(ETL_step_nm, statusValue):
    ''' This calls the ETL API with a Process Step Name and updates the Status Value
     for the Process Step

    :param ETL_step_nm: The name of the ETL Step entry to retrieve the status value
    :param statusValue: The string value of the status to be set for the ETL Step Name
    :return: The request status code from the put command
    '''
    try:
        # Prepare the JSON values to be passed
        body = {"Etl_Process_Status_Cd": statusValue}

        resp = requests.put(f'{IP_Address}/controlAPI/updateProcessStepStatus/{ETL_step_nm}',
                            json=body)
        if resp.status_code != 200:
            # This means something went wrong.
            logging.error(f'Error updating Step Status for "{ETL_step_nm}" - {resp.status_code}')
            raise Exception(f'PUT /updateProcessStepStatus/ {resp.status_code}')

        return resp

    except Exception as e:
        print('Exception ', e)


def getProcessStepData(ETL_step_nm):
    ''' This calls the ETL API with a Process Step Name to retrieve the data
     for the Process Step

    :param ETL_step_nm: The name of the ETL Step entry to retrieve the status value
    :return: The request status code from the get command
    '''
    try:
        resp = requests.get(f'{IP_Address}/controlAPI/getProcessStepData/{ETL_step_nm}')

        if resp.status_code != 200:
            # This means something went wrong.
            logging.error(f'Error getting Step Data for "{ETL_step_nm}" - {resp.status_code}')
            raise Exception(f'GET /controlAPI/getProcessStepData/ {resp.status_code}')

        return resp

    except Exception as e:
        print('Exception ', e)


def insertControlEvent(ETL_step_nm):
    ''' This method is used to insert the initial Copntrol Event for specific ETL Step Name

    :param ETL_step_nm: The name of the ETL Step which requires and insert to the ETL Control event
    :return: JSON object for the API insert call
    '''
    print(ETL_step_nm)
    try:
        step_response = getProcessStepData(ETL_step_nm).json()
        step_data = step_response['Response']
        #print(step_data)

        body = {"Etl_Process_Step_Id": step_data['Etl_Process_Step_Id'],
                "Etl_Project_Id": step_data['Etl_Project_Id'],
                "Etl_Target_Environment_Id": step_data['Etl_Target_Environment_Id'],
                "Execution_Start_Ts": step_data['Execution_Start_Ts'],
                "Execution_End_Ts": step_data['Execution_End_Ts'],
                "Execution_Start_Value": step_data['Execution_Start_Value'],
                "Execution_End_Value": step_data['Execution_End_Value'],
                "Num_Rows_Read": 0, 'Num_Rows_Processed': 0,
                "Total_Rows_From_Source": 0, "Total_Rows_To_Target": 0,
                "Last_Processed_Value": '', "Process_Error_Desc": '',
                "Process_Start_Ts": format(datetime.today().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]),
                "Process_End_Ts": '9999-12-31 00:00:00.000',
                "Etl_Process_Status_Id": step_data['Etl_Process_Status_Id']
                }

        resp = requests.post(f'{IP_Address}/controlAPI/insertControlEvent/{ETL_step_nm}',
                             json=body)

        #print(resp, resp.json())
        if resp.status_code != 200:
            # This means something went wrong.
            logging.error(f'Error Inserting Control Event for "{ETL_step_nm}" - {resp.status_code} - '
                         f'{resp.json()}')
            raise Exception(f'POST /insertControlEvent/ {resp.status_code}')

        return resp

    except Exception as e:
        print('Exception ', e)


def updateControlEvent(ETL_step_nm, etlStatus, event_body):
    ''' This process encapsulates all the steps required for the update of the event after the
     execution of the ETL code. It manages both a failure and success as sent from the ETL program

    :param ETL_step_nm: The name of the ETL Step entry to update the Control Event
    :param etlStatus: The status to set the Control Event (either Fail or OK)
    :param event_body: a Dict data type which either contains the error description from the ETL
        process or the read and write valiues for the ETL program
    :return: The response code of the last API call
    '''

    try:
        etl_process_status = 'STRT'

        # Retrieve the Control Event Id for this ETL execution
        resp = requests.get(f'{IP_Address}/controlAPI/getControlEventId/{ETL_step_nm}/'
                            f'{etl_process_status}')

        if resp.status_code != 200:
            # This means something went wrong.
            logging.error(f'Error getting Control Event ID for "{ETL_step_nm}" - {resp.status_code} - '
                         f'{resp.json()}')
            raise Exception(f'POST /etControlEventId/ {resp.status_code}')
        else:
            resp_data = resp.json()  # convert the response from a JSON to Dict data type
            etl_control_event_id = resp_data['Response']

        # Depending on the passed status the Control Event update will either be for a successful
        # ETL execution, in which case the count values will be updated or if the ETL execution
        # failed then the error description from the process will be updated to the Control Event
        if etlStatus == 'OK':
            new_etl_process_status = 'CPLT'
            api_name = 'updateControlEvent'     # API call for a successful ETL execution

            update_body = {"Num_Rows_Read": f"{event_body['Num_Rows_Read']}",
                    "Num_Rows_Processed": f"{event_body['Num_Rows_Processed']}",
                    "Total_Rows_From_Source": f"{event_body['Total_Rows_From_Source']}",
                    "Total_Rows_To_Target": f"{event_body['Total_Rows_To_Target']}",
                    "Process_End_Ts": format(datetime.today().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]),
                    "Last_Processed_Value": ""
                    }
        else:
            new_etl_process_status = 'FAIL'
            api_name = 'updateControlEventError'    # API call for a failed ETL execution
            update_body = {"Process_Error_Desc": f"{event_body['Process_Error_Desc']}"}

        # Update the Event Status to OK or Fail
        status_body = {"Etl_Process_Status_Cd": f"{new_etl_process_status}"}
        resp = requests.put(f'{IP_Address}/controlAPI/updateControlEventStatus/{etl_control_event_id}',
                            json=status_body)

        if resp.status_code != 200:
            # This means something went wrong.
            logging.error(f'Error Updating Control Event Status for "{ETL_step_nm}" - {resp.status_code} - '
                         f'{resp.json()}')
            raise Exception(f'POST /updateControlEventStatus/ {resp.status_code}')

        # Update the Control Event for Failure or Success
        resp = requests.put(f'{IP_Address}/controlAPI/{api_name}/{etl_control_event_id}',
                            json=update_body)

        if resp.status_code != 200:
            # This means something went wrong.
            logging.error(f'Error Updating Control Event for "{ETL_step_nm}" - {resp.status_code} - '
                         f'{resp.json()}')
            raise Exception(f'POST /{api_name}/ {resp.status_code}')

        return resp

    except Exception as e:
        print('Exception ', e)

def etlStartProcess(ETL_step_nm, end_date_str_value, end_str_value):
    ''' This process encapsulates all the steps required for the initial execution of the
    Automation process, before the ETL code is executed.

    :param ETL_step_nm: The name of the ETL Step entry to retrieve the status value
    :param end_date_str_value: The end datetime used by the MAX_DT increment type
    :param end_str_value: The end String value used by the MAX_VAL increment type
    :return: A JSON object which contains the Status (OK or Fail), Status Code (returned from the API call)
    and the description of the error or success
    '''
    # init_resp = dict()  # Initialize the response
    response_status = 'OK'
    response_result = 'Success'

    # check the previous status. If it is not Completed then return a Fail response
    etl_job_status = getEtlStatus(ETL_step_nm)

    if etl_job_status != 'Completed':
        init_resp = prepare_reponse('Fail', '200',
                                    f'Status NOT Valid to execute, current status is {etl_job_status}')
        logging.info(f'Status NOT Valid to execute, current status is {etl_job_status}')

        email_subject = f'Failure Notification for {ETL_step_nm}'
        email_body = f'Process failed for {ETL_step_nm} - Invalid Status "{etl_job_status}".'
        slack_msg = f'Failure Notification for {ETL_step_nm} - Invalid Status "{etl_job_status}"'
        resp = sendNotification(ETL_step_nm, 'Fail', email_subject, email_body, slack_msg)
    else:
        # Obtain the increment type for the Process Step
        increment_type = getEtlIncrementType(ETL_step_nm)

        # If the Increment type is a date-time then retrieve the previous end timestamp to be used as
        # the starting timestamp for the new run
        if increment_type in ('MAX_DT', 'DAY_END', 'CUR_TS', 'PREV_DAY_END'):
            step_response = getProcessStepData(ETL_step_nm).json()
            step_data = step_response['Response']

            # Set the start date to the previous End Date
            start_date_str_value = step_data['Execution_End_Ts']

            # Determine the End date to be used
            if increment_type == 'DAY_END':
                end_date_str_value = format(datetime.today().strftime('%Y-%m-%d 23:59:59.999999'))
            elif increment_type == 'CUR_TS':
                end_date_str_value = format(datetime.today().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
            elif increment_type == 'PREV_DAY_END':
                end_date_str_value = format((datetime.today() -
                                             timedelta(days=1)).strftime('%Y-%m-%d 23:59:59.999999'))

            # Update the Process Step with the new dates
            setEtlMinMaxDates(ETL_step_nm, start_date_str_value, end_date_str_value)

        # If the Increment type is a String Value then retrieve the previous end string value to be used as
        # the starting string value for the new run
        elif increment_type == 'MAX_VAL':
            step_response = getProcessStepData(ETL_step_nm).json()
            step_data = step_response['Response']

            # Set the start value to the previous End Value
            start_str_value = step_data['Execution_End_Value']

            # Update the Process Step with the new String Values
            setEtlMinMaxValues(ETL_step_nm, start_str_value, end_str_value)

        elif increment_type != 'ALL':
            init_resp = prepare_reponse('Fail', '200', f'Invalid Increment type {increment_type}')

            email_subject = f'Failure Notification for {ETL_step_nm}'
            email_body = f'Process failed for {ETL_step_nm} - Invalid Increment Type "{increment_type}".'
            slack_msg = f'Failure Notification for {ETL_step_nm} - Invalid Increment Type "{increment_type}"'
            resp = sendNotification(ETL_step_nm, 'Fail', email_subject, email_body, slack_msg)
            return init_resp

        resp = updateProcessStepStatus(ETL_step_nm, 'STRT')  # Update Status to 'Started'
        #logging.info(f'Start Process update Step status for "{ETL_step_nm}" - {resp} {resp.json()}')

        # At this point the Process Step is ready for the execution, add an initial control event
        if resp.status_code == 200:
            resp = insertControlEvent(ETL_step_nm)
            logging.info(f'Start Process Insert Event for "{ETL_step_nm}" - {resp} - {resp.json()}')
            if resp.status_code != 200:
                response_status = 'Fail'
                response_result = 'Insert Control Event Failed'
        else:
            response_status = 'Fail'
            response_result = 'Update Process Status Failed'

        init_resp = prepare_reponse(response_status, resp, response_result)

    return init_resp


def prepare_reponse(statusInd, statusCode, result):
    ''' This method formats the passed values into a JSON formatted string

    :param statusInd: The status of the response to be passed (OK or Fail)
    :param statusCode: A status code for debugging (usually the http rerturn call)
    :param result: A formatted string of the issue or success
    :return: The response as a dict data type
    '''
    return {"status": statusInd, "resp_code": statusCode, "result": result}


def sendNotification(ETL_step_nm, etlStatus, email_subject, email_body, slack_msg):
    ''' This process calls the ETL Control Api to send the failure notification as maintained
    in the Control Tables.

    :param ETL_step_nm: The name of the ETL Step to execute a success notification for
    :return: Return the JSON object for the API call
    '''
    try:
        body = {"Email_Subject": f"{email_subject}",
                "Email_Body": f"{email_body}",
                "Slack_Msg": f"{slack_msg}"
                }

        if etlStatus == 'OK':
            api_name = 'sendSuccessNotification'
        else:
            api_name = 'sendFailureNotification'

        resp = requests.post(f'{IP_Address}/controlAPI/{api_name}/{ETL_step_nm}', json=body)

        #print('Send Body -- ', body)
        if resp.status_code != 200:
            # This means something went wrong.
            logging.error(f'Error executing notification for "{ETL_step_nm}" - {resp.status_code} - '
                         f'{resp.json()}')
            raise Exception(f'GET /controlAPI/{api_name}/ {resp.status_code}')

        return resp

    except Exception as e:
        print('Exception ', e)


def etlEndProcess(ETL_step_nm, etlStatus, event_body):
    '''
    This process encapsulates all the steps required for the post execution of the
    Automation process, after the ETL code has executed. Based on the passed status
    this will either be a successful or failure status update and notification

    :param ETL_step_nm: The name of the ETL Step entry to retrieve the status value
    :param etlStatus:
    :return: A JSON object which contains the Status (OK or Fail), Status Code (returned from the API call)
    and the description of the error or success
    '''
    response_status = 'OK'
    response_result = 'Success'
    email_subject = f'Success Notification for {ETL_step_nm}'
    email_body = f'Process completed for {ETL_step_nm}.'
    slack_msg = f'Success Notification for {ETL_step_nm}'
    etl_process_status = 'CPLT'
    etl_status_desc = 'Success'

    if etlStatus != 'OK':
        email_subject = f'Failure Notification for {ETL_step_nm}'
        email_body = f'Process Failure for {ETL_step_nm}.'
        slack_msg = f'Failure Notification for {ETL_step_nm}'
        etl_process_status = 'FAIL'
        etl_status_desc = 'Failure'

    resp = updateProcessStepStatus(ETL_step_nm, etl_process_status)  # Update Status
    #logging.info(f'End Process update Step status for "{ETL_step_nm}" - {resp}')

    if resp.status_code == 200:
        resp = updateControlEvent(ETL_step_nm, etlStatus, event_body)
        logging.info(f'End Process update Event status for "{ETL_step_nm}" - {resp}')

        if resp.status_code == 200:
            resp = sendNotification(ETL_step_nm, etlStatus, email_subject, email_body, slack_msg)
            if resp.status_code != 200:
                response_status = 'Fail'
                response_result = f'{etl_status_desc} Notification Failed'
                logging.warning(f'Error executing notification for "{ETL_step_nm}" - {resp}')
        else:
            response_status = 'Fail'
            response_result = 'Update Control Event Failed'
            logging.warning(f'Error executing Update Control Event for "{ETL_step_nm}" - {resp}'
                         f' - {resp.json()}')
    else:
        response_status = 'Fail'
        response_result = 'Update Process Status Failed in etlEndProcess'

    init_resp = prepare_reponse(response_status, resp, response_result)

    return init_resp
