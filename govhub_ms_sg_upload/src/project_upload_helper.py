import logging
import json
import pandas as pd
import os, shutil
import sys
import pandas as pd 
import xml.etree.ElementTree as et
import re

def iso8601duration_to_seconds(durations):
    '''Converts a list of ISO-8601 Duration datatype into an list of seconds
    '''
    results = []
    for duration in durations:
        if duration:
            ISO_8601 = re.compile(
                'P'   # designates a period
                '(?:(?P<years>\d+)Y)?'   # years
                '(?:(?P<months>\d+)M)?'  # months
                '(?:(?P<weeks>\d+)W)?'   # weeks
                '(?:(?P<days>\d+)D)?'    # days
                '(?:T' # time part must begin with a T
                '(?:(?P<hours>\d+)H)?'   # hours
                '(?:(?P<minutes>\d+)M)?' # minutes
                '(?:(?P<seconds>\d+)S)?' # seconds
                ')?')   # end of time part
            # Convert regex matches into a short list of time units
            units = list(ISO_8601.match(duration).groups()[-3:])
            # Put list in ascending order & remove 'None' types
            units = list(reversed([int(x) if x != None else 0 for x in units]))
            results.append(sum([x*60**units.index(x) for x in units]))
        else:
            results.append('')

    return results

def XMLtoElementTree(filepath):
    '''Reads an XML file and converts into a tree without prefix namespaces removed
    '''
    it = et.iterparse(filepath)
    for _, el in it:
        prefix, has_namespace, postfix = el.tag.partition('}')
        if has_namespace:
            el.tag = postfix 
    return it.root  

#Open the configuration file containing XML to Table Schema mapping
def initializeConfiguration(filepath):
    '''Reads a JSON file and returns a JSON object'''
    with open(filepath) as f:
        data = json.load(f)
    return data

#Parse the XML and create a DataFrame for each table stored in Dictionary
def ElementTreetoDataFrameDictionary(root, conf, filename):
    '''Convert an ElementTree into a dictionary populated by (tablename,dataframe) dictated by configuration json object
    '''

    #Problematic columns containing ISO-8601 Duration datatypes
    taskk = ['Task_Duration', 'Manual_Duration', 'Task_Work','Overtime_Work',
    'Actual_Duration', 'Actual_Work', 'Actual_Overtime_Work', 'Regular_Work',
    'Remaining_Duration','Remaining_Work','Remaining_Overtime_Work']
    resourcee = ['Work','Regular_Work','Overtime_Work','Actual_Work', 'Remaining_Work',
    'Actual_Overtime_Work','Remaining_Overtime_Work']
    assignmentt =['Actual_Overtime_Work', 'Actual_Work', 'Overtime_Work', 'Regular_Work',
    'Remaining_Overtime_Work','Remaining_Work', 'Work', 'Budget_Work']
    assignment_tp = ['Time_Phased_Value']

    result_tables_dict = {}
    for tables in conf.values():
        #Iterate each table in config
        for table in tables.items():
            newDF = pd.DataFrame()
            rowData = {}
            if table[0] == 'Ms_Project_Header_Xml':
                for target_col,src_col in table[1].items():
                    if target_col == 'Project_File_Nm':
                        rowData[target_col] = [filename]
                    else:
                        rowData[target_col] = [root.findtext(src_col)]
                rowData['Batch_Run_Id'] = [filename]
                rowDF = pd.DataFrame(rowData)
                newDF = pd.concat([newDF,rowDF], ignore_index=True)
                result_tables_dict['Ms_Project_Header_Xml'] = newDF
            if table[0] == 'Ms_Project_Resource_Xml':
                for resources in root.iterfind('Resources'):
                    for resource in resources.iterfind('Resource'):
                        for target_col,src_col in table[1].items():
                            rowData[target_col] = [resource.findtext(src_col)]
                        rowData['Batch_Run_Id'] = [filename]
                        rowDF = pd.DataFrame(rowData)
                        newDF = pd.concat([newDF,rowDF], ignore_index=True)
                newDF = newDF.apply(lambda x: iso8601duration_to_seconds(x) if x.name in resourcee else x)
                result_tables_dict['Ms_Project_Resource_Xml'] = newDF
            if table[0] == 'Ms_Project_Task_Xml':
                for tasks in root.iterfind('Tasks'):
                    for task in tasks.iterfind('Task'):
                        for target_col,src_col in table[1].items():
                            rowData[target_col] = [task.findtext(src_col)]
                        rowData['Batch_Run_Id'] = [filename]
                        rowDF = pd.DataFrame(rowData)
                        newDF = pd.concat([newDF,rowDF], ignore_index=True)
                newDF = newDF.apply(lambda x: iso8601duration_to_seconds(x) if x.name in taskk else x)
                result_tables_dict['Ms_Project_Task_Xml'] = newDF
            if table[0] == 'Ms_Project_Assignment_Xml':
                for assignments in root.iterfind('Assignments'):
                    for assignment in assignments.iterfind('Assignment'):
                        for target_col,src_col in table[1].items():
                            rowData[target_col] = [assignment.findtext(src_col)]
                        rowData['Batch_Run_Id'] = [filename]
                        rowDF = pd.DataFrame(rowData)
                        newDF = pd.concat([newDF,rowDF], ignore_index=True)
                newDF = newDF.apply(lambda x: iso8601duration_to_seconds(x) if x.name in assignmentt else x)
                result_tables_dict['Ms_Project_Assignment_Xml'] = newDF
            if table[0] == 'Ms_Project_Calendar_Xml':
                for calendars in root.iterfind('Calendars'):
                    for calendar in calendars.iterfind('Calendar'):
                        for target_col,src_col in table[1].items():
                            rowData[target_col] = [calendar.findtext(src_col)]
                        rowData['Batch_Run_Id'] = [filename]
                        rowDF = pd.DataFrame(rowData)
                        newDF = pd.concat([newDF,rowDF], ignore_index=True)
                result_tables_dict['Ms_Project_Calendar_Xml'] = newDF
            if table[0] == 'Ms_Project_Assignment_Time_Phased_Data_Xml':
                for assignments in root.iterfind('Assignments'):
                    for assignment in assignments.iterfind('Assignment'):
                        for tphd in assignment.iterfind('TimephasedData'):
                            for target_col,src_col in table[1].items():
                                if target_col == 'Ms_Project_Assignment_Id':
                                    rowData[target_col] = [assignment.findtext(src_col)]
                                else:
                                    rowData[target_col] = [tphd.findtext(src_col)]
                            rowData['Batch_Run_Id'] = [filename]
                            rowDF = pd.DataFrame(rowData)
                            newDF = pd.concat([newDF,rowDF], ignore_index=True)
                    newDF = newDF.apply(lambda x: iso8601duration_to_seconds(x) if x.name in assignment_tp else x)
                    result_tables_dict['Ms_Project_Assignment_Time_Phased_Data_Xml'] = newDF
            if table[0] == 'Ms_Project_Task_Time_Phased_Data_Xml':
                for tasks in root.iterfind('Tasks'):
                    for task in tasks.iterfind('Task'):
                        if task.find('TimephasedData'):
                            for tph in task.iterfind('TimephasedData'):
                                for target_col,src_col in table[1].items():
                                    if target_col == 'Ms_Project_Task_Id':
                                        rowData[target_col] = [task.findtext(src_col)]
                                    else:
                                        rowData[target_col] = [tph.findtext(src_col)]
                                rowData['Batch_Run_Id'] = [filename]
                                rowDF = pd.DataFrame(rowData)
                                newDF = pd.concat([newDF,rowDF], ignore_index=True)
                    result_tables_dict['Ms_Project_Task_Time_Phased_Data_Xml'] = newDF
            if table[0] == 'Ms_Project_Calendar_Weekday_Xml':
                for calendars in root.iterfind('Calendars'):
                    for calendar in calendars.iterfind('Calendar'):
                        for weekdays in calendar.iterfind('WeekDays'):
                            for weekday in weekdays.iterfind('WeekDay'):
                                for target_col,src_col in table[1].items():
                                    rowData[target_col] = [weekday.findtext(src_col)]
                                    if target_col == 'Ms_Project_Calendar_Id':
                                        rowData[target_col] = [calendar.findtext(src_col)]
                                    if target_col.split("_")[0] == 'Time':
                                        for time_period in weekday.iterfind('TimePeriod'):
                                            rowData[target_col] = [time_period.findtext(src_col)]
                                rowData['Batch_Run_Id'] = [filename]
                                rowDF = pd.DataFrame(rowData)
                                newDF = pd.concat([newDF,rowDF], ignore_index=True)
                            result_tables_dict['Ms_Project_Calendar_Weekday_Xml'] = newDF
            if table[0] == 'Ms_Project_Calendar_Weekday_Times_Xml':
                for calendars in root.iterfind('Calendars'):
                    for calendar in calendars.iterfind('Calendar'):
                        for weekdays in calendar.iterfind('WeekDays'):
                            for weekday in weekdays.iterfind('WeekDay'):
                                if weekday.find('WorkingTimes'):
                                    for WorkingTimes in weekday.iterfind('WorkingTimes'):
                                        for wt in WorkingTimes.iterfind('WorkingTime'):
                                            for target_col,src_col in table[1].items():
                                                if not target_col.split("_")[1] == 'Working':
                                                    if target_col == 'Ms_Project_Calendar_Id':
                                                        rowData[target_col] = [calendar.findtext(src_col)]
                                                    else:
                                                        rowData[target_col] = [weekday.findtext(src_col)]
                                                else:
                                                    rowData[target_col] = [wt.findtext(src_col)]
                                            rowData['Batch_Run_Id'] = [filename]
                                            rowDF = pd.DataFrame(rowData)
                                            newDF = pd.concat([newDF,rowDF], ignore_index=True)
                            result_tables_dict['Ms_Project_Calendar_Weekday_Times_Xml'] = newDF
            if table[0] == 'Ms_Project_Calendar_Exception_Xml':
                for calendars in root.iterfind('Calendars'):
                    for calendar in calendars.iterfind('Calendar'):
                        for exceptions in calendar.iterfind('Exceptions'):
                            for ex in exceptions.iterfind('Exception'):
                                for target_col,src_col in table[1].items():
                                    if target_col == 'Exception_From_Dt' or target_col == 'Exception_To_Dt':
                                        for tp in ex.iterfind('TimePeriod'):
                                            rowData[target_col] = [tp.findtext(src_col)]
                                    elif target_col == 'Ms_Project_Calendar_Id':
                                        rowData[target_col] = calendar.findtext(src_col)
                                    else:
                                        rowData[target_col] = [ex.findtext(src_col)]
                                rowData['Batch_Run_Id'] = [filename]
                                rowDF = pd.DataFrame(rowData)
                                newDF = pd.concat([newDF,rowDF], ignore_index=True)
                            result_tables_dict['Ms_Project_Calendar_Exception_Xml'] = newDF
            if table[0] == 'Ms_Project_Calendar_Exception_Times_Xml':
                for calendars in root.iterfind('Calendars'):
                    for calendar in calendars.iterfind('Calendar'):
                        for exceptions in calendar.iterfind('Exceptions'):
                            for ex in exceptions.iterfind('Exception'):
                                if ex.find('WorkingTimes'):
                                    for WorkingTimes in ex.iterfind('WorkingTimes'):
                                        for wt in WorkingTimes.iterfind('WorkingTime'):
                                            for target_col,src_col in table[1].items():
                                                if target_col == 'Day_Working':
                                                    rowData[target_col] = [ex.findtext(src_col)]
                                                elif target_col == 'Ms_Project_Calendar_Id':
                                                    rowData[target_col] = calendar.findtext(src_col)
                                                elif target_col == 'Unique_Key':
                                                    rowData[target_col] = [ex.findtext(src_col)]
                                                else:
                                                    rowData[target_col] = [wt.findtext(src_col)]
                                            rowData['Batch_Run_Id'] = [filename]
                                            rowDF = pd.DataFrame(rowData)
                                            newDF = pd.concat([newDF,rowDF], ignore_index=True)
                            result_tables_dict['Ms_Project_Calendar_Exception_Times_Xml'] = newDF
            if table[0] == 'Ms_Project_Predecessor_Link_Xml':
                for tasks in root.iterfind('Tasks'):
                    for task in tasks.iterfind('Task'):
                        if task.find('PredecessorLink'):
                            for pl in task.iterfind('PredecessorLink'):
                                for target_col,src_col in table[1].items():
                                    if target_col == 'Ms_Project_Task_Id':
                                        rowData[target_col] = [task.findtext(src_col)]
                                    else:
                                        rowData[target_col] = [pl.findtext(src_col)]
                                rowData['Batch_Run_Id'] = [filename]
                                rowDF = pd.DataFrame(rowData)
                                newDF = pd.concat([newDF,rowDF], ignore_index=True)
                    result_tables_dict['Ms_Project_Predecessor_Link_Xml'] = newDF

    return result_tables_dict

def execute_sqlfile(CEDW_dbConn, filepath, batch_run_id=None):
    '''Execute one or more string SQL Commands within a sql file using a dbConn object. Replace any instances of 
    {Batch_Run_Id} with batch_run_id
    '''
    with open(filepath, 'r') as sql_file:
        commands = sql_file.read().split(";")

    for command in commands:
        try:
            if command:
                command = command.replace('{Batch_Run_Id}',batch_run_id)
                CEDW_dbConn.executeSqlStmt(command)
        except:
            print(f"Error: {command}")
            exit(1)
