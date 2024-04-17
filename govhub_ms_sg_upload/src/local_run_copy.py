import logging,re
import argparse
import json
import pandas as pd
import os, shutil
import sys
#import dbUtil as db
import pandas as pd 
import xml.etree.ElementTree as et

#parser = argparse.ArgumentParser()
#parser.add_argument('--src', required=True)
#args = parser.parse_args()

it = et.iterparse('MS_TEST.xml')
for _, el in it:
    prefix, has_namespace, postfix = el.tag.partition('}')
    if has_namespace:
        el.tag = postfix 
root = it.root

#Open the configuration file containing XML to Table Schema mapping
with open('config.json') as f:
  data = json.load(f)

result_tables_dict = {}
def yt_time(durations):
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
            print(sum([int(x)*60**units.index(x) for x in units]))
            results.append(sum([int(x)*60**units.index(x) for x in units]))
        else:
            results.append('')
    # Do the maths
    return results
#Parse the XML and create a DataFrame for each table stored in Dictionary
result_tables_dict = {}
for tables in data.values():
    for table in tables.items():
        newDF = pd.DataFrame()
        rowData = {}
        if table[0] == 'Ms_Project_Header_Xml':
            for target_col,src_col in table[1].items():
                if target_col == 'Project_File_Nm':
                    rowData[target_col] = 'sup'
                else:
                    rowData[target_col] = [root.findtext(src_col)]
            rowDF = pd.DataFrame(rowData)
            newDF = pd.concat([newDF,rowDF], ignore_index=True)
            result_tables_dict['Ms_Project_Header_Xml'] = newDF
        if table[0] == 'Ms_Project_Resource_Xml':
            for resources in root.iterfind('Resources'):
                for resource in resources.iterfind('Resource'):
                    for target_col,src_col in table[1].items():
                        rowData[target_col] = [resource.findtext(src_col)]
                    rowDF = pd.DataFrame(rowData)
                    newDF = pd.concat([newDF,rowDF], ignore_index=True)
            #newDF = newDF.apply(lambda x: yt_time(x) if x.name in ['Work'] else x)
            result_tables_dict['Ms_Project_Resource_Xml'] = newDF
        if table[0] == 'Ms_Project_Task_Xml':
            for tasks in root.iterfind('Tasks'):
                for task in tasks.iterfind('Task'):
                    for target_col,src_col in table[1].items():
                        rowData[target_col] = [task.findtext(src_col)]
                    rowDF = pd.DataFrame(rowData)
                    newDF = pd.concat([newDF,rowDF], ignore_index=True)
            newDF = newDF.apply(lambda x: yt_time(x) if x.name in ['Task_Duration'] else x)
            result_tables_dict['Ms_Project_Task_Xml'] = newDF
        if table[0] == 'Ms_Project_Assignment_Xml':
            for assignments in root.iterfind('Assignments'):
                for assignment in assignments.iterfind('Assignment'):
                    for target_col,src_col in table[1].items():
                        rowData[target_col] = [assignment.findtext(src_col)]
                    rowDF = pd.DataFrame(rowData)
                    newDF = pd.concat([newDF,rowDF], ignore_index=True)
            result_tables_dict['Ms_Project_Assignment_Xml'] = newDF
        if table[0] == 'Ms_Project_Calendar_Xml':
            for calendars in root.iterfind('Calendars'):
                for calendar in calendars.iterfind('Calendar'):
                    for target_col,src_col in table[1].items():
                        rowData[target_col] = [calendar.findtext(src_col)]
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
                        rowDF = pd.DataFrame(rowData)
                        newDF = pd.concat([newDF,rowDF], ignore_index=True)
                newDF = newDF.apply(lambda x: yt_time(x) if x.name in ['Time_Phased_Value'] else x)
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
                            rowData['Batch_Run_Id'] = ['hi']
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
                                if target_col.split("_")[0] == 'Time':
                                    for time_period in weekday.iterfind('TimePeriod'):
                                        rowData[target_col] = [time_period.findtext(src_col)]
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
                                                if target_col == 'Ms_Project_Task_Time_Phased_Data_Xml':
                                                    rowData[target_col] = [calendar.findtext(src_col)]
                                                else:
                                                    rowData[target_col] = [weekday.findtext(src_col)]
                                            else:
                                                rowData[target_col] = [wt.findtext(src_col)]
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
                            rowDF = pd.DataFrame(rowData)
                            newDF = pd.concat([newDF,rowDF], ignore_index=True)
                        result_tables_dict['Ms_Project_Calendar_Exception_Xml'] = newDF
        if table[0] == 'Ms_Project_Calendar_Exception_Times_Xml':
            for calendars in root.iterfind('Calendars'):
                for calendar in calendars.iterfind('Calendar'):
                    for exceptions in calendar.iterfind('Exceptions'):
                        for ex in weekdays.iterfind('Exception'):
                            if ex.find('WorkingTimes'):
                                for WorkingTimes in weekday.iterfind('WorkingTimes'):
                                    for wt in WorkingTimes.iterfind('WorkingTime'):
                                        for target_col,src_col in table[1].items():
                                            if not target_col == 'Day_Working':
                                                rowData[target_col] = [wt.findtext(src_col)]
                                            elif target_col == 'Ms_Project_Calendar_Id':
                                                rowData[target_col] = calendar.findtext(src_col)
                                            else:
                                                rowData[target_col] = [ex.findtext(src_col)]
                                        rowDF = pd.DataFrame(rowData)
                                        newDF = pd.concat([newDF,rowDF], ignore_index=True)
                        result_tables_dict['Ms_Project_Calendar_Weekday_Times_Xml'] = newDF

result_tables_dict
'''
def attempt_infer_schema(df):
    for k in list(df):
        df[k]=pd.to_numeric(df[k], errors='ignore')
        df[k]=pd.to_datetime(df[k], errors='ignore')
    return df

result_tables_dict['Ms_Project_Header_Xml']= attempt_infer_schema(result_tables_dict['Ms_Project_Header_Xml'])
'''