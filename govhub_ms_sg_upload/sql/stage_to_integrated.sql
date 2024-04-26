INSERT INTO CEDW_HUB_DEV.MAIN.PROJECT_PLAN_HEADER_WTF (Project_Id
,Staging_Unique_Key
,Source_File_Nm
,Save_Version
,Project_File_Nm
,Project_Nm
,Project_File_Author_Nm
,File_Creation_Dt
,File_Last_Saved_Dt
,Schedule_From_Start_Ind
,Project_Start_Dt
,Project_End_Dt
,Fy_Start_Dt_Ind
,Critical_Slack_Limit
,Currency_Digits
,Currency_Symbol
,Currency_Cd
,Currency_Symbol_Position
,Calendar_Uid
,Default_Start_Tm
,Default_Finish_Tm
,Minutes_Per_Day
,Minutes_Per_Week
,Days_Per_Month
,Default_Task_Type
,Default_Fixed_Cost_Accrual
,Default_Standard_Rate
,Default_Overtime_Rate
,Duration_Format
,Work_Format
,Editable_Actual_Costs
,Honor_Constraints
,Inserted_Projects_Like_Summary
,Multiple_Critical_Paths
,New_Tasks_Effort_Driven
,New_Tasks_Estimated
,Splits_In_Progress_Tasks
,Spread_Actual_Cost
,Spread_Percent_Complete
,Task_Updts_Resource
,Fiscal_Year_Start
,Week_Start_Day
,Move_Completed_Ends_Back
,Move_Remaining_Starts_Back
,Move_Remaining_Starts_Forward
,Move_Completed_Ends_Forward
,Baseline_For_Earned_Value
,Auto_Add_New_Resources_And_Tasks
,Current_Dt
,Microsoft_Project_Server_Url
,Autolink
,New_Task_Start_Dt
,New_Tasks_Are_Manual
,Default_Task_Ev_Method
,Project_Externally_Edited
,Extended_Creation_Dt
,Actuals_In_Sync
,Remove_File_Properties
,Admin_Project
,Update_Manually_Scheduled_Tasks
,Keep_Task_On_Nearest_Working_Time
,Sg_File_Content_Type
,Sg_Project_Title
,Sg_Project_Subject)
SELECT
substring(project_file_nm,charindex('_',project_file_nm)+1,8) 
,Unique_Key
,Project_File_Nm
,Save_Version
,Project_File_Nm
,Project_Nm
,Project_File_Author_Nm
,cast(File_Creation_Dt as datetime)
,cast(File_Last_Saved_Dt as datetime)
,Schedule_From_Start_Ind
,cast(Project_Start_Dt as datetime)
,cast(Project_End_Dt as datetime)
,Fy_Start_Dt_Ind
,Critical_Slack_Limit
,Currency_Digits
,Currency_Symbol
,Currency_Cd
,Currency_Symbol_Position
,Calendar_Uid
,Default_Start_Tm
,Default_Finish_Tm
,Minutes_Per_Day
,Minutes_Per_Week
,Days_Per_Month
,Default_Task_Type
,Default_Fixed_Cost_Accrual
,Default_Standard_Rate
,Default_Overtime_Rate
,Duration_Format
,Work_Format
,Editable_Actual_Costs
,Honor_Constraints
,Inserted_Projects_Like_Summary
,Multiple_Critical_Paths
,New_Tasks_Effort_Driven
,New_Tasks_Estimated
,Splits_In_Progress_Tasks
,Spread_Actual_Cost
,Spread_Percent_Complete
,Task_Updts_Resource
,Fiscal_Year_Start
,Week_Start_Day
,Move_Completed_Ends_Back
,Move_Remaining_Starts_Back
,Move_Remaining_Starts_Forward
,Move_Completed_Ends_Forward
,Baseline_For_Earned_Value
,Auto_Add_New_Resources_And_Tasks
,cast(Current_Dt as datetime)
,Microsoft_Project_Server_Url
,Autolink
,CASE 
WHEN New_Task_Start_Dt = 0 THEN NULL 
ELSE cast(New_Task_Start_Dt as datetime) END
,New_Tasks_Are_Manual
,Default_Task_Ev_Method
,Project_Externally_Edited
,cast(Extended_Creation_Dt as datetime)
,Actuals_In_Sync
,Remove_File_Properties
,Admin_Project
,Update_Manually_Scheduled_Tasks
,Keep_Task_On_Nearest_Working_Time
,Sg_File_Content_Type
,Sg_Project_Title
,Sg_Project_Subject
FROM PXLTD_STAGING_DEV.MAIN.MS_PROJECT_HEADER_XML
where Batch_Run_Id = '{Batch_Run_Id}';

INSERT INTO CEDW_HUB_DEV.MAIN.Project_Plan_Task_WTF 
(
Project_Plan_Header_Id
,Staging_Unique_Key
,Project_Task_Uid
,Project_Task_Parent_Uid
,Task_Nm
,Task_Active_Ind
,Task_Manual_Ind
,Task_Type
,Task_Isnull_Ind
,Task_Create_Dt
,Wbs
,Outline_Number
,Outline_Level
,Task_Priority
,Task_Start_Dt
,Task_Finish_Dt
,Task_Duration
,Manual_Start_Dt
,Manual_Finish_Dt
,Manual_Duration
,Duration_Format
,Task_Work
,Task_Stop_Dt
,Task_Resume_Dt
,Resume_Valid
,Effort_Driven
,Recurring
,Over_Allocated
,Estimated
,Milestone
,Summary
,Display_As_Summary
,Critical
,Is_Subproject
,Is_Subproject_Read_Only
,External_Task
,Early_Start_Dt
,Early_Finish_Dt
,Late_Start_Dt
,Late_Finish_Dt
,Start_Variance
,Finish_Variance
,Work_Variance
,Free_Slack
,Total_Slack
,Start_Slack
,Finish_Slack
,Fixed_Cost
,Fixed_Cost_Accrual
,Percent_Complete
,Percent_Work_Complete
,Cost
,Overtime_Cost
,Overtime_Work
,Actual_Start_Dt
,Actual_Finish_Dt
,Actual_Duration
,Actual_Cost
,Actual_Overtime_Cost
,Actual_Work
,Actual_Overtime_Work
,Regular_Work
,Remaining_Duration
,Remaining_Cost
,Remaining_Work
,Remaining_Overtime_Cost
,Remaining_Overtime_Work
,Acwp
,Cv
,Constraint_Type
,Calendar_Uid
,Deadline_Dt
,Constraint_Dt
,Level_Assignments
,Leveling_Can_Split
,Leveling_Delay
,Leveling_Delay_Format
,Ignore_Resource_Calendar
,Notes
,Hide_Bar
,Rollup
,Bcws
,Bcwp
,Physical_Percent_Complete
,Earned_Value_Method
,Is_Published
,Commitment_Type
,Hyperlink_Nm
,Hyperlink_Address)
SELECT 
B.Project_Plan_Header_Id
,A.Unique_Key
,A.Project_Task_Uid
,A.Project_Task_Parent_Uid
,A.Task_Nm
,1
,A.Task_Manual_Ind
,A.Task_Type
,A.Task_Isnull_Ind
,cast(A.Task_Create_Dt as datetime)
,A.Wbs
,A.Outline_Number
,A.Outline_Level
,A.Task_Priority
,cast(A.Task_Start_Dt as datetime)
,cast(A.Task_Finish_Dt as datetime)
,A.Task_Duration
,cast(A.Manual_Start_Dt as datetime)
,cast(A.Manual_Finish_Dt as datetime)
,A.Manual_Duration
,A.Duration_Format
,A.Task_Work
,cast(A.Task_Stop_Dt as datetime)
,cast(A.Task_Resume_Dt as datetime)
,A.Resume_Valid
,A.Effort_Driven
,A.Recurring
,A.Over_Allocated
,A.Estimated
,A.Milestone
,A.Summary
,A.Display_As_Summary
,A.Critical
,A.Is_Subproject
,A.Is_Subproject_Read_Only
,A.External_Task
,cast(A.Early_Start_Dt as datetime)
,cast(A.Early_Finish_Dt as datetime)
,cast(A.Late_Start_Dt as datetime)
,cast(A.Late_Finish_Dt as datetime)
,A.Start_Variance
,A.Finish_Variance
,A.Work_Variance
,A.Free_Slack
,A.Total_Slack
,A.Start_Slack
,A.Finish_Slack
,A.Fixed_Cost
,A.Fixed_Cost_Accrual
,A.Percent_Complete
,A.Percent_Work_Complete
,A.Cost
,A.Overtime_Cost
,A.Overtime_Work
,cast(A.Actual_Start_Dt as datetime)
,cast(A.Actual_Finish_Dt as datetime)
,A.Actual_Duration
,A.Actual_Cost
,A.Actual_Overtime_Cost
,A.Actual_Work
,A.Actual_Overtime_Work
,A.Regular_Work
,A.Remaining_Duration
,A.Remaining_Cost
,A.Remaining_Work
,A.Remaining_Overtime_Cost
,A.Remaining_Overtime_Work
,A.Acwp
,A.Cv
,A.Constraint_Type
,A.Calendar_Uid
,A.Deadline_Dt
,A.Constraint_Dt
,A.Level_Assignments
,A.Leveling_Can_Split
,A.Leveling_Delay
,A.Leveling_Delay_Format
,A.Ignore_Resource_Calendar
,A.Notes
,A.Hide_Bar
,A.Rollup
,A.Bcws
,A.Bcwp
,A.Physical_Percent_Complete
,A.Earned_Value_Method
,A.Is_Published
,A.Commitment_Type
,A.Hyperlink_Nm
,A.Hyperlink_Address
FROM PXLTD_STAGING_DEV.MAIN.Ms_Project_Task_Xml A
JOIN CEDW_HUB_DEV.MAIN.Project_Plan_Header_WTF B 
ON A.Ms_Project_Header_Id = B.Staging_Unique_Key
where A.Batch_Run_Id = '{Batch_Run_Id}';

INSERT INTO CEDW_HUB_DEV.MAIN.Project_Plan_Resource_WTF
(
Project_Plan_Header_Id
,Staging_Unique_Key
,Resource_Uid
,Resource_Id
,Resource_Nm
,Resource_Type
,Resource_Is_Null
,Initials
,Work_Group
,Max_Units
,Peak_Units
,Over_Allocated
,Can_Level
,Accruel_At
,Work
,Regular_Work
,Overtime_Work
,Actual_Work
,Remaining_Work
,Actual_Overtime_Work
,Remaining_Overtime_Work
,Percent_Work_Complete
,Standard_Rate
,Standard_Rate_Format
,Cost
,Overtime_Rate
,Overtime_Rate_Format
,Overtime_Cost
,Cost_Per_Use
,Actual_Cost
,Actual_Overtime_Cost
,Remaining_Cost
,Remaining_Overtime_Cost
,Work_Variance
,Cost_Variance
,Sv
,Cv
,Acwp
,Calendar_Uid
,Bcws
,Bcwp
,Is_Generic
,Is_Inactive
,Is_Enterprise
,Booking_Type
,Creation_Dt
,Is_Cost_Resource
,Is_Budget
,Email_Address
)
SELECT 
B.Project_Plan_Header_Id
,A.Unique_Key
,A.Resource_Uid
,A.Uid
,A.Resource_Nm
,A.Resource_Type
,A.Resource_Is_Null
,A.Initials
,A.Work_Group
,A.Max_Units
,A.Peak_Units
,A.Over_Allocated
,A.Can_Level
,A.Accruel_At
,A.Work
,A.Regular_Work
,A.Overtime_Work
,A.Actual_Work
,A.Remaining_Work
,A.Actual_Overtime_Work
,A.Remaining_Overtime_Work
,A.Percent_Work_Complete
,A.Standard_Rate
,A.Standard_Rate_Format
,A.Cost
,A.Overtime_Rate
,A.Overtime_Rate_Format
,A.Overtime_Cost
,A.Cost_Per_Use
,A.Actual_Cost
,A.Actual_Overtime_Cost
,A.Remaining_Cost
,A.Remaining_Overtime_Cost
,A.Work_Variance
,A.Cost_Variance
,A.Sv
,A.Cv
,A.Acwp
,A.Calendar_Uid
,A.Bcws
,A.Bcwp
,A.Is_Generic
,A.Is_Inactive
,A.Is_Enterprise
,A.Booking_Type
,cast(A.Creation_Dt as datetime)
,A.Is_Cost_Resource
,A.Is_Budget
,A.Email_Address
FROM PXLTD_STAGING_DEV.MAIN.Ms_Project_Resource_Xml A
JOIN CEDW_HUB_DEV.MAIN.Project_Plan_Header_WTF B 
ON A.Ms_Project_Header_Id = B.Staging_Unique_Key
where A.Batch_Run_Id = '{Batch_Run_Id}';

INSERT INTO CEDW_HUB_DEV.MAIN.Project_Plan_Calendar_WTF
(
Project_Plan_Header_Id
,Staging_Unique_Key
,Calendar_Uid
,Calendar_Nm
,Is_Base_Calendar_Ind
,Is_Baseline_Calendar_Ind
,Base_Calendar_Uid
)
SELECT 
B.Project_Plan_Header_Id
,A.Unique_Key
,A.Calendar_Uid
,A.Calendar_Nm
,A.Is_Base_Calendar_Ind
,A.Is_Baseline_Calendar_Ind
,A.Base_Calendar_Uid
FROM PXLTD_STAGING_DEV.MAIN.Ms_Project_Calendar_Xml A
JOIN CEDW_HUB_DEV.MAIN.Project_Plan_Header_WTF B 
ON A.Ms_Project_Header_Id = B.Staging_Unique_Key
where A.Batch_Run_Id = '{Batch_Run_Id}';

INSERT INTO CEDW_HUB_DEV.MAIN.Project_Plan_Assignment_WTF
(
Project_Plan_Header_Id
,Staging_Unique_Key
,Assignment_Uid
,Project_Task_Uid
,Resource_Uid
,Percent_Work_Complete
,Actual_Cost
,Actual_Finish_Dt
,Actual_Overtime_Cost
,Actual_Overtime_Work
,Actual_Start_Dt
,Actual_Work
,Acwp
,Confirmed
,Cost
,Cost_Rate_Table
,Rate_Scale
,Cost_Variance
,Cv
,Delay
,Finish_Dt
,Finish_Variance
,Work_Variance
,Has_Fixed_Rate_Units
,Fixed_Material
,Leveling_Delay
,Leveling_Delay_Format
,Linked_Fields
,Milestone
,Overallocated
,Overtime_Cost
,Overtime_Work
,Regular_Work
,Remaining_Cost
,Remaining_Overtime_Cost
,Remaining_Overtime_Work
,Remaining_Work
,Response_Pending
,Start_Dt
,Stop_Dt
,Resume_Dt
,Start_Variance
,Units
,Update_Needed
,Vac
,Work
,Work_Contour
,Bcws
,Bcwp
,Booking_Type
,Creation_Dt
,Budget_Cost
,Budget_Work
)
SELECT 
B.Project_Plan_Header_Id
,A.Unique_Key
,A.Assignment_Uid
,A.Project_Task_Uid
,A.Resource_Uid
,A.Percent_Work_Complete
,A.Actual_Cost
,cast(A.Actual_Finish_Dt as datetime)
,A.Actual_Overtime_Cost
,A.Actual_Overtime_Work
,cast(A.Actual_Start_Dt as datetime)
,A.Actual_Work
,A.Acwp
,A.Confirmed
,A.Cost
,A.Cost_Rate_Table
,A.Rate_Scale
,A.Cost_Variance
,A.Cv
,A.Delay
,cast(A.Finish_Dt as datetime)
,A.Finish_Variance
,A.Work_Variance
,A.Has_Fixed_Rate_Units
,A.Fixed_Material
,A.Leveling_Delay
,A.Leveling_Delay_Format
,A.Linked_Fields
,A.Milestone
,A.Overallocated
,A.Overtime_Cost
,A.Overtime_Work
,A.Regular_Work
,A.Remaining_Cost
,A.Remaining_Overtime_Cost
,A.Remaining_Overtime_Work
,A.Remaining_Work
,A.Response_Pending
,cast(A.Start_Dt as datetime)
,cast(A.Stop_Dt as datetime)
,cast(A.Resume_Dt as datetime)
,A.Start_Variance
,A.Units
,A.Update_Needed
,A.Vac
,A.Work
,A.Work_Contour
,A.Bcws
,A.Bcwp
,A.Booking_Type
,cast(A.Creation_Dt as datetime)
,A.Budget_Cost
,A.Budget_Work
FROM PXLTD_STAGING_DEV.MAIN.Ms_Project_Assignment_Xml A 
JOIN CEDW_HUB_DEV.MAIN.Project_Plan_Header_WTF B 
ON A.Ms_Project_Header_Id = B.Staging_Unique_Key
where A.Batch_Run_Id = '{Batch_Run_Id}';

INSERT INTO CEDW_HUB_DEV.MAIN.Project_Plan_Task_Time_Phased_Data_WTF
(Project_Plan_Task_Id
,Staging_Unique_Key
,Task_Type
,Task_Uid
,Time_Phased_Start_Dt
,Time_Phased_Finish_Dt
,Time_Phased_Unit
,Time_Phased_Value)
SELECT 
B.Project_Plan_Task_Id
,A.Unique_Key
,A.Task_Type
,A.Task_Uid
,A.Time_Phased_Start_Dt
,A.Time_Phased_Finish_Dt
,A.Time_Phased_Unit
,A.Time_Phased_Value
FROM PXLTD_STAGING_DEV.MAIN.Ms_Project_Task_Time_Phased_Data_Xml A
JOIN CEDW_HUB_DEV.MAIN.Project_Plan_Task_WTF B 
ON A.Ms_Project_Task_Id = B.Staging_Unique_Key
where A.Batch_Run_Id = '{Batch_Run_Id}';

INSERT INTO CEDW_HUB_DEV.MAIN.Project_Plan_Assignment_Time_Phased_Data_WTF
(Project_Plan_Assignment_Id
,Staging_Unique_Key
,Task_Type
,Task_Uid
,Time_Phased_Start_Dt
,Time_Phased_Finish_Dt
,Time_Phased_Unit
,Time_Phased_Value)
SELECT 
B.Project_Plan_Assignment_Id
,A.Unique_Key
,A.Task_Type
,A.Task_Uid
,A.Time_Phased_Start_Dt
,A.Time_Phased_Finish_Dt
,A.Time_Phased_Unit
,A.Time_Phased_Value
FROM PXLTD_STAGING_DEV.MAIN.Ms_Project_Assignment_Time_Phased_Data_Xml A
JOIN CEDW_HUB_DEV.MAIN.Project_Plan_Assignment_WTF B 
ON A.Ms_Project_Assignment_Id = B.Staging_Unique_Key
where A.Batch_Run_Id = '{Batch_Run_Id}';

INSERT INTO CEDW_HUB_DEV.MAIN.Project_Plan_Predecessor_Link_WTF
(Project_Plan_Task_Id
,Staging_Unique_Key
,Predecessor_Uid
,Predecessor_Type
,Cross_Project
,Link_Lag
,Lag_Format)
SELECT 
B.Project_Plan_Task_Id
,A.Unique_Key
,A.Predecessor_Uid
,A.Predecessor_Type
,A.Cross_Project
,A.Link_Lag
,A.Lag_Format 
FROM PXLTD_STAGING_DEV.MAIN.Ms_Project_Predecessor_Link_Xml A
JOIN CEDW_HUB_DEV.MAIN.Project_Plan_Task_WTF B 
ON A.Ms_Project_Task_Id = B.Staging_Unique_Key
where A.Batch_Run_Id = '{Batch_Run_Id}';

CREATE OR REPLACE TEMPORARY TABLE PXLTD_STAGING_DEV.MAIN.SEPERATED AS 
SELECT *, ROW_NUMBER() OVER (PARTITION BY MS_PROJECT_HEADER_ID, MS_PROJECT_CALENDAR_ID, BATCH_RUN_ID, DAY_TYPE_NUM ORDER BY UNIQUE_KEY DESC) rn  
FROM PXLTD_STAGING_DEV.MAIN.MS_PROJECT_CALENDAR_WEEKDAY_TIMES_XML
WHERE BATCH_RUN_ID = '{Batch_Run_Id}';

INSERT INTO CEDW_HUB_DEV.MAIN.Project_Plan_Calendar_Weekday_WTF
(
Project_Plan_Calendar_Id
,Staging_Unique_Key
,Day_Type_Num
,Working_Day_Ind
,From_Working_Time
,To_Working_Time
,From_Working_Time_2
,To_Working_Time_2
,Time_Period_From_Dt
,Time_Period_To_Dt
)
SELECT 
F.Project_Plan_Calendar_Id,
E.Unique_Key as Staging_Unique_Key,
E.Day_Type_Num,
E.Working_Day_Ind,
E.From_Working_Time,
E.To_Working_Time,
E.FROM_WORKING_TIME_2,
E.TO_WORKING_TIME_2,
E.Time_Period_From_Dt,
E.Time_Period_To_Dt
FROM 
(SELECT D.*, C.From_Working_Time as FROM_WORKING_TIME_2, C.To_Working_Time as TO_WORKING_TIME_2
FROM
(SELECT A.*, B.From_Working_Time, B.To_Working_Time
FROM 
(SELECT * 
FROM PXLTD_STAGING_DEV.MAIN.Ms_Project_Calendar_Weekday_Xml
WHERE Batch_Run_Id = '{Batch_Run_Id}') A 
LEFT JOIN 
(SELECT * 
FROM PXLTD_STAGING_DEV.MAIN.SEPERATED 
where rn = 2) B
ON A.Ms_Project_Header_Id = B.Ms_Project_Header_Id
AND A.Ms_Project_Calendar_Id= B.Ms_Project_Calendar_Id 
AND A.Batch_Run_Id = B.Batch_Run_Id
AND A.Day_Type_Num = B.Day_Type_Num) D
LEFT JOIN (SELECT * 
FROM PXLTD_STAGING_DEV.MAIN.SEPERATED 
where rn = 1) C
ON D.Ms_Project_Header_Id = C.Ms_Project_Header_Id
AND D.Ms_Project_Calendar_Id= C.Ms_Project_Calendar_Id 
AND D.Batch_Run_Id = C.Batch_Run_Id
AND D.Day_Type_Num = C.Day_Type_Num) E
JOIN CEDW_HUB_DEV.MAIN.Project_Plan_Calendar_WTF F 
ON E.Ms_Project_Calendar_Id = F.Staging_Unique_Key;

CREATE OR REPLACE TEMPORARY TABLE PXLTD_STAGING_DEV.MAIN.SEPERATED AS 
SELECT *, ROW_NUMBER() OVER (PARTITION BY MS_PROJECT_HEADER_ID, MS_PROJECT_CALENDAR_ID, BATCH_RUN_ID, DAY_WORKING ORDER BY UNIQUE_KEY DESC) rn  
FROM PXLTD_STAGING_DEV.MAIN.MS_PROJECT_CALENDAR_EXCEPTION_TIMES_XML
WHERE BATCH_RUN_ID = '{Batch_Run_Id}';

INSERT INTO CEDW_HUB_DEV.MAIN.Project_Plan_Calendar_Exception_WTF
(
Project_Plan_Calendar_Id,
Project_Plan_Header_Id,
Staging_Unique_Key,
Entered_By_Occurrences,
Exception_Type,
Day_Working,
From_Working_Time,
To_Working_Time,
FROM_WORKING_TIME_2,
TO_WORKING_TIME_2,
Exception_From_Dt,
Exception_To_Dt
)
SELECT
F.Project_Plan_Calendar_Id,
F.Project_Plan_Header_Id,
E.Unique_Key as Staging_Unique_Key,
E.Entered_By_Occurrences,
E.Exception_Type,
E.Day_Working,
E.From_Working_Time,
E.To_Working_Time,
E.FROM_WORKING_TIME_2,
E.TO_WORKING_TIME_2,
E.Exception_From_Dt,
E.Exception_To_Dt
FROM 
(SELECT D.*, C.From_Working_Time as FROM_WORKING_TIME_2, C.To_Working_Time as TO_WORKING_TIME_2
FROM
(SELECT A.*, B.From_Working_Time, B.To_Working_Time
FROM 
(SELECT * 
FROM PXLTD_STAGING_DEV.MAIN.Ms_Project_Calendar_Exception_Xml
WHERE Batch_Run_Id = '{Batch_Run_Id}') A 
LEFT JOIN 
(SELECT * 
FROM PXLTD_STAGING_DEV.MAIN.SEPERATED 
where rn = 2) B
ON A.Ms_Project_Header_Id = B.Ms_Project_Header_Id
AND A.Ms_Project_Calendar_Id= B.Ms_Project_Calendar_Id 
AND A.Batch_Run_Id = B.Batch_Run_Id
AND A.Day_Working = B.Day_Working) D
LEFT JOIN (SELECT * 
FROM PXLTD_STAGING_DEV.MAIN.SEPERATED 
where rn = 1) C
ON D.Ms_Project_Header_Id = C.Ms_Project_Header_Id
AND D.Ms_Project_Calendar_Id= C.Ms_Project_Calendar_Id 
AND D.Batch_Run_Id = C.Batch_Run_Id
AND D.Day_Working = C.Day_Working) E
JOIN CEDW_HUB_DEV.MAIN.Project_Plan_Calendar_WTF F 
ON E.Ms_Project_Calendar_Id = F.Staging_Unique_Key;