DELETE FROM CEDW_HUB_DEV.MAIN.Project_Plan_Predecessor_Link_WTF
WHERE Project_Plan_Task_Id in (
SELECT DISTINCT Project_Plan_Task_Id
FROM CEDW_HUB_DEV.MAIN.Project_Plan_Task_WTF
WHERE Project_Plan_Header_Id in 
(SELECT DISTINCT Project_Plan_Header_Id 
FROM CEDW_HUB_DEV.MAIN.Project_Plan_Header_WTF
WHERE Source_File_Nm = '{Batch_Run_Id}'));

DELETE FROM CEDW_HUB_DEV.MAIN.Project_Plan_Task_Time_Phased_Data_WTF
WHERE Project_Plan_Task_Id in (
SELECT DISTINCT Project_Plan_Task_Id
FROM CEDW_HUB_DEV.MAIN.Project_Plan_Task_WTF
WHERE Project_Plan_Header_Id in 
(SELECT DISTINCT Project_Plan_Header_Id 
FROM CEDW_HUB_DEV.MAIN.Project_Plan_Header_WTF
WHERE Source_File_Nm = '{Batch_Run_Id}'));

DELETE FROM CEDW_HUB_DEV.MAIN.Project_Plan_Task_WTF
WHERE Project_Plan_Header_Id in 
(SELECT DISTINCT Project_Plan_Header_Id 
FROM CEDW_HUB_DEV.MAIN.Project_Plan_Header_WTF
WHERE Source_File_Nm = '{Batch_Run_Id}');

DELETE FROM CEDW_HUB_DEV.MAIN.Project_Plan_Assignment_Time_Phased_Data_WTF
WHERE Project_Plan_Assignment_Id in (
SELECT DISTINCT Project_Plan_Assignment_Id
FROM CEDW_HUB_DEV.MAIN.Project_Plan_Assignment_WTF
WHERE Project_Plan_Header_Id in 
(SELECT DISTINCT Project_Plan_Header_Id 
FROM CEDW_HUB_DEV.MAIN.Project_Plan_Header_WTF
WHERE Source_File_Nm = '{Batch_Run_Id}'));

DELETE FROM CEDW_HUB_DEV.MAIN.Project_Plan_Assignment_WTF
WHERE Project_Plan_Header_Id in 
(SELECT DISTINCT Project_Plan_Header_Id 
FROM CEDW_HUB_DEV.MAIN.Project_Plan_Header_WTF
WHERE Source_File_Nm = '{Batch_Run_Id}');

DELETE FROM CEDW_HUB_DEV.MAIN.Project_Plan_Calendar_Weekday_WTF
WHERE Project_Plan_Calendar_Id in (
SELECT DISTINCT Project_Plan_Calendar_Id
FROM CEDW_HUB_DEV.MAIN.Project_Plan_Calendar_WTF
WHERE Project_Plan_Header_Id in 
(SELECT DISTINCT Project_Plan_Header_Id 
FROM CEDW_HUB_DEV.MAIN.Project_Plan_Header_WTF
WHERE Source_File_Nm = '{Batch_Run_Id}'));

DELETE FROM CEDW_HUB_DEV.MAIN.Project_Plan_Calendar_Exception_WTF
WHERE Project_Plan_Calendar_Id in (
SELECT DISTINCT Project_Plan_Calendar_Id
FROM CEDW_HUB_DEV.MAIN.Project_Plan_Calendar_WTF
WHERE Project_Plan_Header_Id in 
(SELECT DISTINCT Project_Plan_Header_Id 
FROM CEDW_HUB_DEV.MAIN.Project_Plan_Header_WTF
WHERE Source_File_Nm = '{Batch_Run_Id}'));

DELETE FROM CEDW_HUB_DEV.MAIN.Project_Plan_Calendar_WTF
WHERE Project_Plan_Header_Id in 
(SELECT DISTINCT Project_Plan_Header_Id 
FROM CEDW_HUB_DEV.MAIN.Project_Plan_Header_WTF
WHERE Source_File_Nm = '{Batch_Run_Id}');

DELETE FROM CEDW_HUB_DEV.MAIN.Project_Plan_Resource_WTF
WHERE Project_Plan_Header_Id in 
(SELECT DISTINCT Project_Plan_Header_Id 
FROM CEDW_HUB_DEV.MAIN.Project_Plan_Header_WTF
WHERE Source_File_Nm = '{Batch_Run_Id}');

DELETE FROM CEDW_HUB_DEV.MAIN.Project_Plan_Header_WTF
WHERE Source_File_Nm = '{Batch_Run_Id}';


