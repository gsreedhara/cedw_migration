DELETE FROM PXLTD_STAGING_DEV.MAIN.Ms_Project_Assignment_Time_Phased_Data_Xml
WHERE Batch_Run_Id = '{Batch_Run_Id}';

DELETE FROM PXLTD_STAGING_DEV.MAIN.Ms_Project_Assignment_Xml
WHERE Batch_Run_Id = '{Batch_Run_Id}';

DELETE FROM PXLTD_STAGING_DEV.MAIN.Ms_Project_Calendar_Exception_Times_Xml
WHERE Batch_Run_Id = '{Batch_Run_Id}';

DELETE FROM PXLTD_STAGING_DEV.MAIN.Ms_Project_Calendar_Exception_Xml
WHERE Batch_Run_Id = '{Batch_Run_Id}';

DELETE FROM PXLTD_STAGING_DEV.MAIN.Ms_Project_Calendar_Weekday_Times_Xml
WHERE Batch_Run_Id = '{Batch_Run_Id}';

DELETE FROM PXLTD_STAGING_DEV.MAIN.Ms_Project_Calendar_Weekday_Xml
WHERE Batch_Run_Id = '{Batch_Run_Id}';

DELETE FROM PXLTD_STAGING_DEV.MAIN.Ms_Project_Calendar_Xml
WHERE Batch_Run_Id = '{Batch_Run_Id}';

DELETE FROM PXLTD_STAGING_DEV.MAIN.Ms_Project_Header_Xml
WHERE Batch_Run_Id = '{Batch_Run_Id}';

DELETE FROM PXLTD_STAGING_DEV.MAIN.Ms_Project_Predecessor_Link_Xml
WHERE Batch_Run_Id = '{Batch_Run_Id}';

DELETE FROM PXLTD_STAGING_DEV.MAIN.Ms_Project_Resource_Xml
WHERE Batch_Run_Id = '{Batch_Run_Id}';

DELETE FROM PXLTD_STAGING_DEV.MAIN.Ms_Project_Task_Time_Phased_Data_Xml
WHERE Batch_Run_Id = '{Batch_Run_Id}';

DELETE FROM PXLTD_STAGING_DEV.MAIN.Ms_Project_Task_Xml
WHERE Batch_Run_Id = '{Batch_Run_Id}';


