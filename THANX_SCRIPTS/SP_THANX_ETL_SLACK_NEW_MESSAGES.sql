USE [PXLTD_CEDW]
GO

/****** Object:  StoredProcedure [dbo].[SP_THANX_ETL_SLACK_NEW_MESSAGES]    Script Date: 4/19/2024 9:53:29 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


-- * *********************************************************************************
-- * File Name        : PXLTD_CEDW.dbo.SP_THANX_ETL_SLACK_NEW_MESSAGES
-- *
-- * Description      :    
-- *                   Step1: 
-- *                   Step2: 
-- *				   Step3: 
-- *
-- * Purpose          : Get new Slack messages from Stage table to teh Load Tables
-- *                    
-- * Date Written     : Jan 2019
-- * Input Parameters : days (number of days to consider when retrieving information from Stage
-- * Output Parameters: none
-- * Tables Read      : PXLTD_THANX.dbo.RCGNTN_MSG_SLACK
-- *                    
-- * Tables Updated   : PXLTD_CEDW.dbo.RCGNTN_MSG, RCGNTN_MSG_TO_USR, RCGNTN_MSG_VALUES
-- * Run Frequency    : Every 5 minutes
-- * Developed By     : Tatiana Tagliari - ProjectX
-- * Changeman Member :
-- *   
-- * Version   	Date   		Modified by     Description
-- * v1.0	   	19Jan2019					Code created
-- * *********************************************************************************
CREATE PROCEDURE [dbo].[SP_THANX_ETL_SLACK_NEW_MESSAGES] (@days int)
AS
BEGIN
	--Drop the temporary tables if they exist
	IF OBJECT_ID('tempdb.dbo.#STG1') IS NOT NULL
	BEGIN
	  DROP TABLE #STG1;
	END;

	IF OBJECT_ID('tempdb.dbo.#STG2') IS NOT NULL
	BEGIN
	  DROP TABLE #STG2;
	END;

	-- * *********************************************************************************
	--Select all messages from the stage table and insert them into the #STG1 table
	--Consider messages from the last @days days
	--Get the most updated message from each SRC_TS
	SELECT * INTO #STG1
	FROM PXLTD_THANX.dbo.RCGNTN_MSG_SLACK 
	WHERE INTRNL_TS IN 
		(SELECT MAX(INTRNL_TS) AS INTRNL_TS 
		FROM PXLTD_THANX.dbo.RCGNTN_MSG_SLACK 
		WHERE INTRNL_TS >= GETDATE() - @days
		GROUP BY SRC_TS);
	
	-- * *********************************************************************************
	--From #STG1, select all messages not loaded before through ETL process
	--Select only messages (exclude threads)
	SELECT STG.*
	INTO #STG2
	FROM #STG1 STG
	WHERE (STG.SRC_THRD_TS IS NULL OR STG.SRC_TS = STG.SRC_THRD_TS) --excluding threads
	AND STG.SRC_TS NOT IN 
		(SELECT SRC_TS FROM PXLTD_CEDW.dbo.RCGNTN_MSG)
	
	-- * *********************************************************************************
	--Creating new columns to the temporary table
	ALTER TABLE #STG2 ADD 
		NEW_MSG_TEXT varchar(8000), 
		TAG_CD varchar(500) DEFAULT '' NOT NULL, 
		FRM_EMPL_ID varchar(25), 
		VALUE_CD varchar(500) DEFAULT '' NOT NULL,
		TO_EMPL_ID varchar(5000) DEFAULT '' NOT NULL;
	--Putting MSG_TEXT values into the NEW_MSG_TEXT column
	UPDATE #STG2 SET NEW_MSG_TEXT = MSG_TXT;

	-- * *********************************************************************************
	--Search for TAGS in the messages' text. Only 1 tag per message. If 2 were created, only the last one added is considered.
	--Save the tags on TAG_CD column
	--Clean the message text, deleting the tag part from it and saving into NEW_MSG_TEXT column 
	DECLARE @tag_cd varchar(50); 

	DECLARE tag_cursor CURSOR FOR   
		SELECT tag_cd  
		FROM PXLTD_CEDW.dbo.RCGNTN_TAGS_TYP;  
	OPEN tag_cursor  
  
	FETCH NEXT FROM tag_cursor   
	INTO @tag_cd  
  
	WHILE @@FETCH_STATUS = 0  
	BEGIN     
		UPDATE #STG2 
		SET TAG_CD = @tag_cd -- +'|'+ isnull(TAG_CD,'') ONLY THE LAST TAG ADDED TO THE MESSAGE IS CONSIDERED!
		, NEW_MSG_TEXT = ltrim(replace(NEW_MSG_TEXT, @tag_cd, ''))
		WHERE NEW_MSG_TEXT LIKE '%'+@tag_cd+'%' 

		FETCH NEXT FROM tag_cursor   
		INTO @tag_cd  
	END   
	CLOSE tag_cursor;  
	DEALLOCATE tag_cursor; 
	
	-- * *********************************************************************************
	--Search for VALUES in the messages' text. Eventually, it can be more than 1
	--Save the values on VALUE_CD column, delimited by pipes |
	--Clean the message text, deleting the values part from it and saving into NEW_MSG_TEXT column
	DECLARE @value_cd varchar(50); 

	DECLARE value_cursor CURSOR FOR   
		SELECT value_cd  
		FROM PXLTD_CEDW.dbo.RCGNTN_VALUES_TYP;  
	OPEN value_cursor  
  
	FETCH NEXT FROM value_cursor   
	INTO @value_cd  
  
	WHILE @@FETCH_STATUS = 0  
	BEGIN     
		UPDATE #STG2 
		SET VALUE_CD = @value_cd +'|'+ isnull(VALUE_CD,'')
		, NEW_MSG_TEXT = rtrim(replace(NEW_MSG_TEXT, @value_cd, ''))
		WHERE NEW_MSG_TEXT LIKE '%'+@value_cd+'%' 

		FETCH NEXT FROM value_cursor   
		INTO @value_cd  
	END   
	CLOSE value_cursor;  
	DEALLOCATE value_cursor; 
	
	-- * *********************************************************************************
	--Save the EMP_ID for the FROM user into the temporary table
	UPDATE STG SET STG.FRM_EMPL_ID = EMP.EMPL_ID 
	--SELECT STG.*, EMP.CO_EMAIL, EMP.EMPL_ID
	FROM #STG2 AS STG
	INNER JOIN PXLTD_CEDW.dbo.AV_EMPL_PBLC_DTL AS EMP
		ON STG.FRM_USR_EMAIL = EMP.CO_EMAIL; 
	
	-- * *********************************************************************************
	--Search for TO users into the to_usr_email. Eventually, it can be more than 1
	--Save their EMP_ID on TO_EMPL_ID column, delimited by pipes |
	UPDATE #STG2 SET to_usr_email = to_usr_email + '|' WHERE to_usr_email <> '';
 
	DECLARE @to_usr_email varchar(1000), @intrnl_ts datetime, @src_ts varchar(50), @pos int = 0, @len int = 0, @value varchar(1000); 

	DECLARE emp_cursor CURSOR FOR   
		SELECT intrnl_ts, to_usr_email, src_ts  
		FROM #STG2;  
	OPEN emp_cursor  
  
	FETCH NEXT FROM emp_cursor   
	INTO @intrnl_ts, @to_usr_email, @src_ts
  
	WHILE @@FETCH_STATUS = 0  
	BEGIN 
		--print cast(@intrnl_ts as varchar(50)) +' - '+ @to_usr_email
		WHILE CHARINDEX('|', @to_usr_email, @pos) > 0
			BEGIN
				set @len = CHARINDEX('|', @to_usr_email, @pos) - @pos
				set @value = SUBSTRING(@to_usr_email, @pos, @len) 
				--print @value
				UPDATE STG SET STG.TO_EMPL_ID = EMP.EMPL_ID +'|'+ isnull(STG.TO_EMPL_ID,'')
				--SELECT EMP.EMPL_ID +'|'+ isnull(STG.TO_EMPL_ID,'')
				FROM #STG2 AS STG
				INNER JOIN PXLTD_CEDW.dbo.AV_EMPL_PBLC_DTL AS EMP
					ON @value = EMP.CO_EMAIL
				WHERE @intrnl_ts = STG.intrnl_ts
					AND @src_ts = STG.SRC_TS

				set @pos = CHARINDEX('|', @to_usr_email, @pos+@len) +1
			END
		 set @pos = 0 
		 set @len = 0
		 set @value = ''
	
		FETCH NEXT FROM emp_cursor   
		INTO  @intrnl_ts, @to_usr_email, @src_ts
	END   
	CLOSE emp_cursor;  
	DEALLOCATE emp_cursor; 

	-- * *********************************************************************************
	--Saving the messages to the RCGNTN_MSG table
	--Mandatory to have at least 1 Tag and 1 to_usr
	INSERT INTO PXLTD_CEDW.dbo.RCGNTN_MSG (intrnl_ts, SRC_TS, RCGNTN_MSG_TXT, RCGNTN_TAG_ID, FRM_USR_SLACK_ID, FRM_EMPL_ID)
	SELECT STG.intrnl_ts, STG.SRC_TS, STG.NEW_MSG_TEXT, TAG.RCGNTN_TAG_ID, STG.FRM_USR_SRC_ID, STG.FRM_EMPL_ID
	FROM #STG2 AS STG
	INNER JOIN PXLTD_CEDW.dbo.RCGNTN_TAGS_TYP TAG 
		ON TAG.TAG_CD = STG.TAG_CD
	WHERE STG.TO_USR_EMAIL <> '';
	
	-- * *********************************************************************************
	--Saving the TO_USERS to the RCGNTN_MSG_TO_USR table
	UPDATE #STG2 SET TO_USR_SRC_ID = TO_USR_SRC_ID + '|' WHERE TO_USR_SRC_ID <> '';
 
	DECLARE @TO_USR_SRC_ID varchar(1000), @TO_EMP_ID varchar(1000), @intrnl_ts_ datetime, @src_ts_ varchar(50), 
			@pos1 int = 0, @len1 int = 0, @src_id_found varchar(1000),
			@pos2 int = 0, @len2 int = 0, @emp_id_found varchar(1000);

	DECLARE emp_cursor CURSOR FOR   
		SELECT intrnl_ts, TO_USR_SRC_ID, TO_EMPL_ID, SRC_TS
		FROM #STG2
		WHERE TO_USR_EMAIL <> '';  
	OPEN emp_cursor  
  
	FETCH NEXT FROM emp_cursor   
	INTO @intrnl_ts_, @TO_USR_SRC_ID, @TO_EMP_ID, @src_ts_
  
	WHILE @@FETCH_STATUS = 0  
	BEGIN 
		--print cast(@intrnl_ts as varchar(50)) +' - '+ @TO_USR_SRC_ID + ' - '+ @TO_EMP_ID
		WHILE CHARINDEX('|', @TO_USR_SRC_ID, @pos1) > 0 AND CHARINDEX('|', @TO_EMP_ID, @pos2) > 0
		BEGIN
			set @len1 = CHARINDEX('|', @TO_USR_SRC_ID, @pos1) - @pos1
			set @src_id_found = SUBSTRING(@TO_USR_SRC_ID, @pos1, @len1)

			WHILE PXLTD_THANX.dbo.validateSlackID(@src_id_found) = 0
			BEGIN
				set @pos1 = CHARINDEX('|', @TO_USR_SRC_ID, @pos1+@len1) +1
				set @len1 = CHARINDEX('|', @TO_USR_SRC_ID, @pos1) - @pos1
				set @src_id_found = SUBSTRING(@TO_USR_SRC_ID, @pos1, @len1)
			END
			
			set @len2 = CHARINDEX('|', @TO_EMP_ID, @pos2) - @pos2
			set @emp_id_found = SUBSTRING(@TO_EMP_ID, @pos2, @len2)

			--print @src_id_found +' - '+ @emp_id_found
			INSERT INTO PXLTD_CEDW.dbo.RCGNTN_MSG_TO_USR (RCGNTN_MSG_ID, TO_USR_SLACK_ID, TO_EMPL_ID)
			SELECT MSG.RCGNTN_MSG_ID, @src_id_found, @emp_id_found
			FROM #STG2 STG
			INNER JOIN PXLTD_CEDW.dbo.RCGNTN_MSG MSG 
				ON STG.INTRNL_TS = MSG.INTRNL_TS
			WHERE MSG.INTRNL_TS = @intrnl_ts_
				AND MSG.SRC_TS = @src_ts_
				AND STG.INTRNL_TS = @intrnl_ts_
				AND STG.SRC_TS = @src_ts_

			set @pos1 = CHARINDEX('|', @TO_USR_SRC_ID, @pos1+@len1) +1
			set @pos2 = CHARINDEX('|', @TO_EMP_ID, @pos2+@len2) +1

		END
		set @pos1 = 0 
		set @len1 = 0
		set @pos2 = 0 
		set @len2 = 0
		set @src_id_found = ''
		set @emp_id_found = ''
	
		FETCH NEXT FROM emp_cursor   
		INTO  @intrnl_ts_, @TO_USR_SRC_ID, @TO_EMP_ID, @src_ts_
	END   
	CLOSE emp_cursor;  
	DEALLOCATE emp_cursor; 

	-- * *********************************************************************************
	--Saving VALUES to the RCGNTN_MSG_VALUES table
	DECLARE @value_cd_ varchar(1000), @intrnl_ts__ datetime, @src_ts__ varchar(50),
			@pos1_ int = 0, @len1_ int = 0, @value_cd_found varchar(1000)

	DECLARE val_cursor CURSOR FOR   
		SELECT intrnl_ts, value_cd, SRC_TS
		FROM #STG2
		WHERE value_cd <> '';  
	OPEN val_cursor  
  
	FETCH NEXT FROM val_cursor   
	INTO @intrnl_ts__, @value_cd_, @src_ts__
  
	WHILE @@FETCH_STATUS = 0  
	BEGIN 
		--print cast(@intrnl_ts__ as varchar(50)) +' - '+ @value_cd_
		WHILE CHARINDEX('|', @value_cd_, @pos1_) > 0 
			BEGIN
				set @len1_ = CHARINDEX('|', @value_cd_, @pos1_) - @pos1_
				set @value_cd_found = SUBSTRING(@value_cd_, @pos1_, @len1_)
			 
				--print @value_cd_found
				INSERT INTO PXLTD_CEDW.dbo.RCGNTN_MSG_VALUES (RCGNTN_MSG_ID, RCGNTN_VALUE_ID)
				SELECT MSG.RCGNTN_MSG_ID, VAL.RCGNTN_VALUE_ID
				FROM #STG2 STG
				INNER JOIN PXLTD_CEDW.dbo.RCGNTN_MSG MSG 
					ON STG.INTRNL_TS = MSG.INTRNL_TS
					AND STG.SRC_TS = MSG.SRC_TS
				INNER JOIN PXLTD_CEDW.dbo.RCGNTN_VALUES_TYP VAL 
					ON VAL.VALUE_CD = @value_cd_found
				WHERE MSG.INTRNL_TS = @intrnl_ts__
					AND MSG.SRC_TS = @src_ts__
					AND STG.INTRNL_TS = @intrnl_ts__
					AND STG.SRC_TS = @src_ts__

				set @pos1_ = CHARINDEX('|', @value_cd_, @pos1_+@len1_) +1
			END
		 set @pos1_ = 0 
		 set @len1_ = 0
		 set @value_cd_found = ''
	
		FETCH NEXT FROM val_cursor   
		INTO  @intrnl_ts__, @value_cd_, @src_ts__
	END   
	CLOSE val_cursor;  
	DEALLOCATE val_cursor; 

	-- * *********************************************************************************
	--Delete mensages saved into RCGNTN_MSG which doesn't have TO users found in AV_EMPL_PBLC_DTL table
	DELETE FROM PXLTD_CEDW.dbo.RCGNTN_MSG 
	WHERE RCGNTN_MSG_ID NOT IN 
		(SELECT DISTINCT RCGNTN_MSG_ID FROM PXLTD_CEDW.dbo.RCGNTN_MSG);

	DELETE FROM PXLTD_CEDW.dbo.RCGNTN_MSG_VALUES
	WHERE RCGNTN_MSG_ID NOT IN 
		(SELECT DISTINCT RCGNTN_MSG_ID FROM PXLTD_CEDW.dbo.RCGNTN_MSG);
END

GO


