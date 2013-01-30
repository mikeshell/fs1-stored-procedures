USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[Email_trackingNotImportedToNAV]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[Email_trackingNotImportedToNAV]
AS
BEGIN
	DECLARE @date datetime, @count INT

	DECLARE @body1 VARCHAR(MAX), @email1 VARCHAR(500), @subject1 varchar(150), @bgcolor VARCHAR(500), @counter TINYINT, @subBody VARCHAR(MAX)
	 
	IF EXISTS(SELECT CAST(CAST(lastUpdateDT AS VARCHAR(11)) AS DATETIME), COUNT(*)
				FROM trackingInfo  
				WHERE NAVImported = 0
				GROUP BY CAST(CAST(lastUpdateDT AS VARCHAR(11)) AS DATETIME))
	   BEGIN
		--INITIATE TABLE
		SET @body1 = '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
			"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
			<html xmlns="http://www.w3.org/1999/xhtml">
			<head>
			<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
			<title></title>

			<style type="text/css">
			body,td,th {font-family: Arial, Helvetica, sans-serif;font-size: 10px; vertical-align: top; }
			</style>

			</head>
			<body>
			<table border="0" cellspacing="0" cellpadding="3" style="border:solid 1px #000000;">	
			  <tr style="background-color:#446fb7; color:#FFFFFF; font-weight: bold;">
				<td colspan="6">Products Active on Website (MIVA) but not active according to NAV</td>
			  </tr>	
			  <tr style="background-color:#446fb7; color:#FFFFFF; font-weight: bold;">
				<td style="text-align:center; width: 100px;">Date</td>
				<td style="text-align:center; width: 150px;">Number of Tracking</td>
			  </tr>'
		  
		DECLARE sCursor CURSOR FOR 	
		SELECT CAST(CAST(lastUpdateDT AS VARCHAR(11)) AS DATETIME), COUNT(*)
		FROM trackingInfo  
		WHERE NAVImported = 0
		GROUP BY CAST(CAST(lastUpdateDT AS VARCHAR(11)) AS DATETIME)
		ORDER BY 1

		OPEN sCursor
		FETCH NEXT FROM sCursor INTO @date, @count

		WHILE @@FETCH_STATUS = 0
		   BEGIN	
						
			IF @counter = 1
			   BEGIN
				SET @counter = 2
				SET @bgcolor = '#FFFFFF'
			   END
			ELSE
			   BEGIN
				SET @counter = 1
				SET @bgcolor = '#bed6e9'
			   END
			
			SET @body1 = @body1 + '<tr style="background-color:' + @bgcolor + ';">
				<td align="center" >' + CAST(ISNULL(@date, '') AS VARCHAR(100)) + '</td>
				<td align="center" >' + CAST(ISNULL(@count, 0) AS VARCHAR(100)) + '</td>
			  </tr>' 		
			FETCH NEXT FROM sCursor INTO @date, @count
		   END

		CLOSE sCursor
		DEALLOCATE sCursor
					
		SET @body1 = @body1

		-- PREPPING THE EMAIL TO SEND
		SET @email1 = 'itsupport@katom.com; charley@katom.com'
		--SET @email1 = 'david@katom.com'
		SET @subject1 = 'Number of Trackings that did not get imported into NAV'

		EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients=@email1,	@subject=@subject1,	@body=@body1, @body_format ='HTML',	@blind_copy_recipients='katom.archive@gmail.com'	
	   END
END
GO
