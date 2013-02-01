USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[trackingNum_w_no_ref]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[trackingNum_w_no_ref]
AS
BEGIN
	
	DECLARE @trackingNum VARCHAR(100), @DT VARCHAR(100)		
	DECLARE @body1 VARCHAR(MAX), @email1 varchar(500), @subject1 varchar(150)

	--INITIATE TABLE
	SET @body1 = '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
	"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
	<html xmlns="http://www.w3.org/1999/xhtml">
	<head>
	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
	<title>E-Mail</title>

	<style type="text/css">
	body,td,th {font-family: Arial, Helvetica, sans-serif;font-size: 10px; vertical-align: top; }
	</style>

	</head>
	<body>'
	  
	DECLARE sCursor CURSOR FOR 
	SELECT TrackingNumber, updatedt 
	FROM UPSTracking 
	WHERE Reference1 IS NULL
		AND DATEDIFF(day, updateDT, GETDATE()) = 1

	OPEN sCursor
	FETCH NEXT FROM sCursor INTO @trackingNum, @DT

	WHILE @@FETCH_STATUS = 0
	   BEGIN
				
		SET @body1 = @body1 + @trackingNum + ' (' + @DT + ')<BR />'
				
		FETCH NEXT FROM sCursor INTO @trackingNum, @DT
	   END


	CLOSE sCursor
	DEALLOCATE sCursor
	
	SET @body1 = @body1 + '</table></td></tr></table>'
	
	SET @body1 = @body1 + '	</body>
	</html>'
	
	-- PREPPING THE EMAIL TO SEND
	SET @email1 = 'charley@katom.com; tamara@katom.com'
	SET @subject1 = 'Tracking Number with no Reference Information'

	EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients=@email1,	@subject=@subject1,	@body=@body1, @body_format ='HTML',	@blind_copy_recipients='katom.archive@gmail.com'	

END
GO
