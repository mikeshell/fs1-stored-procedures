USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[category_catxprod_supplement_email]    Script Date: 02/01/2013 11:09:50 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[category_catxprod_supplement_email]
AS
BEGIN
	DECLARE @catCode VARCHAR(100), @prodCode VARCHAR(100), @primarycat VARCHAR(100), 
		    @action VARCHAR(100), @bgcolor VARCHAR(100), @counter TINYINT
	
	DECLARE @body1 VARCHAR(MAX), @email1 VARCHAR(500), @subject1 VARCHAR(150)
			
	SET @counter = 1
	SET @bgcolor = '#bed6e9'

	--INITIATING DATE
	--INITIATE TABLE
	SET @body1 = '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
	"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
	<html xmlns="http://www.w3.org/1999/xhtml">
	<head>
	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
	<title>PRODUCT CATEGORIZATION ERROR REPORT</title>

	<style type="text/css">
	body,td,th {font-family: Arial, Helvetica, sans-serif;font-size: 10px; vertical-align: top; }
	</style>

	</head>
	<body>
	<table border="0" cellspacing="0" cellpadding="3" style="border:solid 1px #000000;">
	  <tr style="background-color:#446fb7; color:#FFFFFF; font-weight: bold;">
		<td style="width: 100px;">catname</td>
		<td style="width: 100px;">CATEGORY</td>
		<td style="width: 100px;">PRODUCT</td>
		<td style="width: 100px;">PRIMARY CAT</td>
		<td style="width: 250px;">STATUS</td>
	  </tr>'
	  
	DECLARE sCursor CURSOR FOR 
	SELECT catCode, prodCode, [primarycat], 
		CASE [action]
			WHEN 'P' THEN 'DID NOT IMPORTED: PRODUCT NOT ACTIVE ONLINE'
			WHEN 'C' THEN 'DID NOT IMPORTED: CATEGORY NOT ACTIVE ONLINE'
			WHEN 'D' THEN 'DID NOT IMPORTED: PRODUCT IS ALREADY ASSIGNED TO THE CATEGORY'
			WHEN 'X' THEN 'IMPORTED: PRIMARY CAT ALREADY EXIST.  CATEGORIZATION REASSIGNED TO SECONDARY'
		END [action]
	FROM catxprodTemp 	
	WHERE [action] <> 'I'
	ORDER BY [action], prodCode

	OPEN sCursor
	FETCH NEXT FROM sCursor INTO @catCode, @prodCode, @primarycat,  @action

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
		
		SET @body1 = @body1 + ' <tr style="background-color:' + @bgcolor + ';">
		<td>' + @catCode + '</td>
		<td>' + @prodCode + '</td>
		<td>' + @primarycat + '</td>
		<td>' + @action + '</td>
	  </tr>'
				
		FETCH NEXT FROM sCursor INTO @catCode, @prodCode, @primarycat,  @action
	   END

	CLOSE sCursor
	DEALLOCATE sCursor
		
	SET @body1 = @body1 + '</table></body>
	</html>'
	
	-- PREPPING THE EMAIL TO SEND
	SET @email1 = 'groberts@katom.com'
--	SET @email1 = 'david@katom.com'
	SET @subject1 = 'PRODUCT CATEGORIZATION ERROR REPORT'

	EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients=@email1,	@subject=@subject1,	@body=@body1, @body_format ='HTML',	@blind_copy_recipients='katom.archive@gmail.com'	

END
GO
