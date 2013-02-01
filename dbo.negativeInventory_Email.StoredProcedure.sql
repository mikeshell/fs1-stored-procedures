USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[negativeInventory_Email]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[negativeInventory_Email]
AS
BEGIN
	DECLARE @CODE VARCHAR(50), @INVENTORYCOUNT FLOAT, @counter SMALLINT, @bgcolor VARCHAR(50)
			
	DECLARE @body1 VARCHAR(MAX), @email1 varchar(500), @subject1 varchar(150)
	
	IF EXISTS(SELECT [CODE], qtyonhand FROM products
				WHERE active = 1 
					AND isnull(qtyonhand, 10) < 0
					AND thresholdMin > 0  )
	   BEGIN			
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
		<body>
		<table border="0" cellspacing="0" cellpadding="3" style="border:solid 1px #000000;">
		  <tr style="background-color:#446fb7; color:#FFFFFF; font-weight: bold;">
			<td width="100">Product Code</td>
			<td style="text-align:center; width: 100px;">Inventory Count</td>
		  </tr>'
		  
		DECLARE sCursor CURSOR FOR 
		SELECT [CODE], qtyonhand 
		FROM products
		WHERE active = 1 
			AND isnull(qtyonhand, 10) < 0
			AND thresholdMin > 0
		ORDER BY 2 ASC, 1 ASC

		OPEN sCursor
		FETCH NEXT FROM sCursor INTO @CODE, @INVENTORYCOUNT

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
			<td>' + @code + '</td>
			<td align="center" >' + CAST(@INVENTORYCOUNT AS VARCHAR(10)) + '</td>
		  </tr>'
					
			FETCH NEXT FROM sCursor INTO @CODE, @INVENTORYCOUNT
		   END


		CLOSE sCursor
		DEALLOCATE sCursor

		SET @body1 = @body1 + '</table>'
		
		SET @body1 = @body1 + '	</body>
		</html>'
		
		-- PREPPING THE EMAIL TO SEND
		SET @email1 = 'mmeade@katom.com; pbible@katom.com'
	--	SET @email1 = 'david@katom.com'
		SET @subject1 = 'Products with NEGATIVE inventory'

		EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients=@email1,	@subject=@subject1,	@body=@body1, @body_format ='HTML',	@blind_copy_recipients='katom.archive@gmail.com'	
	   END
END
GO
