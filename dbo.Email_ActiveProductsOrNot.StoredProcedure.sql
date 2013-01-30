USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[Email_ActiveProductsOrNot]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[Email_ActiveProductsOrNot]
AS
BEGIN
	DECLARE @code VARCHAR(100), @blocked BIT, @webitem BIT, @active BIT, @status VARCHAR(25), @qtyOnHand INT, @counter INT
			
	DECLARE @body1 VARCHAR(MAX), @email1 varchar(500), @subject1 varchar(150), @bgcolor VARCHAR(500)
	
	SET @counter = 1
	--PRODUCTS SHOWING ACTIVE ON MIVA BUT NOT IN NAVISION
	TRUNCATE TABLE prodTemp

	INSERT INTO prodTemp([No_])
	SELECT code FROM ProductsMIVA WHERE active = 1

	DELETE FROM prodTemp
	WHERE [No_] in (SELECT [No_] COLLATE Latin1_General_CS_AS 
					 FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] 
					 WHERE [Blocked] = 0 AND [Web Item] = 1 AND [Web Active] = 1 AND [Status] <> 2)
						 
	--INITIATE TABLE
	SET @body1 = '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
	"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
	<html xmlns="http://www.w3.org/1999/xhtml">
	<head>
	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
	<title>Backorder Item</title>

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
		<td style="text-align:center; width: 100px;">Product Code</td>
		<td style="text-align:center; width: 100px;">Blocked</td>
		<td style="text-align:center; width: 100px;">Web Item</td>
		<td style="text-align:center; width: 100px;">Web Active</td>
		<td style="text-align:center; width: 100px;">Status</td>
		<td style="text-align:center; width: 100px;">Qty On Hand</td>
	  </tr>'
	  
	DECLARE sCursor CURSOR FOR 
	SELECT t.[No_], i.[Blocked], i.[Web Item], i.[Web Active], 
		CASE i.[Status]
			WHEN 2 THEN 'Discontinued'
			ELSE 'Active'
		END [Status], 
		p.qtyOnHand
	FROM prodTemp t LEFT JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item]  i on t.[No_] = i.[No_] COLLATE Latin1_General_CS_AS
					LEFT JOIN products p on p.CODE = t.[No_]
	WHERE p.qtyOnHand = 0

	OPEN sCursor
	FETCH NEXT FROM sCursor INTO @code, @blocked, @webitem, @active, @status, @qtyOnHand

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
		<td>' + ISNULL(@code, '') + '</td>'
		
		IF @blocked IS NULL
		   BEGIN
			SET @body1 = @body1 + '<td align="center" >NOT IN NAV</td>'
		   END
		ELSE
		   BEGIN
			SET @body1 = @body1 + '<td align="center" >' + CAST(ISNULL(@blocked, '') AS VARCHAR(100))  + '</td>'		   
		   END
		SET @body1 = @body1 + '<td align="center" >' + CAST(ISNULL(@webitem, '') AS VARCHAR(100)) + '</td>
			<td align="center" >' + CAST(ISNULL(@active, '') AS VARCHAR(100)) + '</td>
			<td align="center" >' + CAST(ISNULL(@status, '') AS VARCHAR(100)) + '</td>
			<td align="center" >' + CAST(ISNULL(@qtyOnHand, '') AS VARCHAR(11)) + '</td>
		  </tr>' 
				
		FETCH NEXT FROM sCursor INTO @code, @blocked, @webitem, @active, @status, @qtyOnHand
	   END

	CLOSE sCursor
	DEALLOCATE sCursor

	SET @body1 = @body1 + '</table><br /><br />
	  <table border="0" cellspacing="0" cellpadding="3" style="border:solid 1px #000000;">	
	  <tr style="background-color:#446fb7; color:#FFFFFF; font-weight: bold;">
		<td>Products NOT Active on Website (MIVA) but should be active according to NAV</td>
	  </tr>'

	--PRODUCTS SHOWING ACTIVE ON NAV BUT NOT IN MIVA
	truncate table prodTemp
	insert into prodTemp([No_], [Blocked])
	select [No_], [Status]
	from fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] 
	where [Blocked] = 0 and [Web Item] = 1 and [Web Active] = 1

	DELETE FROM prodTemp WHERE [No_] IN (SELECT code COLLATE Latin1_General_CS_AS FROM ProductsMIVA WHERE active = 1)

	DELETE FROM prodTemp WHERE [No_] IN
	(
	SELECT [No_]
	FROM prodTemp t JOIN products p on t.[No_] = p.CODE
	where qtyOnHand > 0 and [Blocked] = 2
	)

	DECLARE sCursor CURSOR FOR 	
	SELECT [No_] FROM prodTemp WHERE [Blocked] <> 2

	OPEN sCursor
	FETCH NEXT FROM sCursor INTO @code

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
	  </tr>'
				
		FETCH NEXT FROM sCursor INTO @code
	   END

	CLOSE sCursor
	DEALLOCATE sCursor

	SET @body1 = @body1 + '</table>'

	-- PREPPING THE EMAIL TO SEND
	SET @email1 = 'smosley@katom.com; david@katom.com; paula@katom.com'
--	SET @email1 = 'david@katom.com'
	SET @subject1 = 'Products: Active or Not?'
EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients=@email1,	@subject=@subject1,	@body=@body1, @body_format ='HTML',	@blind_copy_recipients='katom.archive@gmail.com'	

END
GO
