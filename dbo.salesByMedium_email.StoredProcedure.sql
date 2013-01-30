USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[salesByMedium_email]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[salesByMedium_email]
AS
BEGIN
	DECLARE @medium VARCHAR(100), @WTDos DECIMAL(10, 2), @WTDoo INT, @WTDis DECIMAL(10, 2), @WTDio INT, @counter INT
			
	DECLARE @body1 VARCHAR(MAX), @email1 varchar(500), @subject1 varchar(150), @bgcolor VARCHAR(500)
		
	--INITIATE TABLE
	SET @body1 = '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
	"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
	<html xmlns="http://www.w3.org/1999/xhtml">
	<head>
	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
	<title>Sales By Medium</title>

	<style type="text/css">
	body,td,th {font-family: Arial, Helvetica, sans-serif;font-size: 12px; vertical-align: top; }
	</style>

	</head>
	<body>
	<p>	
	<a href="http://www.katom.local/salesByMedium.asp">Click here for complete report - THIS IS ONLY AVAILABLE AT KATOM FACILITY</a>
	</p>
	<table border="0" cellspacing="0" cellpadding="3" style="border:solid 1px #000000;">
	  <tr style="background-color:#446fb7; color:#FFFFFF; font-weight: bold;">
		<td style="text-align:center; width: 100px;">Order Medium</td>
		<td style="text-align:center; width: 120px;">Week To Date<br />Open Sales</td>
		<td style="text-align:center; width: 120px;">Week To Date<br />Invoiced Sales</td>
		<td style="text-align:center; width: 120px;">Week To Date<br />Total Sales</td>
	  </tr>'
	  
	DECLARE sCursor CURSOR FOR 
	SELECT orderMedium, ISNULL([WTDos], 0) [WTDos], ISNULL([WTDoo], 0) [WTDoo],	ISNULL([WTDis], 0) [WTDis], ISNULL([WTDio], 0) [WTDio]
	FROM salesRptByMedium
	WHERE orderMedium <> 'Cancelled'

	OPEN sCursor
	FETCH NEXT FROM sCursor INTO @medium, @WTDos, @WTDoo, @WTDis, @WTDio

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
		<td>' + @medium + '</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), CAST(@WTDos AS MONEY), 1) + ' (' + CAST(@WTDoo AS VARCHAR(100)) + ')</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), CAST(@WTDis AS MONEY), 1) + ' (' + CAST(@WTDio AS VARCHAR(100)) + ')</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), CAST(@WTDos+@WTDis AS MONEY), 1) + ' (' + CAST(@WTDoo+@WTDio AS VARCHAR(100)) + ')</td>
	  </tr>'
				
		FETCH NEXT FROM sCursor INTO @medium, @WTDos, @WTDoo, @WTDis, @WTDio
	   END

	CLOSE sCursor
	DEALLOCATE sCursor

	SET @body1 = @body1 + '</table><br /><br />
		<table border="0" cellspacing="0" cellpadding="3" style="border:solid 1px #000000;">
		  <tr style="background-color:#446fb7; color:#FFFFFF; font-weight: bold;">
			<td colspan="4">CSR Sales Performer</td>
		  </tr>
		  <tr style="background-color:#446fb7; color:#FFFFFF; font-weight: bold;">
			<td style="width: 100px;">Employee</td>
			<td style="text-align:center; width: 120px;">Week To Date<br />Open Sales</td>
			<td style="text-align:center; width: 120px;">Week To Date<br />Invoiced Sales</td>
			<td style="text-align:center; width: 120px;">Week To Date<br />Total Sales</td>
		  </tr>'

	DECLARE sCursor CURSOR FOR 
	SELECT CASE 
		WHEN e.employeeName IS NULL THEN orderMedium
		ELSE e.employeeName
	   END orderMedium,
		ISNULL([WTDos], 0) [WTDos], ISNULL([WTDoo], 0) [WTDoo],	ISNULL([WTDis], 0) [WTDis], ISNULL([WTDio], 0) [WTDio]
	FROM salesRptBySalesmen s left join employee e on LOWER(s.orderMedium) = LOWER(e.empid)
	WHERE e.department IN ('CSR')
	ORDER BY ISNULL([WTDis], 0) + ISNULL([WTDos], 0) DESC
	
	OPEN sCursor
	FETCH NEXT FROM sCursor INTO @medium, @WTDos, @WTDoo, @WTDis, @WTDio

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
		<td>' + @medium + '</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), CAST(@WTDos AS MONEY), 1) + ' (' + CAST(@WTDoo AS VARCHAR(100)) + ')</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), CAST(@WTDis AS MONEY), 1) + ' (' + CAST(@WTDio AS VARCHAR(100)) + ')</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), CAST(@WTDos+@WTDis AS MONEY), 1) + ' (' + CAST(@WTDoo+@WTDio AS VARCHAR(100)) + ')</td>
	  </tr>'
				
		FETCH NEXT FROM sCursor INTO @medium, @WTDos, @WTDoo, @WTDis, @WTDio
	   END

	CLOSE sCursor
	DEALLOCATE sCursor


	SET @body1 = @body1 + '</table>'

	-- PREPPING THE EMAIL TO SEND
	SET @email1 = 'pbible@katom.com; cbible@katom.com; dlu@katom.com; pchesworth@katom.com; jchesworth@katom.com; mharville@katom.com'
--	SET @email1 = 'david@katom.com'
	SET @subject1 = 'Sales By Medium as of ' + CAST(GETDATE() AS VARCHAR(11))

	EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients=@email1,	@subject=@subject1,	@body=@body1, @body_format ='HTML',	@blind_copy_recipients='katom.archive@gmail.com'	

END
GO
