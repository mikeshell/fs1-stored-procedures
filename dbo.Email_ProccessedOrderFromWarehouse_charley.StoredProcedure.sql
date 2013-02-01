USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[Email_ProccessedOrderFromWarehouse_charley]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[Email_ProccessedOrderFromWarehouse_charley]
AS
BEGIN
	DECLARE @postingDT DATETIME, @SalesTT DECIMAL(10, 2), @orderTT INT

	DECLARE @body1 VARCHAR(MAX), @email1 VARCHAR(500), @subject1 varchar(150), @bgcolor VARCHAR(500), @counter TINYINT
	
	SET @counter = 1
	SET @bgcolor = '#bed6e9'
	 
	--INITIATE TABLE
	SET @body1 = '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
	"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
	<html xmlns="http://www.w3.org/1999/xhtml">
	<head>
	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
	<title>Error Report</title>

	<style type="text/css">
	body,td,th {font-family: Arial, Helvetica, sans-serif;font-size: 10px; vertical-align: top; }
	</style>

	</head>
	<body>
	<table border="0" cellspacing="0" cellpadding="3" style="border:solid 1px #000000;">
	  <tr style="background-color:#446fb7; color:#FFFFFF; font-weight: bold;">
		   <td width="200">Date</td>
		<td style="text-align:center; width: 100px;">Sales Total</td>
		<td style="text-align:center; width: 100px;">Order Total</td>
	  </tr>'
	  
	DECLARE sCursor CURSOR FOR 
	SELECT ih.[Posting Date], CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)),
			COUNT(distinct ISNULL(ih.[No_], 0)) orderTT	
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il
			ON ih.[No_]  = il.[Document No_]
		/**
		LEFT JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Header] p 
			ON ih.[Order No_] = p.[Sales Order No_]	
		LEFT JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Purch_ Inv_ Header] p2 
			ON ih.[Order No_] = p2.[Sales Order No_]
		**/
	WHERE DATEDIFF(DAY, ih.[Posting Date], GETDATE()) < 30
		AND ih.[Source Code] = 'SALES'
		AND LEN(ISNULL(ih.[Customer Source], '')) > 0
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')		
		AND ih.[Order Reason] IN ('WAREHOUSE', 'SENT TO WAREHOUSE!!')
		--and p.[Sales Order No_] is null
		--and p2.[Sales Order No_] is null
	GROUP BY ih.[Posting Date]
	ORDER BY 1 DESC

	OPEN sCursor
	FETCH NEXT FROM sCursor INTO @postingDT, @SalesTT, @orderTT

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
		
		SET @body1 = @body1 + 
			' <tr style="background-color:' + @bgcolor + ';">
				<td>' + CAST(@postingDT AS VARCHAR(11)) + '</td>
				<td align="center" >' + '$' + CONVERT(varchar(50), CAST(ISNULL(@SalesTT, 0) AS MONEY), 1) + '</td>
				<td align="center" >' + CONVERT(varchar(50), ISNULL(@orderTT, 0), 1) + '</td>
			</tr>'
		
				
		FETCH NEXT FROM sCursor INTO @postingDT, @SalesTT, @orderTT
	   END

	CLOSE sCursor
	DEALLOCATE sCursor

	SET @body1 = @body1 + '</table></body></html>'

	-- PREPPING THE EMAIL TO SEND
	SET @email1 = 'cbible@katom.com; jrogers@katom.com; pchesworth@katom.com; dlu@katom.com'
--	SET @email1 = 'dlu@katom.com'	
	SET @subject1 = 'NUMBER OF ORDERS PROCESSED THROUGH THE WAREHOUSE'

	EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients=@email1,	@subject=@subject1,	@body=@body1, @body_format ='HTML',	@blind_copy_recipients='katom.archive@gmail.com'	

END
GO
