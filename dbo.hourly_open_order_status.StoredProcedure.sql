USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[hourly_open_order_status]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[hourly_open_order_status]
AS
BEGIN
	DECLARE @INDEX INT

	SET @INDEX = 13

	TRUNCATE TABLE salesStat10days

	WHILE (@INDEX > -1)
	   BEGIN
		INSERT INTO salesStat10days(orderDT)
		SELECT DATEADD(DAY, -@INDEX, GETDATE())
		WHERE DATEPART(WEEKDAY, DATEADD(DAY, -@INDEX, GETDATE())) NOT IN (1, 7)
		SET @INDEX = @INDEX - 1
	   END
	
	DECLARE @body1 VARCHAR(MAX), @email1 varchar(500), @subject1 varchar(150), @fontColor varchar(50)
	
	SET @body1 = '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
	"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
	<html xmlns="http://www.w3.org/1999/xhtml">
	<head>
	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
	<title>E-Mail</title>

	<style type="text/css">
	body,td,th {font-family: Arial, Helvetica, sans-serif;font-size: 12px; vertical-align: top; }
	</style>

	</head>
	<body>
	<H3>Daily Sales Performance as of ' + CONVERT(VARCHAR(20), GETDATE(), 0) + '</H3>
	<table border="0" cellspacing="0" cellpadding="3" style="border:solid 1px #000000;">
	  <tr style="background-color:#446fb7; color:#FFFFFF; font-weight: bold;">
		<td style="text-align:left; width: 100px;">Date</td>
		<td style="text-align:center; width: 100px;">Invoiced</td>
		<td style="text-align:center; width: 100px;">Open</td>
		<td style="text-align:center; width: 100px;">Ready To Import</td>
		<td style="text-align:center; width: 100px;">Total Sales<br />Performance</td>
		<td style="text-align:center; width: 100px;">Invoiced<br />Posting Date</td>
	  </tr>'
		  				
	--INVOICED LAST 10 DAYS	
	TRUNCATE TABLE salesStat10days_Worktable		

	INSERT INTO salesStat10days_Worktable(orderDT, orderCount, sales)	 	
	SELECT [Order Date], 
		COUNT(distinct i.[No_]), SUM(CAST([Quantity]*[Unit Price] AS DECIMAL(10, 2)))
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l ON i.[No_] = l.[Document No_]
	WHERE DATEDIFF(DAY, [Order Date], GETDATE()) BETWEEN 0 AND 13
		AND [Source code] = 'SALES'
		AND l.[Unit Price] > 0
		AND l.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY [Order Date]
	ORDER BY 1

	UPDATE s
	SET s.iOrderCount = pt.orderCount,
		s.iSales = pt.sales
	FROM salesStat10days s JOIN salesStat10days_Worktable pt ON DATEDIFF(DAY, pt.orderDT, s.orderDT) = 0

	--INVOICED LAST 10 DAYS - POSTING DATE AND NOT SALES DATE
	TRUNCATE TABLE salesStat10days_Worktable		

	INSERT INTO salesStat10days_Worktable(orderDT, orderCount, sales)	
	SELECT i.[Posting Date], 
		COUNT(distinct i.[No_]), SUM(CAST([Quantity]*[Unit Price] AS DECIMAL(10, 2)))
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l ON i.[No_] = l.[Document No_]
	WHERE DATEDIFF(DAY, i.[Posting Date], GETDATE()) BETWEEN 0 AND 13
		AND [Source code] = 'SALES'
		AND l.[Unit Price] > 0
		AND l.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY i.[Posting Date]
	ORDER BY 1

	UPDATE s
	SET s.iOrderCount_actual = pt.orderCount,
		s.iSales_actual = pt.sales
	FROM salesStat10days s JOIN salesStat10days_Worktable pt ON DATEDIFF(DAY, pt.orderDT, s.orderDT) = 0

	--OPEN SALES LAST 10 DAYS
	TRUNCATE TABLE salesStat10days_Worktable		

	INSERT INTO salesStat10days_Worktable(orderDT, orderCount, sales, notinvoicedSales)	 
	SELECT [Order Date],
		COUNT(distinct ISNULL(ih.[No_], 0)) orderTT	, 
		CAST(SUM(ISNULL(il.[Outstanding Amount], 0)) as decimal(10, 2)),
		CAST(SUM(ISNULL(il.[Shipped Not Invoiced], 0)) as decimal(10, 2))
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] il
			ON ih.[No_]  = il.[Document No_]
	WHERE DATEDIFF(DAY, [Order Date], GETDATE()) BETWEEN 0 AND 13
		AND ih.[Document Type] = 1
		AND il.[Unit Price] > 0
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY [Order Date]
	ORDER BY 1

	UPDATE s
	SET s.sOrderCount = pt.orderCount,
		s.sSales = pt.sales,
		s.notinvoicedSales = pt.notinvoicedSales
	FROM salesStat10days s JOIN salesStat10days_Worktable pt ON DATEDIFF(DAY, pt.orderDT, s.orderDT) = 0
		
	--NOT YET IMPORTED LAST 10 DAYS
	TRUNCATE TABLE salesStat10days_Worktable

	INSERT INTO salesStat10days_Worktable(orderDT, orderCount, sales, oldestBatch, lastBatched)
	select cast(cast(ordertimestamp as varchar(11)) as datetime), 
		count(customerPO), sum(AmountSubmitted), MIN(batchtimestamp), MAX(batchtimestamp)
	from order_hdr 
	group by cast(cast(ordertimestamp as varchar(11)) as datetime)

	UPDATE s
	SET s.wOrderCount = pt.orderCount,
		s.wSales = pt.sales,
		s.oldestOrder = pt.oldestBatch,
		s.lastBatched = pt.lastBatched
	FROM salesStat10days s JOIN salesStat10days_Worktable pt ON DATEDIFF(DAY, pt.orderDT, s.orderDT) = 0
	
	--CREATE EMAIL REPORT
	DECLARE @orderDT DATETIME, @iOrderCount INT, @iSales MONEY,  @iOrderCount_actual INT, @iSales_actual MONEY,
			@sOrderCount INT, @sSales MONEY, @wOrderCount INT, @wSales MONEY, @orderCountTT INT, 
			@salesTT MONEY, @oldestOrder DATETIME, @counter INT, @bgcolor VARCHAR(50)
					
	DECLARE sCursor CURSOR FOR 
	SELECT orderDT, 
		ISNULL(iOrderCount, 0) iOrderCount, 
		ISNULL(iSales, 0) iSales,
		ISNULL(iOrderCount_actual, 0) iOrderCount_actual, 
		ISNULL(iSales_actual, 0) iSales_actual,
		ISNULL(sOrderCount, 0) sOrderCount, 
		ISNULL(sSales, 0) + ISNULL(notinvoicedSales, 0)  sSales,
		ISNULL(wOrderCount, 0) wOrderCount, 
		ISNULL(wSales, 0) wSales,
		ISNULL(iOrderCount, 0) + ISNULL(sOrderCount, 0) + ISNULL(wOrderCount, 0) orderCountTT,
		ISNULL(iSales, 0) + ISNULL(sSales, 0) + ISNULL(wSales, 0) salesTT,
		oldestOrder
	FROM salesStat10days ORDER BY orderDT DESC
		
	OPEN sCursor
	FETCH NEXT FROM sCursor INTO @orderDT, @iOrderCount, @iSales, @iOrderCount_actual, @iSales_actual,
					@sOrderCount, @sSales, @wOrderCount, @wSales, @orderCountTT, @salesTT, @oldestOrder

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
		<td>' + CAST(@orderDT AS VARCHAR(11)) + '</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), CAST(@iSales AS MONEY), 1) + '<BR />(' + CAST(@iOrderCount AS VARCHAR(100)) + ')</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), CAST(@sSales AS MONEY), 1) + '<BR />(' + CAST(@sOrderCount AS VARCHAR(100)) + ')</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), CAST(@wSales AS MONEY), 1) + '<BR />(' + CAST(@wOrderCount AS VARCHAR(100)) + ')</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), CAST(@salesTT AS MONEY), 1) + '<BR />(' + CAST(@orderCountTT AS VARCHAR(100)) + ')</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), CAST(@iSales_actual AS MONEY), 1) + '<BR />(' + CAST(@iOrderCount_actual AS VARCHAR(100)) + ')</td>
	  </tr>'
				
		FETCH NEXT FROM sCursor INTO @orderDT, @iOrderCount, @iSales, @iOrderCount_actual, @iSales_actual,
					@sOrderCount, @sSales, @wOrderCount, @wSales, @orderCountTT, @salesTT, @oldestOrder
	   END

	CLOSE sCursor
	DEALLOCATE sCursor
	
	SET @body1 = @body1 + '</body></html>'
	
	print  @body1
	
	-- PREPPING THE EMAIL TO SEND
	SET @email1 = 'dlu@katom.com; pbible@katom.com; pchesworth@katom.com; cbible@katom.com; jrogers@katom.com'
--	SET @email1 = 'david@katom.com'
	SET @subject1 = 'Daily Sales Performance as of ' + CONVERT(VARCHAR(20), GETDATE(), 0)

	EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients=@email1,	@subject=@subject1,	@body=@body1, @body_format ='HTML',	@blind_copy_recipients='katom.archive@gmail.com'	
END
GO
