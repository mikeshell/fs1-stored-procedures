USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[dailyStat_Email]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- DESCriptiON:	<DESCriptiON,,>
-- =============================================
CREATE PROCEDURE [dbo].[dailyStat_Email]
AS
BEGIN
	DECLARE @OrderType VARCHAR(50), @todaySale MONEY, @todayOrder INT, @openSale MONEY, @openOrder INT, 
		@lastweekSale MONEY, @lastweekOrder INT, @6weekAvgSale MONEY, @6weeksAvgOrder INT,
		@WTDSale MONEY, @WTDOrder INT, @lastWTDSale MONEY, @lastWTDOrder INT,
		@MTDSale MONEY, @MTDOrder INT, @lastMTDSale MONEY, @lastMTDOrder INT,
		@YTDSale MONEY, @YTDOrder INT, @lastYTDSale MONEY, @lastYTDOrder INT,
		@todaySaleTT MONEY, @todayOrderTT INT, @openSaleTT MONEY, @openOrderTT INT, 
		@openSales_SD MONEY, @openOrders_SD INT,
		@lastweekSaleTT MONEY, @lastweekOrderTT INT, @6weekAvgSaleTT MONEY, @6weeksAvgOrderTT INT, 
		@WTDSaleTT MONEY, @WTDOrderTT INT, @lastWTDSaleTT MONEY, @lastWTDOrderTT INT, 
		@MTDSaleTT MONEY, @MTDOrderTT INT, @lastMTDSaleTT MONEY, @lastMTDOrderTT INT, 
		@YTDSaleTT MONEY, @YTDOrderTT INT, @lastYTDSaleTT MONEY, @lastYTDOrderTT INT,
		@today DATETIME, @yyyy SMALLINT, @dd TINYINT, @mm TINYINT,
		@dow VARCHAR(20), @wok TINYINT, @COUNTer SMALLINT, @bgcolor VARCHAR(50)
			
	DECLARE @body1 VARCHAR(MAX), @email1 varchar(500), @subject1 varchar(150)
					
	SET @todaySaleTT = 0
	SET @todayOrderTT = 0
	SET @openSaleTT = 0
	SET @openOrderTT = 0
	SET @lastweekSaleTT = 0
	SET @lastweekOrderTT = 0
	SET @6weekAvgSaleTT = 0
	SET @6weeksAvgOrderTT = 0
	SET @WTDSaleTT = 0
	SET @WTDOrderTT = 0
	SET @lastWTDSaleTT = 0
	SET @lastWTDOrderTT = 0
	SET @MTDSaleTT = 0
	SET @MTDOrderTT = 0
	SET @lastMTDSaleTT = 0
	SET @lastMTDOrderTT = 0
	SET @YTDSaleTT = 0
	SET @YTDOrderTT = 0
	SET @lastYTDSaleTT = 0
	SET @lastYTDOrderTT = 0
	SET @COUNTer = 1
	SET @bgcolor = '#bed6e9'

	--INITIATING DATE
	SET @today = GETDATE()
	SET @yyyy = YEAR(@today)
	SET @mm = MONTH(@today)
	SET @dd = DAY(@today)
	SET @dow = DATENAME(WEEKDAY, @today)
	SET @wok = DATEPART(WEEK, @today)

	--INITIATE TABLE
	SET @body1 = '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 TransitiONal//EN"
	"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitiONal.dtd">
	<html xmlns="http://www.w3.org/1999/xhtml">
	<head>
	<meta http-equiv="CONtent-Type" cONtent="text/html; charset=UTF-8" />
	<title>E-Mail</title>

	<style type="text/css">
	body,td,th {fONt-family: Arial, Helvetica, sans-serif;fONt-size: 10px; vertical-align: top; }
	</style>

	</head>
	<body>
	<table border="0" cellspacing="0" cellpadding="3" style="border:solid 1px #000000;">
	  <tr style="background-color:#446fb7; color:#FFFFFF; fONt-weight: bold;">
		   <td width="75">&nbsp;</td>
		<td style="text-align:center; width: 100px;">' + @dow + '</td>
		<td style="text-align:center; width: 100px;">Open Orders</td>
		<td style="text-align:center; width: 100px;">Last ' + @dow + '</td>
		<td style="text-align:center; width: 100px;">6 Weeks Avg</td>
		<td style="text-align:center; width: 100px;">WTD<BR />Invoiced</td>
		<td style="text-align:center; width: 100px;">Last WTD<BR />Invoiced</td>
		<td style="text-align:center; width: 100px;">MTD<BR />' + DATENAME(MONTH, @today) + '</td>
		<td style="text-align:center; width: 100px;">MTD<BR />' + DATENAME(MONTH, DATEADD(MONTH, -1, @today)) + '</td>
		<td style="text-align:center; width: 130px;">YTD<BR />'+ CAST(@yyyy AS VARCHAR(4)) + '</td>
		<td style="text-align:center; width: 130px;">YTD<BR />'+ CAST((@yyyy-1) AS VARCHAR(4)) + '</td>
	  </tr>'
	  
	DECLARE sCursor CURSOR FOR 
	SELECT [OrderType], 
		ISNULL([todaySale], 0) [todaySale], ISNULL([todayOrder], 0) [todayOrder], 
		ISNULL([openSale], 0) [openSale], ISNULL([openOrder], 0) [openOrder], 
		ISNULL([lastweekSale], 0) [lastweekSale], ISNULL([lastweekOrder], 0) [lastweekOrder], 
		ISNULL([6weekAvgSale], 0) [6weekAvgSale], ISNULL([6weeksAvgOrder], 0) [6weeksAvgOrder], 
		ISNULL([WTDSale], 0) [WTDSale], ISNULL([WTDOrder], 0) [WTDOrder], 
		ISNULL([lastWTDSale], 0) [lastWTDSale], ISNULL([lastWTDOrder], 0) [lastWTDOrder], 
		ISNULL([MTDSale], 0) [MTDSale], ISNULL([MTDOrder], 0) [MTDOrder], 
		ISNULL([lastMTDSale], 0) [lastMTDSale], ISNULL([lastMTDOrder], 0) [lastMTDOrder], 
		ISNULL([YTDSale], 0) [YTDSale], ISNULL([YTDOrder], 0) [YTDOrder], 
		ISNULL([lastYTDSale], 0) [lastYTDSale], ISNULL([lastYTDOrder], 0) [lastYTDOrder]
	FROM dailyStat
	WHERE [OrderType] IS NOT NULL

	OPEN sCursor
	FETCH NEXT FROM sCursor INTO @OrderType, @todaySale, @todayOrder, 
								 @openSale, @openOrder, @lastweekSale, @lastweekOrder, 
								 @6weekAvgSale, @6weeksAvgOrder, @WTDSale, @WTDOrder, 
								 @lastWTDSale, @lastWTDOrder, @MTDSale, @MTDOrder, 
								 @lastMTDSale, @lastMTDOrder, @YTDSale, @YTDOrder, 
								 @lastYTDSale, @lastYTDOrder

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		SET @todaySaleTT = @todaySaleTT + @todaySale
		SET @todayOrderTT = @todayOrderTT + @todayOrder
		SET @openSaleTT = @openSaleTT + @openSale
		SET @openOrderTT = @openOrderTT + @openOrder
		SET @lastweekSaleTT = @lastweekSaleTT + @lastweekSale
		SET @lastweekOrderTT = @lastweekOrderTT + @lastweekOrder
		SET @6weekAvgSaleTT = @6weekAvgSaleTT + @6weekAvgSale
		SET @6weeksAvgOrderTT = @6weeksAvgOrderTT + @6weeksAvgOrder
		SET @WTDSaleTT = @WTDSaleTT + @WTDSale
		SET @WTDOrderTT = @WTDOrderTT + @WTDOrder
		SET @lastWTDSaleTT = @lastWTDSaleTT + @lastWTDSale
		SET @lastWTDOrderTT = @lastWTDOrderTT + @lastWTDOrder
		SET @MTDSaleTT = @MTDSaleTT + @MTDSale
		SET @MTDOrderTT = @MTDOrderTT + @MTDOrder
		SET @lastMTDSaleTT = @lastMTDSaleTT + @lastMTDSale
		SET @lastMTDOrderTT = @lastMTDOrderTT + @lastMTDOrder
		SET @YTDSaleTT = @YTDSaleTT + @YTDSale
		SET @YTDOrderTT = @YTDOrderTT + @YTDOrder
		SET @lastYTDSaleTT = @lastYTDSaleTT + @lastYTDSale
		SET @lastYTDOrderTT = @lastYTDOrderTT + @lastYTDOrder
		
		IF @COUNTer = 1
		   BEGIN
			SET @COUNTer = 2
			SET @bgcolor = '#FFFFFF'
		   END
		ELSE
		   BEGIN
			SET @COUNTer = 1
			SET @bgcolor = '#bed6e9'
		   END
		
		SET @body1 = @body1 + ' <tr style="background-color:' + @bgcolor + ';">
		<td>' + @OrderType + '</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), ISNULL(@todaySale, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@todayOrder, 0), 1) + ')</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), ISNULL(@openSale, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@openOrder, 0), 1) + ')</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), ISNULL(@lastweekSale, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@lastweekOrder, 0), 1) + ')</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), ISNULL(@6weekAvgSale, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@6weeksAvgOrder, 0), 1) + ')</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), ISNULL(@WTDSale, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@WTDOrder, 0), 1) + ')</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), ISNULL(@lastWTDSale, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@lastWTDOrder, 0), 1) + ')</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), ISNULL(@MTDSale, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@MTDOrder, 0), 1) + ')</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), ISNULL(@lastMTDSale, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@lastMTDOrder, 0), 1) + ')</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), ISNULL(@YTDSale, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@YTDOrder, 0), 1) + ')</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), ISNULL(@lastYTDSale, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@lastYTDOrder, 0), 1) + ')</td>
	  </tr>'
				
		FETCH NEXT FROM sCursor INTO @OrderType, @todaySale, @todayOrder, 
									 @openSale, @openOrder, @lastweekSale, @lastweekOrder, 
									 @6weekAvgSale, @6weeksAvgOrder, @WTDSale, @WTDOrder, 
									 @lastWTDSale, @lastWTDOrder, @MTDSale, @MTDOrder, 
									 @lastMTDSale, @lastMTDOrder, @YTDSale, @YTDOrder, 
									 @lastYTDSale, @lastYTDOrder
	   END


	CLOSE sCursor
	DEALLOCATE sCursor

	SET @body1 = @body1 + ' <tr style="background-color:#dddddd; fONt-weight: bold;">
		<td >Total</td>
		<td align="center">' + '$' + CONVERT(varchar(50), ISNULL(@todaySaleTT, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@todayOrderTT, 0), 1) + ')</td>
		<td align="center">' + '$' + CONVERT(varchar(50), ISNULL(@openSaleTT, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@openOrderTT, 0), 1) + ')</td>
		<td align="center">' + '$' + CONVERT(varchar(50), ISNULL(@lastweekSaleTT, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@lastweekOrderTT, 0), 1) + ')</td>
		<td align="center">' + '$' + CONVERT(varchar(50), ISNULL(@6weekAvgSaleTT, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@6weeksAvgOrderTT, 0), 1) + ')</td>
		<td align="center">' + '$' + CONVERT(varchar(50), ISNULL(@WTDSaleTT, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@WTDOrderTT, 0), 1) + ')</td>
		<td align="center">' + '$' + CONVERT(varchar(50), ISNULL(@lastWTDSaleTT, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@lastWTDOrderTT, 0), 1) + ')</td>
		<td align="center">' + '$' + CONVERT(varchar(50), ISNULL(@MTDSaleTT, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@MTDOrderTT, 0), 1) + ')</td>
		<td align="center">' + '$' + CONVERT(varchar(50), ISNULL(@lastMTDSaleTT, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@lastMTDOrderTT, 0), 1) + ')</td>
		<td align="center">' + '$' + CONVERT(varchar(50), ISNULL(@YTDSaleTT, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@YTDOrderTT, 0), 1) + ')</td>
		<td align="center">' + '$' + CONVERT(varchar(50), ISNULL(@lastYTDSaleTT, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@lastYTDOrderTT, 0), 1) + ')</td>
	  </tr>
	</table>'

	--POPULATING SHIP DIRECT DATA	
	TRUNCATE TABLE dailyStat_shipdirect
	INSERT INTO dailyStat_shipdirect([date], orderNum, sales)
	SELECT h.[Order Date], COUNT(DISTINCT h.[No_]), sum(DISTINCT l.Amount)
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Header] h
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Line] l ON h.[No_] = l.[Document No_]
	WHERE h.[Ship-to Name] NOT IN ('WAREHOUSE FACILITY' , 'FULFILLMENT CENTER', 'FULFILLMENT FACILITY CENTER')
			AND LEN(ISNULL(l.[No_], '')) > 0 
			AND l.Amount > 0 AND l.[No_] NOT IN ('555', '540', '205')
			AND DATEDIFF(day, h.[Order Date], getdate()) < 15
	GROUP BY h.[Order Date]
	ORDER BY h.[Order Date] DESC

	INSERT INTO dailyStatWorktable(orderDT, orderTT, salesTT)
	SELECT [Order Date], COUNT(DISTINCT h.[No_]), sum(l.Amount)
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Purch_ Inv_ Header] h
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Purch_ Inv_ Line] l ON h.[No_] = l.[Document No_]
	WHERE h.[Ship-to Name] NOT IN ('WAREHOUSE FACILITY' , 'FULFILLMENT CENTER', 'FULFILLMENT FACILITY CENTER')
			AND LEN(ISNULL(l.[No_], '')) > 0 
			AND l.Amount > 0 AND l.[No_] NOT IN ('555', '540', '205')
			AND DATEDIFF(day, [Order Date], getdate()) < 15
	GROUP BY [Order Date]
	ORDER BY [Order Date] DESC

	UPDATE s
	SET s.orderNum = ISNULL(s.orderNum, 0) + w.orderTT,
		s.sales = ISNULL(s.sales, 0) + w.salesTT
	FROM dailyStat_shipdirect s JOIN dailyStatWorktable w ON s.[date] = w.orderDT

	TRUNCATE TABLE dailyStatWorktable
	
	INSERT INTO dailyStatWorktable(orderDT, salesTT, orderTT)
	SELECT	ih.[Order Date],
		CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) AS DECIMAL(10, 2)),
		COUNT(DISTINCT ISNULL(x.[No_], 0))		
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] ih
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] il
			ON ih.[No_]  = il.[Document No_]
		JOIN (SELECT DISTINCT [No_] FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header]) as X 
			ON X.[No_] = ih.[No_]
	WHERE DATEDIFF(DAY, [Order Date], GETDATE()) < 20
		AND LEN(ISNULL(ih.[Customer Source], '')) > 0
		AND ih.[Document Type] = 1
		AND ih.[Customer Source] = 'I'
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
		AND LEN(il.[No_]) > 0
		AND LEFT(il.[No_], 3) IN (SELECT mfgid collate SQL_Latin1_General_CP1_CI_AS FROM mfg WHERE Active = 1 AND shipDirect = 1)
		AND il.[No_] NOT IN (
					SELECT [No_]
					FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item]
					WHERE [Maximum Inventory] > 0
					)
	GROUP BY ih.[Order Date]

	UPDATE s
	SET s.openOrders = w.orderTT,
		s.openSales = w.salesTT
	FROM dailyStat_shipdirect s JOIN dailyStatWorktable w ON s.[date] = w.orderDT

	TRUNCATE TABLE dailyStatWorktable

	SET @COUNTer = 1
	SET @body1 = @body1 + '<br />
	<table border="0" cellspacing="0" cellpadding="3" style="border:solid 1px #000000;">
	<tr>
	<td>
	<table border="0" cellspacing="0" cellpadding="3" style="border:solid 1px #000000;">
  <tr style="background-color:#446fb7; color:#FFFFFF; fONt-weight: bold;">
	<td colspan="6" style="fONt-size:12px;">Order Status In The Last 10 Business Days</td>
  </tr>  
  <tr style="background-color:#446fb7; color:#FFFFFF; fONt-weight: bold;">
    <td style="text-align:center; width: 75px;">&nbsp;</td>
    <td style="text-align:center; width: 100px;">Ship Direct</td>
    <td style="text-align:center; width: 100px;">Invoiced</td>
    <td style="text-align:center; width: 100px;">Open</td>
    <td style="text-align:center; width: 100px;">Open Internet</td>
    <td style="text-align:center; width: 100px;">Open Ship Direct</td>
  </tr>'
	
	DECLARE @orderDT varchar(20), @SalesShipDirect mONey, @orderShipDirect int, 
			@salesTT varchar(20), @orderTT varchar(10), @openSalesTT varchar(20), @openOrderTT2 varchar(10),
			@internetSalesTT varchar(20), @internetOrderTT varchar(10)
			
	DECLARE sCursor CURSOR FOR 
	SELECT CONVERT(VARCHAR(10), d.[date], 101),  d.orderNum, d.Sales, 
		'$' + CONVERT(varchar(50), CAST(LEFT(s.TotalInvoiced, CHARINDEX('(', s.totalinvoiced)-2) AS MONEY), 1) salesTT, 
		RIGHT(s.totalinvoiced, LEN(s.totalinvoiced)-CHARINDEX('(', s.totalinvoiced)+1) orderTT, 
		'$' + CONVERT(varchar(50), CAST(LEFT(s.totalSales, CHARINDEX('(', s.totalSales)-2) AS MONEY), 1) openSalesTT, 
		RIGHT(s.totalSales, LEN(s.totalSales)-CHARINDEX('(', s.totalSales)+1) openOrdersTT, 
		'$' + CONVERT(varchar(50), CAST(LEFT(s.InternetSales, CHARINDEX('(', s.InternetSales)-2) AS MONEY), 1) InternetSalesTT, 
		RIGHT(s.InternetSales, LEN(s.InternetSales)-CHARINDEX('(', s.InternetSales)+1) InternetOrdersTT,
		d.openSales, d.openOrders
	FROM dailyStat_shipdirect d JOIN salesStat30Days s ON DATEDIFF(DAY, d.[date], s.[Date]) = 0
	ORDER BY d.[date] DESC

	OPEN sCursor
	FETCH NEXT FROM sCursor INTO @orderDT, @orderShipDirect, @SalesShipDirect, @salesTT, @orderTT, 
								 @openSalesTT, @openOrderTT2, @internetSalesTT, @internetOrderTT,
								 @openSales_SD, @openOrders_SD
	WHILE @@FETCH_STATUS = 0
	   BEGIN		
		IF @COUNTer = 1
		   BEGIN
			SET @COUNTer = 2
			SET @bgcolor = '#FFFFFF'
		   END
		ELSE
		   BEGIN
			SET @COUNTer = 1
			SET @bgcolor = '#bed6e9'
		   END
			
		SET @body1 = @body1 + ' <tr style="background-color:' + @bgcolor + ';">
		<td align="center" >' + @orderDT + '</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), ISNULL(@SalesShipDirect, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@orderShipDirect, 0), 1) + ')</td>
		<td align="center" >' + @salesTT + ' ' + @orderTT + '</td>
		<td align="center" >' + @openSalesTT + ' ' + @openOrderTT2 + '</td>
		<td align="center" >' + @internetSalesTT + ' ' + @internetOrderTT + '</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), ISNULL(@openSales_SD, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@openOrders_SD, 0), 1) + ')</td>
	  </tr>'
	  	
		FETCH NEXT FROM sCursor INTO @orderDT, @orderShipDirect, @SalesShipDirect, @salesTT, @orderTT, 
								 @openSalesTT, @openOrderTT2, @internetSalesTT, @internetOrderTT,
								 @openSales_SD, @openOrders_SD
	   END
	CLOSE sCursor
	DEALLOCATE sCursor
	
	SET @body1 = @body1 + '</TABLE></td><td>
			<table border="0" cellspacing="0" cellpadding="3" style="border:solid 1px #000000;">
		  <tr style="background-color:#446fb7; color:#FFFFFF; fONt-weight: bold;">
			<td colspan="6" style="fONt-size:12px;">Open Order Status In The Last 10 Business Days</td>
		  </tr>  
		  <tr style="background-color:#446fb7; color:#FFFFFF; fONt-weight: bold;">
			<td style="text-align:center; width: 75px;">&nbsp;</td>
			<td style="text-align:center; width: 100px;">UNPRINTED ORDERS</td>
			<td style="text-align:center; width: 100px;">PRINTED ORDERS</td>
			<td style="text-align:center; width: 100px;">TOTAL ORDERS</td>
		  </tr>'
	
	DECLARE @unprintC INT, @unprintS DECIMAL(10, 2), @printC INT, @printS DECIMAL(10, 2)
	
	DECLARE sCursor CURSOR FOR 
	SELECT * FROM dailyStat_openOrder

	OPEN sCursor
	FETCH NEXT FROM sCursor INTO @orderDT, @unprintC, @unprintS, @printC, @printS, @orderTT, @salesTT
	WHILE @@FETCH_STATUS = 0
	   BEGIN		
		IF @COUNTer = 1
		   BEGIN
			SET @COUNTer = 2
			SET @bgcolor = '#FFFFFF'
		   END
		ELSE
		   BEGIN
			SET @COUNTer = 1
			SET @bgcolor = '#bed6e9'
		   END
			
		SET @body1 = @body1 + ' <tr style="background-color:' + @bgcolor + ';">
		<td align="center" >' + REPLACE(@orderDT, ' 12:00AM', '') + '</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), ISNULL(@unprintS, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@unprintC, 0), 1) + ')</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), ISNULL(@printS, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@printC, 0), 1) + ')</td>
		<td align="center" >' + '$' + CONVERT(varchar(50), ISNULL(@salesTT, 0), 1) + ' (' + CONVERT(varchar(50), ISNULL(@orderTT, 0), 1) + ')</td>
	  </tr>'
	
		FETCH NEXT FROM sCursor INTO @orderDT, @unprintC, @unprintS, @printC, @printS, @orderTT, @salesTT
	   END
	CLOSE sCursor
	DEALLOCATE sCursor
		
	SET @body1 = @body1 + '</TABLE></td></tr></table>'
	
	--TOP 10 MFG DATA
	SET @COUNTer = 1
	SET @body1 = @body1 + '<table border="0" cellspacing=""="0" cellpadding="3">
  <tr>
    <td><table border="0" cellspacing="0" cellpadding="3" style="border:solid 1px #000000;">
  <tr style="background-color:#446fb7; color:#FFFFFF; fONt-weight: bold;">
	<td colspan="5" style="fONt-size:12px;">Top 10 Mfg By Web Sales - Last 30 days</td>
  </tr>  
  <tr style="background-color:#446fb7; color:#FFFFFF; fONt-weight: bold;">
    <td style="text-align:center; width: 150px;">Manufacturers</td>
    <td style="text-align:center; width: 70px;">Sales</td>
  </tr>'
  
	DECLARE @mfgID VARCHAR(3), @mfgName varchar(20), @orderTT2 INT, @salesTT2 MONEY, @perc FLOAT, @fONtColor VARCHAR(7)
  
	DECLARE sCursor CURSOR FOR 	
	SELECT TOP 10 mfgID, mfgName, ROUND(sales30, 0), 
		CASE 
			WHEN sales3060 > 0 THEN ROUND(100*((sales30/(CAST(sales3060 AS FLOAT)))-1), 0)
			ELSE 0
		END
	FROM mfg 
	WHERE Active = 1
	ORDER BY sales30 DESC
	
	OPEN sCursor
	FETCH NEXT FROM sCursor INTO @mfgID, @mfgName, @salesTT2, @perc

	WHILE @@FETCH_STATUS = 0
	   BEGIN		
		IF @COUNTer = 1
		   BEGIN
			SET @COUNTer = 2
			SET @bgcolor = '#FFFFFF'
		   END
		ELSE
		   BEGIN
			SET @COUNTer = 1
			SET @bgcolor = '#bed6e9'
		   END
		   
		IF @perc > 0
			SET @fONtColor = '#000000'
		ELSE
			SET @fONtColor = '#CC0000'
			
		SET @body1 = @body1 + ' <tr style="background-color:' + @bgcolor + ';">
		<td>' + @mfgName + ' (' + @mfgID + ')</td>
		<td align="center" style="color:' + @fONtColor + ';">' + '$' + LEFT(CONVERT(varchar(50), ISNULL(@salesTT2, 0), 1), CHARINDEX('.', @salesTT2)) + ' (' + CAST(@perc AS VARCHAR(100)) + '%)</td>
	  </tr>'
	  	
		FETCH NEXT FROM sCursor INTO @mfgID, @mfgName, @salesTT2, @perc
	   END
	CLOSE sCursor
	DEALLOCATE sCursor
		
	SET @body1 = @body1 + '</TABLE></td>'
	
	SET @COUNTer = 1
	SET @body1 = @body1 + '<td><table border="0" cellspacing="0" cellpadding="3" style="border:solid 1px #000000;">
  <tr style="background-color:#446fb7; color:#FFFFFF; fONt-weight: bold;">
	<td colspan="5" style="fONt-size:12px;">Top 10 Mfg By Qty Sold - Last 30 days</td>
  </tr>  
  <tr style="background-color:#446fb7; color:#FFFFFF; fONt-weight: bold;">
    <td style="width: 150px;">Manufacturers</td>
    <td style="text-align:center; width: 70px;">Qty Sold</td>
  </tr>'    
	
	DECLARE sCursor CURSOR FOR 	
	SELECT TOP 10 mfgID, mfgName, order30, ROUND(100*((order30/(CAST(order3060 AS FLOAT)))-1), 0)
	FROM mfg 
	WHERE Active = 1
	ORDER BY order30 DESC
	
	OPEN sCursor
	FETCH NEXT FROM sCursor INTO @mfgID, @mfgName, @orderTT2, @perc

	WHILE @@FETCH_STATUS = 0
	   BEGIN		
		IF @COUNTer = 1
		   BEGIN
			SET @COUNTer = 2
			SET @bgcolor = '#FFFFFF'
		   END
		ELSE
		   BEGIN
			SET @COUNTer = 1
			SET @bgcolor = '#bed6e9'
		   END
		   
		IF @perc > 0
			SET @fONtColor = '#000000'
		ELSE
			SET @fONtColor = '#CC0000'
			
		SET @body1 = @body1 + ' <tr style="background-color:' + @bgcolor + ';">
		<td lign="top">' + @mfgName + ' (' + @mfgID + ')</td>
		<td align="center" style="color:' + @fONtColor + ';">' + CAST(@orderTT2 AS VARCHAR(10)) + ' (' + CAST(@perc AS VARCHAR(100)) + '%)</td>
	  </tr>'
	  	  	
		FETCH NEXT FROM sCursor INTO @mfgID, @mfgName, @orderTT2, @perc
	   END
	CLOSE sCursor
	DEALLOCATE sCursor
		
	SET @body1 = @body1 + '</TABLE></td>'


	--POPULATING WITH MISSING TRACKING
	SET @body1 = @body1 + '<td><table border="0" cellspacing="0" cellpadding="3" style="border:solid 1px #000000;">
  <tr style="background-color:#446fb7; color:#FFFFFF; fONt-weight: bold;">
	<td colspan="5" style="fONt-size:12px;">TOP 10 Mfg with Missing Tracking</td>
  </tr>  
  <tr style="background-color:#446fb7; color:#FFFFFF; fONt-weight: bold;">
    <td style="width: 150px;">Manufacturers</td>
    <td style="text-align:center; width: 70px;">Num Orders</td>
  </tr>' 

	DECLARE sCursor CURSOR FOR 	
	SELECT TOP 10 CAST(LEFT(il.[No_], 3) AS VARCHAR(3)), mfgName, COUNT(DISTINCT i.[No_])	
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il 
			ON i.[No_]  = il.[Document No_]
		JOIN mfg m ON m.mfgID = LEFT(il.[No_], 3) COLLATE Latin1_General_CS_AS
	WHERE datediff(day, i.[Order Date], getdate()) BETWEEN 3 AND 60
		AND il.[No_] in (SELECT [No_] FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] WHERE LEN(ISNULL([No_], '')) > 3)	
		AND LEN(ISNULL(i.[Package Tracking No_], '')) = 0		
		AND i.[Shipment Method Code] <> 'PICK UP'
		AND CAST(LEFT(il.[No_], 3) AS VARCHAR(3)) <> '100'
	GROUP BY LEFT(il.[No_], 3), mfgName
	ORDER BY 3 DESC
	
	OPEN sCursor
	FETCH NEXT FROM sCursor INTO @mfgID, @mfgName,@orderTT2

	WHILE @@FETCH_STATUS = 0
	   BEGIN		
		IF @COUNTer = 1
		   BEGIN
			SET @COUNTer = 2
			SET @bgcolor = '#FFFFFF'
		   END
		ELSE
		   BEGIN
			SET @COUNTer = 1
			SET @bgcolor = '#bed6e9'
		   END
			
		SET @body1 = @body1 + ' <tr style="background-color:' + @bgcolor + ';">
		<td>' + @mfgName + ' (' + @mfgID + ')</td>
		<td align="center">' + CAST(@orderTT2 AS VARCHAR(10)) + '</td>
	  </tr>'
	  	
		FETCH NEXT FROM sCursor INTO @mfgID, @mfgName,@orderTT2
	   END
	CLOSE sCursor
	DEALLOCATE sCursor  	
  	  	
	SET @body1 = @body1 + '</table></td></tr></table>'
	
	SET @body1 = @body1 + '	</body>
	</html>'
	
	-- PREPPING THE EMAIL TO SEND
	SET @email1 = 'dlu@katom.com; pbible@katom.com; pchesworth@katom.com; cbible@katom.com; jchesworth@katom.com; jrogers@katom.com'
--	SET @email1 = 'david@katom.com'
	SET @subject1 = 'Daily Stat For '  + CAST(@mm AS VARCHAR(2)) + '/' + CAST(@dd AS VARCHAR(2)) + '/' + CAST(@yyyy AS VARCHAR(4))

	EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients=@email1,	@subject=@subject1,	@body=@body1, @body_format ='HTML',	@blind_copy_recipients='katom.archive@gmail.com'	

END
GO
