USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[openOrder_Email]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[openOrder_Email]
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
		@dow VARCHAR(20), @wok TINYINT, @counter SMALLINT, @bgcolor VARCHAR(50)
			
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
	SET @counter = 1
	SET @bgcolor = '#bed6e9'

	--INITIATING DATE
	SET @today = GETDATE()
	SET @yyyy = YEAR(@today)
	SET @mm = MONTH(@today)
	SET @dd = DAY(@today)
	SET @dow = DATENAME(WEEKDAY, @today)
	SET @wok = DATEPART(WEEK, @today)

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

	--POPULATING SHIP DIRECT DATA	
	TRUNCATE TABLE dailyStat_shipdirect
	INSERT INTO dailyStat_shipdirect([date], orderNum, sales)
	select h.[Order Date], count(distinct h.[No_]), sum(distinct l.Amount)
	from fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Header] h
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Line] l ON h.[No_] = l.[Document No_]
	where h.[Ship-to Name] <> 'WAREHOUSE FACILITY' and LEN(ISNULL(l.[No_], '')) > 0 
			and l.Amount > 0 and l.[No_] not in ('555', '540', '205')
			and DATEDIFF(day, h.[Order Date], getdate()) < 15
	group by h.[Order Date]
	order by h.[Order Date] desc

	INSERT INTO dailyStatWorktable(orderDT, orderTT, salesTT)
	select [Order Date], count(distinct h.[No_]), sum(l.Amount)
	from fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Purch_ Inv_ Header] h
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Purch_ Inv_ Line] l ON h.[No_] = l.[Document No_]
	where h.[Ship-to Name] <> 'WAREHOUSE FACILITY' and LEN(ISNULL(l.[No_], '')) > 0 
			and l.Amount > 0 and l.[No_] not in ('555', '540', '205')
			and DATEDIFF(day, [Order Date], getdate()) < 15
	group by [Order Date]
	order by [Order Date] desc

	UPDATE s
	SET s.orderNum = ISNULL(s.orderNum, 0) + w.orderTT,
		s.sales = ISNULL(s.sales, 0) + w.salesTT
	FROM dailyStat_shipdirect s JOIN dailyStatWorktable w on s.[date] = w.orderDT

	TRUNCATE TABLE dailyStatWorktable
	
	INSERT INTO dailyStatWorktable(orderDT, salesTT, orderTT)
	SELECT	ih.[Order Date],
		CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)),
		COUNT(distinct ISNULL(x.[No_], 0))		
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
		AND LEFT(il.[No_], 3) IN (select mfgid collate SQL_Latin1_General_CP1_CI_AS from mfg where Active = 1 and shipDirect = 1)
		AND il.[No_] NOT IN (
					select [No_]
					from fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item]
					where [Maximum Inventory] > 0
					)
	GROUP BY ih.[Order Date]

	UPDATE s
	SET s.openOrders = w.orderTT,
		s.openSales = w.salesTT
	FROM dailyStat_shipdirect s JOIN dailyStatWorktable w on s.[date] = w.orderDT

	TRUNCATE TABLE dailyStatWorktable

	SET @counter = 1
	SET @body1 = @body1 + '<br />
	<table border="0" cellspacing="0" cellpadding="3" style="border:solid 1px #000000;">
	<tr>
	<td>
	<table border="0" cellspacing="0" cellpadding="3" style="border:solid 1px #000000;">
  <tr style="background-color:#446fb7; color:#FFFFFF; font-weight: bold;">
	<td colspan="6" style="font-size:12px;">Order Status In The Last 10 Business Days</td>
  </tr>  
  <tr style="background-color:#446fb7; color:#FFFFFF; font-weight: bold;">
    <td style="text-align:center; width: 75px;">&nbsp;</td>
    <td style="text-align:center; width: 100px;">Ship Direct</td>
    <td style="text-align:center; width: 100px;">Invoiced</td>
    <td style="text-align:center; width: 100px;">Open</td>
    <td style="text-align:center; width: 100px;">Open Internet</td>
    <td style="text-align:center; width: 100px;">Open Ship Direct</td>
  </tr>'
	
	DECLARE @orderDT varchar(20), @SalesShipDirect money, @orderShipDirect int, 
			@salesTT varchar(20), @orderTT varchar(10), @openSalesTT varchar(20), @openOrderTT2 varchar(10),
			@internetSalesTT varchar(20), @internetOrderTT varchar(10)
			
	DECLARE sCursor CURSOR FOR 
	SELECT CONVERT(VARCHAR(10), d.[date], 101),  d.orderNum, d.Sales, 
		'$' + CONVERT(varchar(50), CAST(LEFT(s.TotalInvoiced, CHARINDEX('(', s.totalinvoiced)-2) AS MONEY), 1) salesTT, 
		right(s.totalinvoiced, LEN(s.totalinvoiced)-CHARINDEX('(', s.totalinvoiced)+1) orderTT, 
		'$' + CONVERT(varchar(50), CAST(LEFT(s.totalSales, CHARINDEX('(', s.totalSales)-2) AS MONEY), 1) openSalesTT, 
		right(s.totalSales, LEN(s.totalSales)-CHARINDEX('(', s.totalSales)+1) openOrdersTT, 
		'$' + CONVERT(varchar(50), CAST(LEFT(s.InternetSales, CHARINDEX('(', s.InternetSales)-2) AS MONEY), 1) InternetSalesTT, 
		right(s.InternetSales, LEN(s.InternetSales)-CHARINDEX('(', s.InternetSales)+1) InternetOrdersTT,
		d.openSales, d.openOrders
	FROM dailyStat_shipdirect d JOIN salesStat30Days s on DATEDIFF(DAY, d.[date], s.[Date]) = 0
	ORDER BY d.[date] desc

	OPEN sCursor
	FETCH NEXT FROM sCursor INTO @orderDT, @orderShipDirect, @SalesShipDirect, @salesTT, @orderTT, 
								 @openSalesTT, @openOrderTT2, @internetSalesTT, @internetOrderTT,
								 @openSales_SD, @openOrders_SD

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
		  <tr style="background-color:#446fb7; color:#FFFFFF; font-weight: bold;">
			<td colspan="6" style="font-size:12px;">Open Order Status In The Last 10 Business Days</td>
		  </tr>  
		  <tr style="background-color:#446fb7; color:#FFFFFF; font-weight: bold;">
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
		
	SET @body1 = @body1 + '	</body>
	</html>'
	
	-- PREPPING THE EMAIL TO SEND
	SET @email1 = 'alice@katom.com; betty@katom.com; charley@katom.com'
--	SET @email1 = 'david@katom.com'
	SET @subject1 = 'Open Orders and Ship Direct Stat For '  + CAST(@mm AS VARCHAR(2)) + '/' + CAST(@dd AS VARCHAR(2)) + '/' + CAST(@yyyy AS VARCHAR(4))

	EXEC msdb.dbo.sp_send_dbmail @profile_name='katom', @recipients=@email1,	@subject=@subject1,	@body=@body1, @body_format ='HTML',	@blind_copy_recipients='katom.archive@gmail.com'	

END
GO
