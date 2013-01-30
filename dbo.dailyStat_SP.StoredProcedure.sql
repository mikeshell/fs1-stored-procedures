USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[dailyStat_SP]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[dailyStat_SP]
AS
BEGIN
	DECLARE @year Smallint, @month tinyint, @weekday tinyint, @day tinyint, 
		@salesTT money, @orderTT int, @today datetime

	SET @today = GETDATE()
	SET @year = YEAR(@today)
	SET @month = MONTH(@today)
	SET @weekday = DATEPART(WEEKDAY, @today)
	SET @day = DAY(@today)

	TRUNCATE TABLE dailyStat

	--POPULATING YTD DATA
	INSERT INTO dailyStat(orderType, orderTypeID, YTDSale, YTDOrder)
	SELECT
		CASE ih.[Customer Source]
			WHEN 'G' THEN 'GSA'
			WHEN 'I' THEN 'Internet'
			WHEN 'S' THEN 'Oustide Sales'
			WHEN 'C' THEN 'Catalog'
			WHEN 'B' THEN 'Bid'
			WHEN 'U' THEN 'Uncategorized'
			WHEN 'E' THEN 'eBay'
			WHEN 'FA' THEN 'Frosty Acres'
		END [Customer Source], ih.[Customer Source],
		CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)),
		COUNT(distinct ISNULL(ih.[No_], 0)) orderTT		
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il
			ON ih.[No_]  = il.[Document No_]
	WHERE YEAR(ih.[Posting Date]) = @year
		AND ih.[Source Code] = 'SALES'
		AND LEN(ISNULL(ih.[Customer Source], '')) > 0
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY [Customer Source], ih.[Customer Source]
	ORDER BY 1 ASC

	--POPULATING LAST YTD DATA
	TRUNCATE TABLE dailyStatWorktable
	INSERT INTO dailyStatWorktable(orderTypeID, salesTT, orderTT)
	SELECT
		ih.[Customer Source],
		CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)),
		COUNT(distinct ISNULL(ih.[No_], 0)) orderTT		
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il
			ON ih.[No_]  = il.[Document No_]
	WHERE ih.[Posting Date] BETWEEN '01/01/' + CAST((@year-1) AS VARCHAR(4)) 
								AND DATEADD(YEAR, -1, @today)	
		AND ih.[Source Code] = 'SALES'
		AND LEN(ISNULL(ih.[Customer Source], '')) > 0
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY ih.[Customer Source]
	ORDER BY 1 ASC

	UPDATE d
	SET d.lastYTDSale = ISNULL(w.salesTT, 0),
		d.lastYTDOrder = ISNULL(w.orderTT, 0)
	FROM dailyStat d JOIN dailyStatWorktable w ON d.orderTypeID = w.orderTypeID

	SELECT @salesTT = SUM(salesTT), @orderTT = SUM(orderTT) 
	FROM dailystatworktable  
	WHERE orderTypeID NOT IN (SELECT orderTypeID from dailystat WHERE orderTypeID <> 'U')

	UPDATE dailyStat 
	SET lastYTDSale = @salesTT,
		lastYTDOrder = @orderTT
	WHERE orderTypeID = 'U' 

	--POPULATING MTD DATA
	TRUNCATE TABLE dailyStatWorktable
	INSERT INTO dailyStatWorktable(orderTypeID, salesTT, orderTT)
	SELECT
		ih.[Customer Source],
		CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)),
		COUNT(distinct ISNULL(x.[No_], 0)) orderTT		
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il
			ON ih.[No_]  = il.[Document No_]
		JOIN (SELECT DISTINCT [No_] FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header]) as X 
			ON X.[No_] = ih.[No_]
	WHERE YEAR(ih.[Posting Date]) = @year AND MONTH(ih.[Posting Date]) = @month
		AND ih.[Source Code] = 'SALES'
		AND LEN(ISNULL(ih.[Customer Source], '')) > 0
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY ih.[Customer Source]
	ORDER BY 1 ASC

	UPDATE d
	SET d.MTDSale = ISNULL(w.salesTT, 0),
		d.MTDOrder = ISNULL(w.orderTT, 0)
	FROM dailyStat d JOIN dailyStatWorktable w ON d.orderTypeID = w.orderTypeID

	--POPULATING LAST MTD DATA	
	DECLARE @lastMO tinyint, @lastYR smallint
	
	IF @month = 1
	   BEGIN
		SET @lastMO = 12
	   END
	ELSE
	   BEGIN
		SET @lastMO = @month-1
	   END
	   
	IF @lastMO > @month
	   BEGIN
		SET @lastYR = @year-1
	   END
	ELSE
	   BEGIN
		SET @lastYR = @year
	   END
	   	
	TRUNCATE TABLE dailyStatWorktable
	INSERT INTO dailyStatWorktable(orderTypeID, salesTT, orderTT)
	SELECT
		ih.[Customer Source],
		CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)),
		COUNT(distinct ISNULL(ih.[No_], 0)) orderTT		
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il
			ON ih.[No_]  = il.[Document No_]
	WHERE ih.[Posting Date] BETWEEN CAST(@lastMO AS VARCHAR(10)) + '/01/' + CAST(@lastYR AS VARCHAR(10)) 
								AND DATEADD(MONTH, -1, @today)	
		AND ih.[Source Code] = 'SALES'
		AND LEN(ISNULL(ih.[Customer Source], '')) > 0
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY ih.[Customer Source]
	ORDER BY 1 ASC

	UPDATE d
	SET d.lastMTDSale = ISNULL(w.salesTT, 0),
		d.lastMTDOrder = ISNULL(w.orderTT, 0)
	FROM dailyStat d JOIN dailyStatWorktable w ON d.orderTypeID = w.orderTypeID

	--POPULATING WTD DATA
	DECLARE @STARWK INT, @ENDWK INT

	SET @STARWK = datepart(week, @today)

	IF @STARWK < 5
	   BEGIN
		SET @ENDWK = datepart(week, '12/01/' + CAST(@year-1 AS VARCHAR(4)))-(5-@STARWK)
	   END
	ELSE
	   BEGIN
		SET @ENDWK = @STARWK-5
	   END
	   
	TRUNCATE TABLE dailyStatWorktable
	INSERT INTO dailyStatWorktable(orderTypeID, salesTT, orderTT)
	SELECT
		ih.[Customer Source],
		CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)),
		COUNT(distinct ISNULL(ih.[No_], 0)) orderTT		
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il
			ON ih.[No_]  = il.[Document No_]
	WHERE datepart(week, ih.[Posting Date]) = @STARWK AND YEAR(ih.[Posting Date]) = @year
		AND ih.[Source Code] = 'SALES'
		AND LEN(ISNULL(ih.[Customer Source], '')) > 0
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY ih.[Customer Source]
	ORDER BY 1 ASC

	UPDATE d
	SET d.WTDSale = ISNULL(w.salesTT, 0),
		d.WTDOrder = ISNULL(w.orderTT, 0)
	FROM dailyStat d JOIN dailyStatWorktable w ON d.orderTypeID = w.orderTypeID

	--POPULATING LAST WTD DATA
	DECLARE @LASTWK INT
	
	SET @LASTWK = datepart(week, DATEADD(DAY, -7, @today))
	
	TRUNCATE TABLE dailyStatWorktable
	INSERT INTO dailyStatWorktable(orderTypeID, salesTT, orderTT)
	SELECT
		ih.[Customer Source],
		CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)),
		COUNT(distinct ISNULL(ih.[No_], 0)) orderTT		
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il
			ON ih.[No_]  = il.[Document No_]
	WHERE datepart(week, ih.[Posting Date]) = datepart(week, DATEADD(DAY, -7, GETDATE())) AND YEAR(ih.[Posting Date]) = YEAR(DATEADD(DAY, -7, GETDATE()))
		AND datepart(weekday, ih.[Posting Date]) BETWEEN 1 AND datepart(weekday, @today)
		AND ih.[Source Code] = 'SALES'
		AND LEN(ISNULL(ih.[Customer Source], '')) > 0
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY ih.[Customer Source]
	ORDER BY 1 ASC

	UPDATE d
	SET d.lastWTDSale = ISNULL(w.salesTT, 0),
		d.lastWTDOrder = ISNULL(w.orderTT, 0)
	FROM dailyStat d JOIN dailyStatWorktable w ON d.orderTypeID = w.orderTypeID

	--POPULATING TODAY DATA
	TRUNCATE TABLE dailyStatWorktable
	INSERT INTO dailyStatWorktable(orderTypeID, salesTT, orderTT)
	SELECT
		ih.[Customer Source],
		CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)),
		COUNT(distinct ISNULL(ih.[No_], 0)) orderTT		
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il
			ON ih.[No_]  = il.[Document No_]
	WHERE DATEDIFF(DAY, ih.[Posting Date], @today) = 0
		AND ih.[Source Code] = 'SALES'
		AND LEN(ISNULL(ih.[Customer Source], '')) > 0
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY ih.[Customer Source]
	ORDER BY 1 ASC

	UPDATE d
	SET d.todaySale = ISNULL(w.salesTT, 0),
		d.todayOrder = ISNULL(w.orderTT, 0)
	FROM dailyStat d JOIN dailyStatWorktable w ON d.orderTypeID = w.orderTypeID

	--POPULATING LAST WEEK SAME DAY DATA
	TRUNCATE TABLE dailyStatWorktable
	INSERT INTO dailyStatWorktable(orderTypeID, salesTT, orderTT)
	SELECT
		ih.[Customer Source],
		CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)),
		COUNT(distinct ISNULL(ih.[No_], 0)) orderTT		
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il
			ON ih.[No_]  = il.[Document No_]
	WHERE DATEDIFF(DAY, ih.[Posting Date], @today) = 7
		AND ih.[Source Code] = 'SALES'
		AND LEN(ISNULL(ih.[Customer Source], '')) > 0
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY [Customer Source], ih.[Customer Source]
	ORDER BY 1 ASC

	UPDATE d
	SET d.lastweekSale = ISNULL(w.salesTT, 0),
		d.lastweekOrder = ISNULL(w.orderTT, 0)
	FROM dailyStat d JOIN dailyStatWorktable w ON d.orderTypeID = w.orderTypeID

	--POPULATING TODAY OPEN SALE DATA
	TRUNCATE TABLE dailyStatWorktable
	INSERT INTO dailyStatWorktable(orderTypeID, salesTT, orderTT)
	SELECT
		ih.[Customer Source],
		CAST(SUM(ISNULL(il.[Outstanding Amount], 0)+ISNULL(il.[Shipped Not Invoiced], 0)) as decimal(10, 2)),
		COUNT(distinct ISNULL(ih.[No_], 0)) orderTT		
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] il
			ON ih.[No_]  = il.[Document No_]
	WHERE DATEDIFF(DAY, [Order Date], @today) = 0
		AND LEN(ISNULL(ih.[Customer Source], '')) > 0
		AND ih.[Document Type] = 1
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY ih.[Customer Source]
	ORDER BY 1 ASC

	UPDATE d
	SET d.openSale = ISNULL(w.salesTT, 0),
		d.openOrder = ISNULL(w.orderTT, 0)
	FROM dailyStat d JOIN dailyStatWorktable w ON d.orderTypeID = w.orderTypeID


	--POPULATING 6 WEEK AVG DATA	   
	TRUNCATE TABLE dailyStatWorktable
	INSERT INTO dailyStatWorktable(orderTypeID, orderDT, salesTT, orderTT)
	SELECT
		ih.[Customer Source],
		ih.[Posting Date],
		CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)),
		COUNT(distinct ISNULL(ih.[No_], 0)) orderTT		
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il
			ON ih.[No_]  = il.[Document No_]
	WHERE ih.[Posting Date] BETWEEN DATEADD(Week, -6, @today) AND @today
		AND datepart(weekday, ih.[Posting Date]) = datepart(weekday, @today)
		AND ih.[Source Code] = 'SALES'
		AND LEN(ISNULL(ih.[Customer Source], '')) > 0
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY [Customer Source], ih.[Posting Date]
	ORDER BY 1, 2 ASC

	INSERT INTO dailyStatWorktable(orderTypeID, salesTT, orderTT)
	select ordertypeID, 
			AVG(salesTT),
			AVG(orderTT)
	from dailystatworktable
	group by ordertypeID

	UPDATE d
	SET d.[6weekAvgSale] = ISNULL(w.salesTT, 0),
		d.[6weeksAvgOrder] = ISNULL(w.orderTT, 0)
	FROM dailyStat d JOIN dailyStatWorktable w ON d.orderTypeID = w.orderTypeID
	WHERE w.orderDT IS NULL

	truncate table dailyStatWorktable
	
	--POPULATING MFG SALES STAT
	TRUNCATE TABLE mfgWorktable
	UPDATE mfg SET sales30 = 0, order30 = 0, order3060 = 0, sales3060 = 0

	INSERT INTO mfgWorktable
	SELECT CAST(LEFT(il.[No_], 3) AS VARCHAR(3)), SUM(il.[Quantity]), SUM(il.[Amount])
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il 
			ON i.[No_]  = il.[Document No_]
		LEFT JOIN mfg m ON m.mfgID = LEFT(il.[No_], 3) COLLATE Latin1_General_CS_AS
	WHERE datediff(day, i.[Order Date], getdate()) < 31
		AND il.[No_] in (SELECT [No_] FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Item] WHERE LEN(ISNULL([No_], '')) > 3)	
	GROUP BY LEFT(il.[No_], 3)

	UPDATE m
	SET m.sales30 = w.salesTT,
		m.order30 = w.orderTT
	FROM mfg m JOIN mfgWorktable w ON m.mfgID = w.mfgid

	TRUNCATE TABLE mfgWorktable

	INSERT INTO mfgWorktable
	SELECT CAST(LEFT(il.[No_], 3) AS VARCHAR(3)), SUM(il.[Quantity]), SUM(il.[Amount])
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il 
			ON i.[No_]  = il.[Document No_]
		LEFT JOIN mfg m ON m.mfgID = LEFT(il.[No_], 3) COLLATE Latin1_General_CS_AS
	WHERE datediff(day, i.[Order Date], getdate()) between 31 AND 60
		AND il.[No_] in (SELECT [No_] FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Item] WHERE LEN(ISNULL([No_], '')) > 3)	
	GROUP BY LEFT(il.[No_], 3)

	UPDATE m
	SET m.sales3060 = w.salesTT,
		m.order3060 = w.orderTT
	FROM mfg m JOIN mfgWorktable w ON m.mfgID = w.mfgid
	
	TRUNCATE TABLE mfgWorktable
END
GO
