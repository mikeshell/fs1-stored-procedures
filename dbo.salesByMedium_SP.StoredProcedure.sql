USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[salesByMedium_SP]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		David Lu
-- Create date: 03/28/2011
-- Description:	Pulling Sales Data by category or Mfg
-- =============================================
CREATE PROCEDURE [dbo].[salesByMedium_SP]
AS
BEGIN
	DECLARE @today datetime, @startDT datetime, @endDT datetime, @weekDay tinyint,
			@year Smallint, @month tinyint, @day tinyint, @startingWeek tinyint, @endingWeek tinyint

	SET @today = GETDATE()
	SET @weekDay = DATEPART(WEEKDAY, @today)
	SET @endDT = DATEADD(DAY, -@weekday, @today)
	SET @startDT = DATEADD(DAY, -7, DATEADD(WEEK, -5, @endDT))
	SET @startingWeek = DATEPART(WEEK, @startDT)
	SET @year = YEAR(@today)
	SET @month = MONTH(@today)
	SET @day = DAY(@today)
		
	TRUNCATE TABLE salesRptByMedium
	INSERT INTO salesRptByMedium(orderMedium) VALUES('Phone')
	INSERT INTO salesRptByMedium(orderMedium) VALUES('Ebay')
	INSERT INTO salesRptByMedium(orderMedium) VALUES('Web')
	INSERT INTO salesRptByMedium(orderMedium) VALUES('Frosty Acres')
	INSERT INTO salesRptByMedium(orderMedium) VALUES('Pilot')
	INSERT INTO salesRptByMedium(orderMedium) VALUES('Cancelled')
	INSERT INTO salesRptByMedium(orderMedium) VALUES('B&B')

	DECLARE @wk INT, @oMedium VARCHAR(255), @order INT, @Sales DECIMAL(10, 2), @sql VARCHAR(MAX)

	-- POPULATING SALES INVOICES DATA
	DECLARE catCursor CURSOR FOR 
	SELECT 
		CASE DATEDIFF(WEEK, [Order Date], getdate())
			WHEN 6 THEN 1
			WHEN 5 THEN 2
			WHEN 4 THEN 3
			WHEN 3 THEN 4
			WHEN 2 THEN 5
			WHEN 1 THEN 6
			ELSE 7
		END,
		CASE 
			WHEN LEN(LEFT([Web Order No_], 2)) = 0 THEN 'Phone'
			WHEN ISNUMERIC(LEFT([Web Order No_], 2)) = 1 THEN 'Ebay'
			WHEN LEFT([Web Order No_], 2) = 'KT' THEN 'Web'
			WHEN LEFT([Web Order No_], 2) = 'FA' THEN 'Frosty Acres'
			WHEN LEFT([Web Order No_], 2) = 'PI' THEN 'Pilot'
			WHEN LEFT([Web Order No_], 2) = 'BB' THEN 'B&B'
		END, 
		COUNT(distinct i.[No_]), SUM(CAST([Quantity]*[Unit Price] AS DECIMAL(10, 2)))
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l ON i.[No_] = l.[Document No_]
	WHERE [Order Date] BETWEEN @startDT AND @today
		AND [Source code] = 'SALES'
		AND l.[Unit Price] > 0
		AND l.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY 
		CASE DATEDIFF(WEEK, [Order Date], getdate())
			WHEN 6 THEN 1
			WHEN 5 THEN 2
			WHEN 4 THEN 3
			WHEN 3 THEN 4
			WHEN 2 THEN 5
			WHEN 1 THEN 6
			ELSE 7
		END,
		CASE 
			WHEN LEN(LEFT([Web Order No_], 2)) = 0 THEN 'Phone'
			WHEN ISNUMERIC(LEFT([Web Order No_], 2)) = 1 THEN 'Ebay'
			WHEN LEFT([Web Order No_], 2) = 'KT' THEN 'Web'
			WHEN LEFT([Web Order No_], 2) = 'FA' THEN 'Frosty Acres'
			WHEN LEFT([Web Order No_], 2) = 'PI' THEN 'Pilot'
			WHEN LEFT([Web Order No_], 2) = 'BB' THEN 'B&B'
		END
	UNION
	SELECT 
		CASE DATEDIFF(WEEK, [Order Date], getdate())
			WHEN 6 THEN 1
			WHEN 5 THEN 2
			WHEN 4 THEN 3
			WHEN 3 THEN 4
			WHEN 2 THEN 5
			WHEN 1 THEN 6
			ELSE 7
		END, 'Cancelled',
		COUNT(distinct i.[No_]), SUM(CAST([Quantity]*[Unit Price] AS DECIMAL(10, 2)))
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l ON i.[No_] = l.[Document No_]
	WHERE [Order Date] BETWEEN @startDT AND @today
		AND [Source code] = 'DELETE'
		AND l.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY 
		CASE DATEDIFF(WEEK, [Order Date], getdate())
			WHEN 6 THEN 1
			WHEN 5 THEN 2
			WHEN 4 THEN 3
			WHEN 3 THEN 4
			WHEN 2 THEN 5
			WHEN 1 THEN 6
			ELSE 7
		END
	ORDER BY 2, 1 DESC

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @wk, @oMedium, @order, @Sales

	WHILE @@FETCH_STATUS = 0
	   BEGIN
	    
		IF @wk = 7
		   BEGIN
			SET @sql = 'UPDATE salesRptByMedium SET WTDio = ' + CAST(@order AS VARCHAR(10)) + 
				',  WTDis = ' + CAST(@Sales AS VARCHAR(100)) + ' WHERE orderMedium = ''' + @oMedium + ''''
		   END
		ELSE
		   BEGIN
			SET @sql = 'UPDATE salesRptByMedium SET W' + 
				CAST(@wk AS VARCHAR(10)) + 'io = ' + CAST(@order AS VARCHAR(10)) + ',  W' + 
				CAST(@wk AS VARCHAR(10)) + 'is = ' + CAST(@Sales AS VARCHAR(100)) + ' WHERE orderMedium = ''' + @oMedium + ''''
		   END
	       
		EXEC(@sql)
			   
		FETCH NEXT FROM catCursor INTO @wk, @oMedium, @order, @Sales
	   END

	CLOSE catCursor
	DEALLOCATE catCursor

	-- POPULATING OPEN SALES DATA
	DECLARE catCursor CURSOR FOR 
	SELECT
		CASE DATEDIFF(WEEK, ih.[Order Date], getdate())
			WHEN 6 THEN 1
			WHEN 5 THEN 2
			WHEN 4 THEN 3
			WHEN 3 THEN 4
			WHEN 2 THEN 5
			WHEN 1 THEN 6
			ELSE 7
		END,
		CASE 
			WHEN LEN(LEFT(ih.[Web Order No_], 2)) = 0 THEN 'Phone'
			WHEN ISNUMERIC(LEFT(ih.[Web Order No_], 2)) = 1 THEN 'Ebay'
			WHEN LEFT(ih.[Web Order No_], 2) = 'KT' THEN 'Web'
			WHEN LEFT(ih.[Web Order No_], 2) = 'FA' THEN 'Frosty Acres'
			WHEN LEFT(ih.[Web Order No_], 2) = 'PI' THEN 'Pilot'
			WHEN LEFT([Web Order No_], 2) = 'BB' THEN 'B&B'
		END,
		COUNT(distinct ISNULL(ih.[No_], 0)) orderTT	, 
		CAST(SUM(ISNULL(il.[Outstanding Amount], 0)+ISNULL(il.[Shipped Not Invoiced], 0)) as decimal(10, 2))
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] il
			ON ih.[No_]  = il.[Document No_]
	WHERE [Order Date] BETWEEN @startDT AND @today
		AND ih.[Document Type] = 1
		AND il.[Unit Price] > 0
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY CASE DATEDIFF(WEEK, ih.[Order Date], getdate())
			WHEN 6 THEN 1
			WHEN 5 THEN 2
			WHEN 4 THEN 3
			WHEN 3 THEN 4
			WHEN 2 THEN 5
			WHEN 1 THEN 6
			ELSE 7
		END,
		CASE 
			WHEN LEN(LEFT(ih.[Web Order No_], 2)) = 0 THEN 'Phone'
			WHEN ISNUMERIC(LEFT(ih.[Web Order No_], 2)) = 1 THEN 'Ebay'
			WHEN LEFT(ih.[Web Order No_], 2) = 'KT' THEN 'Web'
			WHEN LEFT(ih.[Web Order No_], 2) = 'FA' THEN 'Frosty Acres'
			WHEN LEFT(ih.[Web Order No_], 2) = 'PI' THEN 'Pilot'
			WHEN LEFT([Web Order No_], 2) = 'BB' THEN 'B&B'
		END
	ORDER BY 2

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @wk, @oMedium, @order, @Sales

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		IF @wk = 7
		   BEGIN
			SET @sql = 'UPDATE salesRptByMedium SET WTDoo = ' + CAST(@order AS VARCHAR(10)) + 
				',  WTDos = ' + CAST(@Sales AS VARCHAR(100)) + ' WHERE orderMedium = ''' + @oMedium + ''''
		   END
		ELSE
		   BEGIN
			SET @sql = 'UPDATE salesRptByMedium SET W' + 
				CAST(@wk AS VARCHAR(10)) + 'oo = ' + CAST(@order AS VARCHAR(10)) + ',  W' + 
				CAST(@wk AS VARCHAR(10)) + 'os = ' + CAST(@Sales AS VARCHAR(100)) + ' WHERE orderMedium = ''' + @oMedium + ''''
		   END
		
		EXEC(@sql)
			   
		FETCH NEXT FROM catCursor INTO @wk, @oMedium, @order, @Sales
	   END

	CLOSE catCursor
	DEALLOCATE catCursor
		
	TRUNCATE TABLE salesRptBySalesmen
	INSERT INTO salesRptBySalesmen(orderMedium)
	SELECT distinct [Salesperson Code]
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header]
	WHERE [Order Date] BETWEEN @startDT AND @today
		AND [Source code] = 'SALES'
		AND [Salesperson Code] <> 'WEBIMPORT'
	UNION
	SELECT distinct [Salesperson Code]
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header]
	WHERE [Order Date] BETWEEN @startDT AND @today
		AND [Document Type] = 1
		AND [Salesperson Code] <> 'WEBIMPORT'


	-- SALES BY SALESMEN
	-- POPULATING SALES INVOICES DATA
	DECLARE catCursor CURSOR FOR 
	SELECT 
		CASE DATEDIFF(WEEK, [Order Date], getdate())
			WHEN 6 THEN 1
			WHEN 5 THEN 2
			WHEN 4 THEN 3
			WHEN 3 THEN 4
			WHEN 2 THEN 5
			WHEN 1 THEN 6
			ELSE 7
		END, [Salesperson Code], 
		COUNT(distinct i.[No_]), SUM(CAST([Quantity]*[Unit Price] AS DECIMAL(10, 2)))
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l ON i.[No_] = l.[Document No_]
	WHERE [Order Date] BETWEEN @startDT AND @today
		AND [Source code] = 'SALES'
		AND [Salesperson Code] <> 'WEBIMPORT'
		AND l.[Unit Price] > 0
		AND l.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY 
		CASE DATEDIFF(WEEK, [Order Date], getdate())
			WHEN 6 THEN 1
			WHEN 5 THEN 2
			WHEN 4 THEN 3
			WHEN 3 THEN 4
			WHEN 2 THEN 5
			WHEN 1 THEN 6
			ELSE 7
		END,[Salesperson Code]

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @wk, @oMedium, @order, @Sales

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		IF @wk = 7
		   BEGIN
			SET @sql = 'UPDATE salesRptBySalesmen SET WTDio = ' + CAST(@order AS VARCHAR(10)) + 
				',  WTDis = ' + CAST(@Sales AS VARCHAR(100)) + ' WHERE orderMedium = ''' + @oMedium + ''''
		   END
		ELSE
		   BEGIN
			SET @sql = 'UPDATE salesRptBySalesmen SET W' + 
				CAST(@wk AS VARCHAR(10)) + 'io = ' + CAST(@order AS VARCHAR(10)) + ',  W' + 
				CAST(@wk AS VARCHAR(10)) + 'is = ' + CAST(@Sales AS VARCHAR(100)) + ' WHERE orderMedium = ''' + @oMedium + ''''
		   END
		
		
		EXEC(@sql)
			   
		FETCH NEXT FROM catCursor INTO @wk, @oMedium, @order, @Sales
	   END

	CLOSE catCursor
	DEALLOCATE catCursor

	-- POPULATING OPEN SALES DATA
	DECLARE catCursor CURSOR FOR
	SELECT
		CASE DATEDIFF(WEEK, ih.[Order Date], getdate())
			WHEN 6 THEN 1
			WHEN 5 THEN 2
			WHEN 4 THEN 3
			WHEN 3 THEN 4
			WHEN 2 THEN 5
			WHEN 1 THEN 6
			ELSE 7
		END, [Salesperson Code], 
		COUNT(distinct ISNULL(ih.[No_], 0)) orderTT	, 
		CAST(SUM(ISNULL(il.[Outstanding Amount], 0)+ISNULL(il.[Shipped Not Invoiced], 0)) as decimal(10, 2))
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] il
			ON ih.[No_]  = il.[Document No_]
	WHERE [Order Date] BETWEEN @startDT AND @today
		AND ih.[Document Type] = 1
		AND ih.[Salesperson Code] <> 'WEBIMPORT'
		AND il.[Unit Price] > 0
		AND il.[Unit Price] > 0
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY CASE DATEDIFF(WEEK, ih.[Order Date], getdate())
			WHEN 6 THEN 1
			WHEN 5 THEN 2
			WHEN 4 THEN 3
			WHEN 3 THEN 4
			WHEN 2 THEN 5
			WHEN 1 THEN 6
			ELSE 7
		END, [Salesperson Code]
	ORDER BY 2

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @wk, @oMedium, @order, @Sales

	WHILE @@FETCH_STATUS = 0
	   BEGIN
		IF @wk = 7
		   BEGIN
			SET @sql = 'UPDATE salesRptBySalesmen SET WTDoo = ' + CAST(@order AS VARCHAR(10)) + 
				',  WTDos = ' + CAST(@Sales AS VARCHAR(100)) + ' WHERE orderMedium = ''' + @oMedium + ''''
		   END
		ELSE
		   BEGIN
			SET @sql = 'UPDATE salesRptBySalesmen SET W' + 
				CAST(@wk AS VARCHAR(10)) + 'oo = ' + CAST(@order AS VARCHAR(10)) + ',  W' + 
				CAST(@wk AS VARCHAR(10)) + 'os = ' + CAST(@Sales AS VARCHAR(100)) + ' WHERE orderMedium = ''' + @oMedium + ''''
		   END
		
		
		EXEC(@sql)
			   
		FETCH NEXT FROM catCursor INTO @wk, @oMedium, @order, @Sales
	   END

	CLOSE catCursor
	DEALLOCATE catCursor
END
GO
