USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[salesStat30DaysPopulationWarehouse_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[salesStat30DaysPopulationWarehouse_SP]
AS
BEGIN
DECLARE @Source varchar(1), @SQL varchar(1000), @NumDate smallint

SET @NumDate = 100


--RESET salesStat30Days TBL
TRUNCATE TABLE salesStat30Days
TRUNCATE TABLE SalesStatWorktable

INSERT INTO salesStat30Days([Date])
SELECT DISTINCT [Posting Date] 
FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] 
WHERE DATEDIFF(DAY, [Posting Date], GETDATE()) < @NumDate
	AND [Source Code] = 'SALES'
ORDER BY [Posting Date] DESC

SET @SQL = ''

DECLARE myCursor CURSOR FOR
SELECT DISTINCT [Customer Source] 
FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header]
WHERE DATEDIFF(DAY, [Posting Date], GETDATE()) < @NumDate
	AND [Source Code] = 'SALES'

OPEN myCursor
FETCH NEXT FROM myCursor INTO @Source

WHILE @@FETCH_STATUS = 0
   BEGIN
	--PROCESSING INVOICED DATA
	INSERT INTO SalesStatWorktable
	SELECT
		ih.[Posting Date],
		CAST(SUM(ISNULL(il.[Amount], 0)) as decimal(10, 2)),
		COUNT(distinct ISNULL(x.[No_], 0)) orderTT		
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il
			ON ih.[No_]  = il.[Document No_]
		JOIN (SELECT DISTINCT [No_] FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header]) as X 
			ON X.[No_] = ih.[No_]
	WHERE DATEDIFF(DAY, ih.[Posting Date], GETDATE()) < @NumDate
		AND ih.[Source Code] = 'SALES'
		AND ih.[Customer Source] = @Source
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
		AND il.[Drop Shipment] = 0
	GROUP BY ih.[Posting Date]
	ORDER BY ih.[Posting Date] DESC

	SET @SQL = 'UPDATE s SET ' + 
				CASE @Source
					WHEN 'G' THEN ' s.GSAInvoiced = '
					WHEN 'I' THEN ' s.InternetInvoiced = '
					WHEN 'S' THEN ' s.salesmenInvoiced = '
					WHEN 'C' THEN ' s.catalogInvoiced = '
					WHEN 'B' THEN ' s.BidInvoiced = '
					WHEN 'U' THEN ' s.unknownInvoiced = '
				END + 
				'CAST(w.sales as varchar(20)) + '' ('' + CAST(w.orderCount as varchar(5)) + '')'' 
				 FROM salesStat30Days s JOIN SalesStatWorktable w ON s.[Date] = w.[Date]'
	EXEC(@SQL)
	SET @SQL = ''
	TRUNCATE TABLE SalesStatWorktable

	--PROCESSING SALES DATA
	INSERT INTO SalesStatWorktable
	SELECT
		ih.[Posting Date],
		CAST(SUM(ISNULL(il.[Amount], 0)) as decimal(10, 2)),
		COUNT(distinct ISNULL(x.[No_], 0)) orderTT		
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] il
			ON ih.[No_]  = il.[Document No_]
		JOIN (SELECT DISTINCT [No_] FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header]) as X 
			ON X.[No_] = ih.[No_]
	WHERE DATEDIFF(DAY, ih.[Posting Date], GETDATE()) < @NumDate
		AND ih.[Customer Source] = @Source
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
		AND il.[Drop Shipment] = 0
	GROUP BY ih.[Posting Date]
	ORDER BY ih.[Posting Date] DESC

	SET @SQL = 'UPDATE s SET ' + 
				CASE @Source
					WHEN 'G' THEN ' s.GSASales = '
					WHEN 'I' THEN ' s.InternetSales = '
					WHEN 'S' THEN ' s.salesmenSales = '
					WHEN 'C' THEN ' s.catalogSales = '
					WHEN 'B' THEN ' s.BidSales = '
					WHEN 'U' THEN ' s.unknownSales = '
				END + 
				'CAST(w.sales as varchar(20)) + '' ('' + CAST(w.orderCount as varchar(5)) + '')'' 
				 FROM salesStat30Days s JOIN SalesStatWorktable w ON s.[Date] = w.[Date]'
	EXEC(@SQL)
	SET @SQL = ''
	TRUNCATE TABLE SalesStatWorktable

	FETCH NEXT FROM myCursor INTO @Source
   END

CLOSE myCursor
DEALLOCATE myCursor


--PROCESSING TOTAL INVOICED DATA
INSERT INTO SalesStatWorktable
SELECT
	ih.[Posting Date],
	CAST(SUM(il.[Amount]) as decimal(10, 2)),
	COUNT(distinct x.[No_]) orderTT		
FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih
	JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il
		ON ih.[No_]  = il.[Document No_]
	JOIN (SELECT DISTINCT [No_] FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header]) as X 
		ON X.[No_] = ih.[No_]
WHERE DATEDIFF(DAY, ih.[Posting Date], GETDATE()) < @NumDate
	AND ih.[Source Code] = 'SALES'
	AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
		AND il.[Drop Shipment] = 0
GROUP BY ih.[Posting Date]
ORDER BY ih.[Posting Date] DESC

UPDATE s 
SET s.TotalInvoiced = CAST(w.sales as varchar(20)) + ' (' + CAST(w.orderCount as varchar(5)) + ')'
FROM salesStat30Days s JOIN SalesStatWorktable w ON s.[Date] = w.[Date]

TRUNCATE TABLE SalesStatWorktable

--PROCESSING TOTAL SALES DATA
INSERT INTO SalesStatWorktable
SELECT
	ih.[Posting Date],
	CAST(SUM(il.[Amount]) as decimal(10, 2)),
	COUNT(distinct x.[No_]) orderTT		
FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] ih
	JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] il
		ON ih.[No_]  = il.[Document No_]
	JOIN (SELECT DISTINCT [No_] FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header]) as X 
		ON X.[No_] = ih.[No_]
WHERE DATEDIFF(DAY, ih.[Posting Date], GETDATE()) < @NumDate
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
		AND il.[Drop Shipment] = 0
GROUP BY ih.[Posting Date]
ORDER BY ih.[Posting Date] DESC

UPDATE s 
SET s.TotalSales = CAST(w.sales as varchar(20)) + ' (' + CAST(w.orderCount as varchar(5)) + ')'
FROM salesStat30Days s JOIN SalesStatWorktable w ON s.[Date] = w.[Date]

TRUNCATE TABLE SalesStatWorktable

END
GO
