USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[salesByMediumMM_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		David Lu
-- Create date: 03/28/2011
-- Description:	Pulling Sales Data by category or Mfg
-- =============================================
CREATE PROCEDURE [dbo].[salesByMediumMM_SP]
AS
BEGIN
	DECLARE @today datetime, @startDT datetime, @endDT datetime, @weekDay tinyint,
			@year Smallint, @month tinyint, @day tinyint, @startingWeek tinyint, @endingWeek tinyint
		
	TRUNCATE TABLE salesRptByMedium_MM
	INSERT INTO salesRptByMedium_MM(orderMedium) VALUES('Phone')
	INSERT INTO salesRptByMedium_MM(orderMedium) VALUES('Ebay')
	INSERT INTO salesRptByMedium_MM(orderMedium) VALUES('Web')
	INSERT INTO salesRptByMedium_MM(orderMedium) VALUES('Frosty Acres')
	INSERT INTO salesRptByMedium_MM(orderMedium) VALUES('Pilot')
	INSERT INTO salesRptByMedium_MM(orderMedium) VALUES('Cancelled')
	INSERT INTO salesRptByMedium_MM(orderMedium) VALUES('B&B')

	DECLARE @wk INT, @oMedium VARCHAR(255), @order INT, @Sales DECIMAL(10, 2), @sql VARCHAR(MAX)

	-- POPULATING SALES INVOICES DATA
	DECLARE catCursor CURSOR FOR 
	SELECT MONTH([Order Date]),
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
	WHERE YEAR([Order Date]) = 2012
		AND [Source code] = 'SALES'
		AND l.[Unit Price] > 0
		AND l.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY MONTH([Order Date]),
		CASE 
			WHEN LEN(LEFT([Web Order No_], 2)) = 0 THEN 'Phone'
			WHEN ISNUMERIC(LEFT([Web Order No_], 2)) = 1 THEN 'Ebay'
			WHEN LEFT([Web Order No_], 2) = 'KT' THEN 'Web'
			WHEN LEFT([Web Order No_], 2) = 'FA' THEN 'Frosty Acres'
			WHEN LEFT([Web Order No_], 2) = 'PI' THEN 'Pilot'
			WHEN LEFT([Web Order No_], 2) = 'BB' THEN 'B&B'
		END
	UNION
	SELECT MONTH([Order Date]), 'Cancelled',
		COUNT(distinct i.[No_]), SUM(CAST([Quantity]*[Unit Price] AS DECIMAL(10, 2)))
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l ON i.[No_] = l.[Document No_]
	WHERE YEAR([Order Date]) = 2012
		AND [Source code] = 'DELETE'
		AND l.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY MONTH([Order Date])
	ORDER BY 2, 1

	OPEN catCursor
	FETCH NEXT FROM catCursor INTO @wk, @oMedium, @order, @Sales

	WHILE @@FETCH_STATUS = 0
	   BEGIN
	    
		SET @sql = 'UPDATE salesRptByMedium_MM SET M' + 
			CAST(@wk AS VARCHAR(10)) + 'io = ' + CAST(@order AS VARCHAR(10)) + ',  M' + 
			CAST(@wk AS VARCHAR(10)) + 'is = ' + CAST(@Sales AS VARCHAR(100)) + 
			' WHERE orderMedium = ''' + @oMedium + ''''		   
	       
		EXEC(@sql)
			   
		FETCH NEXT FROM catCursor INTO @wk, @oMedium, @order, @Sales
	   END

	CLOSE catCursor
	DEALLOCATE catCursor

	-- POPULATING OPEN SALES DATA
	DECLARE catCursor CURSOR FOR 
	SELECT MONTH([Order Date]),
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
	WHERE YEAR([Order Date]) = 2012
		AND ih.[Document Type] = 1
		AND il.[Unit Price] > 0
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY MONTH([Order Date]),
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
		SET @sql = 'UPDATE salesRptByMedium_MM SET M' + 
				CAST(@wk AS VARCHAR(10)) + 'oo = ' + CAST(@order AS VARCHAR(10)) + ',  M' + 
				CAST(@wk AS VARCHAR(10)) + 'os = ' + CAST(@Sales AS VARCHAR(100)) + 
				' WHERE orderMedium = ''' + @oMedium + ''''
		
		EXEC(@sql)
			   
		FETCH NEXT FROM catCursor INTO @wk, @oMedium, @order, @Sales
	   END

	CLOSE catCursor
	DEALLOCATE catCursor		
END
GO
