USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[salesByMediumMM_cat_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		David Lu
-- Create date: 03/28/2011
-- Description:	Pulling Sales Data by category or Mfg
-- =============================================
CREATE PROCEDURE [dbo].[salesByMediumMM_cat_SP]
AS
BEGIN
	DECLARE @today datetime, @startDT datetime, @endDT datetime, @weekDay tinyint,
			@year Smallint, @month tinyint, @day tinyint, @startingWeek tinyint, @endingWeek tinyint
		
	TRUNCATE TABLE salesRptByMedium_MM
	INSERT INTO salesRptByMedium_MM(orderMedium) VALUES('UNCAT')
	INSERT INTO salesRptByMedium_MM(orderMedium) 
	SELECT DISTINCT SUPERCAT
	FROM categories
	WHERE ACTIVE  = 1

	DECLARE @wk INT, @oMedium VARCHAR(255), @order INT, @Sales DECIMAL(10, 2), @sql VARCHAR(MAX)

	-- POPULATING SALES INVOICES DATA
	DECLARE catCursor CURSOR FOR 
	SELECT MONTH([Order Date]),
		isnull(c.SUPERCAT, 'UNCAT'), 
		COUNT(distinct i.[No_]), SUM(CAST([Quantity]*[Unit Price] AS DECIMAL(10, 2)))
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] i
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l ON i.[No_] = l.[Document No_]
		LEFT JOIN products p on p.CODE = l.[No_] COLLATE Latin1_General_CS_AS
		LEFT JOIN categories c on p.primaryCatCode = c.CODE AND c.primaryCat = 1
	WHERE YEAR([Order Date]) = 2012
		AND [Source code] = 'SALES'
		AND l.[Unit Price] > 0
		AND l.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
		AND LEFT(i.[Web Order No_], 2) = 'KT'
	GROUP BY MONTH([Order Date]), isnull(c.SUPERCAT, 'UNCAT')
	order by 2, 1

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
	SELECT MONTH([Order Date]),	isnull(c.SUPERCAT, 'UNCAT'),
		COUNT(distinct ISNULL(ih.[No_], 0)) orderTT	, 
		CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2))
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] il ON ih.[No_]  = il.[Document No_]
		LEFT JOIN products p on p.CODE = il.[No_] COLLATE Latin1_General_CS_AS
		LEFT JOIN categories c on p.primaryCatCode = c.CODE AND c.primaryCat = 1
	WHERE YEAR([Order Date]) = 2012
		AND ih.[Document Type] = 1
		AND il.[Unit Price] > 0
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
		AND LEFT(ih.[Web Order No_], 2) = 'KT'
	GROUP BY MONTH([Order Date]), isnull(c.SUPERCAT, 'UNCAT')
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
