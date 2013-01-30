USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[catPerformanceWeekly]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[catPerformanceWeekly]
AS
BEGIN
	DECLARE @year Smallint, @month TINYINT, @weekday TINYINT, @day TINYINT, @week TINYINT, @startDT datetime, @endDT datetime, 
	@salesTT money, @orderTT int, @today datetime, @sSQL VARCHAR(MAX), @position TINYINT, @lMonth TINYINT, @lMonthy SMALLINT,
	@yyyy SMALLINT, @w TINYINT, @catname VARCHAR(100), @sales MONEY, @order INT, @variance INT, @LP TINYINT, @currentCat VARCHAR(100),
	@salesP MONEY

	SET @today = GETDATE()
	SET @year = YEAR(@today)
	SET @month = MONTH(@today)
	SET @weekday = DATEPART(WEEKDAY, @today)
	SET @week = DATEPART(WEEK, @today)
	SET @day = DAY(@today)
	SET @startDT = CAST(CAST(DATEADD(DAY, -6-DATEPART(WEEKDAY, @today), @today) AS VARCHAR(11)) AS DATETIME)
	SET @endDT = CAST(CAST(DATEADD(DAY, -DATEPART(WEEKDAY, @today), @today) AS VARCHAR(11)) AS DATETIME)
	SET @variance = DATEPART(WEEK, DATEADD(WEEK, -5, @startDT))-1
	SET @LP = 0
	SET @sSQL = ''
	SET @currentCat = ''


	IF @month = 1
	   BEGIN
		SET @lMonth = 12
		SET @lMonthy = @year-1
	   END
	ELSE
	   BEGIN
		SET @lMonth = @month-1
		SET @lMonthy = @year   
	   END

	--INITIATE TABLE
	TRUNCATE TABLE catPerformance_RPT
	INSERT INTO catPerformance_RPT
	SELECT 
		CASE ISNULL(c.superCat, 'Others')
				WHEN 'Go Green! Shop to Save Energy and Money.' THEN 'Others'
				WHEN 'KaTom Kids' THEN 'Others'
				WHEN 'Shop By Vendor' THEN 'UNCAT'
				WHEN 'Special' THEN 'Others'	
				ELSE ISNULL(c.superCat, 'UNCAT')
			END, COUNT(*), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
	FROM products p LEFT JOIN categories c on p.primaryCatID = c.ID AND primaryCat = 1
	WHERE p.ACTIVE = 1
	GROUP BY CASE ISNULL(c.superCat, 'Others')
				WHEN 'Go Green! Shop to Save Energy and Money.' THEN 'Others'
				WHEN 'KaTom Kids' THEN 'Others'
				WHEN 'Shop By Vendor' THEN 'UNCAT'
				WHEN 'Special' THEN 'Others'			
				ELSE ISNULL(c.superCat, 'UNCAT')
			  END
	ORDER BY 1 ASC

	--INSERT FIRST SIX WEEKS SALES FROM INVOICE TABLE
	DECLARE sCursor CURSOR FOR 
	SELECT 
		YEAR(ih.[Order Date]), DATEPART(WEEK, ih.[Order Date]),
		CASE ISNULL(c.superCat, 'Others')
				WHEN 'Go Green! Shop to Save Energy and Money.' THEN 'Others'
				WHEN 'KaTom Kids' THEN 'Others'
				WHEN 'Shop By Vendor' THEN 'UNCAT'
				WHEN 'Special' THEN 'Others'	
				ELSE ISNULL(c.superCat, 'UNCAT')
			END,
		CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)),
		COUNT(distinct ISNULL(ih.[No_], 0)) orderTT		
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il
			ON ih.[No_]  = il.[Document No_]
		JOIN products p ON p.CODE = il.[No_] COLLATE Latin1_General_CS_AS
		LEFT JOIN categories c on p.primaryCatID = c.ID AND primaryCat = 1
	WHERE ih.[Order Date] BETWEEN DATEADD(WEEK, -5, @startDT) AND @endDT
		AND ih.[Source Code] = 'SALES'
		AND LEN(ISNULL(ih.[Customer Source], '')) > 0
		AND ih.[Customer Source] = 'I'
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY YEAR(ih.[Order Date]), DATEPART(WEEK, ih.[Order Date]), 
			CASE ISNULL(c.superCat, 'Others')
				WHEN 'Go Green! Shop to Save Energy and Money.' THEN 'Others'
				WHEN 'KaTom Kids' THEN 'Others'
				WHEN 'Shop By Vendor' THEN 'UNCAT'
				WHEN 'Special' THEN 'Others'			
				ELSE ISNULL(c.superCat, 'UNCAT')
			  END
	UNION
	SELECT 
		YEAR(ih.[Order Date]), DATEPART(WEEK, ih.[Order Date]),
		CASE ISNULL(c.superCat, 'Others')
				WHEN 'Go Green! Shop to Save Energy and Money.' THEN 'Others'
				WHEN 'KaTom Kids' THEN 'Others'
				WHEN 'Shop By Vendor' THEN 'UNCAT'
				WHEN 'Special' THEN 'Others'	
				ELSE ISNULL(c.superCat, 'UNCAT')
			END,
		CAST(SUM(ISNULL(il.[Outstanding Amount], 0)+ISNULL(il.[Shipped Not Invoiced], 0)) as decimal(10, 2)),
		COUNT(distinct ISNULL(ih.[No_], 0)) orderTT		
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] il
			ON ih.[No_]  = il.[Document No_]
		JOIN products p ON p.CODE = il.[No_] COLLATE Latin1_General_CS_AS
		LEFT JOIN categories c on p.primaryCatID = c.ID AND primaryCat = 1
	WHERE ih.[Order Date] BETWEEN DATEADD(WEEK, -5, @startDT) AND @endDT
		AND ih.[Document Type] = 1
		AND LEN(ISNULL(ih.[Customer Source], '')) > 0
		AND ih.[Customer Source] = 'I'
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY YEAR(ih.[Order Date]), DATEPART(WEEK, ih.[Order Date]), 
			CASE ISNULL(c.superCat, 'Others')
				WHEN 'Go Green! Shop to Save Energy and Money.' THEN 'Others'
				WHEN 'KaTom Kids' THEN 'Others'
				WHEN 'Shop By Vendor' THEN 'UNCAT'
				WHEN 'Special' THEN 'Others'			
				ELSE ISNULL(c.superCat, 'UNCAT')
			  END
	ORDER BY 3, 1, 2 ASC

	OPEN sCursor
	FETCH NEXT FROM sCursor INTO @yyyy, @w, @catname, @sales, @order

	WHILE @@FETCH_STATUS = 0
	   BEGIN   
		IF LEN(@currentCat) = 0
		   BEGIN
			SET @currentCat = @catname
		   END
		   
		IF @currentCat <> @catname
		   BEGIN
			SET @currentCat = @catname
			SET @variance = DATEPART(WEEK, DATEADD(WEEK, -5, @startDT))-1
		   END   
	   
		IF (@w-@variance) < 1
		   BEGIN
			SET @variance = @LP
			SET @position = @w+@variance
		   END
		ELSE
		   BEGIN
			SET @LP = @w-@variance
			SET @position = @w-@variance
		   END	
		   
		SET @sSQL = 'UPDATE catPerformance_RPT
					 SET W' + CAST(@position AS VARCHAR(5)) + ' = W' + CAST(@position AS VARCHAR(5)) + ' + ' + CAST(@sales AS VARCHAR(20)) + ', 
						 O' + CAST(@position AS VARCHAR(5)) + ' = O' + CAST(@position AS VARCHAR(5)) + ' + ' + CAST(@order AS VARCHAR(20)) + '
					 WHERE catName = ''' + @catname + ''''
						
		
		exec (@sSQL)	
			
		FETCH NEXT FROM sCursor INTO @yyyy, @w, @catname, @sales, @order
	   END

	CLOSE sCursor
	DEALLOCATE sCursor


	--6 WEEKS AVG	
	TRUNCATE TABLE prodTemp

	INSERT INTO prodTemp([Product Name], [UnitPrice], [UnitPrice2])
	SELECT catName, (W1+W2+W3+W4+W5+W6)/6, (O1+O2+O3+O4+O5+O6)/6
	FROM catPerformance_RPT

	UPDATE c
	SET c.W_AVG = [UnitPrice],
		c.W_AVG_O = [UnitPrice2]
	FROM catPerformance_RPT c JOIN prodTemp p ON c.catName = p.[Product Name]	

	--CALCULATING MONTHLY SALES
	TRUNCATE TABLE prodTemp

	INSERT INTO prodTemp([Product Name], [UnitPrice], [UnitPrice2])
	SELECT 
		CASE ISNULL(c.superCat, 'Others')
				WHEN 'Go Green! Shop to Save Energy and Money.' THEN 'Others'
				WHEN 'KaTom Kids' THEN 'Others'
				WHEN 'Shop By Vendor' THEN 'UNCAT'
				WHEN 'Special' THEN 'Others'	
				ELSE ISNULL(c.superCat, 'UNCAT')
			END,
		CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)),
		COUNT(distinct ISNULL(ih.[No_], 0)) orderTT		
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il
			ON ih.[No_]  = il.[Document No_]
		JOIN products p ON p.CODE = il.[No_] COLLATE Latin1_General_CS_AS
		LEFT JOIN categories c on p.primaryCatID = c.ID AND primaryCat = 1
	WHERE ih.[Order Date] BETWEEN CAST(@month AS VARCHAR(2)) + '/01/' + CAST(@year AS VARCHAR(4)) AND @endDT
		AND ih.[Source Code] = 'SALES'
		AND LEN(ISNULL(ih.[Customer Source], '')) > 0
		AND ih.[Customer Source] = 'I'
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY CASE ISNULL(c.superCat, 'Others')
				WHEN 'Go Green! Shop to Save Energy and Money.' THEN 'Others'
				WHEN 'KaTom Kids' THEN 'Others'
				WHEN 'Shop By Vendor' THEN 'UNCAT'
				WHEN 'Special' THEN 'Others'			
				ELSE ISNULL(c.superCat, 'UNCAT')
			  END
	ORDER BY 1 ASC	
		
	UPDATE c
	SET c.MTD = [UnitPrice],
		c.MTDO = [UnitPrice2]
	FROM catPerformance_RPT c JOIN prodTemp p ON c.catName = p.[Product Name]

	TRUNCATE TABLE prodTemp

	INSERT INTO prodTemp([Product Name], [UnitPrice], [UnitPrice2])
	SELECT 
		CASE ISNULL(c.superCat, 'Others')
				WHEN 'Go Green! Shop to Save Energy and Money.' THEN 'Others'
				WHEN 'KaTom Kids' THEN 'Others'
				WHEN 'Shop By Vendor' THEN 'UNCAT'
				WHEN 'Special' THEN 'Others'	
				ELSE ISNULL(c.superCat, 'UNCAT')
			END,
		CAST(SUM(ISNULL(il.[Outstanding Amount], 0)+ISNULL(il.[Shipped Not Invoiced], 0)) as decimal(10, 2)),
		COUNT(distinct ISNULL(ih.[No_], 0)) orderTT		
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] il
			ON ih.[No_]  = il.[Document No_]
		JOIN products p ON p.CODE = il.[No_] COLLATE Latin1_General_CS_AS
		LEFT JOIN categories c on p.primaryCatID = c.ID AND primaryCat = 1
	WHERE ih.[Order Date] BETWEEN CAST(@month AS VARCHAR(2)) + '/01/' + CAST(@year AS VARCHAR(4)) AND @endDT
		AND ih.[Document Type] = 1
		AND LEN(ISNULL(ih.[Customer Source], '')) > 0
		AND ih.[Customer Source] = 'I'
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY CASE ISNULL(c.superCat, 'Others')
				WHEN 'Go Green! Shop to Save Energy and Money.' THEN 'Others'
				WHEN 'KaTom Kids' THEN 'Others'
				WHEN 'Shop By Vendor' THEN 'UNCAT'
				WHEN 'Special' THEN 'Others'			
				ELSE ISNULL(c.superCat, 'UNCAT')
			  END
	ORDER BY 1 ASC	
		
	UPDATE c
	SET c.MTD = c.MTD + [UnitPrice],
		c.MTDO = c.MTDO + [UnitPrice2]
	FROM catPerformance_RPT c JOIN prodTemp p ON c.catName = p.[Product Name]


	--CALCULATING LAST MONTHL SALES
	TRUNCATE TABLE prodTemp

	INSERT INTO prodTemp([Product Name], [UnitPrice], [UnitPrice2])
	SELECT 
		CASE ISNULL(c.superCat, 'Others')
				WHEN 'Go Green! Shop to Save Energy and Money.' THEN 'Others'
				WHEN 'KaTom Kids' THEN 'Others'
				WHEN 'Shop By Vendor' THEN 'UNCAT'
				WHEN 'Special' THEN 'Others'	
				ELSE ISNULL(c.superCat, 'UNCAT')
			END,
		CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)),
		COUNT(distinct ISNULL(ih.[No_], 0)) orderTT		
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il
			ON ih.[No_]  = il.[Document No_]
		JOIN products p ON p.CODE = il.[No_] COLLATE Latin1_General_CS_AS
		LEFT JOIN categories c on p.primaryCatID = c.ID AND primaryCat = 1
	WHERE MONTH(ih.[Order Date]) = @lMonth AND YEAR(ih.[Order Date]) = @lMonthy
		AND ih.[Source Code] = 'SALES'
		AND LEN(ISNULL(ih.[Customer Source], '')) > 0
		AND ih.[Customer Source] = 'I'
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY CASE ISNULL(c.superCat, 'Others')
				WHEN 'Go Green! Shop to Save Energy and Money.' THEN 'Others'
				WHEN 'KaTom Kids' THEN 'Others'
				WHEN 'Shop By Vendor' THEN 'UNCAT'
				WHEN 'Special' THEN 'Others'			
				ELSE ISNULL(c.superCat, 'UNCAT')
			  END
	ORDER BY 1 ASC	
		
	UPDATE c
	SET c.LM = [UnitPrice],
		c.LMO = [UnitPrice2]
	FROM catPerformance_RPT c JOIN prodTemp p ON c.catName = p.[Product Name]

	TRUNCATE TABLE prodTemp

	INSERT INTO prodTemp([Product Name], [UnitPrice], [UnitPrice2])
	SELECT 
		CASE ISNULL(c.superCat, 'Others')
				WHEN 'Go Green! Shop to Save Energy and Money.' THEN 'Others'
				WHEN 'KaTom Kids' THEN 'Others'
				WHEN 'Shop By Vendor' THEN 'UNCAT'
				WHEN 'Special' THEN 'Others'	
				ELSE ISNULL(c.superCat, 'UNCAT')
			END,
		CAST(SUM(ISNULL(il.[Outstanding Amount], 0)+ISNULL(il.[Shipped Not Invoiced], 0)) as decimal(10, 2)),
		COUNT(distinct ISNULL(ih.[No_], 0)) orderTT		
	FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] ih
		JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] il
			ON ih.[No_]  = il.[Document No_]
		JOIN products p ON p.CODE = il.[No_] COLLATE Latin1_General_CS_AS
		LEFT JOIN categories c on p.primaryCatID = c.ID AND primaryCat = 1
	WHERE MONTH(ih.[Order Date]) = @lMonth AND YEAR(ih.[Order Date]) = @lMonthy
		AND ih.[Document Type] = 1
		AND LEN(ISNULL(ih.[Customer Source], '')) > 0
		AND ih.[Customer Source] = 'I'
		AND il.[No_] NOT IN ('420', '100-FREIGHT', '100-TAXES', '100-SHIPPING')
	GROUP BY CASE ISNULL(c.superCat, 'Others')
				WHEN 'Go Green! Shop to Save Energy and Money.' THEN 'Others'
				WHEN 'KaTom Kids' THEN 'Others'
				WHEN 'Shop By Vendor' THEN 'UNCAT'
				WHEN 'Special' THEN 'Others'			
				ELSE ISNULL(c.superCat, 'UNCAT')
			  END
	ORDER BY 1 ASC	
		
	UPDATE c
	SET c.LM = c.LM + [UnitPrice],
		c.LMO = c.LMO + [UnitPrice2]
	FROM catPerformance_RPT c JOIN prodTemp p ON c.catName = p.[Product Name]

	--PROJECTING SALES FOR THIS MONTH
	SELECT @salesTT = SUM(LM) FROM catPerformance_RPT
	SELECT @salesP = sales FROM salesProjection WHERE YYYY = YEAR(@endDT) AND MM = MONTH(@endDT)

	TRUNCATE TABLE prodTemp

	IF @salesP < @salesTT
	   BEGIN
		INSERT INTO prodTemp([Product Name], [UnitPrice])
		SELECT CATNAME, (1.03*@salesTT*LM)/@salesTT FROM catPerformance_RPT   
	   END
	ELSE
	   BEGIN
		INSERT INTO prodTemp([Product Name], [UnitPrice])
		SELECT CATNAME, (@salesP*LM)/@salesTT FROM catPerformance_RPT     
	   END

	UPDATE c
	SET c.MTD_PROJ = [UnitPrice]
	FROM catPerformance_RPT c JOIN prodTemp p ON c.catName = p.[Product Name]
END
GO
