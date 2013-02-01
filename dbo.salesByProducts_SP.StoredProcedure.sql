USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[salesByProducts_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		David Lu
-- Create date: 03/28/2011
-- Description:	Pulling Sales Data by products in the last 30 days
-- =============================================
CREATE PROCEDURE [dbo].[salesByProducts_SP]
AS
BEGIN
	UPDATE products
	SET sales30 = 0, sales60 = 0, sales90 = 0, sales180 = 0, sales_last_24MO = 0
	
	--CALCULATING 30 DAYS SALES DATA
	TRUNCATE TABLE products_Sales
	
	INSERT INTO products_Sales
	SELECT l.[No_], SUM(ISNULL(l.[Quantity], 0))
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] h
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l ON l.[Document No_] = h.[No_]
	WHERE DATEDIFF(DAY, h.[Posting Date], GETDATE()) < 31
			AND h.[Source Code] = 'SALES'
			AND LEN(ISNULL(h.[Customer Source], '')) > 0
			AND charindex('-', l.[No_]) > 0
			AND l.[No_] not like '100-%'
			AND h.[Customer Source] = 'I'
	GROUP BY l.[No_]
	ORDER BY 2 DESC
	
	UPDATE p
	SET p.sales30 = ps.sales
	FROM products p join products_Sales ps on p.CODE = ps.code
	WHERE ps.sales > 0
	
	--CALCULATING 60 DAYS SALES DATA
	TRUNCATE TABLE products_Sales
	
	INSERT INTO products_Sales
	SELECT l.[No_], SUM(ISNULL(l.[Quantity], 0))
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] h
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l ON l.[Document No_] = h.[No_]
	WHERE DATEDIFF(DAY, h.[Posting Date], GETDATE()) < 61
			AND h.[Source Code] = 'SALES'
			AND LEN(ISNULL(h.[Customer Source], '')) > 0
			AND charindex('-', l.[No_]) > 0
			AND l.[No_] not like '100-%'
			AND h.[Customer Source] = 'I'
	GROUP BY l.[No_]
	ORDER BY 2 DESC
	
	UPDATE p
	SET p.sales60 = ps.sales
	FROM products p join products_Sales ps on p.CODE = ps.code
	WHERE ps.sales > 0
	
	--CALCULATING 90 DAYS SALES DATA
	TRUNCATE TABLE products_Sales
	
	INSERT INTO products_Sales
	SELECT l.[No_], SUM(ISNULL(l.[Quantity], 0))
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] h
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l ON l.[Document No_] = h.[No_]
	WHERE DATEDIFF(DAY, h.[Posting Date], GETDATE()) < 91
			AND h.[Source Code] = 'SALES'
			AND LEN(ISNULL(h.[Customer Source], '')) > 0
			AND charindex('-', l.[No_]) > 0
			AND l.[No_] not like '100-%'
			AND h.[Customer Source] = 'I'
	GROUP BY l.[No_]
	ORDER BY 2 DESC
	
	UPDATE p
	SET p.sales90 = ps.sales
	FROM products p join products_Sales ps on p.CODE = ps.code
	WHERE ps.sales > 0
	
	--CALCULATING 180 DAYS SALES DATA
	TRUNCATE TABLE products_Sales
	
	INSERT INTO products_Sales
	SELECT l.[No_], SUM(ISNULL(l.[Quantity], 0))
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] h
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l ON l.[Document No_] = h.[No_]
	WHERE DATEDIFF(DAY, h.[Posting Date], GETDATE()) < 181
			AND h.[Source Code] = 'SALES'
			AND LEN(ISNULL(h.[Customer Source], '')) > 0
			AND charindex('-', l.[No_]) > 0
			AND l.[No_] not like '100-%'
			AND h.[Customer Source] = 'I'
	GROUP BY l.[No_]
	ORDER BY 2 DESC
	
	UPDATE p
	SET p.sales180 = ps.sales
	FROM products p join products_Sales ps on p.CODE = ps.code
	WHERE ps.sales > 0
	--CALCULATING 24 MONTHS SALES DATA
	TRUNCATE TABLE products_Sales
	
	INSERT INTO products_Sales
	SELECT l.[No_], SUM(ISNULL(l.[Quantity], 0))
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] h
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l ON l.[Document No_] = h.[No_]
	WHERE DATEDIFF(MONTH, h.[Posting Date], GETDATE()) < 25
			AND h.[Source Code] = 'SALES'
			AND LEN(ISNULL(h.[Customer Source], '')) > 0
			AND charindex('-', l.[No_]) > 0
			AND l.[No_] not like '100-%'
			AND h.[Customer Source] = 'I'
	GROUP BY l.[No_]
	ORDER BY 2 DESC
	
	UPDATE p
	SET p.sales_last_24MO = ps.sales
	FROM products p join products_Sales ps on p.CODE = ps.code
	WHERE ps.sales > 0
END
GO
