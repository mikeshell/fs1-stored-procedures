USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[product_featured_HP_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[product_featured_HP_SP]
AS
BEGIN
	DECLARE @supercatList VARCHAR(100), @supercat VARCHAR(25)
	DECLARE @today DATETIME, @lastyear DATETIME, @sSQL VARCHAR(2000)

	SET @sSQL = ''
	SET @today = GETDATE()
	SET @lastyear = DATEADD(YEAR, -1, @today)
	SET @supercatList = 'Restaurant Equipment|Countertop|Kitchen Supplies|Residential|Janitorial|Tabletop|'

	TRUNCATE TABLE featuredProducts_HP

	WHILE CHARINDEX('|', @supercatList) > 0
	   BEGIN
		SET @supercat = LEFT(@supercatList, CHARINDEX('|', @supercatList)-1)
		SET @supercatList = RIGHT(@supercatList, LEN(@supercatList)-CHARINDEX('|', @supercatList))
		
		SET @sSQL = 'INSERT INTO featuredProducts_HP(prodCode)
					SELECT TOP ' +
						CASE @supercat
							WHEN 'Restaurant Equipment' THEN '2'
							WHEN 'Countertop' THEN '2'
							ELSE '1'
						END + ' p.CODE
					FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih
						JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il ON ih.[No_]  = il.[Document No_]
						JOIN products p ON p.CODE = il.[No_] COLLATE Latin1_General_CS_AS
						LEFT JOIN categories c on p.primaryCatCode = c.CODE AND c.primaryCat = 1
					WHERE ih.[Order Date] BETWEEN DATEADD(DAY, -45, ''' + CAST(@lastyear AS VARCHAR(11)) + ''') AND DATEADD(DAY, 45, ''' + CAST(@lastyear AS VARCHAR(11)) + ''')
						AND ih.[Source Code] = ''SALES''
						AND LEN(ISNULL(ih.[Customer Source], '''')) > 0
						AND p.ACTIVE = 1 and p.isWeb = 1
						AND LEN(ih.[Web Order No_]) > 0
						AND c.superCat = ''' + @supercat + '''
					GROUP BY p.CODE
					ORDER BY CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)) DESC'
		
		EXEC(@sSQL)
	   END	
END
GO
