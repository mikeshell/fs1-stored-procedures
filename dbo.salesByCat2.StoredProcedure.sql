USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[salesByCat2]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[salesByCat2]
	@fromDT DATETIME, @toDT DATETIME, @cat VARCHAR(100), @orderBy INT
AS
BEGIN
	DECLARE @sSQL VARCHAR(2000)
		
	--PULL THE SUPERCAT NAME FOR THE CATEGORY DROP DOWN LIST	
	SELECT SUPERCAT FROM categories WHERE ACTIVE = 1  GROUP BY superCat HAVING COUNT(*) > 5 ORDER BY 1

	IF LEN(ISNULL(@fromDT, '')) = 0 
	  BEGIN
		SET @fromDT = GETDATE()
		SET @toDT = DATEADD(DAY, -30, @fromDT)
	  END

	IF @orderBy = 0 
	  BEGIN
		SET @orderBy = 9
	  END	  
	  
	SET @sSQL = ''
	
	SET @sSQL = 'SELECT TOP 100 p.CODE, p.name, p.mfgname, qtyOnHand,
					CASE 
						WHEN c.superCat IS NULL THEN ''Not Categorized''
						ELSE c.superCat
					END category, p.price, 
					CASE
						WHEN p.price > 0 THEN (p.price - p.cost)/p.price
						ELSE 0
					END margin, 
					SUM(ISNULL(il.[Quantity], 0)) qtySold,
					CAST(SUM(ISNULL(il.[Quantity], 0)*ISNULL(il.[Unit Price], 0)) as decimal(10, 2)) salesTT,
					COUNT(DISTINCT ih.[No_]) ordercount
				FROM fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] ih
				JOIN fs1.KaTom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] il ON ih.[No_]  = il.[Document No_]
				JOIN products p ON p.CODE = il.[No_] COLLATE Latin1_General_CS_AS
				LEFT JOIN categories c on p.primaryCatCode = c.CODE AND c.primaryCat = 1
				WHERE ih.[Source Code] = ''SALES''
					AND LEN(ISNULL(ih.[Customer Source], '''')) > 0
					AND p.ACTIVE = 1 and p.isWeb = 1
					AND LEN(ih.[Web Order No_]) > 0
					AND ih.[Order Date] BETWEEN ''' + CAST(@fromDT AS VARCHAR(11)) + ''' AND ''' + CAST(@toDT AS VARCHAR(11)) + '''' +
					CASE
						WHEN LEN(ISNULL(@cat, '')) = 0 THEN ''
						WHEN @cat = 'Not Categorized' THEN ' AND c.superCat IS NULL'
						ELSE ' AND c.superCat = ''' + @cat + ''''
					END + '
				GROUP BY p.CODE, p.name, p.mfgname, qtyOnHand,
					CASE 
						WHEN c.superCat IS NULL THEN ''Not Categorized''
						ELSE c.superCat
					END, p.price,
					CASE
						WHEN p.price > 0 THEN (p.price - p.cost)/p.price
						ELSE 0
					END 
				ORDER BY ' + CAST(@orderBy AS VARCHAR(2)) + ' DESC'

	exec(@sSQL)
	
END
GO
