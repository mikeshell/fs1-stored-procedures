USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[priceDiscrepancyRpt]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[priceDiscrepancyRpt]
	@refreshPrice BIT, @reportType TINYINT
AS
BEGIN
	
	IF @refreshPrice = 1 
	   BEGIN
		EXEC dbo.product_Pricing_Update_SP

		IF LEN(@reportType) = 0 
		   BEGIN
			EXEC dbo.[priceDiscrepancyRpt] 0, 1
		   END
		ELSE
		   BEGIN
			EXEC dbo.[priceDiscrepancyRpt] 0, @reportType		   
		   END	   
	   END
	
	TRUNCATE TABLE prodTemp
	
	IF @reportType = 1 --MAP PRICE
	   BEGIN
		INSERT INTO prodTemp([No_], [Gross Weight])
		SELECT d.[Code], 
			(SELECT [Line Discount %] 
			 FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line Discount] 
			 WHERE [Code] = d.[Code] AND [Sales Code] = 'MAP' 
					AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
					REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE()))
			)
		FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Discount Group] d
		WHERE MAP = 1

		SELECT p.code, p.discountGroup, pt.[Gross Weight] discountPerc, p.listprice, p.mapPrice, 
				CAST(listprice*(100-pt.[Gross Weight])/100 AS DECIMAL(10, 2)) CalculatedValue, p.manualPricingMAP
		FROM products_Price_Worktable p JOIN prodTemp pt on p.discountGroup  = pt.[No_]
				JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i on i.[No_] = p.code COLLATE SQL_Latin1_General_CP1_CI_AS
		WHERE p.mapPrice <> CAST(ISNULL(listprice*(100-pt.[Gross Weight])/100, 0) AS DECIMAL(10, 2))
			AND p.manualPricingMAP = 0
			and i.[Status] <> 2
	   END
	ELSE IF @reportType = 2 --KATOM PRICE
	   BEGIN
		INSERT INTO prodTemp([No_], [Gross Weight])
		SELECT d.[Code], 
			(SELECT [Line Discount %] 
			 FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line Discount] 
			 WHERE [Code] = d.[Code] AND [Sales Code] = 'KATOM' 
					AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
					REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE()))
			)
		FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Discount Group] d


		SELECT p.code, p.discountGroup, pt.[Gross Weight] discountPerc, p.listprice, p.price, 
				CAST(listprice*(100-pt.[Gross Weight])/100 AS DECIMAL(10, 2)) CalculatedValue, p.manualPricingKAT
		FROM products_Price_Worktable p JOIN prodTemp pt on p.discountGroup  = pt.[No_]
				JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i on i.[No_] = p.code COLLATE SQL_Latin1_General_CP1_CI_AS
		WHERE p.price <> CAST(ISNULL(listprice*(100-pt.[Gross Weight])/100, 0) AS DECIMAL(10, 2))
			AND p.manualPricingKAT = 0
			and i.[Status] <> 2
	   END
	ELSE IF @reportType = 3 --BB PRICE
	   BEGIN
		INSERT INTO prodTemp([No_], [Gross Weight])
		SELECT d.[Code], 
			(SELECT [Line Discount %] 
			 FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line Discount] 
			 WHERE [Code] = d.[Code] AND [Sales Code] = 'BB' 
					AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
					REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE()))
			)
		FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Discount Group] d

		SELECT p.code, p.discountGroup, pt.[Gross Weight] discountPerc, p.listprice, p.bbprice, 
				CAST(listprice*(100-pt.[Gross Weight])/100 AS DECIMAL(10, 2)) CalculatedValue, p.manualPricingBB
		FROM products_Price_Worktable p JOIN prodTemp pt on p.discountGroup  = pt.[No_]
				JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i on i.[No_] = p.code COLLATE SQL_Latin1_General_CP1_CI_AS
		WHERE p.bbprice <> CAST(ISNULL(listprice*(100-pt.[Gross Weight])/100, 0) AS DECIMAL(10, 2))
			AND p.manualPricingBB = 0
			and i.[Status] <> 2
	   END
	ELSE IF @reportType = 4 --STD_COST 
	   BEGIN
		INSERT INTO prodTemp([No_], [Gross Weight])
		SELECT d.[Code], 
			(SELECT [Line Discount %] 
			 FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line Discount] 
			 WHERE [Code] = d.[Code] AND [Sales Code] = 'STD_COST' 
					AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
					REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE()))
			)
		FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Discount Group] d

		SELECT p.code, p.discountGroup, pt.[Gross Weight] discountPerc, p.listprice, p.cost, 
				CAST(listprice*(100-pt.[Gross Weight])/100 AS DECIMAL(10, 4)) CalculatedValue, p.manualPricingCost
		FROM products_Price_Worktable p JOIN prodTemp pt on p.discountGroup  = pt.[No_]
				JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i on i.[No_] = p.code COLLATE SQL_Latin1_General_CP1_CI_AS
		WHERE p.cost <> CAST(ISNULL(listprice*(100-pt.[Gross Weight])/100, 0) AS DECIMAL(10, 2))
			AND p.manualPricingCost = 0
			and i.[Status] <> 2
	   END
	ELSE IF @reportType = 5 -- REAL COST
	   BEGIN
		INSERT INTO prodTemp([No_], [Gross Weight])
		SELECT d.[Code], 
			(SELECT [Line Discount %] 
			 FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line Discount] 
			 WHERE [Code] = d.[Code] AND [Sales Code] = '_COST' 
					AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
					REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE()))
			)
		FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Discount Group] d

		SELECT p.code, p.discountGroup, pt.[Gross Weight] discountPerc, p.listprice, p.realcost, 
				CAST(listprice*(100-pt.[Gross Weight])/100 AS DECIMAL(10, 4)) CalculatedValue, p.manualPricingRealCost
		FROM products_Price_Worktable p JOIN prodTemp pt on p.discountGroup  = pt.[No_]
				JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i on i.[No_] = p.code COLLATE SQL_Latin1_General_CP1_CI_AS
		WHERE p.realcost <> CAST(ISNULL(listprice*(100-pt.[Gross Weight])/100, 0) AS DECIMAL(10, 2))
			AND manualPricingRealCost = 0
			and i.[Status] <> 2
	   END
	ELSE IF @reportType = 6 --MULTIPLE ACTIVE COSTS
	   BEGIN		
		SELECT [Item No_], [Unit of Measure Code], COUNT(*) 
		FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Purchase Price] p 
		WHERE GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
					REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE()))
			 AND [Item No_] NOT IN (SELECT [No_] FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] WHERE [Status] = 2)
		group by [Item No_], [Unit of Measure Code]
		having COUNT(*) > 1
	   END
	ELSE IF @reportType = 7 --MULTIPLE ACTIVE PRICES
	   BEGIN		
		SELECT [Item No_], [Sales Code], [Unit of Measure Code], COUNT(*) 
		FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Price] p 
		WHERE GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
					REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE()))
			 AND [Item No_] NOT IN (SELECT [No_] FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] WHERE [Status] = 2)
		group by [Item No_], [Sales Code], [Unit of Measure Code]
		having COUNT(*) > 1
	   END
END
GO
