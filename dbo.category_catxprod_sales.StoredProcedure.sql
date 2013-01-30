USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[category_catxprod_sales]    Script Date: 01/30/2013 16:26:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
-- Modified: 4-14-2011 by Mike S., Removed references to Merchant2 per Beau D.


CREATE PROCEDURE [dbo].[category_catxprod_sales]
AS
BEGIN
	--REMOVE ALL PRODUCTS CATEGORIZED UNDER THE CLEARANCE AND NAFED DEAL
	DELETE c
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Price] s 
			JOIN catxprod c ON c.prodCode = s.[Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS
	WHERE [Manual Pricing] = 1
		AND [Sales Type] = 1
		AND [Sales Code] = 'KATOM'
		AND [Manual Price Reason Code] IN ('SALE_KATOM', 'SALE_NAFED')
		AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
				REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE()))  
		AND (c.catCode LIKE '%clearance%' OR c.catCode = 'quarterly-specials')

	--ADDING SALES PRODUCTS TO THE CLEARANCE / SALES	
	INSERT INTO catxProd (catID, catCode, prodID, prodCode, primaryCat, nav)
	SELECT NULL,
			CASE c.superCat
				WHEN 'Pizza Equipment' THEN 'clearance-equipment'
				WHEN 'Kitchen Supplies' THEN 'clearance-residential'
				WHEN 'Countertop' THEN 'clearance-countertop'			
				WHEN 'Furniture' THEN 'clearance-furniture'
				WHEN 'Catering / Buffet' THEN 'clearance-residential'
				WHEN 'Janitorial' THEN 'clearance-janitorial'
				WHEN 'Tabletop' THEN 'clearance-tabletop'						
				WHEN 'Go Green! Shop to Save Energy and Money.' THEN 'clearance-miscellaneous'
				WHEN 'Restaurant Equipment' THEN 'clearance-equipment'
				WHEN 'Bar Supplies' THEN 'clearance-residential'
				WHEN 'Residential' THEN 'clearance-residential'
				WHEN 'KaTom Kids' THEN 'clearance-miscellaneous'
				ELSE 'clearance-miscellaneous'
			END, 
			p.prodid, p.code,  
			CASE
				WHEN c.superCat IS NULL THEN 1
				ELSE 0
			END, 0
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Price] s 
			JOIN products p ON p.code = s.[Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS
			LEFT JOIN categories c ON c.CODE = p.primaryCatCode and c.primaryCat = 1
	WHERE [Manual Pricing] = 1
		AND [Sales Type] = 1
		AND [Sales Code] = 'KATOM'
		AND [Manual Price Reason Code] IN ('SALE_KATOM', 'SALE_NAFED')
		AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
				REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE()))  

	--ADDING NAFED SALES PRODUCTS TO THE QTRLY SALES CAT
	INSERT INTO catxProd (catID, catCode, prodID, prodCode, primaryCat, nav)
	SELECT NULL, 'quarterly-specials', p.prodid, p.code, 0, 0
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Price] s 
			JOIN products p ON p.code = s.[Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS
	WHERE [Manual Pricing] = 1
		AND [Sales Type] = 1
		AND [Sales Code] = 'KATOM'
		AND [Manual Price Reason Code] IN ('SALE_NAFED')
		AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
				REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE()))  

	--UPDATE CATID WHERE IT IS NULL
	UPDATE cx
	SET cx.catID = c.id
	FROM catxProd cx JOIN categories c ON cx.catCode = c.CODE
	WHERE cx.catID IS NULL	
END
GO
