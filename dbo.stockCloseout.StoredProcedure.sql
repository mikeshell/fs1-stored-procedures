USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[stockCloseout]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[stockCloseout]
AS
BEGIN
	INSERT INTO closeout
	--ADDING ALL ITEMS IN WHICH WE HAVE NOT SOLD IN THE LAST 18 MONTH
	SELECT [Item No_], SUM([Quantity]) qtyOnHand, MAX([Posting Date]) lastSoldDT, NULL sellingPrice
			/**
			CASE 
				WHEN cost < 10 THEN CAST(cost/.9 as decimal(10, 2))
				WHEN cost BETWEEN 10 AND 100 THEN CAST(cost/.94 as decimal(10, 2))
				ELSE CAST(cost/.97 as decimal(10, 2))
			END sellingPrice, 
			**/
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Ledger Entry] i
			JOIN products p ON p.CODE = i.[Item No_] collate Latin1_General_CS_AS
	WHERE CODE NOT LIKE '%-cat%'
		AND CODE NOT IN (SELECT CODE COLLATE Latin1_General_CS_AS FROM closeout)
	GROUP BY [Item No_]
	Having SUM([Quantity]) > 0 and DATEDIFF(MONTH, MAX([Posting Date]), GETDATE()) > 11
	UNION -- ADDING ALL DISCONTINUED ITEMS
	SELECT [Item No_], qtyonhand, MAX([Posting Date]) lastSoldDT, NULL sellingPrice
			/**
			CASE 
				WHEN cost < 10 THEN CAST(cost/.9 as decimal(10, 2))
				WHEN cost BETWEEN 10 AND 100 THEN CAST(cost/.94 as decimal(10, 2))
				ELSE CAST(cost/.97 as decimal(10, 2))
			END sellingPrice, 
			**/ 
	FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Ledger Entry] i
			JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i2 ON i.[Item No_] = i2.[No_]
			JOIN products p ON p.CODE = i.[Item No_] collate Latin1_General_CS_AS
	WHERE i2.[Status] = 2 AND mfgID <> '599' AND clearance = 0 and qtyOnHand > 0
			AND CODE NOT IN (SELECT CODE COLLATE Latin1_General_CS_AS FROM closeout)
	GROUP BY [Item No_], qtyonhand

	--REMOVE ALL CLEARANCE PRODUCTS FROM CLOSEOUT TABLE IF QTY REACH 0
	DELETE FROM closeout WHERE code IN (SELECT code COLLATE SQL_Latin1_General_CP1_CI_AS FROM products WHERE clearance = 1 AND qtyOnHand < 1)

	UPDATE products SET clearance = 0, salesPrice = NULL, salesDT = NULL WHERE clearance = 1 AND qtyOnHand < 1

	UPDATE p
	SET p.clearance = 1,
		p.salesPrice = p.price,
		p.salesDT = GETDATE()
	FROM closeout c JOIN products p ON c.code = p.CODE COLLATE SQL_Latin1_General_CP1_CI_AS
	WHERE p.clearance = 0

	--CATEGORIZE ALL CLEARANCE PRODUCTS - THIS DATA IS NOT IN NAVISION
	DELETE FROM catxprod WHERE catID IN (SELECT id FROM categories WHERE superCat = 'Clearance Sale')

	INSERT INTO catxprod(catID, catCode, prodID, prodCode, primaryCat, nav)
	SELECT CASE c2.superCatID
				WHEN 4367 THEN 10759
				WHEN 5612 THEN 10757
				WHEN 4873 THEN 9024
				WHEN 5853 THEN 8923
				WHEN 5366 THEN 8920
				WHEN 4579 THEN 9025
				WHEN 5433 THEN 9024
				WHEN 7615 THEN 8968
				WHEN 5077 THEN 8924
				WHEN 4515 THEN 10760
				ELSE 10758
			END, 
			CASE c2.superCatID
				WHEN 4367 THEN 'clearance-bar-supplies'
				WHEN 5612 THEN 'clearance-residential'
				WHEN 4873 THEN 'clearance-equipment'
				WHEN 5853 THEN 'clearance-smallwares'
				WHEN 5366 THEN 'clearance-janitorial'
				WHEN 4579 THEN 'clearance-countertop'
				WHEN 5433 THEN 'clearance-equipment'
				WHEN 7615 THEN 'clearance-tabletop'
				WHEN 5077 THEN 'clearance-furniture'
				WHEN 4515 THEN 'clearance-catering-buffet'
				ELSE 'clearance-miscellaneous'
			END, p.prodid, p.code, 0, 1
	FROM closeout c JOIN products p ON c.code = p.CODE COLLATE SQL_Latin1_General_CP1_CI_AS
				LEFT JOIN categories c2 ON p.primaryCatCode = c2.CODE AND c2.primaryCat = 1
	WHERE p.CODE NOT IN 
		(
		SELECT [Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS
		FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Price] 
		WHERE [Sales Type] = 1
			AND [Manual Price Reason Code] = 'SALE_KATOM' 
			AND [Sales Code] = 'KATOM'
			AND GETDATE() BETWEEN REPLACE([Starting Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, -1, GETDATE())) AND
					REPLACE([Ending Date], 'Jan  1 1753 12:00AM', DATEADD(YEAR, 1, GETDATE()))
		)
								
	EXEC dbo.[product_categorization_clearance_SP]
END
GO
