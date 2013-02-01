USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[shareASaleFeed_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[shareASaleFeed_SP]
AS
BEGIN

DECLARE @jobID INT
INSERT INTO jobHistory(jobName, startdT) values('shareASale', GETDATE())
SET @jobID = @@identity

TRUNCATE TABLE feed_shareAsale

INSERT INTO feed_shareAsale(sku, name, url, price, retailPrice, fullImage, thumbnailImage, [description], searchterms,
		custom1, Manufacturer, partNumber,MerchantCategory, MerchantSubCategory)
SELECT 
	p.code,		
	dbo.cleaner(p.NAVDesc) [NAME],
	'http://www.katom.com/' + LTRIM(RTRIM(p.code)) + '.html',
	CAST(p.price as decimal(10, 2))[PRICE],
	CAST(p.listprice as decimal(10, 2)) [RETAILPRICE],
	/**
	'http://www.katom.com/largeproducts/' + p.mfgid + '/' + LOWER(p.image) + '.jpg',		
	'http://www.katom.com/products/' + p.mfgid + '/' + LOWER(p.image) + '.jpg',
	**/
	NULL, NULL,
	dbo.cleaner(p.NAVDesc),		
	NULL,		
	CASE p.freeShipping
		WHEN 1 THEN 'FREE SHIPPING'
		ELSE ''
	END,
	m.mfgName,
	p.mpn,
	CASE c.superCat
		WHEN 'Shop By Vendor' THEN
			CASE
				WHEN P.PRICE < 500 THEN 'Kitchen Supplies'
				ELSE 'Restaurant Equipment'
			END
		WHEN 'Related Items' THEN
			CASE
				WHEN P.PRICE < 500 THEN 'Kitchen Supplies'
				ELSE 'Restaurant Equipment'
			END
		WHEN 'Go Green! Shop to Save Energy and Money.' THEN
			CASE
				WHEN P.PRICE < 500 THEN 'Kitchen Supplies'
				ELSE 'Restaurant Equipment'
			END
		WHEN 'Go Green! Shop to Save Energy and Money.' THEN
			CASE
				WHEN P.PRICE < 500 THEN 'Kitchen Supplies'
				ELSE 'Restaurant Equipment'
			END
		WHEN 'Catering / Buffet' THEN
			CASE
				WHEN P.PRICE < 500 THEN 'Kitchen Supplies'
				ELSE 'Restaurant Equipment'
			END
		WHEN 'Pizza Equipment' THEN 'Restaurant Equipment'
		ELSE ISNULL(c.superCat, 'Restaurant Equipment')
	END,
	c.catname
FROM products p LEFT JOIN mfg m on p.mfgID = m.mfgID 
				LEFT JOIN categories c on p.primaryCatID = c.ID
WHERE p.ACTIVE = 1
	
UPDATE feed_shareAsale
SET commission = 
		CASE MerchantCategory
			WHEN 'Restaurant Equipment' THEN CAST(.03*price AS DECIMAL(10, 2))
			WHEN 'Furniture' THEN CAST(.03*price AS DECIMAL(10, 2))
			ELSE CAST(.07*price AS DECIMAL(10, 2))
		END,
		[ReservedForFuture Use] = 
		CASE MerchantCategory
			WHEN 'Restaurant Equipment' THEN 3.00
			WHEN 'Furniture' THEN 3.00
			ELSE 7.00
		END

--MAP POLICY
UPDATE f
SET f.price = 		
		CASE
			WHEN ISNULL(p.map, 0) = 1 THEN 
				CASE 
					WHEN ISNULL(p.feedMap, 0) = 1 THEN 
						CASE
							WHEN ISNULL(p.MAP_Program, 'K') = 'K' THEN cast(p.price as decimal(10, 2))
							ELSE CAST(p.map_price AS DECIMAL(10, 2))
						END
					ELSE cast(p.price as decimal(10, 2))
				END
			--WHEN ISNULL(P.clearance, 0) = 1 THEN cast(p.salesPrice as decimal(10, 2))
			ELSE cast(p.price as decimal(10, 2))
		END
FROM feed_shareAsale f JOIN products p ON f.sku = p.code

--REMOVING ALL PRODUCTS WITH FEEDMAP = 3
DELETE FROM feed_shareAsale
WHERE sku IN
	(SELECT code FROM products WHERE active = 1 AND feedMAP = 3)

DELETE FROM feed_shareAsale WHERE sku LIKE '%599-%'

UPDATE jobHistory SET endDT = GETDATE() WHERE jobID = @jobID
END
GO
