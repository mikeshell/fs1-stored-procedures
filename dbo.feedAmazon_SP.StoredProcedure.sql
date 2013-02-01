USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[feedAmazon_SP]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[feedAmazon_SP]
 @price money -- MIN  VALUE WE WANT TO INCLUDE IN THE FEED
AS
BEGIN	
	SELECT ISNULL(dbo.breadcrumb(primarycatcode), 'Kitchen Supply & Equipment') category,
		p.mfgName + ' ' + p.mpn + ' ' + p.Name title,
		'http://www.katom.com/' + p.CODE 
			+ '.html?CID=Amazon&utm_source=Amazon& utm_medium=CSE& utm_campaign=CSE&zmam=29342707&zmas=1&zmac=9&zmap=' 
			+ p.CODE link,
		p.CODE sku, 
		CAST(p.price AS MONEY) price,
		p.mfgName Brand,
		'http://' + p.mfgid + '.katomcdn.com/' + LOWER(p.image) + '_large.jpg' [image],
		REPLACE(REPLACE(CAST(p.prodDesc AS VARCHAR(MAX)), CHAR(10), ''), CHAR(13), '') [description],
		p.mfgName manufacturer,
		p.mpn [mfr part number],
		CASE 
			WHEN p.freeShipping = 1 THEN CAST(0 AS VARCHAR(1))
			ELSE ''
		END [shipping cost],
		p.mfgName + ' ' + p.mpn + ' ' + p.NAME [bullet point1],
		CAST(p.prodH AS DECIMAL(10, 2)) Height,		
		CASE 
			WHEN p.uom = 'each' THEN 1
			WHEN p.uom = 'Set' THEN 1
			WHEN p.uom = 'Pair' THEN 1
			WHEN p.uom = 'Pack' THEN 1
			WHEN p.uom = 'Bag' THEN 1
			WHEN p.uom = 'Box' THEN 1
			WHEN p.uom = 'Roll' THEN 1
			WHEN p.uom = 'Case' THEN 1
			WHEN p.uom = 'bottle' THEN 1
			WHEN p.uom = 'dozen' THEN 12
			WHEN p.uom LIKE '%Gallon' THEN 1
			WHEN p.uom LIKE '%each' THEN CAST(REPLACE(p.uom, 'each', '') AS INT)
			WHEN p.uom LIKE '%set' THEN CAST(REPLACE(p.uom, 'set', '') AS INT)
			WHEN p.uom LIKE '%Pair' THEN CAST(REPLACE(p.uom, 'Pair', '') AS INT)
			WHEN p.uom LIKE '%Roll' THEN CAST(REPLACE(p.uom, 'Roll', '') AS INT)
			WHEN p.uom LIKE '%Pack' THEN CAST(REPLACE(p.uom, 'Pack', '') AS INT)
			WHEN p.uom LIKE '%bottle' THEN CAST(REPLACE(p.uom, 'bottle', '') AS INT)
			WHEN p.uom LIKE 'case of%' THEN CAST(REPLACE(p.uom, 'case of', '') AS INT)
			WHEN p.uom LIKE 'pack of%' THEN CAST(REPLACE(p.uom, 'pack of', '') AS INT)
			WHEN p.uom LIKE 'set of%' THEN CAST(REPLACE(p.uom, 'set of', '') AS INT)
			WHEN p.uom LIKE 'box of%' THEN CAST(REPLACE(p.uom, 'box of', '') AS INT)
			WHEN p.uom LIKE '%dozen' THEN CAST(REPLACE(p.uom, 'dozen', '') AS INT)*12
			WHEN p.uom LIKE '%pack of%'
				THEN LEFT(REPLACE(p.uom, 'pack of', ''), CHARINDEX(' ', REPLACE(p.uom, 'pack of', ''))) 
					* CAST(RIGHT(REPLACE(p.uom, 'pack of', ''), LEN(REPLACE(p.uom, 'pack of', '')) - CHARINDEX(' ', REPLACE(p.uom, 'pack of', ''))) AS INT)		
			WHEN p.uom LIKE '%box of%'
				THEN LEFT(REPLACE(p.uom, 'box of', ''), CHARINDEX(' ', REPLACE(p.uom, 'box of', ''))) 
					* CAST(RIGHT(REPLACE(p.uom, 'box of', ''), LEN(REPLACE(p.uom, 'box of', '')) - CHARINDEX(' ', REPLACE(p.uom, 'box of', ''))) AS INT)		
			WHEN p.uom LIKE '%set of%'
				THEN LEFT(REPLACE(p.uom, 'set of', ''), CHARINDEX(' ', REPLACE(p.uom, 'set of', ''))) 
					* CAST(RIGHT(REPLACE(p.uom, 'set of', ''), LEN(REPLACE(p.uom, 'set of', '')) - CHARINDEX(' ', REPLACE(p.uom, 'set of', ''))) AS INT)				
			ELSE 1
		END [Item package quantity],
		LEFT(p.keywords, 50) Keywords1,
		CAST(p.prodL AS DECIMAL(10, 2)) [Length],
		ISNULL(c.amazon, 289814) [Recommended Browse Node],
		CAST(p.WEIGHT AS DECIMAL(10, 2)) [Shipping Weight],
		p.prodW [Width]		
	FROM products p LEFT JOIN cseCatMapping c ON p.primaryCatCode = c.catCode
	WHERE p.active = 1 AND p.isWeb = 1 AND p.price > @price AND p.image NOT LIKE '%logo'
		AND p.CODE NOT IN (select [No_] COLLATE Latin1_General_CS_AS from backOrder_item where DATEDIFF(day, [OldestOrderdt], getdate()) > 15 and price < 250)
END
GO
