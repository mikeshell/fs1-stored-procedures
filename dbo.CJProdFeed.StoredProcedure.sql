USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[CJProdFeed]    Script Date: 02/01/2013 11:09:50 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[CJProdFeed]
AS
	BEGIN
		TRUNCATE TABLE cjFeed

		INSERT INTO cjFeed
		SELECT
			dbo.cleaner(p.NAVDesc),
			dbo.cleaner(p.NAVDesc) [Keywords],
			/**
			CASE 
				WHEN LEN(ISNULL(CAST(p.prodDesc AS VARCHAR(MAX)), '')) = 0 THEN p.[Name]
				ELSE dbo.cleaner(p.prodDesc)
			END [DESCRIPTION],				
			**/
			dbo.cleaner(p.NAVDesc)[DESCRIPTION],
			p.code [SKU],
			'http://www.katom.com/' + p.CODE + '.html' [BUYURL],
			'YES' [AVAILABLE],
			-- 'http://www.katom.com/largeproducts/' + LEFT(p.CODE, 3) + '/' + [image] + '.jpg',
			NULL,
			CAST(p.price as decimal(10, 2))[PRICE],
			CAST(p.listprice as decimal(10, 2)) [RETAILPRICE],
			CASE c.superCat
				WHEN 'Shop By Vendor' THEN
					CASE
						WHEN P.PRICE < 500 THEN 'KitchenSupplies'
						ELSE 'RestaurantEquipment'
					END
				WHEN 'Related Items' THEN
					CASE
						WHEN P.PRICE < 500 THEN 'KitchenSupplies'
						ELSE 'RestaurantEquipment'
					END
				WHEN 'Go Green! Shop to Save Energy and Money.' THEN
					CASE
						WHEN P.PRICE < 500 THEN 'KitchenSupplies'
						ELSE 'RestaurantEquipment'
					END
				WHEN 'Go Green! Shop to Save Energy and Money.' THEN
					CASE
						WHEN P.PRICE < 500 THEN 'KitchenSupplies'
						ELSE 'RestaurantEquipment'
					END
				WHEN 'Catering / Buffet' THEN
					CASE
						WHEN P.PRICE < 500 THEN 'KitchenSupplies'
						ELSE 'RestaurantEquipment'
					END
				WHEN 'KaTom Kids' THEN
					CASE
						WHEN P.PRICE < 500 THEN 'KitchenSupplies'
						ELSE 'RestaurantEquipment'
					END
				WHEN 'Special' THEN 
					CASE
						WHEN P.PRICE < 500 THEN 'KitchenSupplies'
						ELSE 'RestaurantEquipment'
					END
				WHEN 'Restaurant Equipment' THEN 'RestaurantEquipment'
				WHEN 'Bar Supplies' THEN 'BarSupplies'
				WHEN 'Kitchen Supplies' THEN 'KitchenSupplies'
				WHEN 'Pizza Equipment' THEN 'RestaurantEquipment'
				WHEN 'Clearance Sale' THEN 'CLEARANCESALE'
				ELSE ISNULL(c.superCat, 'RestaurantEquipment')
			END	[ADVERTISER CATEGORY],
			CASE p.freeShipping
				WHEN 1 THEN 'FREE SHIPPING'
				ELSE ''
			END [PROMOTIONAL TEXT],
			m.mfgName [MANUFACTURER],
			p.mpn [MANUFACTURERID],
			'NO' [SPECIAL], --yes/no value
			'New' [CONDITION],
			'' [AUTHOR] --use to indicate top seller
		FROM products p LEFT JOIN mfg m on p.mfgID = m.mfgID 
						LEFT JOIN categories c on p.primaryCatID = c.ID
		WHERE p.ACTIVE = 1
			and c.primaryCat = 1
			AND p.price > 0
		
		--MAP POLICY
		UPDATE g
		SET g.price = 		
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
				END, 
				[SPECIAL] = 'YES'
		FROM cjFeed g join products p on g.sku = p.code
			
		--SET CLEARANCE ITEMS TO BE SPECIAL ITEMS	
		UPDATE cj
		SET [SPECIAL] = 'YES'
		FROM cjFeed cj JOIN products p ON cj.sku = p.code
					   JOIN categories c ON c.id = p.primaryCatID
		WHERE p.active = 1 AND c.ACTIVE = 1
			AND c.supercat = 'Clearance Sale'
			AND [SPECIAL] = 'NO'
			
		TRUNCATE TABLE cjTopSeller
		INSERT INTO cjTopSeller
		SELECT top 500 l.[No_], COUNT(DISTINCT h.[No_]), SUM(l.[Amount])
		FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l
				JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] h ON l.[Document No_] = h.[No_]
		WHERE DATEDIFF(day, h.[Order Date], GeTDATE()) < 91 AND LEN(l.[No_]) > 0 AND ISNUMERIC(l.[No_]) = 0
					and	l.[No_] not like '100-%'
		group by l.[No_]
		order by COUNT(DISTINCT h.[No_]) DESC

		UPDATE cjFeed
		SET author = 'TOP SELLER'
		WHERE SKU IN (SELECT code FROM cjTopSeller)

		TRUNCATE TABLE cjTopSeller
		INSERT INTO cjTopSeller
		SELECT top 500 l.[No_], COUNT(DISTINCT h.[No_]), SUM(l.[Amount])
		FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Line] l
				JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Invoice Header] h ON l.[Document No_] = h.[No_]
		WHERE DATEDIFF(day, h.[Order Date], GeTDATE()) < 91 AND LEN(l.[No_]) > 0 AND ISNUMERIC(l.[No_]) = 0
					and	l.[No_] not like '100-%'
		group by l.[No_]
		order by SUM(l.[Amount]) DESC

		UPDATE cjFeed
		SET author = 'TOP SELLER'
		WHERE SKU IN (SELECT code FROM cjTopSeller)
		
		delete from cjFeed where price is null
				
		SELECT * FROM cjFeed
	END
GO
