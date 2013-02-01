USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[productsSynchNAVtoProducts11082012]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[productsSynchNAVtoProducts11082012]
AS
BEGIN
DECLARE @jobID INT
INSERT INTO jobHistory(jobName, startdT) values('PROD_NAV', GETDATE())
SET @jobID = @@identity

--DELETING ALL PRODUCTS WHERE IT DOES NOT EXIST IN NAVISION
DELETE p
FROM products p LEFT JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i on p.CODE = i.[No_] COLLATE Latin1_General_CS_AS
WHERE i.[No_] IS NULL

--CREATING PRODUCTS LIST TO IMPORT
TRUNCATE TABLE prodTemp
INSERT INTO prodTemp
SELECT 
	LEFT(i.[No_], 3) [Mfg ID], i.[Vendor No_], i.[Web Item No_], i.[No_],
	i.[Product Name], i.[Description], LOWER(i.[Image File Name]) [Image File Name], 
	0 [Last Direct Cost], 
	0 [UnitPrice2], 
	0 [UnitPrice], 
	i.[Gross Weight], i.[Shipping Length], i.[Shipping Width], i.[Shipping Height], 
	CASE i.[Blocked]
		WHEN 0 THEN 1
		ELSE 1
	END [Blocked], [Web Item], i.[Freight Only], i.[Base Unit of Measure], i.[Free Shipping], 
	w.[Add Handling Charge], i.[Profit %], i.[Keywords], i.[Equivalent Grp_], 
	i.[Last Date Modified], i.[Last Date Modified] [AddedDT], i.[Ship Alone],  
	CASE	
		WHEN [Reorder Point] > 0 THEN 1
		ELSE 0
	END [stockItem], i.[Reorder Point], i.[Maximum Inventory], i.[Reorder Quantity], 
	i.[Shelf No_], w.[Priority], NULL, w.[UPC], i.[Web Active], 
	0 [UnitPrice1],
	0 [listPrice2],
	0 [activeLP]
FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i LEFT JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Web Item] w ON i.[No_] = w.[No_]
WHERE [Blocked] = 0
		AND [Web Item] = 1 
		AND [Status] <> 2 
		AND LEN(i.[No_]) > 3

DELETE t FROM prodTemp t JOIN products p ON t.[No_] = p.code COLLATE SQL_Latin1_General_CP1_CI_AS

--INSERTING NEW PRODUCTS
INSERT INTO products
	([mfgID], [vendorID], [mpn], [CODE], [NAME], [NAVDesc], [IMAGE], [cost], [Price], [listPrice], [WEIGHT], [prodL], [prodW], 
	 [prodH], [ACTIVE], [isWeb], [freightOnly], [katomUOM], [freeShipping], [handlingCharge], [currentMargin], 
	 [keywords], [EquivGrp], [updateDT], [addedDT], [shipAlone], [stockItem], [thresholdMin], [thresholdMax], 
	 [reorderQty], [binLocation], [sitemapPriority], [ICDiscountGrp], [upc], [webActive], [BBPrice], [listprice2], [ActiveLP], quickShip)
SELECT *, 0 FROM prodTemp

UPDATE products SET relatedItems = NULL
--UPDATE ALL PRODUCTS MARKED AS CHANGED THROUGH THE DIRTY FLAG FIELD
UPDATE P
SET	p.[NAME] =i.[Product Name], 
	p.[NAVDesc] = i.[Description], 
	p.[IMAGE] = LOWER(i.[Image File Name]),
	p.weight = CAST(i.[Gross Weight] AS DECIMAL(10, 2)),
	p.[cube] = 0, --
	p.prodL = i.[Shipping Length], 
    p.prodW = i.[Shipping Width], 
	p.prodH = i.[Shipping Height],
	p.active = CASE i.[Blocked]
					WHEN 0 THEN 1
					ELSE 1
				END, 
	p.[isWeb] = i.[Web Item], 
	p.freightOnly = i.[Freight Only], 
	p.UOM = null,
	p.katomUOM = i.[Base Unit of Measure], 
	p.freeShipping = i.[Free Shipping], 
	p.[handlingCharge] = w.[Add Handling Charge],  
	p.[keywords] = i.[Keywords], 
	p.[EquivGrp] = i.[Equivalent Grp_],  
	p.updatedt = i.[Last Date Modified],
	p.[shipAlone] = i.[Ship Alone],   
	p.[stockItem] = CASE	
						WHEN [Reorder Point] > 0 THEN 1
						ELSE 0
					END,  
	p.[thresholdMin] = i.[Reorder Point],  
	p.[thresholdMax] = i.[Maximum Inventory],  
	p.[reorderQty] = i.[Reorder Quantity],  
	p.[binLocation] = i.[Shelf No_], 
	p.[sitemapPriority] = w.[Priority], 
	p.relatedItems = w.[related Products],  
	p.[upc] = w.[UPC],  
	p.[webActive] = i.[Web Active],
	p.mpn = i.[Web Item No_]
from fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i 
		left join fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Web Item] w on i.[No_] = w.[No_]
		join products p on i.[No_] = p.code	collate Latin1_General_CS_AS 	
--Where i.[Dirty Flag] = 1

UPDATE p
SET p.vendorid = i.[Vendor No_]
FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i 
	join products p on i.[No_] = p.code	collate Latin1_General_CS_AS 
WHERE ISNULL(p.vendorid, '') <> i.[Vendor No_] collate Latin1_General_CS_AS 


--UPDATE proddesc WHERE [Dirty Flag] = 1
UPDATE p
set p.[prodDesc] = dbo.[prodDescriptionBuilder](CODE)
FROM products p JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i ON p.CODE = i.[No_] COLLATE Latin1_General_CS_AS
WHERE  [Dirty Flag] = 1 AND ACTIVE = 1

UPDATE products
set prodDesc = dbo.[prodDescriptionBuilder](CODE)
where LEN(ISNULL(CAST(prodDesc AS VARCHAR(MAX)), '')) = 0 and active = 1

--CUBE CALCULATION
update products 
set [cube] = cast((prodW*prodL*prodH)/1728 as decimal(10, 2))
where isnull([cube], 0) = 0

--UPDATE UOM
update p 
set p.katomUOM = u.[Code],
	p.uom = [dbo].[UOM_Conversation](u.[Description])
from fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Unit of Measure] u	
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i ON u.[Code] = i.[Base Unit of Measure]
		JOIN products p on p.CODE = i.[No_]	 COLLATE Latin1_General_CS_AS

--REMOVE ALL DUPLICATE ENTRIES
DELETE FROM products WHERE code IS NULL

DECLARE @CODE VARCHAR(100), @prodid int, @position varchar(100)

SET @position = ''

DECLARE lagCursor CURSOR FOR 
select prodid, code
from products p 
where code in (select code 
				from products where active = 1
				group by code
				having count(*) > 1)
ORDER BY code

OPEN lagCursor
FETCH NEXT FROM lagCursor INTO @prodid, @CODE

WHILE @@FETCH_STATUS = 0
   BEGIN
	IF @position = @code
	   begin
		delete from products where prodid = @prodid
	   end
	ELSE
	   BEGIN
		set @position = @code
	   END
	FETCH NEXT FROM lagCursor INTO @prodid, @CODE
   END

CLOSE lagCursor
DEALLOCATE lagCursor

--UPDATING MFGNAME
UPDATE p
SET p.mfgname = m.mfgName
FROM products p JOIN mfg m ON p.mfgID = m.mfgID 

--UPDATING PRODUCTS WITH INVENTORY COUNT
TRUNCATE TABLE prodInventory

/**
ORIGINAL WAY I LOOKED AT INVENTORY
INSERT INTO prodInventory
SELECT [Item No_], SUM(CAST([Remaining Quantity] AS FLOAT))-
		ISNULL((SELECT sum(ISNULL([Quantity], 0)) FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line]
			WHERE [No_] = l.[Item No_] AND [Document Type] = 1), 0)
FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Ledger Entry] l
			JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i ON i.[No_] = l.[Item No_]
WHERE [Drop Shipment] = 0 
			AND CHARINDEX('100-', [Item No_]) = 0 
			AND [Remaining Quantity] <> 0
GROUP BY [Item No_]
**/

INSERT INTO prodInventory
SELECT [Item No_], SUM(CAST([Quantity] AS FLOAT)) -
				(SELECT isnull(SUM(CAST([Quantity] AS FLOAT)), 0)
				 FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line]
				 WHERE [No_] = l.[Item No_] AND [Document Type] = 1)
FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Ledger Entry] l
			JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i ON i.[No_] = l.[Item No_]
WHERE CHARINDEX('100-', [Item No_]) = 0 
GROUP BY [Item No_]

UPDATE products SET qtyOnHand = 0

UPDATE p SET p.qtyOnHand = i.qty FROM products p JOIN prodInventory i ON p.code = i.code

UPDATE p SET ACTIVE = 0 
FROM products p JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i ON p.CODE = i.[No_] COLLATE Latin1_General_CS_AS
WHERE p.ACTIVE = 1 AND i.[Status] = 2 AND p.qtyonhand < 1

--CREATING BACKORDER ITEM LIST
TRUNCATE TABLE backOrder_item

INSERT INTO backOrder_item
select l.[No_], p.price, p.thresholdMin, p.ThresholdMax, 
	(SELECT ISNULL(SUM(CAST([Quantity] AS FLOAT)), 0) FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Ledger Entry]
		WHERE [Item No_] = l.[No_]) qtyOnHand, 
	COUNT(distinct  l.[Document No_]) numOfOrder, 
	sum(l.[Quantity]) numItem, p.qtyonhand, MIN(h.[Order Date]) [Oldest Order orderDT]
from fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Line] l
	JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Sales Header] h
		ON l.[Document No_] = h.[No_]
	JOIN products p ON p.CODE = l.[No_] collate Latin1_General_CS_AS
where h.[Document Type] = 1
	and p.thresholdMin > 0
	and qtyOnHand < 1
group by l.[No_], p.price, p.thresholdMin, p.ThresholdMax, p.qtyonhand

--UPDATE MPN VARIANCE
UPDATE p
SET p.mpn = 
	CASE 
		WHEN LEN(i.[Web Item No_]) > 0 THEN i.[Web Item No_]
		ELSE i.[Vendor Item No_]
	END
FROM products p JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i ON p.CODE = i.[No_] COLLATE Latin1_General_CS_AS
WHERE p.mpn <> i.[Web Item No_] COLLATE Latin1_General_CS_AS

--UPDATE DISCOUNT GROUP VARIANCE
UPDATE p
SET	p.ICDiscountGrp = i.[Item Disc_ Group]
FROM products p JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i ON p.CODE = i.[No_] COLLATE Latin1_General_CS_AS
WHERE ISNULL(p.ICDiscountGrp, '') <> i.[Item Disc_ Group] COLLATE Latin1_General_CS_AS

--DELETE ALL 599 PRODUCTS FROM DB
UPDATE products
SET active = 0
WHERE LEFT(code, 3) = '599' AND ACTIVE = 1

/**
UPDATE MAP DATA TO DATA FEED
1. FEED MAP PRICE TO DATA FEED
2. FEED KATOM PRICE TO DATA FEED
3. DON'T FEED TO DATAFEED
**/
UPDATE products SET MAP_Program = NULL, feedMAP = 2, MAP_Price = NULL, MAP = 0

UPDATE p 
SET p.MAP = g.[MAP],
	p.MAP_Program =
		CASE g.[Map Program]
			WHEN 1 THEN 'K' --DISPLAY KATOM PRICE
			WHEN 2 THEN 'E' --EMAIL ME MY PRICE
			WHEN 3 THEN 'A' --ADD TO CART
			WHEN 4 THEN 'C' --CALL FOR INFORMATION
			WHEN 5 THEN 'X' --DON'T LIST
			ELSE 'K'
		END,
	p.feedMAP =   
	CASE 
		WHEN ISNULL(g.[Map Program], 1) = 1 THEN 2
		ELSE ISNULL(g.[Feed Rules], 2)
	END		
from fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Discount Group] g
	join fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i ON i.[Item Disc_ Group] = g.Code
	join products p on p.code = i.[No_] COLLATE Latin1_General_CS_AS
where g.[MAP] = 1 AND p.active = 1

--SET PRODUCTS ACTIVE
UPDATE p
SET p.active = 0
FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i JOIN products p ON i.[No_] = p.CODE collate Latin1_General_CS_AS 	
WHERE (i.[Status] = 2 OR i.[Web Item] = 0) AND p.qtyOnHand < 1 and p.active = 1

--MARK ITEM TO BE SPECIAL ORDER
UPDATE products
SET specialOrder = 0

UPDATE p
SET p.specialOrder = 1
FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i JOIN products p ON i.[No_] = p.CODE collate Latin1_General_CS_AS 	
WHERE i.[Status] = 4 AND p.active = 1
 
--UPDATING PRICING
exec dbo.[product_Pricing_Update_SP]

--ASSIGNING CORRECT CATEGORY TO PRODUCTS
exec dbo.[product_categorization_SP]

-- MOVING PRODUCTS TO CLEARANCE BIN
exec dbo.[stockCloseout]

--COUNT THE NUMBER OF PRODUCTS PER CATEGORY
EXEC dbo.[categories_numProd]

--ASSIGNING CORRECT HOLIDAY CATEGORIES TO PRODUCTS
--exec dbo.[product_categorization_holiday_SP]

--MARK PRODUCTS TO LIST ON FROSTY ACRES SITE
UPDATE products SET fa = 0, NAME = LTRIM(RTRIM(NAME))

UPDATE products 
SET fa = 1 
WHERE qtyOnHand > 0 --OR thresholdMin > 0 

UPDATE p 
SET fa = 1 
FROM products p JOIN mfg m ON p.mfgID = m.mfgID
WHERE ISNULL(m.frostyacres, 0) = 1 AND fa = 0

UPDATE products
SET fa = 1  
where code = '175-40813'

--UPDATE QUICK SHIP FLAG
UPDATE p
SET p.quickShip = i.[Vendor Quick Ship],
	p.fa = 1,
	leadTime = 'Typically ships within <span class="bold">3 - 6 business days</span>'
FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i JOIN products p ON i.[No_] = p.CODE collate Latin1_General_CS_AS 
	
UPDATE products 
SET quickShip = 
	CASE 
		WHEN qtyOnHand > 0 THEN 1
		ELSE 0
	END,
	leadTime = 
	CASE 
		WHEN qtyOnHand > 0 THEN 'Typically ships within <span class="bold"> 1 - 2 business days</span>'
		ELSE NULL
	END,	
	fa = 
	CASE 
		WHEN qtyOnHand > 0 THEN 1		
	END
Where quickShip = 0

--UPDATE PACK SIZE AND STACKABLE INFORMATION FROM EXTERNAL TABLE
UPDATE p
  SET p.packsize = s.packsize,
  p.stackable = s.stackable
  FROM products AS p
  INNER JOIN productpacksize AS s
  ON p.code = s.code;
  
  
  
--Update PDF/Spec information  
UPDATE products
SET specSheet = i.pdf
FROM products p, image_results2 i
WHERE p.code = i.code

UPDATE products
SET image_exists = i.image
FROM products p, image_results2 i
WHERE p.code = i.code

UPDATE products
SET large_image_exists = i.large_image
FROM products p, image_results2 i
WHERE p.code = i.code
 
-- REMOVE THE 2 PURPLE CAMBRO ITEMS WHICH ARE SPECIFIC FOR YOGURT MOUNTAIN
DELETE FROM products WHERE CODE in ('144-SPO10CW100', '144-TG6100')

--REMOVE ALL BLENDTEC RESIDENTIAL FROM BB SITE
UPDATE products SET bb = 0 WHERE mfgID ='579'

--SET EQUIVALENT INFORMATION FOR CORY DISCONTINUED PAGE 
EXEC [dbo].[products_equivalent_sp]

--INSERTING SPECIAL PROMOTION TEXT FOR THE WARING BLENDER
UPDATE products
SET extendedDescription = '<p>Waring offers mixing products of excellence and reliability in today''s growing restaurant supply world. Add these qualities to your own business with the Waring Margarita Madness Blender and you''ll never mix with anything else!</p><p>The Waring drink blender can handle anything from a colorful bar drink to the perfect margarita, but that''s not all. The Waring Margarita Madness drink blender features a 48 ounce clear polycarbonate container that is virtually unbreakable. For a quick, no wait mixing experience, this bar blender features a powerful 1 1/2 horsepower motor balanced to minimize vibration and built to last. With a smooth, rounded design this durable, blender is so easy to clean. For great versatility in drink preparation don''t overlook this Waring blender!</p>'
WHERE CODE = '141-MMB142' and qtyOnHand < 2

UPDATE jobHistory SET endDT = GETDATE() WHERE jobID = @jobID
END
GO
