USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[productsSynchNAVtoProducts]    Script Date: 01/30/2013 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[productsSynchNAVtoProducts]
AS
BEGIN
DECLARE @jobID INT
INSERT INTO jobHistory(jobName, startdT) values('PROD_NAV', GETDATE())
SET @jobID = @@identity

--DELETING ALL products WHERE IT DOES NOT EXIST IN NAVISION
DELETE p
FROM products p LEFT JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i on p.CODE = i.[No_] COLLATE Latin1_General_CS_AS
WHERE i.[No_] IS NULL

--INSERT ALL PRODUCTS FROM NAV NOT IN THE PRODUCTS TABLE
INSERT INTO products
	([mfgID], [vendorID], [mpn], [CODE], [NAME], [NAVDesc], [IMAGE], [cost], [Price], [listPrice], [WEIGHT], [prodL], [prodW], 
	 [prodH], [ACTIVE], [isWeb], [freightOnly], [katomUOM], [freeShipping], [handlingCharge], [currentMargin], 
	 [keywords], [EquivGrp], [updateDT], [addedDT], [shipAlone], [stockItem], [thresholdMin], [thresholdMax], 
	 [reorderQty], [binLocation], [sitemapPriority], [ICDiscountGrp], [upc], quickShip, specialOrder)
SELECT 
	LEFT(i.[No_], 3) [Mfg ID], i.[Vendor No_], i.[Web Item No_], LTRIM(RTRIM(i.[No_])),
	LTRIM(RTRIM(REPLACE(i.[Product Name], CHAR(10), ''))), i.[Description], REPLACE(LOWER(REPLACE(REPLACE(i.[Image File Name], CHAR(13), ''), CHAR(10), '')), '.JPG', '') [Image File Name], 
	0 [Last Direct Cost], 
	0 [UnitPrice2], 
	0 [UnitPrice], 
	i.[Gross Weight], i.[Shipping Length], i.[Shipping Width], i.[Shipping Height], 
	CASE i.[Status]
		WHEN 2 THEN 0
		ELSE 1
	END [Status], [Web Item], i.[Freight Only], i.[Base Unit of Measure], i.[Free Shipping], 
	w.[Add Handling Charge], i.[Profit %], i.[Keywords], i.[Equivalent Grp_], 
	GETDATE(), GETDATE() [AddedDT], i.[Ship Alone],  
	CASE	
		WHEN [Reorder Point] > 0 THEN 1
		ELSE 0
	END [stockItem], i.[Reorder Point], i.[Maximum Inventory], i.[Reorder Quantity], 
	i.[Shelf No_], w.[Priority], NULL, w.[UPC], i.[Vendor Quick Ship],
	CASE i.[Status]
		WHEN 4 THEN 1
		ELSE 0
	END [Special Order]
FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i 
	LEFT JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Web Item] w ON i.[No_] = w.[No_]
	LEFT JOIN products p on p.code = i.[No_] COLLATE Latin1_General_CS_AS
WHERE [Blocked] = 0
		AND [Web Item] = 1 
		AND LEN(i.[No_]) > 3
		AND p.CODE is null
		AND i.[No_] NOT IN ('144-SPO10CW100', '144-TG6100') -- EXCLUDING THE PURPLE TONG FOR YOGURT MOUNTAIN

--UPDATE ALL products MARKED AS CHANGED THROUGH LAST MODIFIED DATE ON NAV
UPDATE P
SET	p.[NAME] = LTRIM(RTRIM(REPLACE(i.[Product Name], CHAR(10), ''))), 
	p.[NAVDesc] = i.[Description], 
	p.[IMAGE] = REPLACE(LOWER(REPLACE(REPLACE(i.[Image File Name], CHAR(10), ''), CHAR(13), '')), '.JPG', ''),
	p.weight = CAST(i.[Gross Weight] AS DECIMAL(10, 2)),
	p.prodL = i.[Shipping Length], 
    p.prodW = i.[Shipping Width], 
	p.prodH = i.[Shipping Height],
	P.[cube] = cast((i.[Shipping Length]*i.[Shipping Width]*i.[Shipping Height])/1728 as decimal(10, 2)),
	p.active = CASE i.[Status]
					WHEN 2 THEN 0
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
	p.relatedItems = w.[related products],
	p.[upc] = w.[UPC],  
	p.[webActive] = i.[Web Active],
	p.mpn = i.[Web Item No_],
	p.vendorid = i.[Vendor No_],
	p.specialOrder = CASE i.[Status]
							WHEN 4 THEN 1
							ELSE 0
						END,
	p.[prodDesc] = dbo.[prodDescriptionBuilder](CODE),
	p.quickShip = i.[Vendor Quick Ship]
FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i 
		LEFT JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Web Item] w on i.[No_] = w.[No_]
		JOIN products p ON LTRIM(RTRIM(i.[No_])) = p.code COLLATE Latin1_General_CS_AS 	
WHERE DATEDIFF(DAY, i.[Last Date Modified], GETDATE()) = 1


--SETTING DUKE OVENS TO DUKE MFG
UPDATE products
SET mfgID = '212'
WHERE mfgID = '066'

--UPDATE UOM
update p 
set p.katomUOM = u.[Code],
	p.uom = [dbo].[UOM_Conversion](u.[Description])
from fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Unit of Measure] u	
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item] i ON u.[Code] = i.[Base Unit of Measure]
		JOIN products p on p.CODE = LTRIM(RTRIM(i.[No_]))	 COLLATE Latin1_General_CS_AS
WHERE p.UOM IS NULL

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
WHERE DATEDIFF(DAY, updatedt, GETDATE()) = 0


--UPDATING products WITH INVENTORY COUNT
TRUNCATE TABLE prodInventory

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

--REMOVE DISCONTINUED PRODUCTS ADDED TODAY WITH NO INVENTORY AND ACTIVATE DISCONT. PROD W/ INVENTORY ON HAND 
DELETE FROM products WHERE qtyOnHand < 1 AND active = 0 AND DATEDIFF(DAY, addedDT, GETDATE()) = 0
UPDATE products SET active = 1 WHERE qtyOnHand > 0 AND active = 0 AND DATEDIFF(DAY, addedDT, GETDATE()) = 0 

--INACTIVATE ALL CURRENT DISCONTINUED PRODUCTS WITH 0 INVENTORY
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

--DELETE ALL 599 products FROM DB
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

--UPDATING PRICING
exec dbo.[product_Pricing_Update_SP]

-- MOVING products TO CLEARANCE BIN
exec dbo.[stockCloseout]

--ASSIGNING CORRECT CATEGORY TO products -- INCREMENTAL
exec dbo.[product_categorization_SP] 'INCREMENTAL'

--SET EQUIVALENT INFORMATION FOR DISCONTINUED PAGE 
EXEC [dbo].[products_equivalent_sp]

--MARK products TO LIST ON FROSTY ACRES SITE
UPDATE products SET fa = 0, NAME = LTRIM(RTRIM(NAME))

--MARK AN ITEM IF IT IS GSA APPROVED
UPDATE products SET GSA = 0

UPDATE p
SET p.GSA = 
		CASE 
			WHEN ISNULL(r.[Refinable Value], 0) = 'Yes' THEN 1
			ELSE 0
		END
FROM products p JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Refinables] r ON p.CODE = r.[Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS
WHERE r.[Refinable No_] = 'GSA'


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

--SET PRODUCTS TO QUICKSHIP IF QTYONHAND > 0
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
	END
Where quickShip = 0

--UPDATE PACK SIZE AND STACKABLE INFORMATION FROM EXTERNAL TABLE
UPDATE p
SET p.packsize = s.packsize,
	p.stackable = s.stackable
FROM products p JOIN productpacksize s ON p.code = s.code


--POPULATE PRODUCTS ATTRIBUTES TABLE
TRUNCATE TABLE products_attributes

INSERT INTO products_attributes
SELECT ri.[Item No_], 
	r.[Description],
	ri.[Refinable Value]
FROM fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Item Refinables] ri 
		JOIN fs1.katom2009.dbo.[B&B Equipment & Supply Inc_$Refinable] r ON  ri.[Refinable No_] = r.[No_]
		JOIN products p ON ri.[Item No_] = p.CODE COLLATE SQL_Latin1_General_CP1_CI_AS
WHERE p.active = 1 
	AND p.isWeb = 1
	AND LEN(ri.[Refinable Value]) > 0	
	AND ri.[Refinable No_] NOT IN ('GSA')
ORDER BY code

--ADD MivaProdID WHERE NONE EXISTS  - added 1/11-2013 by MS - to be removed when FAB migrates
Update products SET mivaProdID = prodid + 1000000 where mivaProdID is null

--REMOVE ALL BLENDTEC RESIDENTIAL FROM BB SITE
UPDATE products SET bb = 0 WHERE mfgID ='579' AND bb = 1

--Update PDF/Spec information  
UPDATE products
SET specSheet = i.pdf,
	image_exists = i.image,
	large_image_exists = i.large_image
FROM products p JOIN image_results2 i ON p.code = i.code

--INSERTING SPECIAL PROMOTION TEXT FOR THE WARING BLENDER
UPDATE products
SET extendedDescription = '<p>Waring offers mixing products of excellence and reliability in today''s growing restaurant supply world. Add these qualities to your own business with the Waring Margarita Madness Blender and you''ll never mix with anything else!</p><p>The Waring drink blender can handle anything from a colorful bar drink to the perfect margarita, but that''s not all. The Waring Margarita Madness drink blender features a 48 ounce clear polycarbonate container that is virtually unbreakable. For a quick, no wait mixing experience, this bar blender features a powerful 1 1/2 horsepower motor balanced to minimize vibration and built to last. With a smooth, rounded design this durable, blender is so easy to clean. For great versatility in drink preparation don''t overlook this Waring blender!</p>'
WHERE CODE = '141-MMB142' and qtyOnHand < 2

--ACTIVATE ALL DISCONTINUED ITEMS WITH QTYONHAND > 0
UPDATE products
SET ACTIVE = 1, isWeb = 1
WHERE qtyOnHand > 0

--RELATEDITEMS CLEAN UP - REMOVE ALL NON-ACTIVE PRODUCTS
EXEC dbo.[product_relatedItems_Cleanup_SP] 'PARTIAL'

--REMOVE ALL NON-ACTIVE PARTS FROM THE WEB
DELETE FROM products WHERE isWeb = 0 AND ICDiscountGrp LIKE '%-part%'

--REMOVE ALL TRUE S&D FROM SITE
DELETE FROM products WHERE LEFT(code, 3) = '599'

UPDATE jobHistory SET endDT = GETDATE() WHERE jobID = @jobID
END
GO
