USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[ebayListing]    Script Date: 02/01/2013 11:09:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[ebayListing] 
AS
BEGIN
	TRUNCATE TABLE ebayProdListing
	
	INSERT INTO ebayProdListing
	SELECT c.[ActionURL], c.[OfferName], ISNULL(c.[OfferDescription], c.[OfferName]) [OfferDescription], c.[ReferenceImageURL], c.[RegularPrice], 
		CASE 
			WHEN Custom6 = 0 AND p.freightOnly = '1' THEN 
				CASE
					WHEN ISNULL(p.clearance, 0) = 1 THEN
						CASE
							WHEN p.[salesPrice] BETWEEN 0 AND 500 THEN [CurrentPrice] + 150 
							WHEN p.[salesPrice] > 500 THEN [CurrentPrice] + 200
							ELSE p.[salesPrice]
						END
					WHEN c.[CurrentPrice] BETWEEN 0 AND 500 THEN [CurrentPrice] + 150
					WHEN c.[CurrentPrice] > 500 THEN [CurrentPrice] + 200
					ELSE c.[CurrentPrice]
				END				
			ELSE c.[CurrentPrice]
		END [CurrentPrice], 
		c.[Model], 
		CASE c.[InStock]
			WHEN 1 THEN '1 '
			ELSE '0 '
		END [InStock], 
		c.[Brand], c.[MerchantCategory], c.[Manufacturer], c.[ManufacturerModel], 
		[PromotionalText], 
		CASE 
			WHEN (prodL*prodH*prodW)/166 > c.[Weight] AND p.freightOnly = '0' THEN CAST((prodL*prodH*prodW)/166 AS DECIMAL(10, 2))+5
			ELSE c.[Weight]
		END [Weight], 
		c.[Condition], c.[Keywords], c.[Custom1], 
		c.[Custom2], c.[Custom3], c.[Custom4], c.[Custom5], c.[productCost], 
		CASE 
			WHEN Custom6 = 0 AND p.freightOnly = '1' AND c.[CurrentPrice] > 99.99 THEN 1
			ELSE c.[Custom6]
		END [Custom6], 
		c.[Custom7], c.[Custom8], c.[Custom9], c.[Custom10], 
		c.[Custom11], c.[custom12], 
		CASE 
			WHEN p.qtyOnHand = 0 THEN 1
			WHEN p.qtyOnHand > 5 THEN 5
			ELSE p.qtyOnHand
		END qtyonHand,
		uom, s.color, s.material, p.freightOnly,
		p.price [Offer Price], 0 --unlimitted
	FROM CAFeed c JOIN products p ON c.[Model] = p.CODE
				LEFT JOIN SLIFeed s ON s.[Product Number] = c.[Model]
	WHERE p.mfgID IN (SELECT mfgID FROM mfg WHERE ebay = 1 AND ACTIVE = 1)
		  AND CAST(c.[CurrentPrice] AS DECIMAL(10, 2)) > 25
	UNION
	SELECT c.[ActionURL], c.[OfferName], ISNULL(c.[OfferDescription], c.[OfferName]) [OfferDescription], c.[ReferenceImageURL], 
		c.[RegularPrice] + 25, 
		CASE 
			WHEN c.[CurrentPrice] < 10 THEN c.[CurrentPrice] + 10
			ELSE c.[CurrentPrice] + 15
		END , 
		c.[Model], '1 ' [InStock], c.[Brand], c.[MerchantCategory], 
		c.[Manufacturer], c.[ManufacturerModel], [PromotionalText], 
		CASE 
			WHEN (prodL*prodH*prodW)/166 > c.[Weight] AND p.freightOnly = '0' THEN CAST((prodL*prodH*prodW)/166 AS DECIMAL(10, 2))+5
			ELSE c.[Weight]
		END [Weight], 
		c.[Condition], c.[Keywords], c.[Custom1], c.[Custom2], c.[Custom3], c.[Custom4], c.[Custom5], c.[productCost],
		'1' [Custom6], c.[Custom7], c.[Custom8], c.[Custom9], c.[Custom10], c.[Custom11], c.[custom12],  
		CASE 
			WHEN p.qtyOnHand = 0 THEN 1
			WHEN p.qtyOnHand > 5 THEN 5
			ELSE p.qtyOnHand
		END qtyonHand,
		uom, s.color, s.material, p.freightOnly,
		p.price [Offer Price], 0 --unlimitted
	FROM CAFeed c JOIN products p ON c.[Model] = p.CODE
				LEFT JOIN SLIFeed s ON s.[Product Number] = c.[Model]
				LEFT JOIN products_Clearance cp ON p.code = cp.code
	WHERE p.qtyOnHand > 0
			  AND CAST(c.[CurrentPrice] AS DECIMAL(10, 2)) BETWEEN 0 AND 25
			  AND p.freightOnly = '0'
	UNION
	SELECT c.[ActionURL], c.[OfferName], ISNULL(c.[OfferDescription], c.[OfferName]) [OfferDescription], c.[ReferenceImageURL], c.[RegularPrice], 
		CASE 
			WHEN Custom6 = 0 AND p.freightOnly = '1' THEN 
				CASE
					WHEN ISNULL(p.clearance, 0) = 1 THEN
						CASE
							WHEN p.[salesPrice] BETWEEN 0 AND 500 THEN [CurrentPrice] + 150 
							WHEN p.[salesPrice] > 500 THEN [CurrentPrice] + 200
							ELSE p.[salesPrice]
						END
					WHEN c.[CurrentPrice] BETWEEN 0 AND 500 THEN [CurrentPrice] + 150
					WHEN c.[CurrentPrice] > 500 THEN [CurrentPrice] + 200
					ELSE c.[CurrentPrice]
				END				
			ELSE c.[CurrentPrice]
		END [CurrentPrice], 
		c.[Model], 
		CASE c.[InStock]
			WHEN 1 THEN '1 '
			ELSE '0 '
		END [InStock], 
		c.[Brand], c.[MerchantCategory], c.[Manufacturer], c.[ManufacturerModel], 
		[PromotionalText], 
		CASE 
			WHEN (prodL*prodH*prodW)/166 > c.[Weight] AND p.freightOnly = '0' THEN CAST((prodL*prodH*prodW)/166 AS DECIMAL(10, 2))+5
			ELSE c.[Weight]
		END [Weight], 
		c.[Condition], c.[Keywords], c.[Custom1], 
		c.[Custom2], c.[Custom3], c.[Custom4], c.[Custom5], c.[productCost], 
		CASE 
			WHEN Custom6 = 0 AND p.freightOnly = '1' AND c.[CurrentPrice] > 99.99 THEN 1
			ELSE c.[Custom6]
		END [Custom6], 
		c.[Custom7], c.[Custom8], c.[Custom9], c.[Custom10], 
		c.[Custom11], c.[custom12],  
		CASE 
			WHEN p.qtyOnHand = 0 THEN 1
			WHEN p.qtyOnHand > 5 THEN 5
			ELSE p.qtyOnHand
		END qtyonHand,
		uom, s.color, s.material, p.freightOnly,
		p.price [Offer Price], 0 --unlimitted
	FROM CAFeed c JOIN products p ON c.[Model] = p.CODE
				LEFT JOIN SLIFeed s ON s.[Product Number] = c.[Model]
				LEFT JOIN products_Clearance cp ON p.code = cp.code
	WHERE p.qtyOnHand > 0
		  AND CAST(c.[CurrentPrice] AS DECIMAL(10, 2)) > 25
	order by 1

	UPDATE e
	SET e.currentPrice = cast(p.MAP_Price as decimal(10, 2))
	from ebayprodlisting e join products p ON p.code = e.Model
	WHERE ISNULL(feedMAP, 0) = 1 AND  e.currentPrice > 24.99
		
	--REMOVE ALL SPECIAL ORDER 141
	DELETE FROM ebayprodlisting
	WHERE Model IN (select CODE	from products where ICDiscountGrp in ('141-NRA','141-PRO','141-SPEC') and qtyOnHand = 0)
	
	/**
	--SETTING FREIGHT ITEM TO FREE SHIPPING AND ADDING SHIPPING COST TO ITEMS ACCORDING TO THE VALUE OF THE SELLING PRICE
	update e
	set e.currentprice = 
			CASE 
				WHEN ISNULL(p.clearance, 0) = 1 THEN
					CASE
						WHEN p.[salesPrice] BETWEEN 0 AND 500 THEN [salesPrice] + 150 
						WHEN p.[salesPrice] > 500 THEN [salesPrice] + 200
						ELSE p.[salesPrice]
					END
				WHEN p.price BETWEEN 0 AND 500 THEN price + 150 
				WHEN p.price BETWEEN 500 AND 1500 THEN price + 200
				WHEN p.price > 1500 THEN price + 250 
				ELSE price
			END, 
		e.custom6 = 1
	from ebayProdListing e join products p on p.CODE = e.Model
	where p.freightOnly = '1' and freeShipping = 0

	update e
	set e.currentprice = 
			CASE 
				WHEN p.freeShipping = 1 THEN t.sellingPrice
				ELSE
					CASE	
						WHEN t.sellingPrice BETWEEN 0 AND 500 THEN t.sellingPrice + 150 
						WHEN t.sellingPrice BETWEEN 500 AND 1500 THEN t.sellingPrice + 200
						WHEN t.sellingPrice > 1500 THEN t.sellingPrice + 250 
						ELSE t.sellingPrice 
					END
				END,
		e.custom6 = 1
	from ebayProdListing e join trueSuperTrailer t on e.Model = t.code
							join products p on p.CODE = t.code
	where t.feedMAP = 0
	
	update e
	set e.currentprice = 
			CASE 
				WHEN p.freeShipping = 1 THEN t.displayPrice
				ELSE
					CASE	
						WHEN t.displayPrice BETWEEN 0 AND 500 THEN t.displayPrice + 150 
						WHEN t.displayPrice BETWEEN 500 AND 1500 THEN t.displayPrice + 200
						WHEN t.displayPrice > 1500 THEN t.displayPrice + 250 
						ELSE t.displayPrice 
					END
				END,
		e.custom6 = 1
	from ebayProdListing e join trueSuperTrailer t on e.Model = t.code
							join products p on p.CODE = t.code
	where t.feedMAP = 1
	**/
	
	UPDATE ebayProdListing SET regularPrice = CAST(2*currentPrice AS DECIMAL(10, 2)) WHERE regularPrice < CurrentPrice
	
	--SET WEIGHT TO 1 LB IF FREE FREIGHT
	UPDATE 	ebayprodlisting
	SET weight = 1
	WHERE custom6 = 1
	
	delete from ebayProdListing where Model like '599-%'
	
	delete from ebayProdListing where Model like '194-%'
	
	--REMOVE ALL BACKORDER PRODUCTS
	DELETE FROM ebayProdListing WHERE Model IN (select [No_] COLLATE Latin1_General_CS_AS from backOrder_item where DATEDIFF(day, [OldestOrderdt], getdate()) > 15 and price < 250)

	--REMOVE ALL VOLLRATH ITEMS WHERE COST < 100
	DELETE FROM ebayProdListing WHERE Model IN (SELECT CODE FROM products WHERE ACTIVE = 1 and mfgID = '175' and cost < 100 and qtyOnHand = 0)
	
	--LIMITTING OFFERNAME LENGTH TO 88 CHARACTERS AND CUT AT THE NEAREST SPACE
	UPDATE ebayProdListing
	SET offername = rtrim(left(offername, dbo.RCHARINDEX(' ', left(offername, 80), 0)))
	WHERE len(offername) > 88
	
	--SET LISTING TO NEVER EXPIRED IF THEY HAVE BEEN SALES WITHIN 180
	UPDATE e
	SET e.unlimitted = 1 
	FROM ebayProdListing e JOIN products p ON e.Model = p.CODE
	WHERE p.sales180 > 0	
	
	UPDATE e
	SET e.MerchantCategory = c.superCat
	FROM ebayProdListing e JOIN products p ON e.Model = p.CODE
						JOIN categories c ON c.CODE = p.primaryCatCode 
						
	--REMOVE ALL TUUCI PRODUCTS
	DELETE FROM CAFeed WHERE LEFT(model, 3) = '198'	
	DELETE FROM ebayProdListing WHERE LEFT(model, 3) = '198'
	
	--REMOVING ALL PRODUCTS THAT WE DON'T STOCK
	DELETE FROM ebayProdListing
	WHERE model IN (SELECT code FROM products WHERE active = 1 and qtyOnHand < 1);
	
END
GO
