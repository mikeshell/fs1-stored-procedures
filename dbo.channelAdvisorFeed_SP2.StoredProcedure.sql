USE [KatomDev]
GO
/****** Object:  StoredProcedure [dbo].[channelAdvisorFeed_SP2]    Script Date: 02/01/2013 11:09:50 ******/
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


CREATE PROCEDURE [dbo].[channelAdvisorFeed_SP2]
AS
BEGIN
	DECLARE @jobID INT
	INSERT INTO jobHistory(jobName, startdT) values('CA_FEED', GETDATE())
	SET @jobID = @@identity

	TRUNCATE TABLE CAFeed

	INSERT INTO CAFeed
	SELECT 
		'http://www.katom.com/' + ltrim(rtrim(code)) + '.html', NULL, NULL, NULL, NULL, NULL, ltrim(rtrim(code)), '1', NULL, NULL, 
		NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, 'http://www.katom.com/pdfspecs/' + code + '.pdf', 
		NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, [id], NULL, 0, 1, 1, 0, NULL, 1, 1, 1, 1, 1
	FROM productsmiva
	WHERE active = 1
	ORDER BY code 

	UPDATE ca
	SET ca.[OfferName] = ISNULL(m.mfgName, '') + ' ' + p.mpn + ' - ' + p.[Name],
		ca.[OfferDescription] = cast(dbo.cleaner(p.[prodDesc]) as varchar(max)) + ' ( ' + isnull(m.mfgName, '') + ' - ' + p.mpn  + ' )',
		ca.[ReferenceImageURL] = 'http://www.katom.com/products/' + p.mfgid + '/' + LOWER(p.image) + '.jpg',		
		ca.[RegularPrice] = cast(p.listprice as decimal(10, 2)),
		ca.[CurrentPrice] = 
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
					WHEN ISNULL(P.clearance, 0) = 1 THEN cast(p.salesPrice as decimal(10, 2))
					ELSE cast(p.price as decimal(10, 2))
				END,
		ca.[Brand] = ISNULL(m.mfgName, ''),
		ca.[MerchantCategory] = c.[Name],
		ca.[Manufacturer] = m.mfgName,
		ca.[ManufacturerModel] = p.mpn,
		ca.[PromotionalText] = 
			CASE isnull(p.freeShipping, 0)
				WHEN '1' THEN 'Free Shipping! Call 1-800-541-8683 for great customer service!'
				WHEN 'T' THEN 'Free Shipping! Call 1-800-541-8683 for great customer service!'
				ELSE 'Call 1-800-541-8683 for great customer service!'
			END,
		ca.[Weight] = 
			CASE 
				WHEN  p.freightOnly = 0 AND (p.prodL*p.prodH*p.prodW)/166 > p.[Weight] THEN CAST((prodL*prodH*prodW)/166 AS DECIMAL(10, 2))+5
				ELSE cast(p.weight as decimal(10, 2))
			END,
		ca.[Keywords] = p.keywords,
		ca.[Custom1] = p.prodH,
		ca.[Custom2] = p.prodL,
		ca.[Custom3] = p.prodW,
		ca.[productCost] = cast(p.cost as decimal(10, 2)),
		ca.[Custom6] = p.freeshipping,
		ca.[Custom7] = p.[Name],
		ca.[Custom11] =
		CASE
			WHEN CHARINDEX('.jpg', LOWER(p.image)) > 0 THEN 'http://www.katom.com/largeproducts/' + p.mfgid + '/' + LOWER(p.image)
			ELSE 'http://www.katom.com/largeproducts/' + p.mfgid + '/' + LOWER(p.image) + '.jpg'
		END,
		ca.profitData = 
			CASE 
				WHEN p.mfgID = '598' THEN CAST((p.price-.8*p.true_cost)*p.sales30 AS DECIMAL(10, 2))
				ELSE CAST((p.price-p.true_cost)*p.sales30 AS DECIMAL(10, 2))
			END,
		ca.[lastProcessed] = GETDATE()
	FROM CAFeed ca JOIN products p ON ca.[Model] = p.code
				   LEFT JOIN Mivacategories c ON p.primaryCatID = c.id
				   LEFT JOIN mfg m ON p.mfgid = m.mfgid
	
	--POPULATING [MerchantCategory] WITH CUSTOM FIELD VALUE
	UPDATE c
	SET c.[MerchantCategory] = f.value,
		c.[custom12] = f.value
	FROM CAFeed c JOIN MIVAProdFieldsValue f ON c.prodID = f.product_ID
	WHERE f.field_ID = 31 --Category

	UPDATE c
	SET c.[custom5] = f.value
	FROM CAFeed c JOIN MIVAProdFieldsValue f ON c.prodID = f.product_ID
	where field_id = 1515 --Residential

	UPDATE c
	SET c.[custom10] = f.value
	FROM CAFeed c JOIN MIVAProdFieldsValue f ON c.prodID = f.product_ID
	WHERE field_id = 152 --Type

	UPDATE c
	SET c.[custom10] = f.value
	FROM CAFeed c JOIN MIVAProdFieldsValue f ON c.prodID = f.product_ID
	WHERE field_id = 34 --Color

	--IMAGES EXTENSION CLEANING
	UPDATE cafeed
	SET ReferenceImageURL = replace(ReferenceImageURL, '.jpg.jpg', '.jpg')
	WHERE ReferenceImageURL like '%.jpg.jpg%' 

	UPDATE cafeed
	SET ReferenceImageURL = replace(custom11, '.jpg.jpg', '.jpg')
	WHERE custom11 like '%.jpg.jpg%' 
	
	--SETTING NEW LISTPRICE IF LISTPRICE < PRICE
	UPDATE cafeed
	SET [RegularPrice] = CAST(2*[CurrentPrice] AS DECIMAL(10, 2))
	WHERE CAST([RegularPrice] AS DECIMAL(10, 2)) < CAST([CurrentPrice] AS DECIMAL(10, 2))

	--TAKING CARE OF DUPLICATE MPN
	DECLARE @MPN varchar(100), @mfgID varchar(100), @tmp varchar(100), @code varchar(100)
	
	DECLARE mCursor CURSOR FOR 
	select Left(Model, 3), ManufacturerModel
	from cafeed
	group by Left(Model, 3), ManufacturerModel 
	having count(*) > 1

	OPEN mCursor
	FETCH NEXT FROM mCursor INTO @mfgID, @MPN

	WHILE @@FETCH_STATUS = 0
	   BEGIN
			DECLARE pCursor CURSOR FOR 
			select Model
			from cafeed
			Where Left(Model, 3) = @mfgID and ManufacturerModel = @MPN

			OPEN pCursor
			FETCH NEXT FROM pCursor INTO @code

			WHILE @@FETCH_STATUS = 0
			   BEGIN
				SET @tmp = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@MPN, ')', ''), '(', ''), '-', ''), ' ', ''), '/', ''), '.', '')
				
				UPDATE cafeed
				SET ManufacturerModel = @MPN + ' ' + REPLACE(RIGHT(@CODE, LEN(@CODE)-4), @TMP, '')
				WHERE MODEL = @CODE
	
				SET @tmp = ''
				FETCH NEXT FROM pCursor INTO @code
			   END

			CLOSE pCursor
			DEALLOCATE pCursor		


		FETCH NEXT FROM mCursor INTO @mfgID, @MPN
	   END

	CLOSE mCursor
	DEALLOCATE mCursor
	
	UPDATE cafeed
	SET [OfferDescription] = REPLACE([OfferDescription], '°', '')
	WHERE CHARINDEX('°', [OfferDescription]) > 0

	UPDATE cafeed
	SET [OfferDescription] = REPLACE([OfferDescription], 'Ö', '')
	WHERE CHARINDEX('Ö', [OfferDescription]) > 0
	
	UPDATE cafeed
	SET [OfferDescription] = REPLACE([OfferDescription], '«', '')	
	WHERE CHARINDEX('«', [OfferDescription]) > 0
	
	UPDATE cafeed
	SET [OfferDescription] = REPLACE([OfferDescription], '¦', '')	
	WHERE CHARINDEX('¦', [OfferDescription]) > 0
	
	UPDATE cafeed
	SET [OfferDescription] = REPLACE([OfferDescription], 'û', '')	
	WHERE CHARINDEX('û', [OfferDescription]) > 0
	
	UPDATE cafeed
	SET [OfferDescription] = REPLACE([OfferDescription], '½', '')	
	WHERE CHARINDEX('½', [OfferDescription]) > 0
	
	UPDATE cafeed
	SET [OfferDescription] = REPLACE([OfferDescription], '¼', '')	
	WHERE CHARINDEX('¼', [OfferDescription]) > 0
	
	UPDATE cafeed
	SET [OfferDescription] = REPLACE([OfferDescription], '  ', '')	
	WHERE CHARINDEX('  ', [OfferDescription]) > 0
		
	delete from cafeed where LEN(ISNULL(offername, '')) = 0
	
	delete from CAFeed where Model LIKE '%599-%'
	
	--REMOVE ALL ITEMS THAT ARE BACKORDERS
	UPDATE CAFeed
	SET listproduct = 0
	WHERE Model IN (select [No_] COLLATE Latin1_General_CS_AS from backOrder_item where DATEDIFF(day, [OldestOrderdt], getdate()) > 15 and price < 250)

	--USE ANALYTICS TO MANAGE
	--UPDATE CAFeed
	--SET apex = 1
	--WHERE Model in 
	--	(			
	--	SELECT p.code
	--	FROM categories c JOIN products p ON c.CODE = p.primaryCatCode
	--	WHERE (c.superCat IN ('Residential', 'Janitorial')
	--			AND c.ACTIVE = 1 and p.active = 1
	--			AND price < 500)
	--			OR price < 250				
	--	 )
		
	--MARK AN ITEMT O BE LISTED AT PRICE + .01 FOR AMAZON ONLY
	UPDATE CAFeed
	SET amazon = 0 
	where model in (select code from products where active = 1 and isnull(feedMAP, 0) = 1 and isnull(MAP_Program, 'K') = 'E' and MAP = 1)
	
	UPDATE c
	SET c.map_price = CAST(p.map_price AS DECIMAL(10, 2))
	FROM products p JOIN cafeed c ON p.CODE = c.Model
	WHERE active = 1 
		AND ISNULL(MAP, 0) = 1
		AND ISNULL(map_program, '') = 'A'
		AND ISNULL(feedMAP, 0) = 0

	--REMOVING PRODUCTS THAT WE HAVENT SOLD IN THE LAST 24 MONTH
	UPDATE CAFeed
	SET listproduct = 0
	WHERE Model IN (SELECT CODE 
					FROM products 
					WHERE sales_last_24MO = 0 
						AND DATEDIFF(MONTH, addedDT, GETDATE()) > 11
					)
	--REMOVING PRODUCTS FROM CSE IF WE ARE SPENDING MORE THAN WE ARE MAKING MONEY
	TRUNCATE TABLE cseclick
	
	INSERT INTO cseclick (SKU, CHANNEL_NAME, CLICKS, CLICK_COST, QTY_SOLD)
	select sku, dbo.[returnCSEName]([channel_Name]),
		sum(CAST(clicks AS INT)), 
		sum(CAST(click_cost AS DECIMAL(10, 2))), 
		SUM(CAST(qty_sold AS INT))
	from cseClick_worktable
	group by sku, dbo.[returnCSEName]([channel_Name])
	
	UPDATE c
	SET c.profit = 
		CASE 
			WHEN p.price < 250 THEN c.qty_sold*(p.price - p.cost)
			ELSE p.sales30*(p.price - p.cost)
		END, 
		c.price = p.price
	FROM cseclick c JOIN products p ON c.sku = p.CODE

	--DISABLING PRODUCTS FROM NEXTAG
	UPDATE c
	SET c.nextag = 0
	FROM CAFeed c JOIN cseclick cc ON c.Model = cc.sku
	WHERE cc.clicks > 24 
		AND cc.profit < cc.click_cost
		AND cc.Channel_Name LIKE '%NexTag%'

	--DISABLING PRODUCTS FROM AMAZON
	UPDATE c
	SET c.amazon = 0
	FROM CAFeed c JOIN cseclick cc ON c.Model = cc.sku
	WHERE cc.clicks > 24 
		AND cc.profit < cc.click_cost
		AND cc.Channel_Name LIKE '%Amazon%'

	--DISABLING PRODUCTS FROM SHOPZILLA
	UPDATE c
	SET c.shopzilla = 0
	FROM CAFeed c JOIN cseclick cc ON c.Model = cc.sku
	WHERE cc.clicks > 24 
		AND cc.profit < cc.click_cost	
--		AND (cc.profit-cc.click_cost)+(c.CurrentPrice-c.productCost) < 0
		AND cc.Channel_Name LIKE '%Shopzilla%'

	--DISABLING PRODUCTS FROM PRICEGRABBER
	UPDATE c
	SET c.pricegrabber = 0
	FROM CAFeed c JOIN cseclick cc ON c.Model = cc.sku
	WHERE cc.clicks > 24 
		AND cc.profit < cc.click_cost
		AND cc.Channel_Name LIKE '%PriceGrabber%'
		
	--DISABLING PRODUCTS FROM SHOPPING.COM
	UPDATE c
	SET c.shopping = 0
	FROM CAFeed c JOIN cseclick cc ON c.Model = cc.sku
	WHERE cc.clicks > 24 
		AND cc.profit < cc.click_cost
		AND cc.Channel_Name LIKE '%Shopping%'
		
	--DISABLING PRODUCTS FROM BECOME.COM
	UPDATE c
	SET c.become = 0
	FROM CAFeed c JOIN cseclick cc ON c.Model = cc.sku
	WHERE cc.clicks > 24 
		AND cc.profit < cc.click_cost
		AND cc.Channel_Name LIKE '%Become%'
	
	--TEMPORARY REMOVAL OF 449-KPEX
	UPDATE c
	SET c.become = 0, c.shopping = 0, c.pricegrabber = 0, c.shopzilla = 0, c.amazon = 0, c.nextag = 0 
	FROM CAFeed c JOIN products p ON c.Model = p.CODE
	WHERE p.qtyOnHand < 1 and p.CODE = '449-KPEX'
	
	UPDATE jobHistory SET endDT = GETDATE() WHERE jobID = @jobID
END
GO
